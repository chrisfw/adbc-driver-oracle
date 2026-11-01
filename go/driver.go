// Copyright 2025 CARTO
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package oracle

import (
	"context"
	"errors"
	"runtime/debug"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	OptionServerHostname = "oracle.server_hostname"
	OptionPort           = "oracle.port"
	OptionServiceName    = "oracle.service_name"
	OptionUser           = "oracle.user"
	OptionPassword       = "oracle.password"
	OptionWalletLocation = "oracle.wallet_location"
	OptionWalletPassword = "oracle.wallet_password"
	OptionWalletContent  = "oracle.wallet_content" // Inline ewallet.pem content (avoids temp file)
	OptionDSN            = "oracle.dsn"

	// pool_size caps the Oracle sessions this database may hold open
	// (default 8). Each ADBC connection owns one session for its whole life,
	// and under autocommit each ingest worker borrows one more for the
	// duration of its batches, so the worst case is roughly
	// connections * (1 + ingest_workers).
	OptionPoolSize = "oracle.pool_size"

	// ingest_workers is how many insert workers a bulk ingest may run in
	// parallel (default 1 for a bound batch, 4 for a bound stream).
	//
	// Only honoured with autocommit on, where each worker inserts and commits
	// on a session of its own. With autocommit off the rows have to land in
	// the caller's transaction and only the connection's own session is in it,
	// so ingest runs serially on that session and the caller's Commit decides
	// the outcome.
	OptionIngestWorkers = "oracle.ingest_workers"

	// Statement options controlling how Arrow string columns are typed in
	// ingest CREATE TABLE DDL. VARCHAR2 caps at 4000 bytes; values wider than
	// that need CLOB or the insert fails with ORA-12899.
	//
	// string_type: "auto" (default) scans the head of the bound data and uses
	// CLOB for any string column with a value over 4000 bytes; "varchar2" and
	// "clob" force the respective type for all string columns.
	OptionIngestStringType = "oracle.ingest.string_type"
	// clob_columns: comma-separated (case-insensitive) column names always
	// created as CLOB, regardless of string_type or scan results. Use this
	// when wide values only appear deep in the stream where the auto scan
	// cannot see them.
	OptionIngestClobColumns = "oracle.ingest.clob_columns"
	// type_scan_limit_bytes: how much of the bound stream "auto" may buffer
	// while sizing string columns (default 64 MiB).
	OptionIngestTypeScanLimit = "oracle.ingest.type_scan_limit_bytes"

	// number_scan_rows is how many rows are sampled to infer an Arrow type
	// for an Oracle NUMBER declared without precision and scale (default
	// 1024; 0 disables inference and such columns stay text).
	//
	// No Arrow decimal can hold a column whose scale varies per value, which
	// is what an unconstrained NUMBER is, so the alternative to sampling is
	// text. See number_infer.go for why the sample cannot be made a promise
	// and why precision is taken at the maximum rather than fitted.
	OptionNumberScanRows = "oracle.number_scan_rows"

	// partition_count is how many slices ExecutePartitions splits a query
	// into, so they can be read concurrently (default 4).
	//
	// It is a request, not a guarantee. A partitioned table yields one slice
	// per partition regardless, since a PARTITION clause names exactly one.
	// A query that cannot be split safely yields a single slice rather than
	// an error. See partition.go for the strategy ladder and why the last
	// rung multiplies server I/O.
	OptionPartitionCount = "oracle.partition_count"

	// number_overflow is what happens when a value does not fit the type
	// inferred from the sample: "error" (default) fails the read naming the
	// column and value, "null" nulls that value and logs once.
	//
	// There is deliberately no option to round into the inferred scale. A
	// rounded value looks right and is wrong, which is a worse failure than
	// either of these.
	OptionNumberOverflow = "oracle.number_overflow"

	// opaque_number, when "true", labels an unconstrained NUMBER that could
	// not be inferred as the canonical arrow.opaque extension over utf8,
	// instead of as a plain utf8 column. The label records that the column is
	// an Oracle NUMBER rather than ordinary text.
	//
	// Off by default, and this one is not as additive as it looks. Across the
	// C data interface an unknown extension does degrade to its storage type,
	// but to an in-process Arrow consumer the concrete array becomes an
	// extension array rather than a String array, and arrow-go's
	// ExtensionArrayBase.ValueStr panics outright. Anything doing generic
	// value extraction breaks. Turn this on when the consumer is known to
	// handle extension types.
	OptionOpaqueNumber = "oracle.opaque_number"

	// compact_decimals, when "true", narrows a declared NUMBER(p,s) to the
	// smallest Arrow decimal that holds it: Decimal32 to 9 digits, Decimal64
	// to 18, Decimal128 beyond. Four bytes a value instead of sixteen for a
	// NUMBER(4,2), which matters on a read-bound driver.
	//
	// Off by default because it is an interop risk, not a free win. The C
	// data interface spells a non-128 decimal as "d:<precision>,<scale>,<bits>"
	// and a consumer that only parses 128-bit decimals rejects it. Turn this
	// on when the consumer is known to handle Decimal32 and Decimal64.
	OptionCompactDecimals = "oracle.compact_decimals"

	// timestamp_with_offset, when "true", carries a TIMESTAMP WITH TIME ZONE
	// as the canonical arrow.timestamp_with_offset extension, a
	// Struct{timestamp: Timestamp(T,"UTC"), offset_minutes: Int16}, instead of
	// as a UTC timestamp beside the "<col>__tz" String column that
	// oracle.tz_offset_column emits.
	//
	// Off by default, and the same interop caution applies: it is a standard
	// type but a recent one, so a consumer that does not know the extension
	// sees the raw struct rather than a timestamp. Supersedes
	// oracle.tz_offset_column when both are set.
	OptionTimestampWithOffset = "oracle.timestamp_with_offset"

	// prefetch_rows is how many rows Oracle returns per fetch round trip
	// (default 10000).
	//
	// Measured on a loopback connection over a five-column table, the default
	// is already near the knee: 25 rows gives 377k rows/s, 1000 gives 417k,
	// 10000 gives 635k, 50000 gives 646k, and 200000 drops back to 442k as
	// the per-fetch buffers stop paying for themselves. The option exists
	// because the knee moves: a high-latency link wants more rows per trip,
	// and very wide rows may need fewer to bound memory, since go-ora sizes
	// its row buffer from the declared column widths.
	OptionPrefetchRows = "oracle.prefetch_rows"

	// lob_fetch chooses how go-ora retrieves CLOB and BLOB values. See the
	// LobFetch* constants. Defaults to inline.
	//
	// Not a byte count: the plan called for oracle.lob_prefetch_bytes, but
	// go-ora models this as a mode rather than a threshold and exposes no
	// size knob, so a byte option would have had nothing to set.
	OptionLobFetch = "oracle.lob_fetch"

	// timestamp_unit selects the Arrow time unit for TIMESTAMP columns. See
	// the TimestampUnit* constants. Defaults to microseconds.
	OptionTimestampUnit = "oracle.timestamp_unit"

	// tz_offset_column, when "true", emits a sibling String column named
	// "<col>__tz" after every TIMESTAMP WITH TIME ZONE column, holding the
	// zone that value arrived in: the IANA region name Oracle stored
	// ("America/New_York"), or a fixed offset ("+05:30") when that is what was
	// stored. Defaults to "false".
	//
	// The timestamp itself stays UTC-normalised either way, so the instant is
	// never ambiguous; this recovers the local zone that the normalisation
	// discards, which is what a report rendered in the originating zone needs.
	// TIMESTAMP WITH LOCAL TIME ZONE gets no sibling: Oracle normalises it on
	// the way in, so the original zone is gone before the driver sees it.
	OptionTzOffsetColumn = "oracle.tz_offset_column"

	// number_mapping selects how Oracle NUMBER columns are typed in the
	// result Arrow schema. See the NumberMapping* constants. Defaults to
	// NumberMappingDecimal, which preserves every digit Oracle stores.
	OptionNumberMapping = "oracle.number_mapping"

	DefaultPort = "1521"

	// defaultNumberScanRows is what an unset oracle.number_scan_rows means.
	// Large enough that a column of ordinary values settles on its true
	// scale, small enough that the peek is not itself a cost.
	defaultNumberScanRows = 1024

	// defaultPrefetchRows is what an unset oracle.prefetch_rows means. It was
	// the hard-coded value before the option existed, and measurement put it
	// at the knee of the curve, so exposing the knob did not change it.
	defaultPrefetchRows = 10000
)

// Accepted values for OptionNumberMapping.
//
// Oracle NUMBER is a base-10 type with up to 38 significant digits, so no
// binary float can represent all of its values exactly. go-ora decodes NUMBER
// to a full-precision decimal string on the wire; the mapping chosen here
// decides how much of that precision survives into Arrow.
const (
	// NumberMappingFloat is the default: NUMBER(p,0) with p <= 18 becomes
	// Int64, wider integral NUMBER becomes String, and everything else
	// becomes Float64. Scaled values lose precision beyond float64's ~15-17
	// significant digits.
	NumberMappingFloat = "float"

	// NumberMappingDecimal maps NUMBER(p,s) to Decimal128(p,s) whenever the
	// column's declared precision and scale fit Decimal128, preserving every
	// digit. NUMBER(p,0) with p <= 18 still maps to Int64 (already exact and
	// half the width). NUMBER whose precision/scale cannot be represented —
	// unconstrained NUMBER and negative scales — falls back to String rather
	// than to a lossy float.
	NumberMappingDecimal = "decimal"

	// NumberMappingString maps every NUMBER to String. Fully lossless and
	// schema-stable at the cost of making the column non-numeric.
	NumberMappingString = "string"

	// defaultNumberMapping is what an unset oracle.number_mapping means.
	//
	// (See TimestampUnit* below for the other type policy.)
	//
	// Decimal rather than float, which is a deliberate departure from the
	// upstream driver. Oracle NUMBER is base-10 with up to 38 significant
	// digits and go-ora hands it over as a full-precision decimal string, so
	// the old default threw away exactness that had already arrived: a
	// NUMBER(12,2) holding a monetary amount became a float64 and stopped
	// summing correctly. Callers who want the old shape can still ask for it.
	defaultNumberMapping = NumberMappingDecimal
)

// Accepted values for OptionLobFetch, which map onto go-ora's LOB FETCH
// connection parameter.
const (
	// LobFetchInline is the default. LOB values arrive with their row, so
	// reading them costs no extra round trip. Each row carries its LOB data,
	// so a batch of large LOBs is held in memory at once.
	LobFetchInline = "inline"

	// LobFetchStream fetches each LOB separately after its row. Lower peak
	// memory for multi-megabyte values, at one round trip per value.
	LobFetchStream = "stream"

	// defaultLobFetch is what an unset oracle.lob_fetch means.
	defaultLobFetch = LobFetchInline
)

// oracleLobFetchParam maps an OptionLobFetch value to go-ora's DSN value. An
// empty value means the option was never set.
func oracleLobFetchParam(val string) (string, bool) {
	if val == "" {
		val = defaultLobFetch
	}
	switch val {
	case LobFetchInline:
		return "INLINE", true
	case LobFetchStream:
		return "STREAM", true
	}
	return "", false
}

// Accepted values for OptionNumberOverflow.
const (
	// NumberOverflowError fails the read when a value does not fit the
	// inferred decimal, naming the column and the value.
	NumberOverflowError = "error"

	// NumberOverflowNull nulls the value and logs once, so a long extract
	// completes instead of dying on one outlier.
	NumberOverflowNull = "null"

	// defaultNumberOverflow is what an unset oracle.number_overflow means.
	defaultNumberOverflow = NumberOverflowError
)

// Accepted values for OptionTimestampUnit.
//
// Oracle TIMESTAMP(p) carries up to 9 fractional digits, more than a
// microsecond Arrow timestamp holds. The declared p cannot drive the choice,
// though: go-ora reports one wire type for every TIMESTAMP precision and
// exposes no scale for it, so only the data dictionary knows p. Selecting ns
// from the dictionary would make GetTableSchema disagree with ExecuteQuery,
// which is the defect 2.1 exists to prevent, so the unit is a connection
// policy both paths read instead.
const (
	// TimestampUnitMicro is the default. Lossless for DATE and for TIMESTAMP
	// up to p=6, which is Oracle's own default precision, and it spans the
	// whole Oracle date range.
	TimestampUnitMicro = "us"

	// TimestampUnitNano keeps all 9 digits of a TIMESTAMP(7..9).
	//
	// Opt-in because a nanosecond Arrow timestamp only spans 1677-09-21 to
	// 2262-04-11, and values outside it do not fail: arrow.TimestampFromTime
	// wraps silently, so a year-1500 date becomes a plausible-looking wrong
	// instant. Only choose this when the data is known to sit inside that
	// window.
	TimestampUnitNano = "ns"

	// defaultTimestampUnit is what an unset oracle.timestamp_unit means.
	defaultTimestampUnit = TimestampUnitMicro
)

// arrowTimeUnit maps an OptionTimestampUnit value to its Arrow unit. An empty
// value means the option was never set.
func arrowTimeUnit(val string) (arrow.TimeUnit, bool) {
	if val == "" {
		val = defaultTimestampUnit
	}
	switch val {
	case TimestampUnitMicro:
		return arrow.Microsecond, true
	case TimestampUnitNano:
		return arrow.Nanosecond, true
	}
	return 0, false
}

var infoVendorVersion string

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch dep.Path {
			case "github.com/sijms/go-ora/v2":
				infoVendorVersion = dep.Version
			}
		}
	}
}

type driverImpl struct {
	driverbase.DriverImplBase
}

func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("Oracle")
	info.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoVendorSql: true,
	})
	if infoVendorVersion != "" {
		if err := info.RegisterInfoCode(adbc.InfoVendorVersion, infoVendorVersion); err != nil {
			panic(err)
		}
	}
	base := driverbase.NewDriverImplBase(info, alloc)
	// Every ErrorHelper in the driver is copied from this one, so installing
	// the inspector here is what gives Oracle codes and SQLSTATEs to errors
	// raised anywhere below.
	base.ErrorHelper.ErrorInspector = oracleErrorInspector{}
	return driverbase.NewDriver(&driverImpl{DriverImplBase: base})
}

func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase)
	if err != nil {
		return nil, err
	}

	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		port:             DefaultPort,
		poolSize:         8,
		ingestWorkers:    1,
		// Negative marks oracle.number_scan_rows unset, since a configured 0
		// means "do not infer" and must survive.
		numberScanRows: -1,
	}

	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}

// parseBoolOption reads an ADBC boolean option value.
func parseBoolOption(val string) (bool, error) {
	switch val {
	case adbc.OptionValueEnabled:
		return true, nil
	case adbc.OptionValueDisabled:
		return false, nil
	}
	return false, errInvalidBoolOption
}

// boolOptionValue renders a bool back as the canonical ADBC option value.
func boolOptionValue(b bool) string {
	if b {
		return adbc.OptionValueEnabled
	}
	return adbc.OptionValueDisabled
}

var errInvalidBoolOption = errors.New("expected true or false")
