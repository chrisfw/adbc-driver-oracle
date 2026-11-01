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
	}

	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
