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

	// number_mapping selects how Oracle NUMBER columns are typed in the
	// result Arrow schema. See the NumberMapping* constants. Defaults to
	// NumberMappingFloat, which is the driver's historical behaviour.
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
)

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
