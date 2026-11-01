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
	"database/sql"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// --- Unit: the data dictionary and the wire describe the same column ---

// ALL_TAB_COLUMNS and go-ora report an unconstrained NUMBER differently: the
// dictionary leaves precision and scale null, the wire sends the 255 scale
// sentinel. decimalSizeFromMetadata has to close that gap, and in particular
// must not let a null scale read as zero, which would claim the column is
// integral and map it to Int64.
func TestDecimalSizeFromMetadata(t *testing.T) {
	n := func(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
	null := sql.NullInt64{}

	cases := []struct {
		name         string
		prec, scale  sql.NullInt64
		wantP, wantS int64
	}{
		// Unconstrained NUMBER, and FLOAT, which has a binary precision and
		// no scale. Both must land on the sentinel, not on scale 0.
		{"unconstrained NUMBER", null, null, 0, oracleScaleSentinel},
		{"FLOAT", n(126), null, 126, oracleScaleSentinel},
		// Declared precision and scale pass through untouched.
		{"NUMBER(10)", n(10), n(0), 10, 0},
		{"NUMBER(19)", n(19), n(0), 19, 0},
		{"NUMBER(12,2)", n(12), n(2), 12, 2},
		{"NUMBER(5,-2)", n(5), n(-2), 5, -2},
		{"NUMBER(38,38)", n(38), n(38), 38, 38},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, s, ok := decimalSizeFromMetadata(tc.prec, tc.scale)
			if !ok {
				t.Error("ok = false, want true: the dictionary described a NUMBER")
			}
			if p != tc.wantP || s != tc.wantS {
				t.Errorf("got (%d, %d), want (%d, %d)", p, s, tc.wantP, tc.wantS)
			}
		})
	}
}

// The two schema paths must agree for every NUMBER shape under every mapping.
// This is the unit-level half: it drives oracleDataTypeToArrow with dictionary
// values and numberToArrow with the wire values go-ora reports for the same
// column, and demands the same Arrow type out of both.
//
// Wire values come from observing go-ora against Oracle Free 23ai, and the
// integration test below re-checks them against a live server so this cannot
// drift into asserting a stale assumption.
func TestSchemaPathsAgreeOnNumber(t *testing.T) {
	n := func(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
	null := sql.NullInt64{}

	cases := []struct {
		ddl string // label only: the declared type this column came from
		// what ALL_TAB_COLUMNS reports. DATA_TYPE carries no precision or
		// scale, so typeName is bare and the digits arrive separately.
		typeName            string
		metaPrec, metaScale sql.NullInt64
		// what sql.ColumnType.DecimalSize reports
		wirePrec, wireScale int64
		wireOK              bool
	}{
		{"NUMBER", "NUMBER", null, null, 0, oracleScaleSentinel, true},
		{"NUMBER(10)", "NUMBER", n(10), n(0), 10, 0, true},
		{"NUMBER(19)", "NUMBER", n(19), n(0), 19, 0, true},
		{"NUMBER(12,2)", "NUMBER", n(12), n(2), 12, 2, true},
		// A declared negative scale wraps through go-ora's uint8, so the wire
		// says 254 where the dictionary says -2. Both are unrepresentable as
		// Decimal128 and must reach the same answer by rule, not by luck.
		{"NUMBER(5,-2)", "NUMBER", n(5), n(-2), 5, 256 - 2, true},
		// FLOAT is NUMBER underneath and go-ora reports it as NUMBER, which is
		// why oracleDataTypeToArrow cannot shortcut it to Float64.
		{"FLOAT", "FLOAT", n(126), null, 126, oracleScaleSentinel, true},
	}

	for _, tc := range cases {
		for _, mapping := range []string{NumberMappingFloat, NumberMappingDecimal, NumberMappingString} {
			t.Run(tc.ddl+"/"+mapping, func(t *testing.T) {
				meta := oracleDataTypeToArrow(tc.typeName, tc.metaPrec, tc.metaScale, mapping)
				wire := numberToArrow(tc.wirePrec, tc.wireScale, tc.wireOK, mapping)
				if !arrow.TypeEqual(meta, wire) {
					t.Errorf("GetTableSchema says %s but ExecuteQuery says %s", meta, wire)
				}
			})
		}
	}
}

// BINARY_FLOAT and BINARY_DOUBLE are genuine IEEE floats, not NUMBER, so the
// number mapping must not touch them. Guards against over-applying the fix
// above to every numeric-looking type.
func TestBinaryFloatsIgnoreNumberMapping(t *testing.T) {
	for _, dt := range []string{"BINARY_FLOAT", "BINARY_DOUBLE"} {
		for _, mapping := range []string{NumberMappingFloat, NumberMappingDecimal, NumberMappingString} {
			got := oracleDataTypeToArrow(dt, sql.NullInt64{}, sql.NullInt64{}, mapping)
			if !arrow.TypeEqual(got, arrow.PrimitiveTypes.Float64) {
				t.Errorf("%s under %s: got %s, want float64", dt, mapping, got)
			}
		}
	}
}

// --- Integration: the same agreement against a live server ---

// The plan's requirement, end to end: for each NUMBER shape under each
// mapping, the type GetTableSchema reports equals the type ExecuteQuery
// returns for SELECT col FROM t. This is what the unit test above stands in
// for, and what keeps its hard-coded wire values honest.
func TestIntegration_TableSchemaMatchesQuerySchema(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	// BINARY_FLOAT and BINARY_DOUBLE ride along: they must agree too, and they
	// must not be affected by the mapping.
	ddls := []string{
		"NUMBER", "NUMBER(10)", "NUMBER(19)", "NUMBER(12,2)", "NUMBER(5,-2)",
		"FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE",
	}

	for _, mapping := range []string{NumberMappingFloat, NumberMappingDecimal, NumberMappingString} {
		t.Run(mapping, func(t *testing.T) {
			drv := NewDriver(memory.DefaultAllocator)
			db, err := drv.NewDatabase(map[string]string{
				OptionDSN:           dsn,
				OptionNumberMapping: mapping,
			})
			if err != nil {
				t.Fatalf("NewDatabase: %v", err)
			}
			defer db.Close()

			cnxn, err := db.Open(ctx)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer cnxn.Close()

			for i, ddl := range ddls {
				table := fmt.Sprintf("ADBC_T21_NUM_%d", i)
				t.Run(ddl, func(t *testing.T) {
					dropTable(ctx, t, cnxn, table)
					execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+" (C "+ddl+")")
					defer dropTable(ctx, t, cnxn, table)

					schema, err := cnxn.GetTableSchema(ctx, nil, nil, table)
					if err != nil {
						t.Fatalf("GetTableSchema: %v", err)
					}
					metaType := schema.Field(0).Type

					stmt, err := cnxn.NewStatement()
					if err != nil {
						t.Fatalf("NewStatement: %v", err)
					}
					defer stmt.Close()
					if err := stmt.SetSqlQuery("SELECT C FROM " + table); err != nil {
						t.Fatalf("SetSqlQuery: %v", err)
					}
					reader, _, err := stmt.ExecuteQuery(ctx)
					if err != nil {
						t.Fatalf("ExecuteQuery: %v", err)
					}
					defer reader.Release()
					queryType := reader.Schema().Field(0).Type

					if !arrow.TypeEqual(metaType, queryType) {
						t.Errorf("%s under %s: GetTableSchema says %s but ExecuteQuery says %s",
							ddl, mapping, metaType, queryType)
					}
				})
			}
		})
	}
}
