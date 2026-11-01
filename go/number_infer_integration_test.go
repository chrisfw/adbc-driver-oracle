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
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// An unconstrained NUMBER has no declared scale, so it used to come back as
// text. Sampling the data gives it a real decimal type instead, which is the
// whole point of the inference.
func TestIntegration_UnconstrainedNumberIsInferred(t *testing.T) {
	const table = "ADBC_T4N_INFER"
	ctx, cnxn := openTypedConnection(t, nil)

	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+` ("V" NUMBER)`)
	defer dropTable(ctx, t, cnxn, table)
	for _, v := range []string{"1.50", "-2.25", "1000.01", "0", "12345.99"} {
		execUpdate(ctx, t, cnxn, "INSERT INTO "+table+" VALUES ("+v+")")
	}

	schema, rec := readOneRow(ctx, t, cnxn, `SELECT "V" FROM `+table+` ORDER BY "V"`)
	defer rec.Release()

	dec, ok := schema.Field(0).Type.(arrow.DecimalType)
	if !ok {
		t.Fatalf("unconstrained NUMBER came back as %s, want an inferred decimal", schema.Field(0).Type)
	}
	// Two fraction digits is the widest in the data, so that is the scale.
	if dec.GetScale() != 2 {
		t.Errorf("inferred scale = %d, want 2", dec.GetScale())
	}
	// Precision is taken at the type maximum on purpose, not fitted, so a
	// later value has room. See number_infer.go.
	if dec.GetPrecision() != maxDecimal128Precision {
		t.Errorf("inferred precision = %d, want the type maximum %d",
			dec.GetPrecision(), maxDecimal128Precision)
	}

	// The values have to be right, not merely typed.
	col, ok := rec.Column(0).(*array.Decimal128)
	if !ok {
		t.Fatalf("column is %T, want *array.Decimal128", rec.Column(0))
	}
	var got []string
	for i := 0; i < col.Len(); i++ {
		got = append(got, col.ValueStr(i))
	}
	// Arrow's ValueStr trims trailing zeros, so 1.50 renders as 1.5.
	want := []string{"-2.25", "0", "1.5", "1000.01", "12345.99"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("values = %v, want %v", got, want)
	}
}

// Both schema paths must name the same type, which for an inferred column
// means GetTableSchema has to sample the table the way the query samples the
// result. This is 2.1's invariant applied to a data-dependent type.
func TestIntegration_InferredNumberSchemaPathsAgree(t *testing.T) {
	const table = "ADBC_T4N_AGREE"
	ctx, cnxn := openTypedConnection(t, nil)

	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+` ("A" NUMBER, "B" NUMBER, "C" NUMBER(10,2))`)
	defer dropTable(ctx, t, cnxn, table)
	// Different scales per column, so a single shared answer would be wrong.
	execUpdate(ctx, t, cnxn, "INSERT INTO "+table+" VALUES (1.5, 2.12345, 3.14)")
	execUpdate(ctx, t, cnxn, "INSERT INTO "+table+" VALUES (-9.25, 0.1, -1.00)")

	meta, err := cnxn.GetTableSchema(ctx, nil, nil, table)
	if err != nil {
		t.Fatalf("GetTableSchema: %v", err)
	}
	querySchema, rec := readOneRow(ctx, t, cnxn, `SELECT "A", "B", "C" FROM `+table)
	defer rec.Release()

	if meta.NumFields() != querySchema.NumFields() {
		t.Fatalf("GetTableSchema has %d columns, ExecuteQuery has %d",
			meta.NumFields(), querySchema.NumFields())
	}
	for i := 0; i < meta.NumFields(); i++ {
		m, q := meta.Field(i), querySchema.Field(i)
		if m.Name != q.Name || !arrow.TypeEqual(m.Type, q.Type) {
			t.Errorf("column %d: GetTableSchema says %s %s but ExecuteQuery says %s %s",
				i, m.Name, m.Type, q.Name, q.Type)
		}
	}
	// And the scales really did differ, or this proved nothing.
	a := querySchema.Field(0).Type.(arrow.DecimalType)
	b := querySchema.Field(1).Type.(arrow.DecimalType)
	if a.GetScale() == b.GetScale() {
		t.Errorf("both columns inferred scale %d; the fixture meant them to differ", a.GetScale())
	}
}

// Sampling cannot promise anything about later rows, and Arrow cannot change a
// column's type mid-stream, so a value past the inferred scale has to either
// fail or be nulled. It must never be rounded into the inferred scale.
func TestIntegration_InferredNumberOverflowPolicy(t *testing.T) {
	const table = "ADBC_T4N_OVERFLOW"

	setup := func(t *testing.T, opts map[string]string) (context.Context, adbc.Connection) {
		ctx, cnxn := openTypedConnection(t, opts)
		dropTable(ctx, t, cnxn, table)
		execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+` ("ID" NUMBER(10), "V" NUMBER)`)
		t.Cleanup(func() { dropTable(ctx, t, cnxn, table) })
		// The sample sees two decimal places; the last row has far more, so
		// it cannot fit whatever scale was inferred.
		for i, v := range []string{"1.25", "2.50", "3.75"} {
			execUpdate(ctx, t, cnxn, fmt.Sprintf("INSERT INTO %s VALUES (%d, %s)", table, i, v))
		}
		execUpdate(ctx, t, cnxn, fmt.Sprintf(
			"INSERT INTO %s VALUES (99, 1.2345678901234567890123456789)", table))
		return ctx, cnxn
	}

	t.Run("error is the default", func(t *testing.T) {
		// A scan window of 3 sees only the narrow rows.
		ctx, cnxn := setup(t, map[string]string{OptionNumberScanRows: "3"})
		err := tryQuery(ctx, cnxn, `SELECT "V" FROM `+table+` ORDER BY "ID"`)
		if err == nil {
			t.Fatal("overflowing value was accepted; it must not be silently rounded")
		}
		// The message has to name the column and point at the way out.
		if !strings.Contains(err.Error(), "V") || !strings.Contains(err.Error(), OptionNumberOverflow) {
			t.Errorf("error does not name the column and the option: %v", err)
		}
	})

	t.Run("null keeps the stream", func(t *testing.T) {
		ctx, cnxn := setup(t, map[string]string{
			OptionNumberScanRows: "3",
			OptionNumberOverflow: NumberOverflowNull,
		})
		schema, rec := readOneRow(ctx, t, cnxn, `SELECT "V" FROM `+table+` ORDER BY "ID"`)
		defer rec.Release()
		if _, ok := schema.Field(0).Type.(arrow.DecimalType); !ok {
			t.Fatalf("type = %s, want a decimal", schema.Field(0).Type)
		}
		col := rec.Column(0)
		if col.Len() != 4 {
			t.Fatalf("got %d rows, want all 4", col.Len())
		}
		for i := 0; i < 3; i++ {
			if col.IsNull(i) {
				t.Errorf("row %d is null; only the overflowing row should be", i)
			}
		}
		if !col.IsNull(3) {
			t.Error("the overflowing row should be null under the null policy")
		}
	})
}

// Turning inference off restores the previous behaviour, so the option is a
// real escape hatch rather than decoration.
func TestIntegration_NumberScanRowsZeroDisablesInference(t *testing.T) {
	const table = "ADBC_T4N_OFF"
	ctx, cnxn := openTypedConnection(t, map[string]string{OptionNumberScanRows: "0"})

	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+` ("V" NUMBER)`)
	defer dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "INSERT INTO "+table+" VALUES (1.5)")

	schema, rec := readOneRow(ctx, t, cnxn, `SELECT "V" FROM `+table)
	defer rec.Release()
	if got := schema.Field(0).Type; !arrow.TypeEqual(got, arrow.BinaryTypes.String) {
		t.Errorf("with inference off, got %s, want plain utf8", got)
	}
}

// A column too wide for any Arrow decimal has to fall back rather than be
// wrong. Oracle NUMBER reaches 126 integer digits; Decimal256 holds 76.
func TestIntegration_NumberTooWideForAnyDecimalFallsBack(t *testing.T) {
	const table = "ADBC_T4N_WIDE"
	ctx, cnxn := openTypedConnection(t, nil)

	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+` ("V" NUMBER)`)
	defer dropTable(ctx, t, cnxn, table)
	// 9.99e125 renders as 126 integer digits.
	execUpdate(ctx, t, cnxn, "INSERT INTO "+table+" VALUES (9.99E125)")

	schema, rec := readOneRow(ctx, t, cnxn, `SELECT "V" FROM `+table)
	defer rec.Release()
	if _, isDec := schema.Field(0).Type.(arrow.DecimalType); isDec {
		t.Errorf("a 126 digit value was typed as %s; no Arrow decimal is that wide",
			schema.Field(0).Type)
	}
	if rec.Column(0).IsNull(0) {
		t.Error("the value was lost; the fallback must keep it")
	}
}

// compact_decimals narrows a declared NUMBER(p,s) to the smallest Arrow
// decimal that holds it, which is four bytes a value instead of sixteen for a
// NUMBER(4,2). Off by default because the C data interface spells a non-128
// decimal as "d:p,s,bits" and a consumer that only parses 128-bit decimals
// rejects it.
func TestIntegration_CompactDecimals(t *testing.T) {
	const table = "ADBC_T4N_COMPACT"

	cols := []struct {
		ddl       string
		plain     arrow.DataType
		compacted arrow.DataType
	}{
		{`"A" NUMBER(4,2)`, &arrow.Decimal128Type{Precision: 4, Scale: 2}, &arrow.Decimal32Type{Precision: 4, Scale: 2}},
		{`"B" NUMBER(12,2)`, &arrow.Decimal128Type{Precision: 12, Scale: 2}, &arrow.Decimal64Type{Precision: 12, Scale: 2}},
		{`"C" NUMBER(30,4)`, &arrow.Decimal128Type{Precision: 30, Scale: 4}, &arrow.Decimal128Type{Precision: 30, Scale: 4}},
	}
	var defs []string
	for _, c := range cols {
		defs = append(defs, c.ddl)
	}

	for _, on := range []bool{false, true} {
		name := "off"
		opts := map[string]string{}
		if on {
			name = "on"
			opts[OptionCompactDecimals] = adbc.OptionValueEnabled
		}
		t.Run(name, func(t *testing.T) {
			ctx, cnxn := openTypedConnection(t, opts)
			dropTable(ctx, t, cnxn, table)
			execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+" ("+strings.Join(defs, ", ")+")")
			defer dropTable(ctx, t, cnxn, table)
			execUpdate(ctx, t, cnxn, "INSERT INTO "+table+" VALUES (1.25, 1234.56, 12345.6789)")

			schema, rec := readOneRow(ctx, t, cnxn, `SELECT "A", "B", "C" FROM `+table)
			defer rec.Release()

			for i, c := range cols {
				want := c.plain
				if on {
					want = c.compacted
				}
				if got := schema.Field(i).Type; !arrow.TypeEqual(got, want) {
					t.Errorf("column %d: got %s, want %s", i, got, want)
				}
				// Values must survive the narrowing, not just the type. A
				// missing builder case would null the whole column.
				if rec.Column(i).IsNull(0) {
					t.Errorf("column %d came back null; the narrowed builder lost the value", i)
				}
			}
			// And the values must be right.
			if got := rec.Column(0).ValueStr(0); got != "1.25" {
				t.Errorf("A = %q, want 1.25", got)
			}
			if got := rec.Column(1).ValueStr(0); got != "1234.56" {
				t.Errorf("B = %q, want 1234.56", got)
			}

			// Both schema paths have to agree about the narrowing too.
			meta, err := cnxn.GetTableSchema(ctx, nil, nil, table)
			if err != nil {
				t.Fatalf("GetTableSchema: %v", err)
			}
			for i := 0; i < meta.NumFields(); i++ {
				if !arrow.TypeEqual(meta.Field(i).Type, schema.Field(i).Type) {
					t.Errorf("column %d: GetTableSchema says %s but ExecuteQuery says %s",
						i, meta.Field(i).Type, schema.Field(i).Type)
				}
			}
		})
	}
}
