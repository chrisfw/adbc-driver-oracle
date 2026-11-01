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
	"math"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// openTypedConnection opens a connection with the given driver options.
func openTypedConnection(t *testing.T, opts map[string]string) (context.Context, adbc.Connection) {
	t.Helper()
	dsn := getTestDSN(t)
	ctx := context.Background()

	full := map[string]string{OptionDSN: dsn}
	for k, v := range opts {
		full[k] = v
	}
	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(full)
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { cnxn.Close() })
	return ctx, cnxn
}

// readOneRow runs a query and returns its schema plus the first record batch.
// The caller must release the batch.
func readOneRow(ctx context.Context, t *testing.T, cnxn adbc.Connection, query string) (*arrow.Schema, arrow.RecordBatch) {
	t.Helper()
	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(query); err != nil {
		t.Fatalf("SetSqlQuery(%q): %v", query, err)
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("ExecuteQuery(%q): %v", query, err)
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		rec.Retain()
		return reader.Schema(), rec
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader error for %q: %v", query, err)
	}
	t.Fatalf("no rows for %q", query)
	return nil, nil
}

// BINARY_FLOAT is an IEEE single, so it must arrive as float32 and carry the
// exact bits. Widening to float64 was lossless in value but doubled the width
// and lied about the column's precision; it also meant a float32 column could
// not round-trip through an ingest without changing type.
func TestIntegration_BinaryFloatIsFloat32(t *testing.T) {
	ctx, cnxn := openTypedConnection(t, nil)

	const table = "ADBC_T32_BINFLOAT"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+" (F BINARY_FLOAT, D BINARY_DOUBLE)")
	defer dropTable(ctx, t, cnxn, table)

	// Values chosen to be exactly representable as float32, plus the edges.
	vals := []float32{0, 1.5, -2.25, float32(math.Pi), math.MaxFloat32, math.SmallestNonzeroFloat32}
	for i, v := range vals {
		execUpdate(ctx, t, cnxn, fmt.Sprintf(
			"INSERT INTO %s (F, D) VALUES (%s, %s)", table,
			oracleFloatLiteral(float64(v)), oracleFloatLiteral(float64(v))))
		_ = i
	}

	schema, rec := readOneRow(ctx, t, cnxn, "SELECT F, D FROM "+table+" ORDER BY F")
	defer rec.Release()

	if got := schema.Field(0).Type; !arrow.TypeEqual(got, arrow.PrimitiveTypes.Float32) {
		t.Errorf("BINARY_FLOAT type = %s, want float32", got)
	}
	if got := schema.Field(1).Type; !arrow.TypeEqual(got, arrow.PrimitiveTypes.Float64) {
		t.Errorf("BINARY_DOUBLE type = %s, want float64", got)
	}

	col, ok := rec.Column(0).(*array.Float32)
	if !ok {
		t.Fatalf("BINARY_FLOAT column is %T, want *array.Float32", rec.Column(0))
	}
	// Every value read back must be one of the ones written, bit for bit.
	written := map[float32]bool{}
	for _, v := range vals {
		written[v] = true
	}
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			t.Errorf("row %d is null", i)
			continue
		}
		if !written[col.Value(i)] {
			t.Errorf("row %d = %v, which was never written (precision lost)", i, col.Value(i))
		}
	}
}

// A float32 Arrow column must survive a full ingest and read back as float32,
// which it could not while BINARY_FLOAT widened on the way out.
func TestIntegration_Float32RoundTripsThroughIngest(t *testing.T) {
	ctx, cnxn := openTypedConnection(t, nil)

	const table = "ADBC_T32_F32RT"
	dropTable(ctx, t, cnxn, table)
	defer dropTable(ctx, t, cnxn, table)

	vals := []float32{0, 1.5, -2.25, float32(math.Pi), math.MaxFloat32}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	fb := b.Field(0).(*array.Float32Builder)
	fb.AppendValues(vals, nil)
	fb.AppendNull()
	rec := b.NewRecordBatch()
	defer rec.Release()

	ingestBatch(ctx, t, cnxn, table, rec)

	outSchema, out := readOneRow(ctx, t, cnxn, `SELECT "V" FROM `+table+` ORDER BY "V" NULLS LAST`)
	defer out.Release()

	if got := outSchema.Field(0).Type; !arrow.TypeEqual(got, arrow.PrimitiveTypes.Float32) {
		t.Fatalf("round-tripped type = %s, want float32", got)
	}
	col := out.Column(0).(*array.Float32)
	written := map[float32]bool{}
	for _, v := range vals {
		written[v] = true
	}
	nulls := 0
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			nulls++
			continue
		}
		if !written[col.Value(i)] {
			t.Errorf("row %d = %v, which was never written", i, col.Value(i))
		}
	}
	if nulls != 1 {
		t.Errorf("got %d nulls, want 1", nulls)
	}
}

// --- helpers ---

// oracleFloatLiteral renders a float64 as a BINARY_FLOAT-safe SQL literal.
// The 'f' suffix is what tells Oracle to parse it as a single rather than a
// NUMBER, which matters at the edges of the float32 range.
func oracleFloatLiteral(v float64) string {
	return fmt.Sprintf("%.*ef", 9, v)
}

// ingestBatch runs a create-mode bulk ingest of one record batch.
func ingestBatch(ctx context.Context, t *testing.T, cnxn adbc.Connection, table string, rec arrow.RecordBatch) {
	t.Helper()
	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, table); err != nil {
		t.Fatalf("SetOption(target table): %v", err)
	}
	if err := stmt.Bind(ctx, rec); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		t.Fatalf("ingest into %s: %v", table, err)
	}
}
