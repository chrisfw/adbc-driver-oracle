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

// --- TIMESTAMP precision ---

func TestArrowTimeUnit(t *testing.T) {
	cases := []struct {
		in     string
		want   arrow.TimeUnit
		wantOK bool
	}{
		{"", arrow.Microsecond, true}, // unset means the default
		{TimestampUnitMicro, arrow.Microsecond, true},
		{TimestampUnitNano, arrow.Nanosecond, true},
		{"ms", 0, false},
		{"s", 0, false},
		{"nanoseconds", 0, false},
	}
	for _, tc := range cases {
		got, ok := arrowTimeUnit(tc.in)
		if ok != tc.wantOK {
			t.Errorf("arrowTimeUnit(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("arrowTimeUnit(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
	if defaultTimestampUnit != TimestampUnitMicro {
		t.Errorf("defaultTimestampUnit = %q, want %q", defaultTimestampUnit, TimestampUnitMicro)
	}
}

// The unit is a connection policy, so both schema paths must honour it and
// DATE must ignore it: Oracle DATE has second granularity, so microseconds is
// always lossless for it.
func TestTypePolicyTimestampUnit(t *testing.T) {
	for _, tc := range []struct {
		unit arrow.TimeUnit
		name string
	}{{arrow.Microsecond, "us"}, {arrow.Nanosecond, "ns"}} {
		t.Run(tc.name, func(t *testing.T) {
			pol := typePolicy{timestampUnit: tc.unit}

			naive := pol.oracleTimestamp(false).(*arrow.TimestampType)
			if naive.Unit != tc.unit || naive.TimeZone != "" {
				t.Errorf("naive = %s, want unit %v and no zone", naive, tc.unit)
			}
			zoned := pol.oracleTimestamp(true).(*arrow.TimestampType)
			if zoned.Unit != tc.unit || zoned.TimeZone != "UTC" {
				t.Errorf("zoned = %s, want unit %v and UTC", zoned, tc.unit)
			}

			// DATE goes through neither, so it stays microseconds.
			date := oracleDataTypeToArrow("DATE", sql.NullInt64{}, sql.NullInt64{},
				typePolicy{timestampUnit: tc.unit}).(*arrow.TimestampType)
			if date.Unit != arrow.Microsecond || date.TimeZone != "" {
				t.Errorf("DATE = %s, want timestamp[us] regardless of the unit policy", date)
			}
		})
	}

	// A zero policy must not produce a zero unit, which would be seconds.
	if got := (typePolicy{}).oracleTimestamp(false).(*arrow.TimestampType).Unit; got != arrow.Microsecond {
		t.Errorf("zero policy unit = %v, want microsecond", got)
	}
}

// TIMESTAMP(9) keeps all nine digits only under the nanosecond unit. Under the
// default it truncates to microseconds, which is what Oracle's own default
// precision holds anyway.
func TestIntegration_TimestampNanoPrecision(t *testing.T) {
	const table = "ADBC_T33_NANOS"
	// 123456789 nanoseconds: the last three digits are what microseconds drop.
	const literal = "TIMESTAMP '2024-06-01 12:00:00.123456789'"

	cases := []struct {
		unit      string
		wantType  string
		wantNanos int
	}{
		{TimestampUnitMicro, "timestamp[us]", 123456000},
		{TimestampUnitNano, "timestamp[ns]", 123456789},
	}

	for _, tc := range cases {
		t.Run(tc.unit, func(t *testing.T) {
			ctx, cnxn := openTypedConnection(t, map[string]string{OptionTimestampUnit: tc.unit})
			dropTable(ctx, t, cnxn, table)
			execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+" (T TIMESTAMP(9))")
			defer dropTable(ctx, t, cnxn, table)
			execUpdate(ctx, t, cnxn, "INSERT INTO "+table+" VALUES ("+literal+")")

			schema, rec := readOneRow(ctx, t, cnxn, "SELECT T FROM "+table)
			defer rec.Release()

			if got := schema.Field(0).Type.String(); got != tc.wantType {
				t.Errorf("type = %s, want %s", got, tc.wantType)
			}
			col := rec.Column(0).(*array.Timestamp)
			ts := col.Value(0)
			got := ts.ToTime(schema.Field(0).Type.(*arrow.TimestampType).Unit)
			if got.Nanosecond() != tc.wantNanos {
				t.Errorf("nanoseconds = %d, want %d (value %s)", got.Nanosecond(), tc.wantNanos, got)
			}
			// The whole-second part must be right either way.
			if got.Year() != 2024 || got.Month() != 6 || got.Day() != 1 || got.Hour() != 12 {
				t.Errorf("wall clock = %s, want 2024-06-01 12:00", got)
			}
		})
	}
}

// Values on both sides of a DST transition must survive a TIMESTAMP WITH TIME
// ZONE round trip as the same instant. The zone is normalised to UTC on the
// way out, so the test compares instants rather than wall clocks.
func TestIntegration_TimestampWithTimeZoneDSTBoundary(t *testing.T) {
	ctx, cnxn := openTypedConnection(t, nil)

	const table = "ADBC_T33_DST"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+" (ID NUMBER(10), T TIMESTAMP(6) WITH TIME ZONE)")
	defer dropTable(ctx, t, cnxn, table)

	// US spring-forward 2024-03-10 02:00 and fall-back 2024-11-03 02:00, taken
	// either side of each jump so both offsets are exercised.
	cases := []struct {
		id      int
		literal string
		wantUTC string
	}{
		{1, "TIMESTAMP '2024-03-10 01:59:59 America/New_York'", "2024-03-10T06:59:59Z"},
		{2, "TIMESTAMP '2024-03-10 03:00:00 America/New_York'", "2024-03-10T07:00:00Z"},
		{3, "TIMESTAMP '2024-11-03 00:59:59 America/New_York'", "2024-11-03T04:59:59Z"},
		{4, "TIMESTAMP '2024-11-03 03:00:00 America/New_York'", "2024-11-03T08:00:00Z"},
	}
	for _, c := range cases {
		execUpdate(ctx, t, cnxn, fmt.Sprintf(
			"INSERT INTO %s VALUES (%d, %s)", table, c.id, c.literal))
	}

	schema, rec := readOneRow(ctx, t, cnxn, `SELECT "T" FROM `+table+` ORDER BY "ID"`)
	defer rec.Release()

	tsType := schema.Field(0).Type.(*arrow.TimestampType)
	if tsType.TimeZone != "UTC" {
		t.Fatalf("type = %s, want a UTC-normalised timestamp", tsType)
	}
	col := rec.Column(0).(*array.Timestamp)
	if col.Len() != len(cases) {
		t.Fatalf("got %d rows, want %d", col.Len(), len(cases))
	}
	for i, c := range cases {
		got := col.Value(i).ToTime(tsType.Unit).UTC().Format("2006-01-02T15:04:05Z")
		if got != c.wantUTC {
			t.Errorf("row %d (%s): got %s, want %s", c.id, c.literal, got, c.wantUTC)
		}
	}
}

// --- oracle.tz_offset_column ---

func TestHasRecoverableZone(t *testing.T) {
	// Wire names, as go-ora reports them.
	if !hasRecoverableZone("TimeStampTZ_DTY") {
		t.Error("TIMESTAMP WITH TIME ZONE should have a recoverable zone")
	}
	for _, name := range []string{"TimeStampLTZ_DTY", "TimeStampDTY", "DATE", "NUMBER"} {
		if hasRecoverableZone(name) {
			t.Errorf("%s should not have a recoverable zone", name)
		}
	}

	// Declared names, as ALL_TAB_COLUMNS spells them.
	for _, ddl := range []string{"TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(9) WITH TIME ZONE"} {
		if !ddlHasRecoverableZone(ddl) {
			t.Errorf("%s should have a recoverable zone", ddl)
		}
	}
	// LOCAL TIME ZONE contains "WITH TIME ZONE" as a substring, so the check
	// has to exclude it explicitly rather than match loosely.
	for _, ddl := range []string{
		"TIMESTAMP(6) WITH LOCAL TIME ZONE",
		"TIMESTAMP(6)",
		"DATE",
		"NUMBER",
	} {
		if ddlHasRecoverableZone(ddl) {
			t.Errorf("%s should not have a recoverable zone", ddl)
		}
	}
}

// With the option on, a TIMESTAMP WITH TIME ZONE gains a sibling column
// carrying the zone the value was written with, which UTC normalisation
// otherwise discards. The instant stays correct in the main column.
func TestIntegration_TzOffsetColumn(t *testing.T) {
	const table = "ADBC_T34_TZCOL"

	type row struct {
		id      int
		literal string
		wantUTC string
		wantTZ  string
	}
	rows := []row{
		// A stored IANA region comes back by name.
		{1, "TIMESTAMP '2024-07-01 12:00:00 America/New_York'", "2024-07-01T16:00:00Z", "America/New_York"},
		{2, "TIMESTAMP '2024-01-15 12:00:00 America/New_York'", "2024-01-15T17:00:00Z", "America/New_York"},
		{3, "TIMESTAMP '2024-07-01 12:00:00 Europe/London'", "2024-07-01T11:00:00Z", "Europe/London"},
		// A stored fixed offset comes back as that offset.
		{4, "TIMESTAMP '2024-07-01 12:00:00 +05:30'", "2024-07-01T06:30:00Z", "+05:30"},
		{5, "TIMESTAMP '2024-07-01 12:00:00 UTC'", "2024-07-01T12:00:00Z", "UTC"},
	}

	ctx, cnxn := openTypedConnection(t, map[string]string{
		OptionTzOffsetColumn: adbc.OptionValueEnabled,
	})
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+
		" (ID NUMBER(10), T TIMESTAMP(6) WITH TIME ZONE, L TIMESTAMP(6) WITH LOCAL TIME ZONE, N TIMESTAMP(6))")
	defer dropTable(ctx, t, cnxn, table)
	for _, r := range rows {
		execUpdate(ctx, t, cnxn, fmt.Sprintf("INSERT INTO %s VALUES (%d, %s, %s, %s)",
			table, r.id, r.literal, r.literal, r.literal))
	}

	schema, rec := readOneRow(ctx, t, cnxn,
		`SELECT "ID", "T", "L", "N" FROM `+table+` ORDER BY "ID"`)
	defer rec.Release()

	// Only the zoned column gets a sibling, and it lands immediately after it.
	var names []string
	for _, f := range schema.Fields() {
		names = append(names, f.Name)
	}
	want := []string{"ID", "T", "T" + tzColumnSuffix, "L", "N"}
	if len(names) != len(want) {
		t.Fatalf("columns = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("columns = %v, want %v", names, want)
		}
	}
	if got := schema.Field(2).Type; !arrow.TypeEqual(got, arrow.BinaryTypes.String) {
		t.Errorf("sibling type = %s, want utf8", got)
	}

	tsType := schema.Field(1).Type.(*arrow.TimestampType)
	if tsType.TimeZone != "UTC" {
		t.Errorf("value column = %s, want UTC-normalised", tsType)
	}
	tsCol := rec.Column(1).(*array.Timestamp)
	tzCol := rec.Column(2).(*array.String)

	for i, r := range rows {
		gotUTC := tsCol.Value(i).ToTime(tsType.Unit).UTC().Format("2006-01-02T15:04:05Z")
		if gotUTC != r.wantUTC {
			t.Errorf("row %d instant = %s, want %s", r.id, gotUTC, r.wantUTC)
		}
		if got := tzCol.Value(i); got != r.wantTZ {
			t.Errorf("row %d zone = %q, want %q", r.id, got, r.wantTZ)
		}
	}
}

// The option is off by default, so an existing consumer sees no extra column.
func TestIntegration_TzOffsetColumnDefaultsOff(t *testing.T) {
	const table = "ADBC_T34_TZOFF"
	ctx, cnxn := openTypedConnection(t, nil)

	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+" (T TIMESTAMP(6) WITH TIME ZONE)")
	defer dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "INSERT INTO "+table+" VALUES (SYSTIMESTAMP)")

	schema, rec := readOneRow(ctx, t, cnxn, `SELECT "T" FROM `+table)
	defer rec.Release()
	if schema.NumFields() != 1 {
		var names []string
		for _, f := range schema.Fields() {
			names = append(names, f.Name)
		}
		t.Errorf("columns = %v, want just T", names)
	}
}

// GetTableSchema has to emit the sibling as well, or it stops describing the
// batches ExecuteQuery returns. This is 2.1's invariant applied to a feature
// that changes the column list rather than a column's type.
func TestIntegration_TzOffsetColumnAppearsInTableSchema(t *testing.T) {
	const table = "ADBC_T34_TZMETA"
	ctx, cnxn := openTypedConnection(t, map[string]string{
		OptionTzOffsetColumn: adbc.OptionValueEnabled,
	})

	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+
		" (T TIMESTAMP(6) WITH TIME ZONE, L TIMESTAMP(6) WITH LOCAL TIME ZONE)")
	defer dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "INSERT INTO "+table+" VALUES (SYSTIMESTAMP, SYSTIMESTAMP)")

	meta, err := cnxn.GetTableSchema(ctx, nil, nil, table)
	if err != nil {
		t.Fatalf("GetTableSchema: %v", err)
	}
	querySchema, rec := readOneRow(ctx, t, cnxn, `SELECT "T", "L" FROM `+table)
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
}
