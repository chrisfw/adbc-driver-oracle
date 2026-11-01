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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/url"
	"testing"
	"time"

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

// --- INTERVAL ---

// Intervals were String, which reads fine and cannot be computed with. They
// now arrive as the two Arrow interval types, with the exact values Oracle
// stored.
func TestIntegration_Intervals(t *testing.T) {
	const table = "ADBC_T35_INTERVALS"
	ctx, cnxn := openTypedConnection(t, nil)

	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+
		" (ID NUMBER(10), DS INTERVAL DAY(5) TO SECOND(6), YM INTERVAL YEAR(4) TO MONTH)")
	defer dropTable(ctx, t, cnxn, table)

	rows := []struct {
		id       int
		ds, ym   string
		wantDS   time.Duration
		wantMons int32
	}{
		{1, "INTERVAL '3 04:05:06.789' DAY TO SECOND", "INTERVAL '7-3' YEAR TO MONTH",
			3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second + 789*time.Millisecond, 7*12 + 3},
		{2, "INTERVAL '0 00:00:00' DAY TO SECOND", "INTERVAL '0-0' YEAR TO MONTH", 0, 0},
		{3, "INTERVAL '-3 04:05:06.789' DAY TO SECOND", "INTERVAL '-7-3' YEAR TO MONTH",
			-(3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second + 789*time.Millisecond), -(7*12 + 3)},
		{4, "INTERVAL '1 00:00:00' DAY TO SECOND", "INTERVAL '0-1' YEAR TO MONTH", 24 * time.Hour, 1},
	}
	for _, r := range rows {
		execUpdate(ctx, t, cnxn, fmt.Sprintf("INSERT INTO %s VALUES (%d, %s, %s)",
			table, r.id, r.ds, r.ym))
	}
	// NULLs have to survive as nulls, not as zero intervals.
	execUpdate(ctx, t, cnxn, fmt.Sprintf("INSERT INTO %s VALUES (99, NULL, NULL)", table))

	schema, rec := readOneRow(ctx, t, cnxn, `SELECT "DS", "YM" FROM `+table+` ORDER BY "ID"`)
	defer rec.Release()

	if got, want := schema.Field(0).Type, arrow.FixedWidthTypes.Duration_us; !arrow.TypeEqual(got, want) {
		t.Fatalf("INTERVAL DAY TO SECOND type = %s, want %s", got, want)
	}
	if got, want := schema.Field(1).Type, arrow.FixedWidthTypes.MonthInterval; !arrow.TypeEqual(got, want) {
		t.Fatalf("INTERVAL YEAR TO MONTH type = %s, want %s", got, want)
	}

	ds := rec.Column(0).(*array.Duration)
	ym := rec.Column(1).(*array.MonthInterval)
	for i, r := range rows {
		if ds.IsNull(i) {
			t.Errorf("row %d: DS is null", r.id)
		} else if got := time.Duration(ds.Value(i)) * time.Microsecond; got != r.wantDS {
			t.Errorf("row %d: DS = %v, want %v", r.id, got, r.wantDS)
		}
		if ym.IsNull(i) {
			t.Errorf("row %d: YM is null", r.id)
		} else if got := int32(ym.Value(i)); got != r.wantMons {
			t.Errorf("row %d: YM = %d months, want %d", r.id, got, r.wantMons)
		}
	}
	last := len(rows)
	if !ds.IsNull(last) || !ym.IsNull(last) {
		t.Errorf("NULL row came back as DS=%v YM=%v, want nulls", ds.Value(last), ym.Value(last))
	}
}

// An interval stays microseconds even under the nanosecond timestamp unit,
// because go-ora cannot deliver more than 6 fractional digits. Oracle stores
// all 9 and TO_CHAR shows all 9; the driver receives ".123456". Pinned so the
// cap is a recorded limitation rather than a surprise, and so that lifting it
// in a go-ora fork is a visible decision.
func TestIntegration_IntervalFractionIsCappedAtMicroseconds(t *testing.T) {
	const table = "ADBC_T35_INTERVAL_NS"
	ctx, cnxn := openTypedConnection(t, map[string]string{
		OptionTimestampUnit: TimestampUnitNano,
	})

	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+" (DS INTERVAL DAY(2) TO SECOND(9))")
	defer dropTable(ctx, t, cnxn, table)
	// Explicit precision on the literal, or Oracle rounds it to 6 itself.
	execUpdate(ctx, t, cnxn, "INSERT INTO "+table+
		" VALUES (INTERVAL '0 00:00:01.123456789' DAY(2) TO SECOND(9))")

	// Oracle really did store all nine digits.
	if got := querySingleString(ctx, t, cnxn, `SELECT TO_CHAR("DS") FROM `+table); got != "+00 00:00:01.123456789" {
		t.Fatalf("Oracle stored %q, want the full nine digits", got)
	}

	schema, rec := readOneRow(ctx, t, cnxn, `SELECT "DS" FROM `+table)
	defer rec.Release()

	// The unit does not become nanoseconds, since nothing could fill it.
	if got, want := schema.Field(0).Type, arrow.FixedWidthTypes.Duration_us; !arrow.TypeEqual(got, want) {
		t.Fatalf("type = %s, want %s even under the nanosecond unit", got, want)
	}
	got := time.Duration(rec.Column(0).(*array.Duration).Value(0)) * time.Microsecond
	want := time.Second + 123456*time.Microsecond
	if got != want {
		t.Errorf("got %v, want %v (go-ora renders 6 fractional digits)", got, want)
	}
}

// Both schema paths must describe an interval column the same way.
func TestIntegration_IntervalSchemaPathsAgree(t *testing.T) {
	ctx, cnxn := openTypedConnection(t, nil)

	ddls := []string{
		"INTERVAL DAY TO SECOND",
		"INTERVAL DAY(5) TO SECOND(9)",
		"INTERVAL YEAR TO MONTH",
		"INTERVAL YEAR(4) TO MONTH",
	}
	for i, ddl := range ddls {
		table := fmt.Sprintf("ADBC_T35_IVAGREE_%d", i)
		t.Run(ddl, func(t *testing.T) {
			dropTable(ctx, t, cnxn, table)
			execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+" (C "+ddl+")")
			defer dropTable(ctx, t, cnxn, table)

			meta, err := cnxn.GetTableSchema(ctx, nil, nil, table)
			if err != nil {
				t.Fatalf("GetTableSchema: %v", err)
			}
			execUpdate(ctx, t, cnxn, "INSERT INTO "+table+" VALUES (NULL)")
			querySchema, rec := readOneRow(ctx, t, cnxn, "SELECT C FROM "+table)
			defer rec.Release()

			if !arrow.TypeEqual(meta.Field(0).Type, querySchema.Field(0).Type) {
				t.Errorf("%s: GetTableSchema says %s but ExecuteQuery says %s",
					ddl, meta.Field(0).Type, querySchema.Field(0).Type)
			}
		})
	}
}

// --- LOBs ---

func TestOracleLobFetchParam(t *testing.T) {
	cases := []struct {
		in     string
		want   string
		wantOK bool
	}{
		{"", "INLINE", true}, // unset means the default
		{LobFetchInline, "INLINE", true},
		{LobFetchStream, "STREAM", true},
		{"INLINE", "", false}, // the option takes the lower-case spelling
		{"pre", "", false},
		{"nonsense", "", false},
	}
	for _, tc := range cases {
		got, ok := oracleLobFetchParam(tc.in)
		if ok != tc.wantOK {
			t.Errorf("oracleLobFetchParam(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("oracleLobFetchParam(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if defaultLobFetch != LobFetchInline {
		t.Errorf("defaultLobFetch = %q, want %q", defaultLobFetch, LobFetchInline)
	}
}

// The mode is a connection parameter, so it has to reach go-ora through the
// DSN. Asserted on the assembled URL because that is the only place it is
// expressible.
func TestAssembledDSNCarriesLobFetch(t *testing.T) {
	cases := map[string]string{
		"":             "INLINE",
		LobFetchInline: "INLINE",
		LobFetchStream: "STREAM",
	}
	for opt, want := range cases {
		dsn := assembleOracleURL("u", "p", "h", "1521", "s", "", "", opt)
		parsed, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("assembled DSN does not parse: %v", err)
		}
		if got := parsed.Query().Get("LOB FETCH"); got != want {
			t.Errorf("lob_fetch %q produced LOB FETCH=%q, want %q", opt, got, want)
		}
	}
}

// Character and binary LOBs use the 64-bit offset layout. A String's offsets
// are int32, so a batch holding more than 2 GiB of text overflows them, and a
// CLOB column is where that much text comes from. VARCHAR2 and RAW are capped
// well inside int32 and keep the cheaper 32-bit layout.
func TestIntegration_LobsUseLargeOffsets(t *testing.T) {
	const table = "ADBC_T36_LOBTYPES"
	ctx, cnxn := openTypedConnection(t, nil)

	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+
		` ("C" CLOB, "NC" NCLOB, "B" BLOB, "V" VARCHAR2(10), "R" RAW(16))`)
	defer dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "INSERT INTO "+table+
		` VALUES (TO_CLOB('c'), TO_NCLOB('n'), HEXTORAW('AABB'), 'v', HEXTORAW('CCDD'))`)

	want := map[string]arrow.DataType{
		"C":  arrow.BinaryTypes.LargeString,
		"NC": arrow.BinaryTypes.LargeString,
		"B":  arrow.BinaryTypes.LargeBinary,
		"V":  arrow.BinaryTypes.String,
		"R":  arrow.BinaryTypes.Binary,
	}

	querySchema, rec := readOneRow(ctx, t, cnxn,
		`SELECT "C", "NC", "B", "V", "R" FROM `+table)
	defer rec.Release()
	for i, f := range querySchema.Fields() {
		if got := f.Type; !arrow.TypeEqual(got, want[f.Name]) {
			t.Errorf("ExecuteQuery %s = %s, want %s", f.Name, got, want[f.Name])
		}
		if rec.Column(i).IsNull(0) {
			t.Errorf("%s came back null", f.Name)
		}
	}

	// And the metadata path has to say the same, or the schemas diverge.
	meta, err := cnxn.GetTableSchema(ctx, nil, nil, table)
	if err != nil {
		t.Fatalf("GetTableSchema: %v", err)
	}
	for _, f := range meta.Fields() {
		if got := f.Type; !arrow.TypeEqual(got, want[f.Name]) {
			t.Errorf("GetTableSchema %s = %s, want %s", f.Name, got, want[f.Name])
		}
	}
}

// A multi-megabyte CLOB and BLOB must round-trip byte for byte under both
// fetch modes. Inline carries the value with its row; stream fetches it
// separately, which is the point of the option.
func TestIntegration_MultiMegabyteLobRoundTrip(t *testing.T) {
	const table = "ADBC_T36_BIGLOB"
	const size = 3 << 20 // 3 MiB, comfortably past any inline threshold

	// Non-repeating content, so a truncation or a misplaced offset shows up as
	// a mismatch rather than coinciding with itself.
	text := make([]byte, size)
	for i := range text {
		text[i] = byte('a' + (i*7)%26)
	}
	blob := make([]byte, size)
	for i := range blob {
		blob[i] = byte(i * 31)
	}

	for _, mode := range []string{LobFetchInline, LobFetchStream} {
		t.Run(mode, func(t *testing.T) {
			ctx, cnxn := openTypedConnection(t, map[string]string{OptionLobFetch: mode})
			dropTable(ctx, t, cnxn, table)
			defer dropTable(ctx, t, cnxn, table)

			// Write through the ingest path, so this covers both directions.
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "txt", Type: arrow.BinaryTypes.LargeString, Nullable: true},
				{Name: "bin", Type: arrow.BinaryTypes.LargeBinary, Nullable: true},
			}, nil)
			b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			defer b.Release()
			b.Field(0).(*array.LargeStringBuilder).Append(string(text))
			b.Field(1).(*array.BinaryBuilder).Append(blob)
			rec := b.NewRecordBatch()
			defer rec.Release()
			ingestBatch(ctx, t, cnxn, table, rec)

			outSchema, out := readOneRow(ctx, t, cnxn, `SELECT "TXT", "BIN" FROM `+table)
			defer out.Release()

			if got := outSchema.Field(0).Type; !arrow.TypeEqual(got, arrow.BinaryTypes.LargeString) {
				t.Errorf("txt type = %s, want large_utf8", got)
			}
			gotText := out.Column(0).(*array.LargeString).Value(0)
			if len(gotText) != size {
				t.Errorf("txt length = %d, want %d", len(gotText), size)
			} else if gotText != string(text) {
				t.Error("txt content differs from what was written")
			}

			gotBlob := out.Column(1).(*array.LargeBinary).Value(0)
			if len(gotBlob) != size {
				t.Errorf("bin length = %d, want %d", len(gotBlob), size)
			} else if !bytes.Equal(gotBlob, blob) {
				t.Error("bin content differs from what was written")
			}
		})
	}
}
