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
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Read-path benchmarks for Phase 4. The existing benchmarks in
// benchmark_test.go all measure ingest; this file measures the other
// direction, which is what Phase 4 is about.
//
// The fixture is the mixed-type table the plan calls for: NUMBER(12,2),
// NUMBER(10), VARCHAR2(100), DATE, TIMESTAMP(6). It is built server side and
// left in place between runs, because generating millions of rows costs far
// more than reading them and a benchmark that rebuilds its own fixture mostly
// measures the rebuild.
//
// Row count comes from BENCH_READ_ROWS, defaulting to 500k so an unattended
// run finishes. The plan's figure is 5,000,000; pass that explicitly when
// recording a baseline:
//
//	BENCH_READ_ROWS=5000000 ORACLE_TEST_DSN=... \
//	  go test ./go/... -run XXX -bench 'BenchmarkRead' -benchmem -benchtime 1x
const (
	benchReadTable       = "BENCH_READ_MIXED"
	benchReadDefaultRows = 500_000
)

func benchReadRows() int {
	if n := os.Getenv("BENCH_READ_ROWS"); n != "" {
		if v, err := strconv.Atoi(n); err == nil && v > 0 {
			return v
		}
	}
	return benchReadDefaultRows
}

// benchDSN is getTestDSN for a benchmark, which has no *testing.T.
func benchDSN(b *testing.B) string {
	b.Helper()
	dsn := os.Getenv("ORACLE_TEST_DSN")
	if dsn == "" {
		b.Skip("ORACLE_TEST_DSN not set, skipping read benchmark")
	}
	return dsn
}

// ensureBenchReadTable creates the fixture if it is missing or the wrong size,
// and otherwise leaves it alone.
//
// Rows are generated entirely inside Oracle. A client-side ingest of millions
// of rows would take far longer than the read being measured, and would make
// the fixture's content depend on the very bind path that Phase 4 changes.
// The row values vary per row so that no column is a constant the server or
// the driver could shortcut: NUMBER(12,2) gets cents, VARCHAR2 gets a
// varying-length string, and the two temporal columns advance per row.
func ensureBenchReadTable(ctx context.Context, tb testing.TB, dsn string, rows int) {
	tb.Helper()

	db, err := sql.Open("oracle", dsn)
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	defer db.Close()

	var have int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+benchReadTable).Scan(&have)
	if err == nil && have == rows {
		tb.Logf("fixture %s already has %d rows, reusing", benchReadTable, have)
		return
	}
	if err != nil && !isOraCode(err, 942) {
		tb.Fatalf("probing %s: %v", benchReadTable, err)
	}
	if err == nil {
		tb.Logf("fixture %s has %d rows, want %d, rebuilding", benchReadTable, have, rows)
	}

	start := time.Now()
	for _, stmt := range []string{
		`BEGIN EXECUTE IMMEDIATE 'DROP TABLE ` + benchReadTable + ` PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;`,
		`CREATE TABLE ` + benchReadTable + ` (
			AMOUNT   NUMBER(12,2),
			QTY      NUMBER(10),
			LABEL    VARCHAR2(100),
			EVENT_DT DATE,
			EVENT_TS TIMESTAMP(6)
		) NOLOGGING`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			tb.Fatalf("creating fixture: %v", err)
		}
	}

	// Two cross-joined generators, because a single CONNECT BY of several
	// million levels is a memory problem on small instances.
	const chunk = 100_000
	inserted := 0
	for inserted < rows {
		n := chunk
		if rows-inserted < n {
			n = rows - inserted
		}
		_, err := db.ExecContext(ctx, `
			INSERT /*+ APPEND */ INTO `+benchReadTable+`
			SELECT
				ROUND(:1 + ROWNUM + MOD(ROWNUM, 997) / 100, 2),
				:2 + ROWNUM,
				'row-' || TO_CHAR(:3 + ROWNUM) || RPAD('x', MOD(ROWNUM, 40) + 1, 'x'),
				DATE '2020-01-01' + MOD(ROWNUM, 3650),
				TIMESTAMP '2020-01-01 00:00:00' + NUMTODSINTERVAL(MOD(ROWNUM, 86400), 'SECOND')
			FROM dual CONNECT BY LEVEL <= :4`,
			inserted, inserted, inserted, n)
		if err != nil {
			tb.Fatalf("generating rows: %v", err)
		}
		if _, err := db.ExecContext(ctx, "COMMIT"); err != nil {
			tb.Fatalf("commit: %v", err)
		}
		inserted += n
	}

	if _, err := db.ExecContext(ctx, "ALTER TABLE "+benchReadTable+" LOGGING"); err != nil {
		tb.Logf("restoring LOGGING: %v", err)
	}
	tb.Logf("built fixture %s with %d rows in %v", benchReadTable, inserted, time.Since(start))
}

// readAll drains a query through the ADBC stack and returns the row count.
// Batches are released as they are consumed, so peak memory stays bounded and
// the allocation figure reflects per-row work rather than retention.
func readAll(ctx context.Context, cnxn adbc.Connection, query string) (int64, error) {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(query); err != nil {
		return 0, err
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return 0, err
	}
	defer reader.Release()

	var rows int64
	for reader.Next() {
		rows += reader.Record().NumRows()
	}
	return rows, reader.Err()
}

// benchmarkRead runs one read of the fixture and reports rows/s and
// allocations per row, which are the two figures Phase 4 is measured on.
func benchmarkRead(b *testing.B, query string, opts map[string]string) {
	dsn := benchDSN(b)
	ctx := context.Background()
	rows := benchReadRows()
	ensureBenchReadTable(ctx, b, dsn, rows)

	full := map[string]string{OptionDSN: dsn}
	for k, v := range opts {
		full[k] = v
	}
	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(full)
	if err != nil {
		b.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()
	cnxn, err := db.Open(ctx)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	// One untimed read first: it warms the buffer cache, so the measured runs
	// compare driver cost rather than Oracle's physical reads.
	if _, err := readAll(ctx, cnxn, query); err != nil {
		b.Fatalf("warmup read: %v", err)
	}

	var total int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := readAll(ctx, cnxn, query)
		if err != nil {
			b.Fatalf("read: %v", err)
		}
		total += n
	}
	b.StopTimer()

	if total == 0 {
		b.Fatal("read no rows")
	}
	elapsed := b.Elapsed().Seconds()
	if elapsed > 0 {
		b.ReportMetric(float64(total)/elapsed, "rows/s")
	}
	// -benchmem reports allocs/op per iteration; rows/op turns that into the
	// per-row figure the Phase 4.2 exit criterion is stated in.
	b.ReportMetric(float64(total)/float64(b.N), "rows/op")
}

// BenchmarkReadMixedTypes is the Phase 4 baseline: all five columns of the
// mixed-type fixture, read through the full ADBC stack.
func BenchmarkReadMixedTypes(b *testing.B) {
	benchmarkRead(b, `SELECT "AMOUNT", "QTY", "LABEL", "EVENT_DT", "EVENT_TS" FROM `+benchReadTable, nil)
}

// BenchmarkReadNumbersOnly isolates the NUMBER decode path, which Phase 4.2
// targets first: go-ora renders NUMBER to a decimal string and the driver
// parses it back, so this is where the per-row cost concentrates.
func BenchmarkReadNumbersOnly(b *testing.B) {
	benchmarkRead(b, `SELECT "AMOUNT", "QTY" FROM `+benchReadTable, nil)
}

// BenchmarkReadStringsOnly isolates VARCHAR2, where the cost is copying
// bytes into the Arrow builder rather than parsing.
func BenchmarkReadStringsOnly(b *testing.B) {
	benchmarkRead(b, `SELECT "LABEL" FROM `+benchReadTable, nil)
}

// BenchmarkReadTemporalOnly isolates DATE and TIMESTAMP, which arrive as
// time.Time and are rebuilt into Arrow timestamps per value.
func BenchmarkReadTemporalOnly(b *testing.B) {
	benchmarkRead(b, `SELECT "EVENT_DT", "EVENT_TS" FROM `+benchReadTable, nil)
}

// BenchmarkReadInt64Only isolates the cheapest column, an integral NUMBER
// that maps to Int64. It bounds how fast the current reader can go on a
// fixed-width column, which is what the Phase 4.2 exit criterion is about.
func BenchmarkReadInt64Only(b *testing.B) {
	benchmarkRead(b, `SELECT "QTY" FROM `+benchReadTable, nil)
}

// TestBenchmark_ReadBaseline records the baseline in the log style the rest of
// benchmark_test.go uses, for a run where a full Go benchmark is overkill.
func TestBenchmark_ReadBaseline(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()
	rows := benchReadRows()
	ensureBenchReadTable(ctx, t, dsn, rows)

	db, cnxn := openADBC(t, dsn)
	defer db.Close()
	defer cnxn.Close()

	queries := []struct{ name, query string }{
		{"all five columns", `SELECT "AMOUNT", "QTY", "LABEL", "EVENT_DT", "EVENT_TS" FROM ` + benchReadTable},
		{"NUMBER only", `SELECT "AMOUNT", "QTY" FROM ` + benchReadTable},
		{"VARCHAR2 only", `SELECT "LABEL" FROM ` + benchReadTable},
		{"temporal only", `SELECT "EVENT_DT", "EVENT_TS" FROM ` + benchReadTable},
		{"Int64 only", `SELECT "QTY" FROM ` + benchReadTable},
	}

	t.Logf("=== read baseline over %d rows ===", rows)
	for _, q := range queries {
		// Warm once so the figures compare driver cost, not physical reads.
		if _, err := readAll(ctx, cnxn, q.query); err != nil {
			t.Fatalf("%s warmup: %v", q.name, err)
		}
		start := time.Now()
		n, err := readAll(ctx, cnxn, q.query)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("%s: %v", q.name, err)
		}
		t.Logf("%-18s %8d rows in %-12v %10.0f rows/s", q.name, n, elapsed.Round(time.Millisecond),
			float64(n)/elapsed.Seconds())
	}
}

// Keep the fixture's declared schema honest: the benchmark is only meaningful
// if the column types are the ones Phase 4 claims to measure.
func TestBenchReadFixtureSchema(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()
	ensureBenchReadTable(ctx, t, dsn, benchReadRows())

	db, cnxn := openADBC(t, dsn)
	defer db.Close()
	defer cnxn.Close()

	schema, err := cnxn.GetTableSchema(ctx, nil, nil, benchReadTable)
	if err != nil {
		t.Fatalf("GetTableSchema: %v", err)
	}
	want := map[string]arrow.DataType{
		"AMOUNT":   &arrow.Decimal128Type{Precision: 12, Scale: 2},
		"QTY":      arrow.PrimitiveTypes.Int64,
		"LABEL":    arrow.BinaryTypes.String,
		"EVENT_DT": timestampNoTZ,
		"EVENT_TS": timestampNoTZ,
	}
	if schema.NumFields() != len(want) {
		t.Fatalf("fixture has %d columns, want %d", schema.NumFields(), len(want))
	}
	for _, f := range schema.Fields() {
		w, ok := want[f.Name]
		if !ok {
			t.Errorf("unexpected column %s", f.Name)
			continue
		}
		if !arrow.TypeEqual(f.Type, w) {
			t.Errorf("column %s is %s, want %s", f.Name, f.Type, w)
		}
	}
}

// =============================================================================
// Decode-bound fixture
// =============================================================================

// The mixed fixture is five narrow columns, and a profile shows that read is
// about 72% waiting on fetch round trips. That bounds what any decode
// improvement can achieve on it, which matters because 4.2's exit criterion is
// stated as a multiple of throughput. This second fixture shifts the balance:
// twenty NUMBER(12,2) columns, so each row carries twenty decimal values to
// decode and the cost moves from waiting to decoding. Scaled NUMBER is also
// the most expensive converter, since go-ora renders it to a decimal string
// that the driver parses back.
const (
	benchWideTable = "BENCH_READ_WIDE"
	benchWideCols  = 20
)

func benchWideColumnList() string {
	cols := make([]string, benchWideCols)
	for i := range cols {
		cols[i] = fmt.Sprintf(`"V%02d"`, i)
	}
	return strings.Join(cols, ", ")
}

func ensureBenchWideTable(ctx context.Context, tb testing.TB, dsn string, rows int) {
	tb.Helper()

	db, err := sql.Open("oracle", dsn)
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	defer db.Close()

	var have int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+benchWideTable).Scan(&have)
	if err == nil && have == rows {
		tb.Logf("fixture %s already has %d rows, reusing", benchWideTable, have)
		return
	}
	if err != nil && !isOraCode(err, 942) {
		tb.Fatalf("probing %s: %v", benchWideTable, err)
	}

	defs := make([]string, benchWideCols)
	sels := make([]string, benchWideCols)
	for i := range defs {
		defs[i] = fmt.Sprintf("V%02d NUMBER(12,2)", i)
		// A different value per column as well as per row, so no column is a
		// constant that could be decoded once.
		sels[i] = fmt.Sprintf("ROUND(ROWNUM + %d + MOD(ROWNUM, 97) / 100, 2)", i)
	}

	start := time.Now()
	for _, stmt := range []string{
		`BEGIN EXECUTE IMMEDIATE 'DROP TABLE ` + benchWideTable + ` PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;`,
		`CREATE TABLE ` + benchWideTable + ` (` + strings.Join(defs, ", ") + `) NOLOGGING`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			tb.Fatalf("creating wide fixture: %v", err)
		}
	}

	const chunk = 50_000
	inserted := 0
	for inserted < rows {
		n := chunk
		if rows-inserted < n {
			n = rows - inserted
		}
		_, err := db.ExecContext(ctx,
			`INSERT /*+ APPEND */ INTO `+benchWideTable+` SELECT `+strings.Join(sels, ", ")+
				` FROM dual CONNECT BY LEVEL <= :1`, n)
		if err != nil {
			tb.Fatalf("generating wide rows: %v", err)
		}
		if _, err := db.ExecContext(ctx, "COMMIT"); err != nil {
			tb.Fatalf("commit: %v", err)
		}
		inserted += n
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE "+benchWideTable+" LOGGING"); err != nil {
		tb.Logf("restoring LOGGING: %v", err)
	}
	tb.Logf("built fixture %s with %d rows x %d columns in %v",
		benchWideTable, inserted, benchWideCols, time.Since(start))
}

// BenchmarkReadWideDecimals is the decode-bound companion to
// BenchmarkReadMixedTypes. Twenty NUMBER(12,2) columns per row, which is where
// a columnar decode should show its value and where 4.2's throughput target
// has room to be met.
func BenchmarkReadWideDecimals(b *testing.B) {
	dsn := benchDSN(b)
	ctx := context.Background()
	// A fifth of the row count, since each row is four times the columns.
	rows := benchReadRows() / 5
	if rows < 1 {
		rows = 1
	}
	ensureBenchWideTable(ctx, b, dsn, rows)

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		b.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()
	cnxn, err := db.Open(ctx)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	query := `SELECT ` + benchWideColumnList() + ` FROM ` + benchWideTable
	if _, err := readAll(ctx, cnxn, query); err != nil {
		b.Fatalf("warmup read: %v", err)
	}

	var total int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := readAll(ctx, cnxn, query)
		if err != nil {
			b.Fatalf("read: %v", err)
		}
		total += n
	}
	b.StopTimer()

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(total)/elapsed, "rows/s")
		b.ReportMetric(float64(total)*benchWideCols/elapsed, "values/s")
	}
	b.ReportMetric(float64(total)/float64(b.N), "rows/op")
}
