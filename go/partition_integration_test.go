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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The property that matters for a partitioned read is that the union of the
// slices is exactly the whole result: every row once, no row twice, no row
// missing. A row count alone would not catch a slice that returned a
// duplicate in place of a dropped row, so these tests compare the full
// multiset of rendered rows.

// rowsOf renders every row of a reader as one string, so two reads can be
// compared as multisets regardless of the order they arrived in.
func rowsOf(t *testing.T, reader array.RecordReader) []string {
	t.Helper()
	var out []string
	for reader.Next() {
		rec := reader.Record()
		for r := 0; r < int(rec.NumRows()); r++ {
			parts := make([]string, rec.NumCols())
			for c := 0; c < int(rec.NumCols()); c++ {
				col := rec.Column(c)
				if col.IsNull(r) {
					parts[c] = "<null>"
					continue
				}
				parts[c] = col.ValueStr(r)
			}
			out = append(out, strings.Join(parts, "\x1f"))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}
	return out
}

// readWholeQuery is the baseline the union has to match.
func readWholeQuery(ctx context.Context, t *testing.T, cnxn adbc.Connection, query string) ([]string, *arrow.Schema) {
	t.Helper()
	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(query); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	defer reader.Release()
	return rowsOf(t, reader), reader.Schema()
}

// partitionedRead plans the slices and reads each one, deliberately on its own
// fresh connection: reading a partition from a connection that did not plan it
// is the whole point of a serialised descriptor, and a descriptor that only
// worked on the planning connection would pass a weaker test.
func partitionedRead(ctx context.Context, t *testing.T, db adbc.Database, cnxn adbc.Connection,
	query string, opts map[string]string) (rows []string, schema *arrow.Schema, descs []partitionDescriptor) {
	t.Helper()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(query); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	for k, v := range opts {
		if err := stmt.SetOption(k, v); err != nil {
			t.Fatalf("SetOption(%s=%s): %v", k, v, err)
		}
	}

	schema, parts, _, err := stmt.ExecutePartitions(ctx)
	if err != nil {
		t.Fatalf("ExecutePartitions: %v", err)
	}
	if parts.NumPartitions != uint64(len(parts.PartitionIDs)) {
		t.Fatalf("NumPartitions is %d but %d descriptors were returned",
			parts.NumPartitions, len(parts.PartitionIDs))
	}
	if parts.NumPartitions == 0 {
		t.Fatal("ExecutePartitions returned no partitions, so no data could be read")
	}

	for i, id := range parts.PartitionIDs {
		var d partitionDescriptor
		if err := json.Unmarshal(id, &d); err != nil {
			t.Fatalf("partition %d is not readable as a descriptor: %v", i, err)
		}
		descs = append(descs, d)

		reader, schema2, got := readOnePartition(ctx, t, db, id)
		// Each slice must announce the schema the planner promised. A slice
		// that decided its own types would hand the caller columns that do
		// not line up with the others.
		if !schema.Equal(schema2) {
			t.Errorf("partition %d schema disagrees with the planned schema:\n slice %s\n plan  %s",
				i, schema2, schema)
		}
		rows = append(rows, got...)
		reader.Release()
	}
	return rows, schema, descs
}

// readOnePartition opens a connection of its own and reads the slice on it.
func readOnePartition(ctx context.Context, t *testing.T, db adbc.Database, id []byte) (array.RecordReader, *arrow.Schema, []string) {
	t.Helper()
	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open for partition read: %v", err)
	}
	t.Cleanup(func() { cnxn.Close() })

	reader, err := cnxn.ReadPartition(ctx, id)
	if err != nil {
		t.Fatalf("ReadPartition: %v", err)
	}
	return reader, reader.Schema(), rowsOf(t, reader)
}

func assertSameMultiset(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: read %d rows, want %d", label, len(got), len(want))
	}
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)

	if len(g) == len(w) {
		for i := range g {
			if g[i] != w[i] {
				t.Errorf("%s: rows differ at sorted position %d:\n got %q\nwant %q", label, i, g[i], w[i])
				return
			}
		}
		return
	}

	// Different lengths: name what was lost or duplicated rather than only
	// reporting the counts, since that is what a broken predicate looks like.
	count := func(rows []string) map[string]int {
		m := make(map[string]int, len(rows))
		for _, r := range rows {
			m[r]++
		}
		return m
	}
	gc, wc := count(g), count(w)
	shown := 0
	for row, n := range wc {
		if gc[row] != n && shown < 5 {
			t.Errorf("%s: row %q appears %d times, want %d", label, row, gc[row], n)
			shown++
		}
	}
	for row, n := range gc {
		if _, ok := wc[row]; !ok && shown < 5 {
			t.Errorf("%s: row %q was returned but is not in the full result (%d times)", label, row, n)
			shown++
		}
	}
}

// openPartitionDatabase returns both the database and a connection, since
// reading partitions needs to open further connections.
func openPartitionDatabase(t *testing.T, opts map[string]string) (context.Context, adbc.Database, adbc.Connection) {
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
	return ctx, db, cnxn
}

// waitForFlashback blocks until the table can be read AS OF the SCN the
// driver would capture.
//
// Oracle maps a timestamp to an SCN in coarse steps, so for a few seconds
// after a CREATE TABLE the mapped SCN still predates the table's definition
// and a flashback query raises ORA-01466. The driver handles that by reading
// the query unsplit, which is correct but is not the path these tests are
// about, so they wait for it to pass rather than racing it.
func waitForFlashback(ctx context.Context, t *testing.T, cnxn adbc.Connection, table string) {
	t.Helper()
	const attempts = 15
	for i := 0; i < attempts; i++ {
		err := tryExec(ctx, cnxn,
			"SELECT COUNT(*) FROM "+table+" AS OF SCN TIMESTAMP_TO_SCN(SYSTIMESTAMP)")
		if err == nil {
			return
		}
		if !flashbackUnusable(err) {
			t.Fatalf("flashback probe on %s failed for an unrelated reason: %v", table, err)
		}
		time.Sleep(time.Second)
	}
	t.Skipf("%s could not be read with a flashback query after %ds, so the partitioning strategies "+
		"cannot be exercised on this database", table, attempts)
}

// A heap table has no partitions, so this is the ROWID range path: the one
// that divides the scan rather than repeating it.
func TestIntegration_PartitionsCoverHeapTable(t *testing.T) {
	ctx, db, cnxn := openPartitionDatabase(t, nil)

	const table = "P43_HEAP"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, `CREATE TABLE `+table+` (
		ID NUMBER(10) PRIMARY KEY, LABEL VARCHAR2(40), AMOUNT NUMBER(12,2), PAD VARCHAR2(200))`)
	t.Cleanup(func() { dropTable(ctx, t, cnxn, table) })

	// Enough rows to span several blocks, so the chunker has something to
	// divide. A single-extent table would come back as one slice and the test
	// would prove nothing about coverage.
	execUpdate(ctx, t, cnxn, `INSERT INTO `+table+`
		SELECT LEVEL, 'row-' || LEVEL, LEVEL * 1.25, RPAD('x', 200, 'x')
		FROM DUAL CONNECT BY LEVEL <= 20000`)
	waitForFlashback(ctx, t, cnxn, table)

	query := "SELECT ID, LABEL, AMOUNT FROM " + table
	want, wantSchema := readWholeQuery(ctx, t, cnxn, query)
	if len(want) != 20000 {
		t.Fatalf("fixture has %d rows, want 20000", len(want))
	}

	got, schema, descs := partitionedRead(ctx, t, db, cnxn, query, nil)

	if !wantSchema.Equal(schema) {
		t.Errorf("partitioned schema differs from the unpartitioned one:\n part %s\n full %s", schema, wantSchema)
	}
	assertSameMultiset(t, "heap union", got, want)

	// The strategy is asserted, not just the result. A silent fall through to
	// ORA_HASH would still be correct and would quietly multiply server I/O
	// by the partition count, which is the opposite of the point.
	if descs[0].Strategy != partitionByRowidRange {
		t.Errorf("heap table was split by %q, want %q: the ROWID range path is the one that divides the scan",
			descs[0].Strategy, partitionByRowidRange)
	}
	if len(descs) < 2 {
		t.Errorf("heap table produced %d partitions, so nothing was actually split", len(descs))
	}
	if descs[0].SCN == "" {
		t.Error("the descriptor carries no SCN, so the slices are not pinned to one snapshot")
	}
	for i, d := range descs {
		if d.SCN != descs[0].SCN {
			t.Errorf("partition %d reads at SCN %s but partition 0 reads at %s: the slices do not share a snapshot",
				i, d.SCN, descs[0].SCN)
		}
		if !strings.Contains(d.Query, "AS OF SCN "+d.SCN) {
			t.Errorf("partition %d query does not read at the captured SCN: %s", i, d.Query)
		}
	}
}

// A partitioned table splits by partition, which is the best case: each slice
// reads its own segments.
func TestIntegration_PartitionsCoverPartitionedTable(t *testing.T) {
	ctx, db, cnxn := openPartitionDatabase(t, nil)

	const table = "P43_RANGE"
	dropTable(ctx, t, cnxn, table)
	err := tryExec(ctx, cnxn, `CREATE TABLE `+table+` (ID NUMBER(10), BUCKET NUMBER(3), LABEL VARCHAR2(40))
		PARTITION BY RANGE (BUCKET) (
			PARTITION P1 VALUES LESS THAN (10),
			PARTITION P2 VALUES LESS THAN (20),
			PARTITION P3 VALUES LESS THAN (MAXVALUE))`)
	if err != nil {
		t.Skipf("partitioning is not available on this database: %v", err)
	}
	t.Cleanup(func() { dropTable(ctx, t, cnxn, table) })

	execUpdate(ctx, t, cnxn, `INSERT INTO `+table+`
		SELECT LEVEL, MOD(LEVEL, 30), 'row-' || LEVEL FROM DUAL CONNECT BY LEVEL <= 3000`)
	waitForFlashback(ctx, t, cnxn, table)

	query := "SELECT ID, BUCKET, LABEL FROM " + table
	want, _ := readWholeQuery(ctx, t, cnxn, query)

	got, _, descs := partitionedRead(ctx, t, db, cnxn, query, nil)

	assertSameMultiset(t, "partitioned union", got, want)

	if descs[0].Strategy != partitionByPartition {
		t.Errorf("split by %q, want %q", descs[0].Strategy, partitionByPartition)
	}
	// One slice per partition, regardless of the requested count: a PARTITION
	// clause names exactly one partition.
	if len(descs) != 3 {
		t.Errorf("produced %d slices for a 3 partition table", len(descs))
	}
	for i, d := range descs {
		if !strings.Contains(strings.ToUpper(d.Query), "PARTITION (") {
			t.Errorf("partition %d does not name a partition: %s", i, d.Query)
		}
		// The order is a syntax requirement: the reverse is ORA-03048.
		if p, s := strings.Index(d.Query, "PARTITION ("), strings.Index(d.Query, "AS OF SCN"); p > s {
			t.Errorf("partition %d puts AS OF SCN before PARTITION, which Oracle rejects: %s", i, d.Query)
		}
	}
}

// Requesting more slices than the table has partitions must not drop a
// partition, and requesting fewer must not either.
func TestIntegration_PartitionCountDoesNotDropPartitions(t *testing.T) {
	ctx, db, cnxn := openPartitionDatabase(t, nil)

	const table = "P43_RANGE2"
	dropTable(ctx, t, cnxn, table)
	err := tryExec(ctx, cnxn, `CREATE TABLE `+table+` (ID NUMBER(10), BUCKET NUMBER(3))
		PARTITION BY RANGE (BUCKET) (
			PARTITION Q1 VALUES LESS THAN (5),
			PARTITION Q2 VALUES LESS THAN (10),
			PARTITION Q3 VALUES LESS THAN (15),
			PARTITION Q4 VALUES LESS THAN (MAXVALUE))`)
	if err != nil {
		t.Skipf("partitioning is not available on this database: %v", err)
	}
	t.Cleanup(func() { dropTable(ctx, t, cnxn, table) })

	execUpdate(ctx, t, cnxn, `INSERT INTO `+table+`
		SELECT LEVEL, MOD(LEVEL, 20) FROM DUAL CONNECT BY LEVEL <= 800`)
	waitForFlashback(ctx, t, cnxn, table)

	query := "SELECT ID, BUCKET FROM " + table
	want, _ := readWholeQuery(ctx, t, cnxn, query)

	for _, count := range []string{"2", "4", "9"} {
		t.Run("count="+count, func(t *testing.T) {
			got, _, descs := partitionedRead(ctx, t, db, cnxn, query,
				map[string]string{OptionPartitionCount: count})
			assertSameMultiset(t, "count="+count, got, want)
			if len(descs) != 4 {
				t.Errorf("produced %d slices for a 4 partition table with partition_count=%s", len(descs), count)
			}
		})
	}
}

// A predicate the caller wrote has to survive the rewrite. A slice that lost
// it would return rows the caller filtered out.
func TestIntegration_PartitionsKeepTheWhereClause(t *testing.T) {
	ctx, db, cnxn := openPartitionDatabase(t, nil)

	const table = "P43_WHERE"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, `CREATE TABLE `+table+` (ID NUMBER(10), KEEP NUMBER(1), PAD VARCHAR2(200))`)
	t.Cleanup(func() { dropTable(ctx, t, cnxn, table) })
	execUpdate(ctx, t, cnxn, `INSERT INTO `+table+`
		SELECT LEVEL, MOD(LEVEL, 2), RPAD('y', 200, 'y') FROM DUAL CONNECT BY LEVEL <= 10000`)
	waitForFlashback(ctx, t, cnxn, table)

	// An OR, because appending AND to an unparenthesised OR widens the slice
	// to every row matching the first branch.
	query := "SELECT ID, KEEP FROM " + table + " WHERE KEEP = 1 OR ID = 2"
	want, _ := readWholeQuery(ctx, t, cnxn, query)
	if len(want) != 5001 {
		t.Fatalf("fixture filter matched %d rows, want 5001", len(want))
	}

	got, _, _ := partitionedRead(ctx, t, db, cnxn, query, nil)
	assertSameMultiset(t, "filtered union", got, want)
}

// Inference asks the data what an unconstrained NUMBER should become. Slices
// see different rows, so without pinning they would answer differently and
// the union would be columns of different types claiming to be one result.
//
// The fixture is built so that an unpinned slice would get it wrong: the
// four-decimal values are all at the front of the table, so the planner, which
// samples from the start, infers a scale of 4, while a slice holding only
// later rows would see nothing but one-decimal values and infer a scale of 1.
// Without pinning, that slice announces decimal(38, 1) for a column the caller
// was promised as decimal(38, 4).
func TestIntegration_PartitionsPinInferredNumberTypes(t *testing.T) {
	ctx, db, cnxn := openPartitionDatabase(t, nil)

	const table = "P43_INFER"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, `CREATE TABLE `+table+` (ID NUMBER(10), VAL NUMBER, PAD VARCHAR2(200))`)
	t.Cleanup(func() { dropTable(ctx, t, cnxn, table) })

	execUpdate(ctx, t, cnxn, `INSERT INTO `+table+`
		SELECT LEVEL, LEVEL / 10000, RPAD('z', 200, 'z') FROM DUAL CONNECT BY LEVEL <= 1000`)
	execUpdate(ctx, t, cnxn, `INSERT INTO `+table+`
		SELECT 100000 + LEVEL, LEVEL / 10, RPAD('z', 200, 'z') FROM DUAL CONNECT BY LEVEL <= 9000`)
	waitForFlashback(ctx, t, cnxn, table)

	query := "SELECT ID, VAL FROM " + table
	want, wantSchema := readWholeQuery(ctx, t, cnxn, query)

	got, schema, descs := partitionedRead(ctx, t, db, cnxn, query, nil)

	// The unpartitioned read and the partitioned read must describe the same
	// data the same way, which is the agreement property the whole driver is
	// held to.
	if !wantSchema.Equal(schema) {
		t.Errorf("partitioned schema differs from the unpartitioned one:\n part %s\n full %s", schema, wantSchema)
	}
	assertSameMultiset(t, "inferred union", got, want)

	// The sample has to have actually inferred something, or this test would
	// pass without exercising pinning at all.
	valType := schema.Field(1).Type
	if valType.ID() != arrow.DECIMAL128 {
		t.Fatalf("VAL came back as %s, so no decimal was inferred and this test proves nothing", valType)
	}
	if len(descs) < 2 {
		t.Fatalf("the table was not split, so no slice could disagree")
	}
	if len(descs[0].NumberTypes) == 0 {
		t.Error("no column types were pinned, so each slice inferred its own and could disagree")
	}
	for i, d := range descs {
		if fmt.Sprint(d.NumberTypes) != fmt.Sprint(descs[0].NumberTypes) {
			t.Errorf("partition %d pins %v but partition 0 pins %v", i, d.NumberTypes, descs[0].NumberTypes)
		}
	}

	// Prove the teeth: read a late slice with the pinning stripped out and
	// confirm it would have announced a different type. If this comes back
	// equal, the fixture no longer distinguishes pinned from unpinned and the
	// assertions above are vacuous.
	last := descs[len(descs)-1]
	stripped := partitionDescriptor{Query: last.Query, Strategy: last.Strategy, Count: last.Count}
	blob, err := stripped.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	unpinnedCnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer unpinnedCnxn.Close()
	reader, err := unpinnedCnxn.ReadPartition(ctx, blob)
	if err != nil {
		t.Fatalf("ReadPartition without pinning: %v", err)
	}
	defer reader.Release()
	if reader.Schema().Equal(schema) {
		t.Errorf("the last slice infers %s on its own, the same as the planned schema, "+
			"so this fixture does not distinguish pinned types from inferred ones",
			reader.Schema().Field(1).Type)
	} else {
		t.Logf("teeth confirmed: unpinned, the last slice would announce %s instead of %s",
			reader.Schema().Field(1).Type, valType)
	}
}

// A query that cannot be split safely still has to return the right data, so
// it comes back as one slice rather than as an error.
func TestIntegration_UnsplittableQueryBecomesOnePartition(t *testing.T) {
	ctx, db, cnxn := openPartitionDatabase(t, nil)

	const table = "P43_SINGLE"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, `CREATE TABLE `+table+` (ID NUMBER(10), G NUMBER(3))`)
	t.Cleanup(func() { dropTable(ctx, t, cnxn, table) })
	execUpdate(ctx, t, cnxn, `INSERT INTO `+table+`
		SELECT LEVEL, MOD(LEVEL, 7) FROM DUAL CONNECT BY LEVEL <= 500`)

	cases := []struct {
		name  string
		query string
	}{
		{"aggregate", "SELECT G, COUNT(*) AS N FROM " + table + " GROUP BY G"},
		{"ordered", "SELECT ID FROM " + table + " ORDER BY ID"},
		{"row limited", "SELECT ID FROM " + table + " WHERE ROWNUM <= 10"},
		{"distinct", "SELECT DISTINCT G FROM " + table},
		{"dual", "SELECT 1 AS ONE FROM DUAL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want, _ := readWholeQuery(ctx, t, cnxn, tc.query)
			got, _, descs := partitionedRead(ctx, t, db, cnxn, tc.query, nil)

			if len(descs) != 1 {
				t.Fatalf("produced %d partitions for a query that cannot be split", len(descs))
			}
			if descs[0].Strategy != partitionSingle {
				t.Errorf("strategy = %q, want %q", descs[0].Strategy, partitionSingle)
			}
			assertSameMultiset(t, tc.name, got, want)
		})
	}
}

// A table whose definition changed a moment ago cannot be flashed back to,
// and the right answer is one correct unpartitioned read rather than four
// descriptors that all fail. This is the common ETL shape: create a staging
// table, load it, extract it.
func TestIntegration_FreshDDLFallsBackToOnePartition(t *testing.T) {
	ctx, db, cnxn := openPartitionDatabase(t, nil)

	const table = "P43_FRESH"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, `CREATE TABLE `+table+` (ID NUMBER(10), PAD VARCHAR2(200))`)
	t.Cleanup(func() { dropTable(ctx, t, cnxn, table) })
	execUpdate(ctx, t, cnxn, `INSERT INTO `+table+`
		SELECT LEVEL, RPAD('w', 200, 'w') FROM DUAL CONNECT BY LEVEL <= 5000`)

	// Deliberately no wait: the DDL is seconds old, so the SCN the driver can
	// reach predates it.
	query := "SELECT ID FROM " + table
	probeErr := tryExec(ctx, cnxn, "SELECT COUNT(*) FROM "+table+" AS OF SCN TIMESTAMP_TO_SCN(SYSTIMESTAMP)")
	if probeErr == nil {
		t.Skip("the table can already be flashed back to, so the fresh DDL path cannot be exercised")
	}
	if !flashbackUnusable(probeErr) {
		t.Fatalf("flashback probe failed for an unrelated reason: %v", probeErr)
	}

	want, _ := readWholeQuery(ctx, t, cnxn, query)
	got, _, descs := partitionedRead(ctx, t, db, cnxn, query, nil)

	if len(descs) != 1 || descs[0].Strategy != partitionSingle {
		t.Errorf("produced %d partitions with strategy %q, want 1 single: the slices could not have shared a snapshot",
			len(descs), descs[0].Strategy)
	}
	assertSameMultiset(t, "fresh ddl", got, want)
}

// A view has no partitions and no usable ROWID, so it must land on the single
// partition path rather than producing a slice query that fails at read time.
func TestIntegration_ViewBecomesOnePartition(t *testing.T) {
	ctx, db, cnxn := openPartitionDatabase(t, nil)

	const table = "P43_VBASE"
	const view = "P43_VIEW"
	execIgnoreError(ctx, t, cnxn, "DROP VIEW "+view)
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, `CREATE TABLE `+table+` (ID NUMBER(10))`)
	t.Cleanup(func() { dropTable(ctx, t, cnxn, table) })
	execUpdate(ctx, t, cnxn, `INSERT INTO `+table+` SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 100`)

	if err := tryExec(ctx, cnxn, "CREATE VIEW "+view+" AS SELECT ID FROM "+table); err != nil {
		t.Skipf("cannot create a view: %v", err)
	}
	t.Cleanup(func() { execIgnoreError(ctx, t, cnxn, "DROP VIEW "+view) })

	query := "SELECT ID FROM " + view
	want, _ := readWholeQuery(ctx, t, cnxn, query)
	got, _, descs := partitionedRead(ctx, t, db, cnxn, query, nil)

	if len(descs) != 1 {
		t.Errorf("produced %d partitions for a view", len(descs))
	}
	assertSameMultiset(t, "view", got, want)
}

// partition_count=1 means do not split, and the result still has to be right.
func TestIntegration_PartitionCountOne(t *testing.T) {
	ctx, db, cnxn := openPartitionDatabase(t, map[string]string{OptionPartitionCount: "1"})

	const table = "P43_ONE"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, `CREATE TABLE `+table+` (ID NUMBER(10))`)
	t.Cleanup(func() { dropTable(ctx, t, cnxn, table) })
	execUpdate(ctx, t, cnxn, `INSERT INTO `+table+` SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 250`)

	query := "SELECT ID FROM " + table
	want, _ := readWholeQuery(ctx, t, cnxn, query)
	got, _, descs := partitionedRead(ctx, t, db, cnxn, query, nil)

	if len(descs) != 1 {
		t.Errorf("partition_count=1 produced %d partitions", len(descs))
	}
	if descs[0].SCN != "" {
		t.Error("a single partition does not need a flashback SCN: one query is already a consistent read")
	}
	assertSameMultiset(t, "single", got, want)
}

// A descriptor carries only SQL, so a bound parameter would arrive unbound.
// Refusing is the only answer that does not return wrong data.
func TestIntegration_PartitionsRejectBoundParameters(t *testing.T) {
	ctx, _, cnxn := openPartitionDatabase(t, nil)

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery("SELECT ID FROM DUAL WHERE 1 = :1"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	rec := intRecord(t, "ID", 1)
	defer rec.Release()
	if err := stmt.Bind(ctx, rec); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	_, _, _, err = stmt.ExecutePartitions(ctx)
	if err == nil {
		t.Fatal("ExecutePartitions accepted bound parameters, so the slices would run with unbound placeholders")
	}
	var adbcErr adbc.Error
	if !strings.Contains(err.Error(), "bound parameters") {
		t.Errorf("error does not explain the problem: %v", err)
	}
	if errors.As(err, &adbcErr) && adbcErr.Code != adbc.StatusNotImplemented {
		t.Errorf("status = %v, want StatusNotImplemented", adbcErr.Code)
	}
}

// A descriptor is opaque bytes a caller may store, so a corrupted or
// unrecognised one has to be refused rather than executed.
func TestIntegration_ReadPartitionRejectsBadDescriptors(t *testing.T) {
	ctx, _, cnxn := openPartitionDatabase(t, nil)

	for _, bad := range [][]byte{
		[]byte("not json"),
		[]byte(`{"version": 999, "query": "SELECT 1 FROM DUAL"}`),
		[]byte(`{"version": 1, "query": ""}`),
		[]byte(`{"version": 1, "query": "SELECT 1 FROM DUAL", "number_types": {"0": "utf8"}}`),
		nil,
	} {
		if _, err := cnxn.ReadPartition(ctx, bad); err == nil {
			t.Errorf("ReadPartition accepted %q", bad)
		}
	}
}

// The point of partitioning is reading the slices at the same time, so the
// coverage property has to hold when they are actually concurrent and not
// only when they are read one after another. Under -race this also exercises
// the connections and readers running in parallel, which is new concurrency
// in the driver.
func TestIntegration_PartitionsReadConcurrently(t *testing.T) {
	ctx, db, cnxn := openPartitionDatabase(t, nil)

	const table = "P43_CONC"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, `CREATE TABLE `+table+` (ID NUMBER(10), LABEL VARCHAR2(40), PAD VARCHAR2(200))`)
	t.Cleanup(func() { dropTable(ctx, t, cnxn, table) })
	execUpdate(ctx, t, cnxn, `INSERT INTO `+table+`
		SELECT LEVEL, 'row-' || LEVEL, RPAD('c', 200, 'c') FROM DUAL CONNECT BY LEVEL <= 20000`)
	waitForFlashback(ctx, t, cnxn, table)

	query := "SELECT ID, LABEL FROM " + table
	want, wantSchema := readWholeQuery(ctx, t, cnxn, query)

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(query); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	schema, parts, _, err := stmt.ExecutePartitions(ctx)
	if err != nil {
		t.Fatalf("ExecutePartitions: %v", err)
	}
	if parts.NumPartitions < 2 {
		t.Fatalf("got %d partitions, so nothing would run concurrently", parts.NumPartitions)
	}

	type result struct {
		rows   []string
		schema *arrow.Schema
	}
	results := make([]result, len(parts.PartitionIDs))
	var wg sync.WaitGroup
	for i, id := range parts.PartitionIDs {
		wg.Add(1)
		go func(i int, id []byte) {
			defer wg.Done()
			slice, err := db.Open(ctx)
			if err != nil {
				t.Errorf("partition %d: Open: %v", i, err)
				return
			}
			defer slice.Close()
			reader, err := slice.ReadPartition(ctx, id)
			if err != nil {
				t.Errorf("partition %d: ReadPartition: %v", i, err)
				return
			}
			defer reader.Release()
			results[i] = result{rows: rowsOf(t, reader), schema: reader.Schema()}
		}(i, id)
	}
	wg.Wait()

	var got []string
	for i, r := range results {
		if r.schema != nil && !schema.Equal(r.schema) {
			t.Errorf("partition %d schema disagrees with the planned schema:\n slice %s\n plan  %s",
				i, r.schema, schema)
		}
		got = append(got, r.rows...)
	}
	if !wantSchema.Equal(schema) {
		t.Errorf("partitioned schema differs from the unpartitioned one:\n part %s\n full %s", schema, wantSchema)
	}
	assertSameMultiset(t, "concurrent union", got, want)
}

// The last rung of the ladder is correct but expensive, so its correctness is
// worth asserting even though the better strategies normally win. This checks
// the property in SQL: the hash buckets partition the rows exactly once.
func TestIntegration_HashBucketsPartitionTheTable(t *testing.T) {
	ctx, _, cnxn := openPartitionDatabase(t, nil)

	const table = "P43_HASH"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, `CREATE TABLE `+table+` (ID NUMBER(10))`)
	t.Cleanup(func() { dropTable(ctx, t, cnxn, table) })
	execUpdate(ctx, t, cnxn, `INSERT INTO `+table+` SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 2000`)

	pq, err := parseSingleTableQuery("SELECT ID FROM " + table)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	const buckets = 4
	total := 0
	nonEmpty := 0
	for k := 0; k < buckets; k++ {
		q := pq.sliceQuery("", "", fmt.Sprintf("ORA_HASH(ROWID, %d) = %d", buckets-1, k))
		n := querySingleString(ctx, t, cnxn, "SELECT COUNT(*) FROM ("+q+")")
		var count int
		if _, err := fmt.Sscanf(n, "%d", &count); err != nil {
			t.Fatalf("count %q: %v", n, err)
		}
		total += count
		if count > 0 {
			nonEmpty++
		}
	}

	if total != 2000 {
		t.Errorf("hash buckets cover %d rows, want 2000: the slices do not partition the table", total)
	}
	if nonEmpty < 2 {
		t.Errorf("only %d of %d hash buckets held any rows", nonEmpty, buckets)
	}
}
