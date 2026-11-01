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
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// --- Unit: bind shape ---

// Only string width can force a Clob bind, and it has to be decided from the
// batch before anything is sent. Non-string columns never need one.
func TestBoundBindShape(t *testing.T) {
	ctx := context.Background()
	c, _, _ := newStubConnection(t)
	defer closeStubConnection(t, c)
	// The stub server has no v$parameter, so inlineStringLimit falls back to
	// the STANDARD cap, which is the boundary under test.
	s := &statementImpl{cnxn: c}

	narrow := strings.Repeat("a", maxInlineStringBytes)
	wide := strings.Repeat("b", maxInlineStringBytes+1)

	cases := []struct {
		name   string
		schema *arrow.Schema
		build  func(*array.RecordBuilder)
		want   []bool
	}{
		{
			name: "no string columns needs no probe",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "n", Type: arrow.PrimitiveTypes.Int64},
			}, nil),
			build: func(b *array.RecordBuilder) {
				b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
			},
			want: []bool{false},
		},
		{
			name: "strings at the cap stay varchar",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "s", Type: arrow.BinaryTypes.String},
			}, nil),
			build: func(b *array.RecordBuilder) {
				b.Field(0).(*array.StringBuilder).AppendValues([]string{narrow, "x"}, nil)
			},
			want: []bool{false},
		},
		{
			name: "one value past the cap forces clob for that column only",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "small", Type: arrow.BinaryTypes.String},
				{Name: "big", Type: arrow.BinaryTypes.String},
				{Name: "n", Type: arrow.PrimitiveTypes.Int64},
			}, nil),
			build: func(b *array.RecordBuilder) {
				b.Field(0).(*array.StringBuilder).AppendValues([]string{"x", "y"}, nil)
				b.Field(1).(*array.StringBuilder).AppendValues([]string{"x", wide}, nil)
				b.Field(2).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
			},
			want: []bool{false, true, false},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := array.NewRecordBuilder(memory.DefaultAllocator, tc.schema)
			defer b.Release()
			tc.build(b)
			rec := b.NewRecordBatch()
			defer rec.Release()

			got := s.boundBindShape(ctx, rec)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("column %d: got %v, want %v (full: %v)", i, got[i], tc.want[i], got)
				}
			}
		})
	}
}

// Every column of a bound batch becomes one array bind, so go-ora can execute
// the batch once instead of once per row.
func TestArrayBindArgsProducesOneSlicePerColumn(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	s := &statementImpl{}
	args := s.arrayBindArgs(rec, []bool{false, false})
	if len(args) != 2 {
		t.Fatalf("got %d args, want 2", len(args))
	}
	for i, arg := range args {
		// Each argument must be a slice covering every row, which is what
		// makes this one array execute rather than three.
		v := reflect.ValueOf(arg)
		if v.Kind() != reflect.Slice {
			t.Errorf("arg %d is %T, want a slice", i, arg)
			continue
		}
		if v.Len() != 3 {
			t.Errorf("arg %d (%T) covers %d rows, want 3", i, arg, v.Len())
		}
	}
}

// --- Integration ---

// The plan's requirement: a 10,000-row bound INSERT lands 10,000 rows in a
// single array execute rather than 10,000 round trips.
//
// Counted at the server, not by timing. go-ora exposes no round-trip counter,
// and a wall-clock bound is the kind of test that fails on a slow CI box for
// no reason. Rows affected cannot tell the two apart either: the per-row loop
// this replaced returned 1 per execute and summed to 10,000 as well. What does
// distinguish them is V$SQL.EXECUTIONS, which Oracle increments once per
// execute: one for an array bind, 10,000 for a loop.
func TestIntegration_BoundBatchIsOneArrayExecute(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	const table = "ADBC_T24_BOUND"
	const rows = 10000
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+" (ID NUMBER(10), NAME VARCHAR2(50))")
	defer dropTable(ctx, t, cnxn, table)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	ids := make([]int64, rows)
	names := make([]string, rows)
	for i := range ids {
		ids[i] = int64(i)
		names[i] = "row"
	}
	b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	b.Field(1).(*array.StringBuilder).AppendValues(names, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery("INSERT INTO " + table + " (ID, NAME) VALUES (:1, :2)"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	if err := stmt.Bind(ctx, rec); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	affected, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		t.Fatalf("ExecuteUpdate: %v", err)
	}
	if affected != rows {
		t.Errorf("ExecuteUpdate reported %d rows, want %d", affected, rows)
	}
	if got := countRows(ctx, t, cnxn, table); got != "10000" {
		t.Errorf("row count = %q, want 10000", got)
	}

	// The point of the test: how many times the server was asked to execute.
	execs, err := sqlExecutions(ctx, cnxn, "INSERT INTO "+table+" (ID, NAME)%")
	if err != nil {
		t.Skipf("cannot read V$SQL to count executions (needs SELECT on v_$sql): %v", err)
	}
	if execs != 1 {
		t.Errorf("Oracle executed the statement %d times, want 1: the batch was not "+
			"sent as a single array execute", execs)
	}
}

// sqlExecutions reports how many times Oracle executed the statement matching
// a LIKE pattern, which is the server's own count of round trips for it.
func sqlExecutions(ctx context.Context, cnxn adbc.Connection, pattern string) (int64, error) {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(
		"SELECT NVL(SUM(executions), -1) FROM v$sql WHERE sql_text LIKE '" + pattern + "'"); err != nil {
		return 0, err
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return 0, err
	}
	defer reader.Release()
	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		n, convErr := strconv.ParseInt(strings.TrimSpace(rec.Column(0).ValueStr(0)), 10, 64)
		if convErr != nil {
			return 0, convErr
		}
		if n < 0 {
			return 0, errors.New("statement not found in v$sql")
		}
		return n, nil
	}
	if err := reader.Err(); err != nil {
		return 0, err
	}
	return 0, errors.New("no rows from v$sql")
}

// Values wider than the negotiated varchar limit must go in as CLOBs rather
// than be rejected with ORA-01461. Mixed widths in one batch, so the decision
// has to be per column and made before anything is sent.
func TestIntegration_BoundBatchWithWideStrings(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	const table = "ADBC_T24_WIDE"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+" (ID NUMBER(10), SMALL VARCHAR2(20), BIG CLOB)")
	defer dropTable(ctx, t, cnxn, table)

	wide := strings.Repeat("w", 60000)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "small", Type: arrow.BinaryTypes.String},
		{Name: "big", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"narrow", "also narrow"}, nil)
	b.Field(2).(*array.StringBuilder).AppendValues([]string{"short", wide}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery("INSERT INTO " + table + " (ID, SMALL, BIG) VALUES (:1, :2, :3)"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	if err := stmt.Bind(ctx, rec); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		t.Fatalf("ExecuteUpdate with a 60000-byte value: %v", err)
	}

	if got := querySingleString(ctx, t, cnxn,
		`SELECT DBMS_LOB.GETLENGTH("BIG") FROM `+table+` WHERE "ID" = 2`); got != "60000" {
		t.Errorf("wide value length = %q, want 60000", got)
	}
}

// A prepared statement reused across batches of different widths must not
// switch a parameter's bind type on the live statement, which is what
// desynchronizes go-ora's TTC stream. Narrow first, because the reverse order
// is harmless.
func TestIntegration_BoundBatchRepreparesOnShapeChange(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	const table = "ADBC_T24_RESHAPE"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+" (ID NUMBER(10), PAYLOAD CLOB)")
	defer dropTable(ctx, t, cnxn, table)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "payload", Type: arrow.BinaryTypes.String},
	}, nil)
	mk := func(id int64, payload string) arrow.RecordBatch {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{id}, nil)
		b.Field(1).(*array.StringBuilder).AppendValues([]string{payload}, nil)
		return b.NewRecordBatch()
	}

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery("INSERT INTO " + table + " (ID, PAYLOAD) VALUES (:1, :2)"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	if err := stmt.Prepare(ctx); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	widths := []int{10, 50000, 20, 70000}
	for i, w := range widths {
		rec := mk(int64(i), strings.Repeat("p", w))
		if err := stmt.Bind(ctx, rec); err != nil {
			rec.Release()
			t.Fatalf("Bind %d: %v", i, err)
		}
		_, err := stmt.ExecuteUpdate(ctx)
		rec.Release()
		if err != nil {
			t.Fatalf("ExecuteUpdate %d (width %d): %v", i, w, err)
		}
	}

	for i, w := range widths {
		got := querySingleString(ctx, t, cnxn,
			`SELECT DBMS_LOB.GETLENGTH("PAYLOAD") FROM `+table+` WHERE "ID" = `+strconv.Itoa(i))
		if got != strconv.Itoa(w) {
			t.Errorf("id=%d payload length = %q, want %d", i, got, w)
		}
	}
}

// ADBC specifies one result set per bound row. Returning row 0's answer and
// discarding the rest is plausible-looking wrong data, so a multi-row bound
// query is refused outright.
func TestIntegration_MultiRowBoundQueryIsNotImplemented(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
	mk := func(vals ...int64) arrow.RecordBatch {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()
		b.Field(0).(*array.Int64Builder).AppendValues(vals, nil)
		return b.NewRecordBatch()
	}

	run := func(rec arrow.RecordBatch) error {
		stmt, err := cnxn.NewStatement()
		if err != nil {
			t.Fatalf("NewStatement: %v", err)
		}
		defer stmt.Close()
		if err := stmt.SetSqlQuery("SELECT :1 AS N FROM DUAL"); err != nil {
			t.Fatalf("SetSqlQuery: %v", err)
		}
		if err := stmt.Bind(ctx, rec); err != nil {
			t.Fatalf("Bind: %v", err)
		}
		reader, _, err := stmt.ExecuteQuery(ctx)
		if err == nil {
			reader.Release()
		}
		return err
	}

	// One row is supported and must keep working.
	one := mk(7)
	defer one.Release()
	if err := run(one); err != nil {
		t.Errorf("single bound row: %v", err)
	}

	// More than one is refused, with the status a caller can act on.
	many := mk(1, 2, 3)
	defer many.Release()
	err = run(many)
	if err == nil {
		t.Fatal("multi-row bound query succeeded, want StatusNotImplemented")
	}
	var adbcErr adbc.Error
	if !errors.As(err, &adbcErr) {
		t.Fatalf("got %T, want adbc.Error", err)
	}
	if adbcErr.Code != adbc.StatusNotImplemented {
		t.Errorf("status = %v, want StatusNotImplemented", adbcErr.Code)
	}
	// The message has to say how many rows and what to do instead, or the
	// caller is left guessing.
	if !strings.Contains(adbcErr.Msg, "3") {
		t.Errorf("message does not name the row count: %s", adbcErr.Msg)
	}
}

// GetParameterSchema stays unimplemented: go-ora describes no bind metadata.
// Its Stmt exposes only NumInput, and the parameter info it holds is built
// from what the caller bound, not from the server. Pinned so a future change
// is a decision rather than an accident.
func TestGetParameterSchemaIsNotImplemented(t *testing.T) {
	c, _, _ := newStubConnection(t)
	defer closeStubConnection(t, c)
	s := &statementImpl{cnxn: c}
	s.ErrorHelper = c.ErrorHelper

	_, err := s.GetParameterSchema()
	if err == nil {
		t.Fatal("GetParameterSchema succeeded; go-ora exposes no bind metadata")
	}
	var adbcErr adbc.Error
	if !errors.As(err, &adbcErr) {
		t.Fatalf("got %T, want adbc.Error", err)
	}
	if adbcErr.Code != adbc.StatusNotImplemented {
		t.Errorf("status = %v, want StatusNotImplemented", adbcErr.Code)
	}
}
