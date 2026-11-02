package oracle

import (
	"context"
	"database/sql"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestNewRowsReaderRejectsUnknownMapping(t *testing.T) {
	pool, _ := newStubPool(t)
	rows, err := pool.QueryContext(context.Background(), "SELECT x FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRowsReader(context.Background(), nil, rows, RowsReaderOptions{NumberMapping: "bogus"}); err == nil {
		t.Fatal("an unknown number mapping was accepted")
	}
	if rows.Next() {
		t.Error("rows were left open after the refusal")
	}
}

// readAllStr returns the schema and every value (as ValueStr) of a reader.
func readAllStr(t *testing.T, rdr array.RecordReader) (*arrow.Schema, [][]string) {
	t.Helper()
	defer rdr.Release()
	var out [][]string
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			row := make([]string, rec.NumCols())
			for c := range row {
				col := rec.Column(c)
				if col.IsNull(r) {
					row[c] = "<null>"
				} else {
					row[c] = col.ValueStr(r)
				}
			}
			out = append(out, row)
		}
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	return rdr.Schema(), out
}

// A reader over caller-held rows types and converts columns exactly as
// ExecuteQuery does for the same query and options.
func TestNewRowsReaderMatchesExecuteQuery(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()
	const q = `SELECT CAST(123456789012 AS NUMBER(12,0)) n12, CAST(-42.5 AS NUMBER(9,2)) amt,
		CAST(NULL AS NUMBER(5,0)) nnull, 7 nfree, 'abc' txt, CAST(NULL AS VARCHAR2(10)) snull,
		DATE '2026-11-02' d, TIMESTAMP '2026-11-02 10:11:12.123456' ts, CAST(1.5 AS BINARY_FLOAT) bf
		FROM dual CONNECT BY LEVEL <= 3`

	for _, mapping := range []string{"", NumberMappingString, NumberMappingFloat} {
		drv := NewDriver(memory.DefaultAllocator)
		opts := map[string]string{OptionDSN: dsn}
		if mapping != "" {
			opts[OptionNumberMapping] = mapping
		}
		db, err := drv.NewDatabase(opts)
		if err != nil {
			t.Fatal(err)
		}
		cn, err := db.Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		st, err := cn.NewStatement()
		if err != nil {
			t.Fatal(err)
		}
		if err := st.SetSqlQuery(q); err != nil {
			t.Fatal(err)
		}
		rdr, _, err := st.ExecuteQuery(ctx)
		if err != nil {
			t.Fatal(err)
		}
		wantSchema, want := readAllStr(t, rdr)
		st.Close()
		cn.Close()
		db.Close()

		pool, err := sql.Open("oracle", dsn)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := pool.QueryContext(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		got, err := NewRowsReader(ctx, nil, rows, RowsReaderOptions{NumberMapping: mapping, BatchRowLimit: 2})
		if err != nil {
			t.Fatal(err)
		}
		gotSchema, gotRows := readAllStr(t, got)
		pool.Close()

		if !gotSchema.Equal(wantSchema) {
			t.Errorf("mapping %q: schema\n got %s\nwant %s", mapping, gotSchema, wantSchema)
		}
		if len(gotRows) != len(want) {
			t.Fatalf("mapping %q: %d rows, want %d", mapping, len(gotRows), len(want))
		}
		for i := range want {
			for c := range want[i] {
				if gotRows[i][c] != want[i][c] {
					t.Errorf("mapping %q row %d col %d: %q, want %q", mapping, i, c, gotRows[i][c], want[i][c])
				}
			}
		}
	}
}
