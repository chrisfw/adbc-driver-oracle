package oracle

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestIntegration_IngestTwoBatchesOf65536 is the Oracle 26ai ORA-03106 case:
// the second array execute of a prepared INSERT with 65536 rows sent a row
// count of 0 (go-ora writes a re-execute's count in 2 bytes). Every array
// execute is now at most maxArrayExec rows.
func TestIntegration_IngestTwoBatchesOf65536(t *testing.T) {
	cnxn, query := openTestConnection(t)
	table := "ADBC_TEST_TWO_65536"
	execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE")
	t.Cleanup(func() { execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE") })
	if err := execTestSQL(t, cnxn, "CREATE TABLE "+table+" (ID NUMBER(12), FLAG VARCHAR2(1))"); err != nil {
		t.Fatal(err)
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ID", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "FLAG", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	mk := func(base int64) arrow.RecordBatch {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()
		for i := int64(0); i < 65536; i++ {
			b.Field(0).(*array.Int64Builder).Append(base + i)
			b.Field(1).(*array.StringBuilder).Append("Y")
		}
		return b.NewRecordBatch()
	}
	recs := []arrow.RecordBatch{mk(0), mk(65536)}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()
	affected, err := ingestStream(t, cnxn, table, adbc.OptionValueIngestModeAppend, nil, schema, recs)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if affected != 131072 {
		t.Errorf("affected = %d, want 131072", affected)
	}
	res := query(`SELECT COUNT(*), COUNT(DISTINCT ID), MAX(ID) FROM ` + table)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if got := strings.Join(res.rows[0], " "); got != "131072 131072 131071" {
		t.Errorf("count, distinct, max = %s", got)
	}
}

// TestIntegration_IngestBlobAndClob is the ORA-01461 case: a table with a
// BLOB and a CLOB whose values exceed the RAW/VARCHAR2 limits. Binary values
// for a BLOB column now bind as go_ora.Blob.
func TestIntegration_IngestBlobAndClob(t *testing.T) {
	cnxn, query := openTestConnection(t)
	table := "ADBC_TEST_BLOB_CLOB"
	execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE")
	t.Cleanup(func() { execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE") })
	if err := execTestSQL(t, cnxn, "CREATE TABLE "+table+" (ID NUMBER(12), NAME VARCHAR2(10), BIN BLOB, TXT CLOB)"); err != nil {
		t.Fatal(err)
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ID", Type: arrow.PrimitiveTypes.Int64},
		{Name: "NAME", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "BIN", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "TXT", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	sizes := []int{0, 10, 40000, 300000, 5}
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	for i, n := range sizes {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append("n" + strconv.Itoa(i))
		if n == 0 {
			b.Field(2).AppendNull()
			b.Field(3).AppendNull()
			continue
		}
		b.Field(2).(*array.BinaryBuilder).Append(bytes.Repeat([]byte{byte(i + 1)}, n))
		b.Field(3).(*array.StringBuilder).Append(strings.Repeat("t", n))
	}
	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()
	if _, err := ingestStream(t, cnxn, table, adbc.OptionValueIngestModeAppend, nil, schema, []arrow.RecordBatch{rec}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	for i, n := range sizes {
		res := query(`SELECT NVL(DBMS_LOB.GETLENGTH(BIN), 0), NVL(DBMS_LOB.GETLENGTH(TXT), 0) FROM ` + table + ` WHERE ID = ` + strconv.Itoa(i))
		if res.err != nil {
			t.Fatal(res.err)
		}
		if got, want := strings.Join(res.rows[0], " "), strconv.Itoa(n)+" "+strconv.Itoa(n); got != want {
			t.Errorf("row %d: lengths %s, want %s", i, got, want)
		}
	}
}
