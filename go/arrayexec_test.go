package oracle

import (
	"context"
	"database/sql"
	"github.com/apache/arrow-go/v18/arrow"
	"testing"
)

type recExec struct {
	calls [][]any
}

type rowsRes int64

func (r rowsRes) LastInsertId() (int64, error) { return 0, nil }
func (r rowsRes) RowsAffected() (int64, error) { return int64(r), nil }

func (r *recExec) ExecContext(_ context.Context, args ...any) (sql.Result, error) {
	cp := append([]any(nil), args...)
	r.calls = append(r.calls, cp)
	n := 0
	if s, ok := args[0].([]int64); ok {
		n = len(s)
	}
	return rowsRes(n), nil
}

func TestExecArrayChunks(t *testing.T) {
	const nrow = 2*maxArrayExec + 5
	ints := make([]int64, nrow)
	strs := make([]string, nrow)
	for i := range ints {
		ints[i] = int64(i)
		strs[i] = "x"
	}
	scalar := "same"
	ex := &recExec{}
	got, err := execArray(context.Background(), ex, []any{ints, strs, scalar}, nrow)
	if err != nil || got != nrow {
		t.Fatalf("rows %d err %v", got, err)
	}
	if len(ex.calls) != 3 {
		t.Fatalf("%d executes", len(ex.calls))
	}
	next := int64(0)
	for i, c := range ex.calls {
		a, b := c[0].([]int64), c[1].([]string)
		if len(a) > maxArrayExec || len(a) != len(b) || c[2] != scalar {
			t.Fatalf("call %d: %d/%d rows", i, len(a), len(b))
		}
		if a[0] != next {
			t.Fatalf("call %d starts at %d, want %d", i, a[0], next)
		}
		next += int64(len(a))
	}
	// at or under the limit: one execute, arguments untouched
	ex = &recExec{}
	if _, err := execArray(context.Background(), ex, []any{ints[:maxArrayExec]}, maxArrayExec); err != nil || len(ex.calls) != 1 {
		t.Fatalf("small: %d calls, %v", len(ex.calls), err)
	}
}

func TestLobBind(t *testing.T) {
	for _, c := range []struct {
		dt   arrow.DataType
		ddl  string
		want bool
	}{
		{arrow.BinaryTypes.String, "CLOB", true}, {arrow.BinaryTypes.String, "NCLOB", true},
		{arrow.BinaryTypes.String, "VARCHAR2", false}, {arrow.BinaryTypes.String, "BLOB", false},
		{arrow.BinaryTypes.Binary, "BLOB", true}, {arrow.BinaryTypes.Binary, "RAW", false},
		{arrow.BinaryTypes.Binary, "CLOB", false}, {arrow.PrimitiveTypes.Int64, "BLOB", false},
	} {
		if got := lobBind(c.dt, c.ddl); got != c.want {
			t.Errorf("%s into %s: %v, want %v", c.dt, c.ddl, got, c.want)
		}
	}
}
