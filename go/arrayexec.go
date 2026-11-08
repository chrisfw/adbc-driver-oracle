package oracle

import (
	"context"
	"database/sql"
	"reflect"
)

// maxArrayExec is the most rows one array execute sends. go-ora writes the
// row count of a re-execute (a statement already parsed on the server) as a
// 2-byte field, so 65536 rows become 0: Oracle 26ai rejects the message with
// ORA-03106 ("fatal two-task communication protocol error"), and older
// servers get a wrong count. Any execute of more rows is split into chunks
// of at most this many, each a valid array execute on every server.
const maxArrayExec = 32768

// execer is a prepared statement or a session that runs one statement.
type execer interface {
	ExecContext(ctx context.Context, args ...any) (sql.Result, error)
}

// execArray runs an array execute of nrow rows in chunks of at most
// maxArrayExec. Every argument that is a slice of nrow values is sliced with
// the chunk; any other argument is passed as it is. It returns the rows
// affected across the chunks.
func execArray(ctx context.Context, ex execer, args []any, nrow int) (int64, error) {
	if nrow <= maxArrayExec {
		res, err := ex.ExecContext(ctx, args...)
		if err != nil {
			return 0, err
		}
		return rowsAffected(res, nrow), nil
	}
	vals := make([]reflect.Value, len(args))
	for i, a := range args {
		vals[i] = reflect.ValueOf(a)
	}
	var total int64
	chunk := make([]any, len(args))
	for start := 0; start < nrow; start += maxArrayExec {
		end := min(start+maxArrayExec, nrow)
		for i, v := range vals {
			if v.IsValid() && v.Kind() == reflect.Slice && v.Len() == nrow {
				chunk[i] = v.Slice(start, end).Interface()
			} else {
				chunk[i] = args[i]
			}
		}
		res, err := ex.ExecContext(ctx, chunk...)
		if err != nil {
			return total, err
		}
		total += rowsAffected(res, end-start)
	}
	return total, nil
}

func rowsAffected(res sql.Result, fallback int) int64 {
	if res == nil {
		return int64(fallback)
	}
	if n, err := res.RowsAffected(); err == nil {
		return n
	}
	return int64(fallback)
}

// execerFunc adapts a session's ExecContext(query, args) to execer.
type execerFunc func(ctx context.Context, args ...any) (sql.Result, error)

func (f execerFunc) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	return f(ctx, args...)
}
