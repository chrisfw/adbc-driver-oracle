package mask

import (
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Wrap turns the hook on for rdr: the returned reader yields rdr's batches with
// every column masked per plan. The plan must have one entry per column of
// rdr's schema and pass Validate; otherwise nothing is returned (fail closed).
// key keys the tokens. Wrap takes ownership of rdr.
func Wrap(rdr array.RecordReader, plan Plan, key []byte, alloc memory.Allocator) (array.RecordReader, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	in := rdr.Schema()
	if len(plan) != in.NumFields() {
		rdr.Release()
		return nil, fmt.Errorf("mask: plan has %d columns, the result has %d", len(plan), in.NumFields())
	}
	if err := plan.Validate(); err != nil {
		rdr.Release()
		return nil, err
	}
	needsKey := false
	kernels := make([]kernel, len(plan))
	fields := make([]arrow.Field, len(plan))
	for i, c := range plan {
		f := in.Field(i)
		switch c.Disposition {
		case PassRaw:
			fields[i] = f
		case Mask:
			kernels[i], _ = kernelFor(c) // Validate checked it
			needsKey = needsKey || c.Style == StyleToken
			fields[i] = arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.String, Nullable: true, Metadata: f.Metadata}
		case Withhold:
			kernels[i] = func(string, []byte) string { return WithheldText }
			fields[i] = arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.String, Nullable: true, Metadata: f.Metadata}
		}
	}
	if needsKey && len(key) < 16 {
		rdr.Release()
		return nil, fmt.Errorf("mask: the plan tokenizes but the key is shorter than 16 bytes")
	}
	md := in.Metadata()
	r := &maskedReader{src: rdr, plan: plan, kernels: kernels, key: append([]byte(nil), key...), alloc: alloc,
		schema: arrow.NewSchema(fields, &md)}
	r.refs.Store(1)
	return r, nil
}

type maskedReader struct {
	refs    atomic.Int64
	src     array.RecordReader
	plan    Plan
	kernels []kernel
	key     []byte
	alloc   memory.Allocator
	schema  *arrow.Schema
	cur     arrow.RecordBatch
	err     error
}

func (r *maskedReader) Retain() { r.refs.Add(1) }

func (r *maskedReader) Release() {
	if r.refs.Add(-1) == 0 {
		if r.cur != nil {
			r.cur.Release()
			r.cur = nil
		}
		r.src.Release()
		clear(r.key)
	}
}

func (r *maskedReader) Schema() *arrow.Schema { return r.schema }

func (r *maskedReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.err != nil || !r.src.Next() {
		if r.err == nil {
			r.err = r.src.Err()
		}
		return false
	}
	rec := r.src.RecordBatch()
	cols := make([]arrow.Array, len(r.plan))
	for i, c := range r.plan {
		col := rec.Column(i)
		if c.Disposition == PassRaw {
			col.Retain()
			cols[i] = col
			continue
		}
		cols[i] = r.apply(r.kernels[i], col)
	}
	r.cur = array.NewRecordBatch(r.schema, cols, rec.NumRows())
	for _, c := range cols {
		c.Release()
	}
	return true
}

// apply runs one kernel over a whole column. NULL stays NULL.
func (r *maskedReader) apply(k kernel, col arrow.Array) arrow.Array {
	b := array.NewStringBuilder(r.alloc)
	defer b.Release()
	b.Reserve(col.Len())
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(k(col.ValueStr(i), r.key))
	}
	return b.NewArray()
}

func (r *maskedReader) RecordBatch() arrow.RecordBatch { return r.cur }

// Deprecated: Use RecordBatch instead.
func (r *maskedReader) Record() arrow.RecordBatch { return r.cur }

func (r *maskedReader) Err() error { return r.err }
