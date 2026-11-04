package mask

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// IdentifierCounts tallies what MaskIdentifierArray did, for the run report.
type IdentifierCounts struct {
	Masked, Nulls, Zero, Outside, InFree, Unreadable int64
}

func (c *IdentifierCounts) Add(o IdentifierCounts) {
	c.Masked += o.Masked
	c.Nulls += o.Nulls
	c.Zero += o.Zero
	c.Outside += o.Outside
	c.InFree += o.InFree
	c.Unreadable += o.Unreadable
}

func (c *IdentifierCounts) note(o Outcome) {
	switch o {
	case Masked:
		c.Masked++
	case Zero:
		c.Zero++
	case Outside:
		c.Outside++
	case InFree:
		c.InFree++
	}
}

// IdentifierValues calls fn with every value of an identifier column that
// MaskIdentifierArray would read (non-NULL, non-negative whole numbers), to
// collect the values in use before masking.
func IdentifierValues(col arrow.Array, fn func(uint64)) error {
	return eachIdentifier(col, func(_ int, v uint64, ok bool) {
		if ok {
			fn(v)
		}
	})
}

// MaskIdentifierArray masks an identifier column and keeps its type: integer,
// decimal with scale 0, or a string of digits. NULL stays NULL; a value that is
// not a non-negative whole number (a negative, a fraction, a non-digit string)
// is kept and counted as unreadable.
func MaskIdentifierArray(col arrow.Array, id *Identifier, alloc memory.Allocator) (arrow.Array, IdentifierCounts, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	var n IdentifierCounts
	b := array.NewBuilder(alloc, col.DataType())
	defer b.Release()
	b.Reserve(col.Len())
	err := eachIdentifier(col, func(i int, v uint64, ok bool) {
		if col.IsNull(i) {
			n.Nulls++
			b.AppendNull()
			return
		}
		if !ok {
			n.Unreadable++
			appendSame(b, col, i)
			return
		}
		m, o := id.Mask(v)
		n.note(o)
		appendValue(b, col, i, m)
	})
	if err != nil {
		return nil, n, err
	}
	return b.NewArray(), n, nil
}

// eachIdentifier reads col's values as whole numbers: fn gets ok=false for a
// NULL or a value that is not a non-negative whole number.
func eachIdentifier(col arrow.Array, fn func(i int, v uint64, ok bool)) error {
	switch a := col.(type) {
	case *array.Int64:
		for i := range a.Len() {
			x := a.Value(i)
			fn(i, uint64(x), !a.IsNull(i) && x >= 0)
		}
	case *array.Int32:
		for i := range a.Len() {
			x := a.Value(i)
			fn(i, uint64(x), !a.IsNull(i) && x >= 0)
		}
	case *array.Uint64:
		for i := range a.Len() {
			fn(i, a.Value(i), !a.IsNull(i))
		}
	case *array.Decimal128:
		if s := a.DataType().(*arrow.Decimal128Type).Scale; s != 0 {
			return fmt.Errorf("mask: an identifier column must be a whole number, not decimal scale %d", s)
		}
		for i := range a.Len() {
			x := a.Value(i)
			fn(i, x.LowBits(), !a.IsNull(i) && x.HighBits() == 0)
		}
	case *array.String:
		for i := range a.Len() {
			v, ok := digitsValue(a.Value(i))
			fn(i, v, !a.IsNull(i) && ok)
		}
	case *array.LargeString:
		for i := range a.Len() {
			v, ok := digitsValue(a.Value(i))
			fn(i, v, !a.IsNull(i) && ok)
		}
	default:
		return fmt.Errorf("mask: an identifier column cannot be %s", col.DataType())
	}
	return nil
}

func digitsValue(s string) (uint64, bool) {
	s = strings.TrimSpace(s)
	if s == "" || strings.Trim(s, "0123456789") != "" {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	return v, err == nil
}

// appendValue appends m in col's type; a string keeps its original width
// (leading zeros) when m fits in it.
func appendValue(b array.Builder, col arrow.Array, i int, m uint64) {
	switch bb := b.(type) {
	case *array.Int64Builder:
		bb.Append(int64(m))
	case *array.Int32Builder:
		bb.Append(int32(m))
	case *array.Uint64Builder:
		bb.Append(m)
	case *array.Decimal128Builder:
		bb.Append(decimal128.FromU64(m))
	case *array.StringBuilder:
		bb.Append(padLike(col.(*array.String).Value(i), m))
	case *array.LargeStringBuilder:
		bb.Append(padLike(col.(*array.LargeString).Value(i), m))
	}
}

func appendSame(b array.Builder, col arrow.Array, i int) {
	switch bb := b.(type) {
	case *array.Int64Builder:
		bb.Append(col.(*array.Int64).Value(i))
	case *array.Int32Builder:
		bb.Append(col.(*array.Int32).Value(i))
	case *array.Uint64Builder:
		bb.Append(col.(*array.Uint64).Value(i))
	case *array.Decimal128Builder:
		bb.Append(col.(*array.Decimal128).Value(i))
	case *array.StringBuilder:
		bb.Append(col.(*array.String).Value(i))
	case *array.LargeStringBuilder:
		bb.Append(col.(*array.LargeString).Value(i))
	}
}

func padLike(orig string, m uint64) string {
	s := strconv.FormatUint(m, 10)
	w := len(strings.TrimSpace(orig))
	for len(s) < w {
		s = "0" + s
	}
	return s
}
