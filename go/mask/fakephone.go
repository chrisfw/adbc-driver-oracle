package mask

// Fake phone numbers for seeding a test database: the area code is kept and
// the number becomes 555-01xx, the range reserved for fiction in every North
// American area code, so a fake can never reach a real line. The last two
// digits are keyed by the real 7-digit local number, so the same number gives
// the same fake everywhere and in every form (10 digits, 7 digits, a leading
// 1, text with any separators), and split columns stay consistent: a fake
// local number joined to its kept area code is the fake of the whole number.
// A fake never equals the number it replaces.
//
// Only 100 fakes exist per area code: searching a seeded database by phone
// finds many customers. That is the price of numbers that can never ring.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FakePhones makes fake phone numbers. It is safe for concurrent use.
type FakePhones struct {
	key []byte
	// Area, when set, replaces an area code (the demo profile's relocation);
	// codes it does not name are kept.
	Area map[string]string
}

// NewFakePhones keys fake phone numbers with key (at least 16 bytes).
func NewFakePhones(key []byte) (*FakePhones, error) {
	if len(key) < 16 {
		return nil, errors.New("mask: the fake-phone key is shorter than 16 bytes")
	}
	return &FakePhones{key: derive(key, "xdb-fake/phone/v1")}, nil
}

// Local is the fake of a 7-digit local number (exchange and line): 55501xx.
func (f *FakePhones) Local(local string) string {
	m := hmac.New(sha256.New, f.key)
	m.Write([]byte(local))
	xx := binary.BigEndian.Uint64(m.Sum(nil)) % 100
	out := fmt.Sprintf("55501%02d", xx)
	if out == local {
		out = fmt.Sprintf("55501%02d", (xx+1)%100)
	}
	return out
}

func (f *FakePhones) area(a string) string {
	if r, ok := f.Area[a]; ok && len(r) == 3 {
		return r
	}
	return a
}

// Digits fakes a phone number given as its digits: 7 (local), 10 (area code
// and local) or 11 (a leading 1). ok is false for any other shape.
func (f *FakePhones) Digits(d string) (string, bool) {
	for _, r := range d {
		if r < '0' || r > '9' {
			return "", false
		}
	}
	switch {
	case len(d) == 7:
		return f.Local(d), true
	case len(d) == 10:
		return f.area(d[:3]) + f.Local(d[3:]), true
	case len(d) == 11 && d[0] == '1':
		return "1" + f.area(d[1:4]) + f.Local(d[4:]), true
	}
	return "", false
}

// Number fakes a phone number stored as a number.
func (f *FakePhones) Number(v uint64) (uint64, bool) {
	out, ok := f.Digits(strconv.FormatUint(v, 10))
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(out, 10, 64)
	return n, err == nil
}

// phoneExt splits a text phone number from an extension written after it.
var phoneExt = regexp.MustCompile(`(?i)^(.*?)(\s*(?:x|ext\.?|extension|#)\s*\d+\s*)$`)

// Text fakes a phone number written as text: its digits are replaced in
// place, so separators, brackets and spacing stay as they were; an extension
// after it is kept. ok is false when the digits are not a phone number.
func (f *FakePhones) Text(v string) (string, bool) {
	body, ext := v, ""
	if m := phoneExt.FindStringSubmatch(v); m != nil && strings.ContainsAny(m[1], "0123456789") {
		body, ext = m[1], m[2]
	}
	var d strings.Builder
	for _, r := range body {
		switch {
		case r >= '0' && r <= '9':
			d.WriteRune(r)
		case r == '+' || r == '(' || r == ')' || r == '-' || r == '.' || r == ' ' || r == '/' || r == '\t':
		default:
			return "", false // letters or other marks: not a plain number
		}
	}
	fake, ok := f.Digits(d.String())
	if !ok {
		return "", false
	}
	var b strings.Builder
	i := 0
	for _, r := range body {
		if r >= '0' && r <= '9' {
			b.WriteByte(fake[i])
			i++
		} else {
			b.WriteRune(r)
		}
	}
	return b.String() + ext, true
}

// FakePhoneArray fakes a phone column: text, or a whole number (int32, int64
// or a decimal of scale 0). NULL stays NULL, 0 and blank stay as they are; a
// value that is not a phone number becomes 0 (numbers) or NULL (text) and is
// counted unreadable, never passed through.
func FakePhoneArray(in arrow.Array, f *FakePhones, alloc memory.Allocator) (arrow.Array, IdentifierCounts, error) {
	var n IdentifierCounts
	if f == nil {
		return nil, n, errors.New("mask: a fake-phone column needs its generator")
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	if dt, ok := in.DataType().(*arrow.Decimal128Type); ok && dt.Scale != 0 {
		return nil, n, fmt.Errorf("mask: a phone column cannot be a decimal with scale %d", dt.Scale)
	}
	b := array.NewBuilder(alloc, in.DataType())
	defer b.Release()
	b.Reserve(in.Len())
	for i := 0; i < in.Len(); i++ {
		if in.IsNull(i) {
			n.Nulls++
			b.AppendNull()
			continue
		}
		switch a := in.(type) {
		case *array.String, *array.LargeString:
			v := a.(interface{ Value(int) string }).Value(i)
			put := func(s string) {
				switch bb := b.(type) {
				case *array.StringBuilder:
					bb.Append(s)
				case *array.LargeStringBuilder:
					bb.Append(s)
				}
			}
			t := strings.TrimSpace(v)
			if t == "" || strings.Trim(t, "0") == "" {
				n.Zero++
				put(v)
				continue
			}
			if out, ok := f.Text(v); ok {
				n.Masked++
				put(out)
			} else {
				n.Unreadable++
				b.AppendNull()
			}
		case *array.Int64, *array.Int32, *array.Decimal128:
			var v uint64
			neg := false
			switch c := a.(type) {
			case *array.Int64:
				v, neg = uint64(c.Value(i)), c.Value(i) < 0
			case *array.Int32:
				v, neg = uint64(c.Value(i)), c.Value(i) < 0
			case *array.Decimal128:
				x := c.Value(i)
				v, neg = x.LowBits(), x.HighBits() != 0
			}
			out := uint64(0)
			switch {
			case !neg && v == 0:
				n.Zero++
			case neg:
				n.Unreadable++
			default:
				var ok bool
				if out, ok = f.Number(v); ok {
					n.Masked++
				} else {
					out = 0
					n.Unreadable++
				}
			}
			switch bb := b.(type) {
			case *array.Int64Builder:
				bb.Append(int64(out))
			case *array.Int32Builder:
				if out > 1<<31-1 {
					out = 0
				}
				bb.Append(int32(out))
			case *array.Decimal128Builder:
				bb.Append(decimal128.FromU64(out))
			}
		default:
			return nil, n, fmt.Errorf("mask: a phone column cannot be %s", in.DataType())
		}
	}
	return b.NewArray(), n, nil
}

// PhoneType reports whether a column type can hold a fake phone number.
func PhoneType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT64, arrow.INT32, arrow.STRING, arrow.LARGE_STRING:
		return true
	case arrow.DECIMAL128:
		return dt.(*arrow.Decimal128Type).Scale == 0
	}
	return false
}
