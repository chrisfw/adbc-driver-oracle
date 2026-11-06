package mask

// Fake codes (driver's license and similar numbers) for seeding a test
// database: the same shape, new characters. Every letter becomes a keyed
// letter and every digit a keyed digit, from an HMAC stream of the whole
// value, so a code keeps its length, its letter/digit pattern (a state's
// license format still looks like one) and its separators, and the same code
// gives the same fake everywhere (joins on it hold). A fake never equals its
// code.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FakeCodes makes fake codes. It is safe for concurrent use.
type FakeCodes struct{ key []byte }

// NewFakeCodes keys fake codes with key (at least 16 bytes).
func NewFakeCodes(key []byte) (*FakeCodes, error) {
	if len(key) < 16 {
		return nil, errors.New("mask: the fake-code key is shorter than 16 bytes")
	}
	return &FakeCodes{key: derive(key, "xdb-fake/code/v1")}, nil
}

// Fake is v with every ASCII letter and digit replaced by a keyed one of the
// same class and case; everything else is kept.
func (f *FakeCodes) Fake(v string) string {
	if strings.TrimSpace(v) == "" {
		return v
	}
	norm := strings.ToUpper(v)
	for attempt := uint32(0); ; attempt++ {
		out := f.fake(v, norm, attempt)
		if out != v || !hasAlnum(v) {
			return out
		}
	}
}

func (f *FakeCodes) fake(v, norm string, attempt uint32) string {
	var stream []byte
	block := uint32(0)
	next := func() uint64 {
		if len(stream) < 8 {
			m := hmac.New(sha256.New, f.key)
			m.Write([]byte(norm))
			var b [8]byte
			binary.BigEndian.PutUint32(b[:4], attempt)
			binary.BigEndian.PutUint32(b[4:], block)
			m.Write(b[:])
			stream = append(stream, m.Sum(nil)...)
			block++
		}
		x := binary.BigEndian.Uint64(stream[:8])
		stream = stream[8:]
		return x
	}
	b := []byte(v)
	for i, c := range b {
		switch {
		case c >= '0' && c <= '9':
			b[i] = '0' + byte(next()%10)
		case c >= 'A' && c <= 'Z':
			b[i] = 'A' + byte(next()%26)
		case c >= 'a' && c <= 'z':
			b[i] = 'a' + byte(next()%26)
		}
	}
	return string(b)
}

func hasAlnum(v string) bool {
	return strings.IndexFunc(v, func(r rune) bool {
		return (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')
	}) >= 0
}

// FakeCodeArray replaces every code in a string column (string or large
// string). NULL stays NULL; changed counts the values that changed.
func FakeCodeArray(col arrow.Array, f *FakeCodes, alloc memory.Allocator) (arrow.Array, int64, error) {
	if f == nil {
		return nil, 0, errors.New("mask: a fake-code column needs its generator")
	}
	return mapStrings(col, f.Fake, alloc)
}
