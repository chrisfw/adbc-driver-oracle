package mask

// Fake names for seeding a test database: each word of a name becomes a fake
// name word, chosen by a keyed hash of the word itself. The same word gives
// the same fake everywhere (first-name column, sort key, customer name, a
// bill's copy of the name), so derived and copied names stay consistent
// without any join; case, separators and punctuation are kept. Initials
// become a keyed letter; honorifics and suffixes (MR, JR, III, ...) and
// connectives (AND, &, OF, THE) are kept as they are. A fake never equals
// the word it replaces.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FakeNames makes fake names. It is safe for concurrent use.
type FakeNames struct {
	key     []byte
	byLen   map[int][]string // name words by length
	maxLen  int
	allWord []string
}

var keepWords = map[string]bool{
	"JR": true, "SR": true, "II": true, "III": true, "IV": true, "V": true, "MR": true, "MRS": true, "MS": true,
	"DR": true, "AND": true, "OF": true, "THE": true, "ESTATE": true, "C/O": true,
}

// NewFakeNames keys fake names with key (at least 16 bytes; the customer's
// seed key, so names map the same way in every export of the customer).
func NewFakeNames(key []byte) (*FakeNames, error) {
	if len(key) < 16 {
		return nil, errors.New("mask: the fake-name key is shorter than 16 bytes")
	}
	f := &FakeNames{key: derive(key, "xdb-fake/name/v1"), byLen: map[int][]string{}}
	seen := map[string]bool{}
	for _, l := range [][]string{fakeSurnames, fakeFirstNames} {
		for _, w := range l {
			if seen[w] {
				continue
			}
			seen[w] = true
			f.byLen[len(w)] = append(f.byLen[len(w)], w)
			f.allWord = append(f.allWord, w)
			f.maxLen = max(f.maxLen, len(w))
		}
	}
	return f, nil
}

func (f *FakeNames) pick(label, word string) uint64 {
	m := hmac.New(sha256.New, f.key)
	m.Write([]byte(label))
	m.Write([]byte{0})
	m.Write([]byte(word))
	return binary.BigEndian.Uint64(m.Sum(nil))
}

// word is the fake for one upper-case word of n letters: a name of the same
// length when there is one, else the nearest length below, else any.
func (f *FakeNames) word(up string) string {
	n := utf8.RuneCountInString(up)
	if n == 1 {
		r := rune('A' + f.pick("initial", up)%26)
		if string(r) == up {
			r = 'A' + (r-'A'+1)%26
		}
		return string(r)
	}
	var cands []string
	for l := min(n, f.maxLen); l >= 2 && len(cands) == 0; l-- {
		cands = f.byLen[l]
	}
	if len(cands) == 0 {
		cands = f.allWord
	}
	h := f.pick("name", up)
	w := cands[h%uint64(len(cands))]
	if w == up {
		w = cands[(h+1)%uint64(len(cands))]
	}
	return w
}

// Fake is v with every word replaced.
func (f *FakeNames) Fake(v string) string {
	var b strings.Builder
	b.Grow(len(v))
	rs := []rune(v)
	for i := 0; i < len(rs); {
		if !unicode.IsLetter(rs[i]) {
			b.WriteRune(rs[i])
			i++
			continue
		}
		j := i
		for j < len(rs) && unicode.IsLetter(rs[j]) {
			j++
		}
		w := string(rs[i:j])
		up := strings.ToUpper(w)
		if keepWords[up] {
			b.WriteString(w)
		} else {
			b.WriteString(caseLike(w, f.word(up)))
		}
		i = j
	}
	return b.String()
}

// caseLike gives fake the case pattern of orig: lower, Title or UPPER.
func caseLike(orig, fake string) string {
	switch {
	case orig == strings.ToLower(orig):
		return strings.ToLower(fake)
	case orig == strings.ToUpper(orig):
		return fake
	}
	r, size := utf8.DecodeRuneInString(fake)
	return string(r) + strings.ToLower(fake[size:])
}

// FakeNameArray replaces every name in a string column (string or large
// string). NULL stays NULL; changed counts the values that changed.
func FakeNameArray(col arrow.Array, f *FakeNames, alloc memory.Allocator) (arrow.Array, int64, error) {
	if f == nil {
		return nil, 0, errors.New("mask: a fake-name column needs its generator")
	}
	return mapStrings(col, f.Fake, alloc)
}

// mapStrings applies fn to every non-NULL value of a string column, keeping
// its type.
func mapStrings(col arrow.Array, fn func(string) string, alloc memory.Allocator) (arrow.Array, int64, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	type strArr interface {
		arrow.Array
		Value(int) string
	}
	sa, ok := col.(strArr)
	if !ok || (col.DataType().ID() != arrow.STRING && col.DataType().ID() != arrow.LARGE_STRING) {
		return nil, 0, fmt.Errorf("mask: only text can be faked, not %s", col.DataType())
	}
	b := array.NewBuilder(alloc, col.DataType())
	defer b.Release()
	b.Reserve(sa.Len())
	var changed int64
	for i := 0; i < sa.Len(); i++ {
		if sa.IsNull(i) {
			b.AppendNull()
			continue
		}
		v := sa.Value(i)
		out := fn(v)
		if out != v {
			changed++
		}
		switch sb := b.(type) {
		case *array.StringBuilder:
			sb.Append(out)
		case *array.LargeStringBuilder:
			sb.Append(out)
		}
	}
	return b.NewArray(), changed, nil
}
