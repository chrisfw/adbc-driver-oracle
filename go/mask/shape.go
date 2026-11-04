package mask

// Structure-preserving masking of numbered identifiers, for seeding a test
// database from a customer's data.
//
// A token (kernels.go) hides a value but not its shape: an account number
// becomes 16 hex characters, which no enQuesta program can load. Seeding needs
// the masked copy to stay a working database, so an identifier is replaced by
// another value of the same form:
//
//   - the result is still a number of the same width, and a value keeps its
//     unmasked low digits where they carry meaning (an account's commodity and
//     resident code, a premise number's trailing 0000);
//   - the mapping is a keyed permutation, so distinct values stay distinct and
//     equal values stay equal in every table: joins hold;
//   - values stay away from the ranges the application will issue next. Each
//     next-number counter (a frontier) marks where new numbers start; the run of
//     unused values from there up to the next value in use is a free zone, and
//     no masked value lands in one. Copied as they are, the counters stay valid
//     and new test records never collide with masked ones.
//
// Between free zones the values are permuted within their segment, so magnitudes
// stay realistic and nothing crosses a counter. The unused values below the
// lowest one in use are a free zone as well. With a block size (enQuesta's
// route-encoded account numbers, whose leading digits are the route) segments
// also stop at every block boundary, so the leading digits are kept.
//
// Large segments use FF1 (NIST SP 800-38G) over decimal digits with cycle
// walking; segments of at most tableMax values, below FF1's recommended domain
// size, are shuffled by a keyed table (each position ordered by an HMAC of it).
// There is no inverse here: the export path never unmasks.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"sync"

	"github.com/capitalone/fpe/ff1"
)

// tableMax is the largest segment shuffled by a keyed table; larger ones use FF1.
const tableMax = 1 << 20

// IdentifierSpec describes one family of numbered identifiers.
type IdentifierSpec struct {
	// Family names the identifier ("account", "cis", "premise"); with the
	// caller's scope it makes the permutation's tweak, so each family (and each
	// customer) gets an independent mapping.
	Family string
	// Unit is the size of the kept low part: 1000 keeps an account's last three
	// digits, 10000 a premise number's last four, 1 keeps nothing.
	Unit uint64
	// Width is the number of digits above the kept part (9 for an account,
	// 7 for a premise, 8 for a CIS number). Values at or above 10^Width units
	// are outside the family and pass unchanged.
	Width int
	// Block, when set, keeps each value within its block of Block units
	// (10000 keeps a route-encoded account's five route digits).
	Block uint64
	// Frontiers are the family's next-number counters, as stored (full values,
	// low part included). Each one starts a free zone.
	Frontiers []uint64
}

// Identifier masks one identifier family. It is safe for concurrent use.
type Identifier struct {
	spec  IdentifierSpec
	limit uint64      // 10^Width
	free  [][2]uint64 // free zones [start, end) in units, sorted and disjoint
	tweak []byte
	ffKey []byte // AES key for FF1, derived from the caller's key
	tbKey []byte // HMAC key for table shuffles

	mu    sync.Mutex
	perms map[[2]uint64]perm
	memo  map[uint64]uint64
}

// Outcome says what Mask did with one value.
type Outcome int

const (
	Masked  Outcome = iota // replaced by its masked value
	Zero                   // 0 (or a zero high part): kept, it means "none"
	Outside                // at or above 10^Width units: kept, and worth reporting
	InFree                 // inside a free zone, so not a value in use: kept, and worth reporting
)

// NewIdentifier builds the mask for one family. key is the customer's seed key
// (at least 16 bytes); scope separates customers ("amo"). occupied lists every
// value of the family found in the data (full values, any order, duplicates
// allowed): the free zones are computed from it, so it must cover every column
// that will be masked, or a value could be taken for free space.
func NewIdentifier(key []byte, scope string, spec IdentifierSpec, occupied []uint64) (*Identifier, error) {
	if len(key) < 16 {
		return nil, fmt.Errorf("mask: the seed key is shorter than 16 bytes")
	}
	if spec.Unit == 0 {
		spec.Unit = 1
	}
	if spec.Width < 2 || spec.Width > 18 {
		return nil, fmt.Errorf("mask: %s: width %d is out of range", spec.Family, spec.Width)
	}
	limit := pow10(spec.Width)
	if spec.Block != 0 && limit%spec.Block != 0 {
		return nil, fmt.Errorf("mask: %s: block %d does not divide 10^%d", spec.Family, spec.Block, spec.Width)
	}
	units := make([]uint64, 0, len(occupied))
	for _, v := range occupied {
		if u := v / spec.Unit; u < limit {
			units = append(units, u)
		}
	}
	slices.Sort(units)
	units = slices.Compact(units)

	id := &Identifier{
		spec:  spec,
		limit: limit,
		free:  freeZones(units, spec.Frontiers, spec.Unit, spec.Block, limit),
		tweak: []byte(scope + "|" + spec.Family),
		ffKey: derive(key, "xdb-seed/ff1/v1"),
		tbKey: derive(key, "xdb-seed/table/v1"),
		perms: map[[2]uint64]perm{},
		memo:  map[uint64]uint64{},
	}
	return id, nil
}

// FreeZones reports the free zones, in units, for the run report.
func (id *Identifier) FreeZones() [][2]uint64 { return slices.Clone(id.free) }

// Mask returns v's masked value. Only Masked changes the value.
func (id *Identifier) Mask(v uint64) (uint64, Outcome) {
	hi, lo := v/id.spec.Unit, v%id.spec.Unit
	switch {
	case hi == 0:
		return v, Zero
	case hi >= id.limit:
		return v, Outside
	}
	id.mu.Lock()
	defer id.mu.Unlock()
	if m, ok := id.memo[hi]; ok {
		return m*id.spec.Unit + lo, Masked
	}
	a, b, ok := id.segment(hi)
	if !ok {
		return v, InFree
	}
	p, ok := id.perms[[2]uint64{a, b}]
	if !ok {
		p = id.newPerm(a, b)
		id.perms[[2]uint64{a, b}] = p
	}
	m := a + p.apply(hi-a)
	id.memo[hi] = m
	return m*id.spec.Unit + lo, Masked
}

// segment returns the segment [a, b) holding unit u, or false when u lies in a
// free zone. Unit 0 is never part of a segment (it maps to itself).
func (id *Identifier) segment(u uint64) (a, b uint64, ok bool) {
	// The first free zone that ends after u.
	i, _ := slices.BinarySearchFunc(id.free, u, func(z [2]uint64, u uint64) int {
		if z[1] <= u {
			return -1
		}
		return 1
	})
	a, b = 1, id.limit
	if i > 0 {
		a = id.free[i-1][1]
	}
	if i < len(id.free) {
		if id.free[i][0] <= u {
			return 0, 0, false
		}
		b = id.free[i][0]
	}
	if blk := id.spec.Block; blk != 0 {
		a = max(a, u/blk*blk)
		b = min(b, (u/blk+1)*blk)
		if a == 0 {
			a = 1
		}
	}
	return a, b, true
}

// freeZones turns frontiers into free zones: from the first value at or above
// the frontier that is not in use, up to the next value in use (or the limit).
// A counter may hold the next number to issue or the last one issued; starting
// at the first unused value covers both. The unused values below the lowest
// one in use (in each block, with blocks) are a free zone too, so masked values
// keep the data's magnitudes rather than reaching down to 1.
func freeZones(units, frontiers []uint64, unit, block, limit uint64) [][2]uint64 {
	var zones [][2]uint64
	for i, u := range units {
		start := uint64(1)
		if block != 0 {
			start = max(start, u/block*block)
		}
		if i > 0 && units[i-1] >= start {
			continue // not the lowest in its block
		}
		if u > start {
			zones = append(zones, [2]uint64{start, u})
		}
	}
	for _, f := range frontiers {
		s := f / unit // a counter with a low part issues from its own unit first
		if s == 0 {
			s = 1
		}
		i, _ := slices.BinarySearch(units, s)
		for i < len(units) && units[i] == s {
			s++
			i++
		}
		if s >= limit {
			continue
		}
		e := limit
		if i < len(units) {
			e = units[i]
		}
		zones = append(zones, [2]uint64{s, e})
	}
	slices.SortFunc(zones, func(x, y [2]uint64) int {
		switch {
		case x[0] < y[0]:
			return -1
		case x[0] > y[0]:
			return 1
		}
		return 0
	})
	var out [][2]uint64
	for _, z := range zones {
		if n := len(out); n > 0 && z[0] <= out[n-1][1] {
			out[n-1][1] = max(out[n-1][1], z[1])
			continue
		}
		out = append(out, z)
	}
	return out
}

// perm is a bijection of [0, n).
type perm interface{ apply(x uint64) uint64 }

func (id *Identifier) newPerm(a, b uint64) perm {
	n := b - a
	tw := binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(append([]byte(nil), id.tweak...), a), b)
	if n <= tableMax {
		return newTablePerm(id.tbKey, tw, n)
	}
	return newFF1Perm(id.ffKey, tw, n)
}

// tablePerm shuffles [0, n) by ordering each position by a keyed HMAC of it.
type tablePerm []uint32

func newTablePerm(key, tweak []byte, n uint64) tablePerm {
	type kv struct {
		k uint64
		i uint32
	}
	ks := make([]kv, n)
	m := hmac.New(sha256.New, key)
	var buf [8]byte
	for i := range ks {
		m.Reset()
		m.Write(tweak)
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		m.Write(buf[:])
		ks[i] = kv{binary.BigEndian.Uint64(m.Sum(nil)), uint32(i)}
	}
	slices.SortFunc(ks, func(x, y kv) int {
		switch {
		case x.k < y.k:
			return -1
		case x.k > y.k:
			return 1
		}
		return int(x.i) - int(y.i)
	})
	t := make(tablePerm, n)
	for pos, e := range ks {
		t[e.i] = uint32(pos)
	}
	return t
}

func (t tablePerm) apply(x uint64) uint64 { return uint64(t[x]) }

// ff1Perm permutes [0, n) with FF1 over n-1's digit count, walking the cycle
// until the result is below n (on average fewer than ten steps).
type ff1Perm struct {
	c      ff1.Cipher
	n      uint64
	digits int
}

func newFF1Perm(key, tweak []byte, n uint64) *ff1Perm {
	c, err := ff1.NewCipher(10, len(tweak), key, tweak)
	if err != nil {
		// The key is 32 bytes and radix 10 is valid: this cannot fail.
		panic(fmt.Sprintf("mask: ff1: %v", err))
	}
	return &ff1Perm{c: c, n: n, digits: len(strconv.FormatUint(n-1, 10))}
}

func (p *ff1Perm) apply(x uint64) uint64 {
	for {
		s := strconv.FormatUint(x, 10)
		for len(s) < p.digits {
			s = "0" + s
		}
		out, err := p.c.Encrypt(s)
		if err != nil {
			panic(fmt.Sprintf("mask: ff1: %v", err))
		}
		x, _ = strconv.ParseUint(out, 10, 64)
		if x < p.n {
			return x
		}
	}
}

func derive(key []byte, label string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(label))
	return m.Sum(nil)
}

func pow10(n int) uint64 {
	p := uint64(1)
	for range n {
		p *= 10
	}
	return p
}
