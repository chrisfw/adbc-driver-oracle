package mask

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func newID(t *testing.T, scope string, spec IdentifierSpec, occupied []uint64) *Identifier {
	t.Helper()
	id, err := NewIdentifier(testKey, scope, spec, occupied)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func inFree(id *Identifier, u uint64) bool {
	for _, z := range id.FreeZones() {
		if u >= z[0] && u < z[1] {
			return true
		}
	}
	return false
}

// Over a whole small domain: every unit outside the free zones maps to a unit
// outside them, one to one, and nothing crosses a counter.
func TestIdentifierIsABijection(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	var occupied []uint64
	for u := uint64(1); u < 10000; u++ {
		// Two issued ranges, [1, 3000) and [5000, 8000), mostly in use.
		if (u < 3000 || (u >= 5000 && u < 8000)) && r.IntN(10) < 8 {
			occupied = append(occupied, u)
		}
	}
	spec := IdentifierSpec{Family: "cis", Unit: 1, Width: 4, Frontiers: []uint64{3000, 8000}}
	id := newID(t, "amo", spec, occupied)
	if len(id.FreeZones()) != 2 {
		t.Fatalf("free zones = %v, want two", id.FreeZones())
	}
	seen := map[uint64]uint64{}
	freeSeen := map[uint64]uint64{}
	for u := uint64(1); u < 10000; u++ {
		m, o := id.Mask(u)
		if z, ok := zoneOf(id, u); ok {
			// An orphan value in a free zone is permuted within that zone.
			if o != InFree || m < z[0] || m >= z[1] {
				t.Fatalf("free unit %d: got %d, %v; want inside %v", u, m, o, z)
			}
			if prev, dup := freeSeen[m]; dup {
				t.Fatalf("free units %d and %d both mask to %d", prev, u, m)
			}
			freeSeen[m] = u
			continue
		}
		if o != Masked {
			t.Fatalf("unit %d: outcome %v", u, o)
		}
		if inFree(id, m) {
			t.Fatalf("unit %d masked into a free zone (%d)", u, m)
		}
		if prev, dup := seen[m]; dup {
			t.Fatalf("units %d and %d both mask to %d", prev, u, m)
		}
		seen[m] = u
		// Below a counter stays below it; the first zone starts at or after 3000.
		if u < 3000 && m >= id.FreeZones()[0][0] {
			t.Fatalf("unit %d below the first counter masked above it (%d)", u, m)
		}
	}
	moved := 0
	for m, u := range seen {
		if m != u {
			moved++
		}
	}
	if moved < len(seen)*9/10 {
		t.Fatalf("only %d of %d values moved", moved, len(seen))
	}
}

// A segment larger than tableMax goes through FF1: still one to one, still
// below its counter.
func TestIdentifierLargeSegmentUsesFF1(t *testing.T) {
	spec := IdentifierSpec{Family: "account", Unit: 1000, Width: 9, Frontiers: []uint64{400_000_000_000}}
	id := newID(t, "amo", spec, []uint64{1000, 399_999_999_000})
	if _, ok := id.newPerm(1, 400_000_000).(*ff1Perm); !ok {
		t.Fatal("a 4e8-unit segment should use FF1")
	}
	r := rand.New(rand.NewPCG(3, 4))
	seen := map[uint64]bool{}
	for range 3000 {
		u := 1 + r.Uint64N(399_999_999)
		m, o := id.Mask(u * 1000)
		if o != Masked || m%1000 != 0 || m/1000 >= 400_000_000 || m/1000 == 0 {
			t.Fatalf("unit %d: got %d, %v", u, m, o)
		}
		seen[m] = true
	}
	if len(seen) < 2990 { // the samples may repeat a little; masked values may not collide more
		t.Fatalf("only %d distinct masked values from 3000 samples", len(seen))
	}
}

func TestIdentifierIsKeyedAndScoped(t *testing.T) {
	spec := IdentifierSpec{Family: "cis", Unit: 1, Width: 8, Frontiers: []uint64{500000}}
	a := newID(t, "amo", spec, nil)
	b := newID(t, "amo", spec, nil)
	c := newID(t, "met", spec, nil)
	other, err := NewIdentifier([]byte("another key, another customer!!!"), "amo", spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	same, diffScope, diffKey := 0, 0, 0
	for v := uint64(1); v <= 200; v++ {
		x, _ := a.Mask(v)
		y, _ := b.Mask(v)
		z, _ := c.Mask(v)
		w, _ := other.Mask(v)
		if x == y {
			same++
		}
		if x != z {
			diffScope++
		}
		if x != w {
			diffKey++
		}
	}
	if same != 200 || diffScope < 190 || diffKey < 190 {
		t.Fatalf("same=%d diffScope=%d diffKey=%d", same, diffScope, diffKey)
	}
	if _, err := NewIdentifier([]byte("short"), "amo", spec, nil); err == nil {
		t.Fatal("a short key should be refused")
	}
}

// An account keeps its commodity and resident code, and every account at one
// location keeps one (masked) location base.
func TestAccountKeepsLowDigitsAndGrouping(t *testing.T) {
	occupied := []uint64{123456789_0_01, 123456789_2_01, 123456789_2_02, 555_0_01}
	spec := IdentifierSpec{Family: "account", Unit: 1000, Width: 9, Frontiers: []uint64{900_000_000_000}}
	id := newID(t, "amo", spec, occupied)
	var bases []uint64
	for _, v := range occupied[:3] {
		m, o := id.Mask(v)
		if o != Masked || m%1000 != v%1000 {
			t.Fatalf("%d -> %d (%v): low digits changed", v, m, o)
		}
		bases = append(bases, m/1000)
	}
	if bases[0] != bases[1] || bases[1] != bases[2] || bases[0] == 123456789 {
		t.Fatalf("location bases %v: want one masked base", bases)
	}
	if m, o := id.Mask(0); m != 0 || o != Zero {
		t.Fatalf("0 -> %d (%v)", m, o)
	}
	if m, o := id.Mask(5); m != 5 || o != Zero {
		t.Fatalf("a zero base -> %d (%v)", m, o)
	}
	if m, o := id.Mask(1_000_000_000_000); m != 1_000_000_000_000 || o != Outside {
		t.Fatalf("13 digits -> %d (%v)", m, o)
	}
}

// Route-encoded accounts keep their route: the first five digits of the
// twelve, i.e. the base's units of 10000 folios.
func TestRouteBlocksKeepTheRoute(t *testing.T) {
	var occupied, frontiers []uint64
	for _, route := range []uint64{12, 345, 9999} {
		for folio := uint64(1); folio < 300; folio++ {
			occupied = append(occupied, (route*10000+folio)*1000)
		}
		frontiers = append(frontiers, (route*10000+300)*1000)
	}
	spec := IdentifierSpec{Family: "account", Unit: 1000, Width: 9, Block: 10000, Frontiers: frontiers}
	id := newID(t, "amo", spec, occupied)
	got := map[uint64]bool{}
	for _, v := range occupied {
		m, o := id.Mask(v)
		if o != Masked || m/1000/10000 != v/1000/10000 || m/1000%10000 >= 300 {
			t.Fatalf("%d -> %d (%v): route or folio range changed", v, m, o)
		}
		got[m] = true
	}
	if len(got) != len(occupied) {
		t.Fatalf("%d distinct masked values for %d accounts", len(got), len(occupied))
	}
}

// A premise number keeps its four trailing digits; a counter that holds the
// last number issued (FCIPRMNO) still starts the free zone after it.
func TestPremiseCounterHoldingLastIssued(t *testing.T) {
	occupied := []uint64{10000, 20000, 30000, 40000}
	spec := IdentifierSpec{Family: "premise", Unit: 10000, Width: 7, Frontiers: []uint64{40000}}
	id := newID(t, "amo", spec, occupied)
	if z := id.FreeZones(); len(z) != 1 || z[0][0] != 5 {
		t.Fatalf("free zones %v, want one starting at unit 5", z)
	}
	for _, v := range occupied {
		m, o := id.Mask(v)
		if o != Masked || m%10000 != 0 || m/10000 > 4 || m == 0 {
			t.Fatalf("%d -> %d (%v)", v, m, o)
		}
	}
}

func TestMaskIdentifierArrayKeepsTypes(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	spec := IdentifierSpec{Family: "cis", Unit: 1, Width: 8, Frontiers: []uint64{1000}}
	id := newID(t, "amo", spec, []uint64{7, 42, 999})

	ib := array.NewInt64Builder(alloc)
	ib.AppendValues([]int64{7, 0, -3, 42}, []bool{true, true, true, true})
	ib.AppendNull()
	ints := ib.NewArray()
	ib.Release()
	defer ints.Release()

	db := array.NewDecimal128Builder(alloc, &arrow.Decimal128Type{Precision: 12, Scale: 0})
	db.Append(decimal128.FromU64(42))
	db.AppendNull()
	decs := db.NewArray()
	db.Release()
	defer decs.Release()

	sb := array.NewStringBuilder(alloc)
	sb.AppendValues([]string{"00000042", "A12"}, nil)
	strs := sb.NewArray()
	sb.Release()
	defer strs.Release()

	m42, _ := id.Mask(42)
	for _, c := range []struct {
		in    arrow.Array
		check func(arrow.Array)
		want  IdentifierCounts
	}{
		{ints, func(a arrow.Array) {
			v := a.(*array.Int64)
			if x, _ := id.Mask(7); v.Value(0) != int64(x) || v.Value(1) != 0 || v.Value(2) != -3 || v.Value(3) != int64(m42) || !v.IsNull(4) {
				t.Fatalf("int64 result %v", v)
			}
		}, IdentifierCounts{Masked: 2, Zero: 1, Unreadable: 1, Nulls: 1}},
		{decs, func(a arrow.Array) {
			v := a.(*array.Decimal128)
			if v.Value(0) != decimal128.FromU64(m42) || !v.IsNull(1) {
				t.Fatalf("decimal result %v", v)
			}
		}, IdentifierCounts{Masked: 1, Nulls: 1}},
		{strs, func(a arrow.Array) {
			v := a.(*array.String)
			if len(v.Value(0)) != 8 || v.Value(1) != "A12" {
				t.Fatalf("string result %v", v)
			}
		}, IdentifierCounts{Masked: 1, Unreadable: 1}},
	} {
		out, n, err := MaskIdentifierArray(c.in, id, alloc)
		if err != nil {
			t.Fatal(err)
		}
		if !arrow.TypeEqual(out.DataType(), c.in.DataType()) {
			t.Fatalf("type %s became %s", c.in.DataType(), out.DataType())
		}
		c.check(out)
		if n != c.want {
			t.Fatalf("%s counts %+v, want %+v", c.in.DataType(), n, c.want)
		}
		out.Release()
	}

	var vals []uint64
	if err := IdentifierValues(ints, func(v uint64) { vals = append(vals, v) }); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(vals, []uint64{7, 0, 42}) {
		t.Fatalf("values %v", vals)
	}
}

func TestMaskArrayMatchesWrap(t *testing.T) {
	b := array.NewStringBuilder(memory.DefaultAllocator)
	b.AppendValues([]string{"jane@example.com"}, nil)
	b.AppendNull()
	col := b.NewArray()
	b.Release()
	defer col.Release()
	out, err := MaskArray(col, ColumnPlan{Disposition: Mask, Kind: KindEmail, Style: StyleToken}, testKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if v := out.(*array.String); v.Value(0) != token("jane@example.com", testKey) || !v.IsNull(1) {
		t.Fatalf("got %v", v)
	}
	if _, err := MaskArray(col, ColumnPlan{Disposition: Mask, Kind: KindEmail, Style: StyleToken}, nil, nil); err == nil {
		t.Fatal("a token column without a key should be refused")
	}
}

func zoneOf(id *Identifier, u uint64) ([2]uint64, bool) {
	for _, z := range id.FreeZones() {
		if u >= z[0] && u < z[1] {
			return z, true
		}
	}
	return [2]uint64{}, false
}

// Zones given directly mask exactly as the same zones computed from values in
// use and counters.
func TestIdentifierZonesMatchesComputedZones(t *testing.T) {
	occupied := []uint64{3, 4, 5, 9, 10, 40, 41, 42, 70}
	spec := IdentifierSpec{Family: "cis", Unit: 1, Width: 2, Frontiers: []uint64{11, 43}}
	computed := newID(t, "amo", spec, occupied)
	given, err := NewIdentifierZones(testKey, "amo", IdentifierSpec{Family: "cis", Unit: 1, Width: 2},
		// out of order and overlapping; merged to the same zones
		[][2]uint64{{43, 70}, {11, 40}, {1, 3}, {12, 20}})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(computed.FreeZones(), given.FreeZones()) {
		t.Fatalf("zones %v, want %v", given.FreeZones(), computed.FreeZones())
	}
	for u := uint64(0); u < 100; u++ {
		a, ao := computed.Mask(u)
		b, bo := given.Mask(u)
		if a != b || ao != bo {
			t.Fatalf("unit %d: computed %d (%v), given %d (%v)", u, a, ao, b, bo)
		}
	}
}

func TestIdentifierZonesRejectsBadInput(t *testing.T) {
	spec := IdentifierSpec{Family: "cis", Unit: 1, Width: 4}
	if _, err := NewIdentifierZones(testKey, "amo", spec, [][2]uint64{{5, 5}}); err == nil {
		t.Fatal("an empty zone was accepted")
	}
	if _, err := NewIdentifierZones(testKey[:8], "amo", spec, nil); err == nil {
		t.Fatal("a short key was accepted")
	}
	// Clipped to [1, 10^Width).
	id, err := NewIdentifierZones(testKey, "amo", spec, [][2]uint64{{0, 3}, {9000, 20000}})
	if err != nil {
		t.Fatal(err)
	}
	if z := id.FreeZones(); !slices.Equal(z, [][2]uint64{{1, 3}, {9000, 10000}}) {
		t.Fatalf("zones %v", z)
	}
}

// With route blocks, an orphan in a free zone keeps its block (its route).
func TestFreeZoneOrphanKeepsItsBlock(t *testing.T) {
	spec := IdentifierSpec{Family: "account", Unit: 1000, Width: 9, Block: 10000}
	id, err := NewIdentifierZones(testKey, "amo", spec, [][2]uint64{{120005000, 120020000}})
	if err != nil {
		t.Fatal(err)
	}
	for u := uint64(120005000); u < 120020000; u += 7 {
		m, o := id.Mask(u*1000 + 120)
		if o != InFree || m%1000 != 120 || (m/1000)/10000 != u/10000 {
			t.Fatalf("unit %d -> %d (%v)", u, m, o)
		}
		if mu := m / 1000; mu < 120005000 || mu >= 120020000 {
			t.Fatalf("unit %d left its zone: %d", u, mu)
		}
	}
}
