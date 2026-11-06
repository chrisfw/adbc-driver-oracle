package mask

// Synthetic values only: reserved 555-01xx phone numbers, example.com
// addresses, the SSA's advertising SSN range (987-65-432x), the industry test
// card numbers, and made-up account numbers.

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func newTestScrubber(t testing.TB, extra ...Detector) *Scrubber {
	t.Helper()
	s, err := NewScrubber(testKey, "", extra...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func tok(v string) string { return token(v, testKey) }

func TestScrubDetectors(t *testing.T) {
	s := newTestScrubber(t)
	for _, c := range []struct {
		in, want string
		kinds    map[string]int64
	}{
		{"callback 802-555-0142 pls", "callback " + tok("8025550142") + " pls", map[string]int64{"phone": 1}},
		{"call 1-800-555-0199.", "call " + tok("8005550199") + ".", map[string]int64{"phone": 1}},
		// "(" is trimmed from a span's edge (the spec's trim set), so it stays.
		{"(802) 555-0142", "(" + tok("8025550142"), map[string]int64{"phone": 1}},
		{"+1 802.555.0142", tok("8025550142"), map[string]int64{"phone": 1}},
		{"mail Jane.Doe@Example.com now", "mail " + tok("jane.doe@example.com") + " now", map[string]int64{"email": 1}},
		{"ssn 987-65-4321 on file", "ssn " + tok("987654321") + " on file", map[string]int64{"ssn": 1}},
		{"ssn 987 65 4320", "ssn " + tok("987654320"), map[string]int64{"ssn": 1}},
		{"card 4111 1111 1111 1111 exp", "card " + tok("4111111111111111") + " exp", map[string]int64{"card": 1}},
		{"card 4111-1111-1111-1111", "card " + tok("4111111111111111"), map[string]int64{"card": 1}},
		{"card 5555555555554444", "card " + tok("5555555555554444"), map[string]int64{"card": 1}},
		{"routing 011000015 acct", "routing " + tok("011000015") + " acct", map[string]int64{"routing": 1}},
	} {
		got, n := s.Scrub(c.in)
		if got != c.want {
			t.Errorf("%q:\n got %q\nwant %q", c.in, got, c.want)
		}
		for k, v := range c.kinds {
			if n.ByKind[k] != v {
				t.Errorf("%q: %s spans %d, want %d (%v)", c.in, k, n.ByKind[k], v, n.ByKind)
			}
		}
		if n.Changed != 1 || n.FailedClosed != 0 {
			t.Errorf("%q: counts %+v", c.in, n)
		}
	}
}

func TestScrubLeavesOrdinaryText(t *testing.T) {
	s := newTestScrubber(t)
	for _, in := range []string{
		"meter read 12/01/2025, due 1/15/2026",            // dates
		"paid $1,234.56 on 2025-11-06 by check #10442",    // amount, ISO date, check number
		"amount 123456.78 and 4,500.00",                   // amounts
		"card 4111 1111 1111 1112 failed",                 // fails Luhn
		"routing 011000016 is wrong",                      // fails the ABA checksum
		"ref 123456789012 is a work order",                // 12 digits: not a built-in kind (identifier forms decide)
		"plain text with no numbers at all, café ünïcode", // nothing
		"kwh 1234567 on 3 meters",                         // 7 digits
		"no at sign here.example.com",                     // not an e-mail
	} {
		if got, n := s.Scrub(in); got != in || n.Changed != 0 {
			t.Errorf("%q changed to %q (%+v)", in, got, n)
		}
	}
}

func TestScrubTrimAndOverlap(t *testing.T) {
	// Trimming: punctuation around a span stays; offsets in multi-byte text.
	sp, ok := trimSpan(`x "(802)" y`, Span{Start: 2, End: 9})
	if !ok || sp.Start != 4 || sp.End != 7 {
		t.Fatalf("trim: %+v", sp)
	}
	if _, ok := trimSpan("x ,; y", Span{Start: 2, End: 4}); ok {
		t.Fatal("a span of punctuation only was kept")
	}
	sp, _ = trimSpan("a-b.c@d_e/f+g", Span{Start: 0, End: 13})
	if sp.Start != 0 || sp.End != 13 {
		t.Fatal("@ - _ . / + were trimmed")
	}
	// Overlaps: higher score wins, then the longer span.
	r := func(string) (string, error) { return "", nil }
	got := resolve([]Span{
		{Start: 0, End: 5, Kind: "a", Score: 1, Replace: r},
		{Start: 3, End: 12, Kind: "b", Score: 1, Replace: r},
		{Start: 10, End: 14, Kind: "c", Score: 2, Replace: r},
		{Start: 20, End: 22, Kind: "d", Score: 1, Replace: r},
	})
	var kinds []string
	for _, k := range got {
		kinds = append(kinds, k.Kind)
	}
	if strings.Join(kinds, ",") != "a,c,d" {
		t.Fatalf("resolve kept %v", kinds)
	}

	// Multi-byte text before, between and after spans.
	s := newTestScrubber(t)
	in := "Ça va: «802-555-0142» — écrit à jane@example.com ✓ fin"
	want := "Ça va: «" + tok("8025550142") + "» — écrit à " + tok("jane@example.com") + " ✓ fin"
	if got, _ := s.Scrub(in); got != want {
		t.Fatalf("multi-byte:\n got %q\nwant %q", got, want)
	}
}

func TestScrubFailsClosed(t *testing.T) {
	s, _ := NewScrubber(testKey, "[NOTE REDACTED]")
	s.fault = func(string) error { return errors.New("forced") }
	got, n := s.Scrub("callback 802-555-0142")
	if got != "[NOTE REDACTED]" || n.FailedClosed != 1 || s.Failures() != 1 {
		t.Fatalf("forced error: %q %+v", got, n)
	}
	s.fault = func(string) error { panic("boom") }
	if got, n := s.Scrub("anything"); got != "[NOTE REDACTED]" || n.FailedClosed != 1 {
		t.Fatalf("panic: %q %+v", got, n)
	}
	// A detector that returns a bad span fails the value closed.
	bad := detectorFunc(func(string) []Span { return []Span{{Start: 5, End: 2, Kind: "x", Replace: func(string) (string, error) { return "", nil }}} })
	s2 := newTestScrubber(t, bad)
	if got, n := s2.Scrub("hello world"); got != DefaultRedaction || n.FailedClosed != 1 {
		t.Fatalf("bad span: %q %+v", got, n)
	}
	// A replacement error fails the value closed.
	errd := detectorFunc(func(v string) []Span {
		return []Span{{Start: 0, End: 1, Kind: "x", Score: 1, Replace: func(string) (string, error) { return "", errors.New("no") }}}
	})
	if got, _ := newTestScrubber(t, errd).Scrub("hello"); got != DefaultRedaction {
		t.Fatalf("replace error: %q", got)
	}
	if _, err := NewScrubber([]byte("short"), ""); err == nil {
		t.Fatal("a short key was accepted")
	}
}

type detectorFunc func(string) []Span

func (f detectorFunc) Detect(v string) []Span { return f(v) }

// A phone, e-mail or SSN in text gets the token the column kernel gives the
// same value stored in its normalized form (phone and SSN as digits, e-mail
// lower case).
func TestScrubTokensMatchColumnKernel(t *testing.T) {
	s := newTestScrubber(t)
	col := func(kind Kind, v string) string { return run(t, ColumnPlan{Disposition: Mask, Kind: kind, Style: StyleToken}, v) }
	for _, c := range []struct {
		kind       Kind
		text, norm string
	}{
		{KindPhone, "802-555-0142", "8025550142"},
		{KindPhone, "1 (802) 555-0142", "8025550142"},
		{KindEmail, "Jane.Doe@Example.com", "jane.doe@example.com"},
		{KindSSN, "987-65-4321", "987654321"},
		{KindCard, "4111 1111 1111 1111", "4111111111111111"},
	} {
		got, _ := s.Scrub(c.text)
		if want := col(c.kind, c.norm); got != want {
			t.Errorf("%s: text token %q, column token %q", c.kind, got, want)
		}
	}
}

// --- identifier families in text ---

// testAccountForms is a synthetic account layout (BBBBBBBBB C TT, kept low
// part CTT) in two display forms, R-FFFF.CTT and a bare 12-digit run, with
// C limited to the digits in apps.
func testAccountForms(apps string) []IdentifierForm {
	okC := func(c byte) bool { return strings.IndexByte(apps, c) >= 0 }
	return []IdentifierForm{
		{
			Family:  "account",
			Pattern: regexp.MustCompile(`\b(\d{1,5})-(\d{4})\.(\d)(\d{2})\b`),
			Value: func(sub []string) (uint64, bool) {
				if !okC(sub[3][0]) {
					return 0, false
				}
				v, err := strconv.ParseUint(sub[1]+sub[2]+sub[3]+sub[4], 10, 64)
				return v, err == nil
			},
			Render: func(sub []string, m uint64) string {
				s := fmt.Sprintf("%012d", m)
				r := strings.TrimLeft(s[:5], "0")
				if r == "" {
					r = "0"
				}
				return r + "-" + s[5:9] + "." + s[9:]
			},
		},
		{
			Family:  "account",
			Pattern: regexp.MustCompile(`\b\d{12}\b`),
			Value: func(sub []string) (uint64, bool) {
				if !okC(sub[0][9]) {
					return 0, false
				}
				v, err := strconv.ParseUint(sub[0], 10, 64)
				return v, err == nil
			},
			Render: func(sub []string, m uint64) string { return fmt.Sprintf("%012d", m) },
		},
	}
}

func testAccountID(t testing.TB, block uint64, zones [][2]uint64) *Identifier {
	t.Helper()
	id, err := NewIdentifierZones(testKey, "test", IdentifierSpec{Family: "account", Unit: 1000, Width: 9, Block: block}, zones)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// The invariant: an account in a note masks to exactly the display form of
// what the identifier column mask gives the same account in the same
// connection. Route sites keep the route; zero is kept; an orphan in a free
// zone is masked within its zone.
func TestScrubIdentifierInvariant(t *testing.T) {
	for _, site := range []struct {
		name  string
		block uint64
		zones [][2]uint64
	}{
		{"plain", 0, [][2]uint64{{1, 100000}, {500000000, 1000000000}}},
		{"route", 10000, [][2]uint64{{123450000, 123451000}}},
	} {
		id := testAccountID(t, site.block, site.zones)
		det, err := NewIdentifierDetector(testAccountForms("0259"), map[string]*Identifier{"account": id})
		if err != nil {
			t.Fatal(err)
		}
		s := newTestScrubber(t, det)
		for _, v := range []uint64{123456789001, 123450001502, 98765432902, 500001234001, 123450500001} {
			m, _ := id.Mask(v)
			col := fmt.Sprintf("%012d", m)
			got, n := s.Scrub(fmt.Sprintf("re acct %012d, see note", v))
			if want := "re acct " + col + ", see note"; got != want {
				t.Errorf("%s bare %d: got %q want %q", site.name, v, got, want)
			}
			if n.ByKind["account"] != 1 {
				t.Errorf("%s: counts %+v", site.name, n.ByKind)
			}
			if col[9:] != fmt.Sprintf("%012d", v)[9:] {
				t.Errorf("%s: C TT changed", site.name)
			}
			if site.block != 0 && col[:5] != fmt.Sprintf("%012d", v)[:5] {
				t.Errorf("route site: the route changed")
			}
			// The R-FFFF.CTT form of the same value.
			raw := fmt.Sprintf("%012d", v)
			disp := strings.TrimLeft(raw[:5], "0") + "-" + raw[5:9] + "." + raw[9:]
			got, _ = s.Scrub("acct " + disp + ".")
			mdisp := strings.TrimLeft(col[:5], "0") + "-" + col[5:9] + "." + col[9:]
			if want := "acct " + mdisp + "."; got != want {
				t.Errorf("%s display %s: got %q want %q", site.name, disp, got, want)
			}
		}
		// An orphan inside a free zone is masked, within its zone.
		orphan := uint64(600000000)*1000 + 1
		if site.block == 0 {
			m, o := id.Mask(orphan)
			if o != InFree || m == orphan {
				t.Fatalf("orphan: %d %v", m, o)
			}
			got, _ := s.Scrub(fmt.Sprintf("%012d", orphan))
			if got != fmt.Sprintf("%012d", m) {
				t.Errorf("orphan in text %q, column %012d", got, m)
			}
		}
		// Zero is kept.
		if got, _ := s.Scrub("acct 000000000001"); got != "acct 000000000001" {
			t.Errorf("zero base: %q", got)
		}
		// C not an application digit in use: not an account (left alone).
		if got, _ := s.Scrub("ref 123456789301"); got != "ref 123456789301" {
			t.Errorf("C=3 not in use but masked: %q", got)
		}
	}
	if _, err := NewIdentifierDetector(testAccountForms("0"), map[string]*Identifier{}); err == nil {
		t.Fatal("a form without its family's mask was accepted")
	}
}

func TestScrubArrayAndWrap(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	s := newTestScrubber(t)
	b := array.NewStringBuilder(alloc)
	b.AppendValues([]string{"call 802-555-0142", "nothing here"}, nil)
	b.AppendNull()
	col := b.NewArray()
	b.Release()
	defer col.Release()

	out, n, err := ScrubArray(col, s, alloc)
	if err != nil {
		t.Fatal(err)
	}
	v := out.(*array.String)
	if v.Value(0) != "call "+tok("8025550142") || v.Value(1) != "nothing here" || !v.IsNull(2) {
		t.Fatalf("ScrubArray: %v", v)
	}
	if n.Values != 2 || n.Nulls != 1 || n.Changed != 1 || n.ByKind["phone"] != 1 {
		t.Fatalf("counts %+v", n)
	}
	out.Release()

	ib := array.NewInt64Builder(alloc)
	ib.Append(1)
	ints := ib.NewArray()
	ib.Release()
	if _, _, err := ScrubArray(ints, s, alloc); err == nil {
		t.Fatal("a number column was scrubbed")
	}
	ints.Release()

	// Through Wrap, as the model path uses it.
	schema := arrow.NewSchema([]arrow.Field{{Name: "NOTE", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{col}, int64(col.Len()))
	rdr, _ := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	rec.Release()
	w, err := Wrap(rdr, Plan{{Disposition: Mask, Kind: KindGeneric, Style: StyleScrub, Scrubber: s}}, testKey, alloc)
	if err != nil {
		t.Fatal(err)
	}
	if !w.Next() || w.RecordBatch().Column(0).(*array.String).Value(0) != "call "+tok("8025550142") {
		t.Fatal("Wrap did not scrub")
	}
	w.Release()
}

func TestScrubAndIdentifierStylesNeedTheirParts(t *testing.T) {
	for _, c := range []ColumnPlan{
		{Disposition: Mask, Kind: KindGeneric, Style: StyleScrub},
		{Disposition: Mask, Kind: KindGeneric, Style: StyleIdentifier},
		{Disposition: Mask, Kind: KindGeneric, Style: "scrubbish"},
	} {
		if err := (Plan{c}).Validate(); err == nil {
			t.Errorf("%s without its part was accepted", c.Style)
		}
	}
	id := testAccountID(t, 0, nil)
	c := ColumnPlan{Disposition: Mask, Kind: KindGeneric, Style: StyleIdentifier, Identifier: id}
	m, _ := id.Mask(123456789001)
	if got := run(t, c, "123456789001"); got != strconv.FormatUint(m, 10) {
		t.Fatalf("identifier kernel: %q", got)
	}
	if got := run(t, c, "12345-6789.001"); got != DefaultRedaction {
		t.Fatalf("a non-number in an identifier column: %q", got)
	}
}

// --- benchmark: 100k synthetic notes of about 200 bytes ---

func syntheticNotes(n int) []string {
	parts := []string{
		"Customer called about the high bill; explained the estimated read and rate change. ",
		"Callback requested at 802-555-01%02d after 5pm. ",
		"Payment arrangement set up, first installment due 12/15/2025. ",
		"Sent copy of bill to user%d@example.com as requested. ",
		"Re acct 1234%05d001, meter swap scheduled. ",
		"No personal data in this line, just a long-ish comment about service. ",
	}
	out := make([]string, n)
	for i := range out {
		var b strings.Builder
		for b.Len() < 200 {
			p := parts[(i+b.Len())%len(parts)]
			if strings.Contains(p, "%") {
				p = fmt.Sprintf(p, i%100)
			}
			b.WriteString(p)
		}
		out[i] = b.String()
	}
	return out
}

func BenchmarkScrubNotes(b *testing.B) {
	notes := syntheticNotes(100000)
	id := testAccountID(b, 0, [][2]uint64{{1, 100000}})
	det, _ := NewIdentifierDetector(testAccountForms("0259"), map[string]*Identifier{"account": id})
	s := newTestScrubber(b, det)
	alloc := memory.NewGoAllocator()
	sb := array.NewStringBuilder(alloc)
	sb.AppendValues(notes, nil)
	col := sb.NewArray()
	sb.Release()
	defer col.Release()
	var bytes int64
	for _, n := range notes {
		bytes += int64(len(n))
	}
	b.SetBytes(bytes)
	b.ResetTimer()
	for range b.N {
		out, _, err := ScrubArray(col, s, alloc)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
	b.ReportMetric(float64(len(notes))*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
}
