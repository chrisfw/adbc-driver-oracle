package mask

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Synthetic numbers only (555-01xx and made-up area codes).
var fakeTestKey = []byte("0123456789abcdef-test-key")

func TestFakePhoneForms(t *testing.T) {
	f, err := NewFakePhones(fakeTestKey)
	if err != nil {
		t.Fatal(err)
	}
	local := f.Local("8675309")
	if len(local) != 7 || !strings.HasPrefix(local, "55501") {
		t.Fatalf("local %q", local)
	}
	// Every form of one number gives the same fake, the area code kept.
	for in, want := range map[string]string{
		"8675309":     local,
		"9998675309":  "999" + local,
		"19998675309": "1999" + local,
	} {
		if got, ok := f.Digits(in); !ok || got != want {
			t.Errorf("Digits(%s) = %q %v, want %q", in, got, ok, want)
		}
	}
	if n, ok := f.Number(9998675309); !ok || strconv.FormatUint(n, 10) != "999"+local {
		t.Errorf("Number = %d %v", n, ok)
	}
	// Text keeps its punctuation and an extension.
	for in, want := range map[string]string{
		"(999) 867-5309":    "(999) " + local[:3] + "-" + local[3:],
		"999.867.5309 x 42": "999." + local[:3] + "." + local[3:] + " x 42",
		"+1 999-867-5309":   "+1 999-" + local[:3] + "-" + local[3:],
		"867-5309 ext. 7":   local[:3] + "-" + local[3:] + " ext. 7",
	} {
		if got, ok := f.Text(in); !ok || got != want {
			t.Errorf("Text(%q) = %q %v, want %q", in, got, ok, want)
		}
	}
	for _, bad := range []string{"12345", "call me", "999-867-5309-12", "N/A"} {
		if got, ok := f.Text(bad); ok {
			t.Errorf("Text(%q) accepted: %q", bad, got)
		}
	}
	// A fake never equals the number, even one already in the fiction range.
	for xx := range 100 {
		l := "55501" + strconv.Itoa(100 + xx)[1:]
		if f.Local(l) == l {
			t.Fatalf("Local(%s) kept it", l)
		}
	}
	// The demo relocation replaces area codes it names.
	f.Area = map[string]string{"999": "888"}
	if got, _ := f.Digits("9998675309"); got != "888"+local {
		t.Errorf("relocated %q", got)
	}
}

func TestFakePhoneArray(t *testing.T) {
	f, _ := NewFakePhones(fakeTestKey)
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	sb := array.NewStringBuilder(alloc)
	sb.AppendValues([]string{"999-867-5309", "", "0000000", "not a phone"}, nil)
	sb.AppendNull()
	s := sb.NewArray()
	defer s.Release()
	out, n, err := FakePhoneArray(s, f, alloc)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	so := out.(*array.String)
	if !strings.HasPrefix(so.Value(0), "999-555-01") || so.Value(1) != "" || so.Value(2) != "0000000" || !so.IsNull(3) || !so.IsNull(4) {
		t.Fatalf("strings %v", so)
	}
	if n.Masked != 1 || n.Zero != 2 || n.Unreadable != 1 || n.Nulls != 1 {
		t.Fatalf("counts %+v", n)
	}

	db := array.NewDecimal128Builder(alloc, &arrow.Decimal128Type{Precision: 12, Scale: 0})
	db.Append(decimal128.FromU64(9998675309))
	db.Append(decimal128.FromU64(0))
	db.Append(decimal128.FromU64(12345))
	d := db.NewArray()
	defer d.Release()
	out2, n2, err := FakePhoneArray(d, f, alloc)
	if err != nil {
		t.Fatal(err)
	}
	defer out2.Release()
	got := out2.(*array.Decimal128)
	if v := got.Value(0).LowBits(); strconv.FormatUint(v, 10) != "999"+f.Local("8675309") || got.Value(1).LowBits() != 0 || got.Value(2).LowBits() != 0 {
		t.Fatalf("numbers %v", got)
	}
	if n2.Masked != 1 || n2.Zero != 1 || n2.Unreadable != 1 {
		t.Fatalf("counts %+v", n2)
	}
	db.Release()
	sb.Release()
}

func TestFakeEmail(t *testing.T) {
	names, _ := NewFakeNames(fakeTestKey)
	f, err := NewFakeEmails(fakeTestKey, names)
	if err != nil {
		t.Fatal(err)
	}
	// The local part's words are the same fakes the name columns get.
	got := f.Address("john.smith77@mail.test")
	want := strings.ToLower(names.Fake("JOHN")) + "." + strings.ToLower(names.Fake("SMITH"))
	if !strings.HasPrefix(got, want) {
		t.Fatalf("%q does not start with %q", got, want)
	}
	at := strings.IndexByte(got, '@')
	if digits := got[len(want):at]; len(digits) != 2 || digits == "77" || strings.Trim(digits, "0123456789") != "" {
		t.Fatalf("digits %q", digits)
	}
	if d := got[at+1:]; d != "example.com" && d != "example.net" && d != "example.org" {
		t.Fatalf("domain %q", d)
	}
	// Same domain, same reserved domain; same address, same fake.
	if f.Domain("MAIL.TEST") != strings.ToUpper(f.Domain("mail.test")) {
		t.Fatalf("case: %q", f.Domain("MAIL.TEST"))
	}
	if f.Address("john.smith77@mail.test") != got {
		t.Fatal("not deterministic")
	}
	// Lists and display names.
	list := f.Fake("Jane Doe <jane@one.test>; ops@two.test")
	if strings.Contains(list, "Jane") || strings.Contains(list, "one.test") || strings.Count(list, "@example.") != 2 || !strings.Contains(list, ">; ") {
		t.Fatalf("list %q", list)
	}
	// Fit shortens the local part, never the domain.
	long := f.Fit("abcdefghij.klmnopqrst@mail.test", 20)
	if len(long) > 20 || !strings.Contains(long, "@example.") {
		t.Fatalf("fit %q", long)
	}
	if f.Fake("") != "" || f.Fake("   ") != "   " {
		t.Fatal("blank changed")
	}
}

func TestFakePhoneDemoExchanges(t *testing.T) {
	f, err := NewFakePhones(testKey)
	if err != nil {
		t.Fatal(err)
	}
	f.Area = map[string]string{"603": "302"}
	f.Exchange = map[string]string{"224": "391"}
	full, ok := f.Digits("6032241234")
	if !ok || full[:6] != "302391" || full == "3023911234" {
		t.Fatalf("relocated: %q", full)
	}
	// The split column (local number alone) agrees with the whole number.
	if loc := f.Local("2241234"); loc != full[3:] {
		t.Fatalf("split %q vs whole %q", loc, full)
	}
	// Deterministic, and a different line gets a different fake line.
	if again, _ := f.Digits("6032241234"); again != full {
		t.Fatal("not deterministic")
	}
	// Every line of the exchange gets its own fake line.
	seen := map[string]bool{}
	for line := 0; line < 10000; line++ {
		v, _ := f.Digits(fmt.Sprintf("603224%04d", line))
		if seen[v] {
			t.Fatalf("two lines, one fake: %s", v)
		}
		seen[v] = true
	}
	// An unmapped exchange, and toll-free numbers, keep 555-01xx.
	if v, _ := f.Digits("6039991234"); v[:8] != "30255501" {
		t.Fatalf("unmapped exchange: %q", v)
	}
	if v, _ := f.Digits("18002241234"); v[:9] != "180055501" {
		t.Fatalf("toll-free: %q", v)
	}
	if v, ok := f.Text("(603) 224-1234 x12"); !ok || v != "("+full[:3]+") "+full[3:6]+"-"+full[6:]+" x12" {
		t.Fatalf("text: %q", v)
	}
}
