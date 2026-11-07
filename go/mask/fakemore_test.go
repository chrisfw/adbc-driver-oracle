package mask

import (
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Synthetic values only (routing numbers built here with a valid check digit).
func validRouting(prefix, mid string) string {
	d8 := prefix + mid
	return d8 + strconv.Itoa(ABACheckDigit(d8))
}

func TestFakeABA(t *testing.T) {
	f, err := NewFakeABA(fakeTestKey)
	if err != nil {
		t.Fatal(err)
	}
	in := validRouting("12", "345678")
	if !ValidABA(in) {
		t.Fatalf("test routing %s invalid", in)
	}
	out, ok := f.Digits(in)
	if !ok || !ValidABA(out) || out[:2] != "12" || out == in {
		t.Fatalf("%s -> %s %v", in, out, ok)
	}
	if again, _ := f.Digits(in); again != out {
		t.Fatal("not deterministic")
	}
	// A leading-zero routing number stored as a NUMBER (8 digits).
	z := validRouting("01", "234567")
	n, _ := strconv.ParseUint(z, 10, 64)
	fn, ok := f.Number(n)
	if s := strconv.FormatUint(fn, 10); !ok || !ValidABA("0"+s) || len(s) != 8 {
		t.Fatalf("number %d -> %d", n, fn)
	}
	// Text keeps its layout.
	tx, ok := f.Text(in[:4] + "-" + in[4:8] + "-" + in[8:])
	if !ok || strings.Count(tx, "-") != 2 || !ValidABA(strings.ReplaceAll(tx, "-", "")) {
		t.Fatalf("text %q", tx)
	}
	if _, ok := f.Digits("12345"); ok {
		t.Fatal("5 digits accepted")
	}
}

func TestFakeTaxIDs(t *testing.T) {
	f, _ := NewFakeTaxIDs(fakeTestKey)
	out := f.Fake("12-3456789")
	if len(out) != 10 || out[2] != '-' || out == "12-3456789" {
		t.Fatalf("%q", out)
	}
	pre := out[:2]
	found := false
	for _, p := range einPrefixes {
		found = found || p == pre
	}
	if !found {
		t.Fatalf("prefix %s is no IRS campus prefix", pre)
	}
	if o := f.Fake("123456789"); len(o) != 9 || strings.Trim(o, "0123456789") != "" {
		t.Fatalf("plain EIN %q", o)
	}
	if o := f.Fake("ST-AB12345"); len(o) != 10 || o == "ST-AB12345" || o[2] != '-' {
		t.Fatalf("state ID %q", o)
	}
	if n, ok := f.Number(123456789); !ok || n == 123456789 || n > 999999999 {
		t.Fatalf("number %d", n)
	}
}

func TestFakeNet(t *testing.T) {
	f, _ := NewFakeNet(fakeTestKey)
	out := f.Fake("from 10.1.2.3 and fe80::1 via gw.corp.example.com, local 127.0.0.1")
	parts := strings.Fields(strings.NewReplacer(",", " ").Replace(out))
	ip4, ip6, host := net.ParseIP(parts[1]), net.ParseIP(parts[3]), parts[5]
	if ip4 == nil || !(strings.HasPrefix(parts[1], "192.0.2.") || strings.HasPrefix(parts[1], "198.51.100.") || strings.HasPrefix(parts[1], "203.0.113.")) {
		t.Fatalf("v4 %q", out)
	}
	if ip6 == nil || !strings.HasPrefix(parts[3], "2001:db8:") {
		t.Fatalf("v6 %q", out)
	}
	if !strings.HasSuffix(host, ".example.net") || !strings.Contains(out, "127.0.0.1") {
		t.Fatalf("host/loopback %q", out)
	}
}

func TestFakeWebAddr(t *testing.T) {
	names, _ := NewFakeNames(fakeTestKey)
	f, _ := NewFakeWebAddrs(names)
	out := f.Fake("www.smithplumbing.com/contact")
	if !strings.HasPrefix(out, "https://www.") || !strings.HasSuffix(out, ".example.org") || strings.Contains(out, "smithplumbing") {
		t.Fatalf("%q", out)
	}
}

func TestPerturbAmounts(t *testing.T) {
	for _, c := range []int64{12345, -500, 1, 99999999} {
		out := PerturbCents(fakeTestKey, c)
		if out == c || float64(out) < float64(c)*0.849-1 && c > 0 || float64(out) > float64(c)*1.151+1 && c > 0 {
			t.Errorf("%d -> %d", c, out)
		}
	}
	if PerturbCents(fakeTestKey, 0) != 0 {
		t.Error("0 moved")
	}
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	b := array.NewDecimal128Builder(alloc, &arrow.Decimal128Type{Precision: 11, Scale: 2})
	b.Append(decimal128.FromI64(12345))
	b.Append(decimal128.FromI64(-500))
	b.AppendNull()
	a := b.NewArray()
	b.Release()
	defer a.Release()
	out, changed, err := PerturbAmountArray(a, fakeTestKey, alloc)
	if err != nil || changed != 2 || !out.IsNull(2) {
		t.Fatalf("%v %d %v", out, changed, err)
	}
	if v := out.(*array.Decimal128).Value(1); v.Sign() >= 0 {
		t.Fatalf("a negative amount turned %v", v)
	}
	out.Release()
}

func TestFakeNumberArray(t *testing.T) {
	f, _ := NewFakeABA(fakeTestKey)
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	in := validRouting("12", "345678")
	n, _ := strconv.ParseUint(in, 10, 64)
	b := array.NewDecimal128Builder(alloc, &arrow.Decimal128Type{Precision: 12})
	b.Append(decimal128.FromU64(n))
	b.Append(decimal128.FromU64(0))
	b.Append(decimal128.FromU64(42))
	a := b.NewArray()
	b.Release()
	defer a.Release()
	out, c, err := FakeNumberArray(a, f.Number, f.Text, alloc)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	v := out.(*array.Decimal128)
	if !ValidABA(strconv.FormatUint(v.Value(0).LowBits(), 10)) || v.Value(1).LowBits() != 0 || v.Value(2).LowBits() != 0 {
		t.Fatalf("%v", v)
	}
	if c.Masked != 1 || c.Zero != 1 || c.Unreadable != 1 {
		t.Fatalf("%+v", c)
	}
}
