package mask

// More fakes for seeding a test or demo database (all keyed, deterministic:
// the same input gives the same fake everywhere; a fake never equals its
// input):
//
//   - FakeABA: bank routing (ABA) numbers. The 2-digit Federal Reserve prefix
//     is kept, digits 3-8 are keyed, digit 9 is the ABA check digit (weights
//     3,7,1), so the fake validates like a real routing number; the same bank
//     maps to the same fake. NUMBER columns hold a leading-zero routing
//     number as 8 digits; it is read and written that way.
//   - FakeTaxID: an EIN (NN-NNNNNNN or 9 digits) becomes an EIN with a valid
//     IRS campus prefix; any other tax ID gets format-preserving digits.
//   - FakeNet: IPv4 addresses move into the RFC 5737 TEST-NETs, IPv6 into
//     2001:db8::/32 (RFC 3849), host names into example.net (RFC 2606).
//   - FakeWebAddr: https://<fake name>.example.org.
//   - PerturbAmount: an amount moved by a keyed factor within ±15%, rounded
//     to the cent (demo payroll).

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// keyed is an HMAC stream for one label and input.
type keyed struct{ key []byte }

func (k keyed) u64(label, in string, n uint32) uint64 {
	m := hmac.New(sha256.New, k.key)
	m.Write([]byte(label))
	m.Write([]byte{0})
	m.Write([]byte(in))
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], n)
	m.Write(b[:])
	return binary.BigEndian.Uint64(m.Sum(nil))
}

// --- ABA routing numbers ---

// FakeABA makes fake routing numbers. It is safe for concurrent use.
type FakeABA struct{ k keyed }

// NewFakeABA keys fake routing numbers with key (at least 16 bytes).
func NewFakeABA(key []byte) (*FakeABA, error) {
	if len(key) < 16 {
		return nil, errors.New("mask: the fake-ABA key is shorter than 16 bytes")
	}
	return &FakeABA{k: keyed{derive(key, "xdb-fake/aba/v1")}}, nil
}

// ABACheckDigit is the check digit of the first 8 digits.
func ABACheckDigit(d8 string) int {
	w := [8]int{3, 7, 1, 3, 7, 1, 3, 7}
	s := 0
	for i := 0; i < 8; i++ {
		s += int(d8[i]-'0') * w[i]
	}
	return (10 - s%10) % 10
}

// ValidABA reports whether d is 9 digits with a correct check digit.
func ValidABA(d string) bool {
	if len(d) != 9 || strings.Trim(d, "0123456789") != "" {
		return false
	}
	return ABACheckDigit(d[:8]) == int(d[8]-'0')
}

// Digits fakes a routing number given as 9 digits (8 are read as a lost
// leading zero). ok is false for any other shape.
func (f *FakeABA) Digits(d string) (string, bool) {
	if strings.Trim(d, "0123456789") != "" {
		return "", false
	}
	if len(d) == 8 {
		d = "0" + d
	}
	if len(d) != 9 {
		return "", false
	}
	for n := uint32(0); ; n++ {
		x := f.k.u64("aba", d, n)
		mid := fmt.Sprintf("%06d", x%1000000)
		d8 := d[:2] + mid
		out := d8 + strconv.Itoa(ABACheckDigit(d8))
		if out != d {
			return out, true
		}
	}
}

// Text fakes a routing number written as text (separators kept).
func (f *FakeABA) Text(v string) (string, bool) {
	return digitsInPlace(v, f.Digits)
}

// Number fakes a routing number stored as a number.
func (f *FakeABA) Number(v uint64) (uint64, bool) {
	out, ok := f.Digits(strconv.FormatUint(v, 10))
	if !ok {
		return 0, false
	}
	n, _ := strconv.ParseUint(out, 10, 64)
	return n, true
}

// digitsInPlace runs fn on v's digits and writes the result back over them,
// keeping every other character; the digit count must not change (a leading
// zero fn adds to 8 digits is dropped again).
func digitsInPlace(v string, fn func(string) (string, bool)) (string, bool) {
	var d strings.Builder
	for _, r := range v {
		if r >= '0' && r <= '9' {
			d.WriteRune(r)
		} else if !strings.ContainsRune(" -./()", r) {
			return "", false
		}
	}
	in := d.String()
	out, ok := fn(in)
	if !ok {
		return "", false
	}
	if len(out) == len(in)+1 && out[0] == '0' {
		out = out[1:]
	}
	if len(out) != len(in) {
		return "", false
	}
	var b strings.Builder
	i := 0
	for _, r := range v {
		if r >= '0' && r <= '9' {
			b.WriteByte(out[i])
			i++
		} else {
			b.WriteRune(r)
		}
	}
	return b.String(), true
}

// --- tax IDs ---

// einPrefixes are the IRS campus prefixes an EIN may start with.
var einPrefixes = []string{"01", "02", "03", "04", "05", "06", "10", "11", "12", "13", "14", "15", "16", "20", "21", "22", "23",
	"24", "25", "26", "27", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46",
	"47", "48", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67", "68",
	"71", "72", "73", "74", "75", "76", "77", "80", "81", "82", "83", "84", "85", "86", "87", "88", "90", "91", "92", "93", "94",
	"95", "98", "99"}

var einRe = regexp.MustCompile(`^\s*(\d{2})-?(\d{7})\s*$`)

// FakeTaxIDs makes fake tax IDs. It is safe for concurrent use.
type FakeTaxIDs struct {
	k     keyed
	codes *FakeCodes
}

// NewFakeTaxIDs keys fake tax IDs with key (at least 16 bytes).
func NewFakeTaxIDs(key []byte) (*FakeTaxIDs, error) {
	if len(key) < 16 {
		return nil, errors.New("mask: the fake-tax-ID key is shorter than 16 bytes")
	}
	codes, err := NewFakeCodes(derive(key, "xdb-fake/taxid-codes/v1"))
	if err != nil {
		return nil, err
	}
	return &FakeTaxIDs{k: keyed{derive(key, "xdb-fake/taxid/v1")}, codes: codes}, nil
}

// Fake is v's fake: an EIN of the same layout, else a format-preserving code.
func (f *FakeTaxIDs) Fake(v string) string {
	m := einRe.FindStringSubmatch(v)
	if m == nil {
		return f.codes.Fake(v)
	}
	in := m[1] + m[2]
	for n := uint32(0); ; n++ {
		x := f.k.u64("ein", in, n)
		out := einPrefixes[x%uint64(len(einPrefixes))] + fmt.Sprintf("%07d", (x/uint64(len(einPrefixes)))%10000000)
		if out == in {
			continue
		}
		res, _ := digitsInPlace(v, func(string) (string, bool) { return out, true })
		return res
	}
}

// Number fakes a tax ID stored as a number (9 digits: an EIN).
func (f *FakeTaxIDs) Number(v uint64) (uint64, bool) {
	s := fmt.Sprintf("%09d", v)
	if len(s) != 9 {
		return 0, false
	}
	out := f.Fake(s)
	n, err := strconv.ParseUint(out, 10, 64)
	return n, err == nil
}

// --- IP addresses and host names ---

// FakeNet makes fake IP addresses and host names. It is safe for concurrent
// use.
type FakeNet struct{ k keyed }

// NewFakeNet keys fake addresses with key (at least 16 bytes).
func NewFakeNet(key []byte) (*FakeNet, error) {
	if len(key) < 16 {
		return nil, errors.New("mask: the fake-network key is shorter than 16 bytes")
	}
	return &FakeNet{k: keyed{derive(key, "xdb-fake/net/v1")}}, nil
}

var (
	ipv4Re = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)
	ipv6Re = regexp.MustCompile(`(?i)\b(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{0,4}\b`)
	hostRe = regexp.MustCompile(`(?i)\b(?:[a-z0-9](?:[a-z0-9\-]*[a-z0-9])?\.)+[a-z]{2,}\b`)
)

var testNets = [3]string{"192.0.2.", "198.51.100.", "203.0.113."}

// IP is an address's fake (v4 into a TEST-NET, v6 into 2001:db8::/32);
// loopback stays.
func (f *FakeNet) IP(s string) string {
	ip := net.ParseIP(s)
	if ip == nil || ip.IsLoopback() || ip.IsUnspecified() {
		return s
	}
	x := f.k.u64("ip", ip.String(), 0)
	if ip.To4() != nil {
		out := fmt.Sprintf("%s%d", testNets[x%3], 1+(x/3)%254)
		if out == s {
			out = fmt.Sprintf("%s%d", testNets[(x+1)%3], 1+(x/3)%254)
		}
		return out
	}
	y := f.k.u64("ip6", ip.String(), 1)
	return fmt.Sprintf("2001:db8:%x:%x::%x", x&0xffff, (x>>16)&0xffff, y&0xffff)
}

// Host is a host name's fake in example.net.
func (f *FakeNet) Host(h string) string {
	l := strings.ToLower(h)
	if l == "localhost" || strings.HasSuffix(l, ".example.net") || strings.HasSuffix(l, ".invalid") {
		return h
	}
	return fmt.Sprintf("h%06d.example.net", f.k.u64("host", l, 0)%1000000)
}

// Fake is v with every address and host name in it faked.
func (f *FakeNet) Fake(v string) string {
	out := ipv4Re.ReplaceAllStringFunc(v, f.IP)
	out = ipv6Re.ReplaceAllStringFunc(out, func(m string) string {
		if net.ParseIP(m) == nil {
			return m
		}
		return f.IP(m)
	})
	return hostRe.ReplaceAllStringFunc(out, func(m string) string {
		if net.ParseIP(m) != nil {
			return m
		}
		return f.Host(m)
	})
}

// --- web addresses ---

// FakeWebAddrs makes a customer's fake web address from the fake-name words
// of its host. It is safe for concurrent use.
type FakeWebAddrs struct{ names *FakeNames }

// NewFakeWebAddrs needs the export's fake-name generator.
func NewFakeWebAddrs(names *FakeNames) (*FakeWebAddrs, error) {
	if names == nil {
		return nil, errors.New("mask: fake web addresses need the fake-name generator")
	}
	return &FakeWebAddrs{names: names}, nil
}

var webHostRe = regexp.MustCompile(`(?i)^\s*(?:[a-z][a-z0-9+.\-]*://)?(?:www\.)?([^/:?#\s]+)`)

// Fake is https://<fake words>.example.org for v's host; a blank stays.
func (f *FakeWebAddrs) Fake(v string) string {
	m := webHostRe.FindStringSubmatch(v)
	if m == nil {
		return v
	}
	labels := strings.Split(m[1], ".")
	if len(labels) > 1 {
		labels = labels[:len(labels)-1] // the TLD
	}
	word := strings.ToLower(f.names.Fake(strings.Join(labels, "-")))
	word = strings.Trim(regexp.MustCompile(`[^a-z0-9\-]+`).ReplaceAllString(word, "-"), "-")
	if word == "" {
		word = "site"
	}
	out := "https://www." + word + ".example.org"
	if v == strings.ToUpper(v) {
		out = strings.ToUpper(out)
	}
	return out
}

// --- amounts ---

// PerturbCents moves an amount (in cents) by a keyed factor within ±15%,
// the same amount always moving the same way. 0 stays 0.
func PerturbCents(key []byte, cents int64) int64 {
	if cents == 0 {
		return 0
	}
	k := keyed{derive(key, "xdb-fake/amount/v1")}
	x := k.u64("amount", strconv.FormatInt(cents, 10), 0)
	factor := 0.85 + float64(x%30001)/100000 // 0.85 .. 1.15
	out := int64(float64(cents)*factor + 0.5)
	if cents < 0 {
		out = int64(float64(cents)*factor - 0.5)
	}
	if out == cents {
		out++
	}
	return out
}

// --- Arrow helpers ---

// FakeTextArray applies fn to every non-NULL value of a string column;
// changed counts the values that changed.
func FakeTextArray(col arrow.Array, fn func(string) string, alloc memory.Allocator) (arrow.Array, int64, error) {
	return mapStrings(col, fn, alloc)
}

// FakeNumberArray fakes a whole-number column (int32, int64, decimal of
// scale 0) or a text one with numberFn / textFn. 0, NULL and blank stay; a
// value the fake cannot read becomes 0 (numbers) or NULL (text), counted
// unreadable.
func FakeNumberArray(in arrow.Array, numberFn func(uint64) (uint64, bool), textFn func(string) (string, bool), alloc memory.Allocator) (arrow.Array, IdentifierCounts, error) {
	var n IdentifierCounts
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	if dt, ok := in.DataType().(*arrow.Decimal128Type); ok && dt.Scale != 0 {
		return nil, n, fmt.Errorf("mask: a decimal with scale %d cannot be faked as a number", dt.Scale)
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
			if out, ok := textFn(v); ok {
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
				if out, ok = numberFn(v); ok {
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
			return nil, n, fmt.Errorf("mask: a %s column cannot be faked as a number", in.DataType())
		}
	}
	return b.NewArray(), n, nil
}

// PerturbAmountArray perturbs a money column: decimal (any scale: values are
// moved in units of the column's last digit, cents at scale 2) or float64.
func PerturbAmountArray(in arrow.Array, key []byte, alloc memory.Allocator) (arrow.Array, int64, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	var changed int64
	switch a := in.(type) {
	case *array.Decimal128:
		b := array.NewDecimal128Builder(alloc, a.DataType().(*arrow.Decimal128Type))
		defer b.Release()
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			x := a.Value(i)
			if !x.FitsInPrecision(18) {
				b.Append(x)
				continue
			}
			v := int64(x.LowBits())
			if x.Sign() < 0 {
				v = -int64(x.Negate().LowBits())
			}
			out := PerturbCents(key, v)
			if out != v {
				changed++
			}
			b.Append(decimal128.FromI64(out))
		}
		return b.NewArray(), changed, nil
	case *array.Float64:
		b := array.NewFloat64Builder(alloc)
		defer b.Release()
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			c := int64(a.Value(i)*100 + 0.5)
			if a.Value(i) < 0 {
				c = int64(a.Value(i)*100 - 0.5)
			}
			out := PerturbCents(key, c)
			if out != c {
				changed++
			}
			b.Append(float64(out) / 100)
		}
		return b.NewArray(), changed, nil
	}
	return nil, 0, fmt.Errorf("mask: a %s column cannot be perturbed as an amount", in.DataType())
}
