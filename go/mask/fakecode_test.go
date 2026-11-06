package mask

import (
	"testing"
	"unicode"
)

// Synthetic license-shaped values only.
func TestFakeCodes(t *testing.T) {
	f, err := NewFakeCodes(testKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range []string{"S123-4567-8901", "A1234567", "123456789", "D12 34 567", "ab-12cd", "X"} {
		out := f.Fake(in)
		if out == in || len(out) != len(in) {
			t.Errorf("%q -> %q", in, out)
			continue
		}
		for i := range in {
			a, b := rune(in[i]), rune(out[i])
			switch {
			case unicode.IsDigit(a) && !unicode.IsDigit(b),
				unicode.IsUpper(a) && !unicode.IsUpper(b),
				unicode.IsLower(a) && !unicode.IsLower(b),
				!unicode.IsLetter(a) && !unicode.IsDigit(a) && a != b:
				t.Errorf("%q -> %q: position %d changed class", in, out, i)
			}
		}
		if f.Fake(in) != out {
			t.Errorf("%q is not deterministic", in)
		}
	}
	// Case does not change the code's identity: the same license typed in
	// either case gets the same characters.
	if f.Fake("ab12") != "ab12" && f.Fake("AB12") == f.Fake("ab12") {
		t.Error("case was not kept")
	}
	for _, in := range []string{"", "   ", "--/"} {
		if f.Fake(in) != in {
			t.Errorf("%q changed", in)
		}
	}
	other, _ := NewFakeCodes([]byte("another-key-another-key-another!"))
	if other.Fake("A1234567") == f.Fake("A1234567") {
		t.Fatal("fakes do not depend on the key")
	}
	if _, err := NewFakeCodes([]byte("short")); err == nil {
		t.Fatal("a short key was accepted")
	}
}
