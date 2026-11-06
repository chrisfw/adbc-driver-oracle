package mask

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestFakeNames(t *testing.T) {
	f, err := NewFakeNames(testKey)
	if err != nil {
		t.Fatal(err)
	}
	last, first := f.Fake("SMITH"), f.Fake("JOHN")
	// The same word maps the same way in every column shape.
	for in, want := range map[string]string{
		"SMITH":          last,
		"JOHN":           first,
		"SMITH JOHN":     last + " " + first,
		"SMITH, JOHN":    last + ", " + first,
		"John Smith":     caseLike("John", first) + " " + caseLike("Smith", last),
		"john smith":     strings.ToLower(first) + " " + strings.ToLower(last),
		"SMITH JOHN JR":  last + " " + first + " JR",
		"MR JOHN SMITH":  "MR " + first + " " + last,
		"SMITH-JOHN":     last + "-" + first,
		"SMITH & SMITH":  last + " & " + last,
		"SMITH JOHN 2ND": last + " " + first + " 2" + f.Fake("ND"),
		"":               "",
		"  ":             "  ",
	} {
		if got := f.Fake(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
	if last == "SMITH" || first == "JOHN" || f.Fake("A") == "A" {
		t.Fatal("a fake equals its word")
	}
	if len(f.Fake("Q")) != 1 {
		t.Fatal("an initial is not a letter")
	}
	other, _ := NewFakeNames([]byte("another-key-another-key-another!"))
	if other.Fake("SMITH WILLIAMS JOHNSON") == f.Fake("SMITH WILLIAMS JOHNSON") {
		t.Fatal("fakes do not depend on the key")
	}
	// Lengths stay close (same length when the lists have one).
	if n := len(f.Fake("BROWN")); n != 5 {
		t.Errorf("BROWN -> %d letters", n)
	}
	if _, err := NewFakeNames([]byte("short")); err == nil {
		t.Fatal("a short key was accepted")
	}
}

func TestFakeNameArray(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	f, _ := NewFakeNames(testKey)
	b := array.NewStringBuilder(alloc)
	b.AppendValues([]string{"DOE JANE", ""}, nil)
	b.AppendNull()
	in := b.NewArray()
	b.Release()
	defer in.Release()
	out, changed, err := FakeNameArray(in, f, alloc)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	s := out.(*array.String)
	if s.Value(0) == "DOE JANE" || s.Value(1) != "" || !s.IsNull(2) || changed != 1 {
		t.Fatalf("%v changed=%d", s, changed)
	}
}
