// Copyright 2025 CARTO
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package oracle

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The fast path has to agree with the general one exactly. Anything else is a
// silently wrong number, which is worse than the slow parse it replaces, so
// this compares the two across the shapes go-ora actually produces plus the
// edges.
func TestParseDecimal128AgreesWithArrow(t *testing.T) {
	cases := []struct {
		s     string
		scale int32
	}{
		// Plain values at the scale of the column.
		{"0", 2}, {"0.00", 2}, {"1.00", 2}, {"-1.00", 2},
		{"123.45", 2}, {"-123.45", 2}, {"99999999.99", 2},
		// Fewer fraction digits than the scale, which needs zero padding.
		{"1", 2}, {"1.5", 2}, {"-1.5", 2}, {"1.5", 6},
		// Scale zero.
		{"0", 0}, {"42", 0}, {"-42", 0}, {"999999999999999999", 0},
		// Leading zeros and signs.
		{"007.10", 2}, {"+7.10", 2}, {"-0.01", 2}, {"0.01", 2},
		{"-0", 2}, {"+0", 0},
		// The int64 digit budget, from both sides.
		{"999999999999999.999", 3},
		{"99999999999999999", 0},
		{"-99999999999999999", 0},
		// High scale with a small integer part.
		{"0.000001", 6}, {"-0.000001", 6},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s@%d", tc.s, tc.scale), func(t *testing.T) {
			got, ok := parseDecimal128(tc.s, tc.scale)
			if !ok {
				t.Skipf("fast path declined %q at scale %d, which is allowed", tc.s, tc.scale)
			}
			dt := &arrow.Decimal128Type{Precision: 38, Scale: tc.scale}
			b := array.NewDecimal128Builder(memory.DefaultAllocator, dt)
			defer b.Release()
			if err := b.AppendValueFromString(tc.s); err != nil {
				t.Fatalf("arrow rejected %q at scale %d: %v", tc.s, tc.scale, err)
			}
			arr := b.NewDecimal128Array()
			defer arr.Release()
			want := arr.Value(0)
			if got != want {
				t.Errorf("parseDecimal128(%q, %d) = %v, arrow says %v", tc.s, tc.scale, got, want)
			}
		})
	}
}

// Everything the fast path refuses must still be handled, by falling through
// to the general parse. These are refusals, not errors.
func TestParseDecimal128Declines(t *testing.T) {
	cases := []struct {
		name  string
		s     string
		scale int32
	}{
		{"empty", "", 2},
		{"sign only", "-", 2},
		{"exponent", "1.5e10", 2},
		{"capital exponent", "1E5", 0},
		{"two points", "1.2.3", 2},
		{"letters", "abc", 2},
		{"trailing space", "1.00 ", 2},
		{"leading space", " 1.00", 2},
		{"hex", "0x10", 0},
		{"negative scale", "1.00", -2},
		// More fraction digits than the column's scale would need rounding.
		{"fraction longer than scale", "1.234", 2},
		// Past what an int64 holds, so the general path must take it.
		{"19 digits", "1234567890123456789", 0},
		{"38 digits", strings.Repeat("9", 38), 0},
		{"wide with scale", strings.Repeat("9", 20) + ".99", 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := parseDecimal128(tc.s, tc.scale); ok {
				t.Errorf("parseDecimal128(%q, %d) accepted it; the general path should take this",
					tc.s, tc.scale)
			}
		})
	}
}

// appendDecimal128 must produce the same column whichever path a value takes,
// so a batch mixing fast and declined values is still correct throughout.
func TestAppendDecimal128MixedPaths(t *testing.T) {
	dt := &arrow.Decimal128Type{Precision: 38, Scale: 2}
	values := []string{
		"1.00",                          // fast
		strings.Repeat("9", 30) + ".99", // declined, needs big
		"-2.50",                         // fast
		"1.234",                         // declined, would round
		"0.00",                          // fast
	}

	viaAppend := array.NewDecimal128Builder(memory.DefaultAllocator, dt)
	defer viaAppend.Release()
	viaArrow := array.NewDecimal128Builder(memory.DefaultAllocator, dt)
	defer viaArrow.Release()

	for _, v := range values {
		appendDecimal128(viaAppend, v)
		_ = viaArrow.AppendValueFromString(v)
	}
	gotArr := viaAppend.NewDecimal128Array()
	defer gotArr.Release()
	wantArr := viaArrow.NewDecimal128Array()
	defer wantArr.Release()

	if gotArr.Len() != wantArr.Len() {
		t.Fatalf("lengths differ: %d vs %d", gotArr.Len(), wantArr.Len())
	}
	for i := range values {
		if gotArr.IsNull(i) != wantArr.IsNull(i) {
			t.Errorf("%q: null-ness differs", values[i])
			continue
		}
		if !gotArr.IsNull(i) && gotArr.Value(i) != wantArr.Value(i) {
			t.Errorf("%q: got %v, arrow says %v", values[i], gotArr.Value(i), wantArr.Value(i))
		}
	}
}

// The fast path must not allocate, which is the reason it exists.
func TestParseDecimal128DoesNotAllocate(t *testing.T) {
	allocs := testing.AllocsPerRun(200, func() {
		if _, ok := parseDecimal128("-123456.78", 2); !ok {
			t.Fatal("fast path declined a value it should take")
		}
	})
	if allocs > 0 {
		t.Errorf("parseDecimal128 allocated %.1f times per call, want 0", allocs)
	}
}
