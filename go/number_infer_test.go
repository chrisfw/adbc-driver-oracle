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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestNumberDigitsObserve(t *testing.T) {
	cases := []struct {
		name     string
		values   []string
		wantInt  int
		wantFrac int
		wantBad  bool
	}{
		{"plain integers", []string{"1", "22", "333"}, 3, 0, false},
		{"scale from the widest", []string{"1.5", "2.25", "3.1"}, 1, 2, false},
		{"sign ignored", []string{"-1.25", "+2.5"}, 1, 2, false},
		// Leading zeros say nothing about magnitude, trailing fraction zeros
		// nothing about scale, or every column would infer a huge scale.
		{"leading zeros ignored", []string{"0007.5"}, 1, 1, false},
		{"trailing fraction zeros ignored", []string{"1.500000"}, 1, 1, false},
		{"zero", []string{"0"}, 0, 0, false},
		{"fraction only", []string{"0.001"}, 0, 3, false},
		// 126 integer digits is what Oracle renders for 9.99e125.
		{"very wide integer", []string{"9" + strings.Repeat("0", 125)}, 126, 0, false},
		// Anything that is not plain decimal text stops inference, rather
		// than being guessed around.
		{"exponent gives up", []string{"1.5", "1e10"}, 0, 0, true},
		{"junk gives up", []string{"abc"}, 0, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var d numberDigits
			for _, v := range tc.values {
				d.observe(v)
			}
			if d.unparseable != tc.wantBad {
				t.Fatalf("unparseable = %v, want %v", d.unparseable, tc.wantBad)
			}
			if tc.wantBad {
				return
			}
			if d.maxIntDigits != tc.wantInt || d.maxFracDigits != tc.wantFrac {
				t.Errorf("got (int %d, frac %d), want (int %d, frac %d)",
					d.maxIntDigits, d.maxFracDigits, tc.wantInt, tc.wantFrac)
			}
		})
	}

	// An empty sample is not the same as a sample of small values: it means
	// nothing was learned, so no type can be inferred from it.
	var empty numberDigits
	if inferredNumberType(empty) != nil {
		t.Error("an empty sample produced a type; it should decline")
	}
	var nullsOnly numberDigits
	nullsOnly.observe("")
	if nullsOnly.seen != 0 {
		t.Error("an empty string counted as a value")
	}
}

func TestInferredNumberType(t *testing.T) {
	mk := func(i, f, seen int) numberDigits {
		return numberDigits{maxIntDigits: i, maxFracDigits: f, seen: seen}
	}
	cases := []struct {
		name string
		d    numberDigits
		want arrow.DataType
	}{
		// Precision is the type maximum, not a fit, so later rows have room.
		{"small fits decimal128", mk(5, 2, 10),
			&arrow.Decimal128Type{Precision: 38, Scale: 2}},
		{"exactly 38 digits", mk(36, 2, 10),
			&arrow.Decimal128Type{Precision: 38, Scale: 2}},
		{"39 digits needs decimal256", mk(37, 2, 10),
			&arrow.Decimal256Type{Precision: 76, Scale: 2}},
		{"exactly 76 digits", mk(70, 6, 10),
			&arrow.Decimal256Type{Precision: 76, Scale: 6}},
		// Oracle reaches 126 integer digits; no Arrow decimal does.
		{"too wide for any decimal", mk(126, 0, 10), nil},
		{"wide fraction too", mk(10, 70, 10), nil},
		// Nothing learned, nothing inferred.
		{"empty sample", mk(0, 0, 0), nil},
		{"unparseable", numberDigits{seen: 10, unparseable: true}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := inferredNumberType(tc.d)
			if tc.want == nil {
				if got != nil {
					t.Errorf("got %s, want a declined inference", got)
				}
				return
			}
			if got == nil || !arrow.TypeEqual(got, tc.want) {
				t.Errorf("got %v, want %s", got, tc.want)
			}
		})
	}
}

// The property that matters most: a value is never rounded into the inferred
// scale. Arrow would do exactly that without complaint, so decimalFits is what
// stands between a long fraction and a silently wrong number.
func TestDecimalFitsRefusesWhatArrowWouldRound(t *testing.T) {
	cases := []struct {
		text             string
		precision, scale int32
		want             bool
	}{
		{"1.25", 38, 2, true},
		{"1.2", 38, 2, true},
		{"1", 38, 2, true},
		{"-1.25", 38, 2, true},
		// Trailing zeros past the scale are not a loss.
		{"1.2500", 38, 2, true},
		// These are what Arrow silently rounds.
		{"1.239", 38, 2, false},
		{"1.2345678901234567890123456789", 38, 2, false},
		// Integer part past the precision.
		{strings.Repeat("9", 39), 38, 0, false},
		{strings.Repeat("9", 37), 38, 2, false},
		{strings.Repeat("9", 36), 38, 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.text+"/"+string(rune('0'+tc.scale)), func(t *testing.T) {
			if got := decimalFits(tc.text, tc.precision, tc.scale); got != tc.want {
				t.Errorf("decimalFits(%q, %d, %d) = %v, want %v",
					tc.text, tc.precision, tc.scale, got, tc.want)
			}
		})
	}

	// Demonstrate the thing being prevented, so the test records why the
	// check exists rather than just asserting it.
	dt := &arrow.Decimal128Type{Precision: 38, Scale: 2}
	b := array.NewDecimal128Builder(memory.DefaultAllocator, dt)
	defer b.Release()
	if err := b.AppendValueFromString("1.239"); err != nil {
		t.Fatalf("arrow unexpectedly refused the value: %v", err)
	}
	arr := b.NewDecimal128Array()
	defer arr.Release()
	if got := arr.ValueStr(0); got == "1.239" {
		t.Skip("arrow no longer rounds; decimalFits could be relaxed")
	} else {
		t.Logf("confirmed: arrow stored %q for 1.239 without error, which is why decimalFits exists", got)
	}
}

func TestCompactDecimalType(t *testing.T) {
	cases := []struct {
		precision, scale int32
		want             arrow.DataType
	}{
		{1, 0, &arrow.Decimal32Type{Precision: 1, Scale: 0}},
		{9, 2, &arrow.Decimal32Type{Precision: 9, Scale: 2}},
		{10, 2, &arrow.Decimal64Type{Precision: 10, Scale: 2}},
		{18, 4, &arrow.Decimal64Type{Precision: 18, Scale: 4}},
		{19, 4, &arrow.Decimal128Type{Precision: 19, Scale: 4}},
		{38, 10, &arrow.Decimal128Type{Precision: 38, Scale: 10}},
	}
	for _, tc := range cases {
		got := compactDecimalType(tc.precision, tc.scale)
		if !arrow.TypeEqual(got, tc.want) {
			t.Errorf("compactDecimalType(%d,%d) = %s, want %s", tc.precision, tc.scale, got, tc.want)
		}
	}
}
