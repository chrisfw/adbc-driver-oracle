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
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestParseOracleIntervalDS(t *testing.T) {
	cases := []struct {
		in     string
		want   time.Duration
		wantOK bool
	}{
		// The exact shape go-ora produces, observed against Oracle Free.
		{"+03 04:05:06.789000", 3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second + 789*time.Millisecond, true},
		{"+00 00:00:00.000000", 0, true},
		{"-03 04:05:06.789000", -(3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second + 789*time.Millisecond), true},
		// No sign, and no fraction, are both legal renderings.
		{"03 04:05:06", 3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second, true},
		{"+01 00:00:00", 24 * time.Hour, true},
		// Fractions are padded to nanoseconds, not read as a raw integer.
		{"+00 00:00:00.5", 500 * time.Millisecond, true},
		{"+00 00:00:00.000000001", time.Nanosecond, true},
		{"+00 00:00:00.123456789", 123456789 * time.Nanosecond, true},
		// Wide day fields follow the declared precision, and are accepted up
		// to what a duration holds.
		{"+106751 00:00:00", 106751 * 24 * time.Hour, true},
		// INTERVAL DAY(9) TO SECOND reaches 999999999 days, which a duration
		// cannot represent. Refused rather than wrapped.
		{"+999999999 23:59:59", 0, false},
		{"+106752 00:00:00", 0, false},

		// Anything unrecognised must be refused, so the caller nulls rather
		// than storing a plausible wrong number.
		{"", 0, false},
		{"   ", 0, false},
		{"garbage", 0, false},
		{"+03", 0, false},
		{"+03 04:05", 0, false},
		{"+03 04:05:06:07", 0, false},
		{"+03 24:00:00", 0, false}, // hours out of range
		{"+03 00:60:00", 0, false}, // minutes out of range
		{"+03 00:00:60", 0, false}, // seconds out of range
		{"+03 0a:00:00", 0, false},
		{"+03 00:00:00.", 0, false},
		{"+03 00:00:00.abc", 0, false},
		{"+03 00:00:00.1234567890", 0, false}, // more digits than exist
		{"+-03 00:00:00", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, ok := parseOracleIntervalDS(tc.in)
			if ok != tc.wantOK {
				t.Fatalf("parseOracleIntervalDS(%q) ok = %v, want %v (got %v)", tc.in, ok, tc.wantOK, got)
			}
			if ok && got != tc.want {
				t.Errorf("parseOracleIntervalDS(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseOracleIntervalYM(t *testing.T) {
	cases := []struct {
		in     string
		want   int32
		wantOK bool
	}{
		{"+07-03", 7*12 + 3, true}, // the shape go-ora produces
		{"+00-00", 0, true},
		{"-07-03", -(7*12 + 3), true},
		{"07-03", 7*12 + 3, true},
		{"+00-11", 11, true},
		{"+000000001-00", 12, true},

		{"", 0, false},
		{"garbage", 0, false},
		{"+07", 0, false},
		{"+07-12", 0, false}, // months out of range
		{"+07-99", 0, false},
		{"+07-ab", 0, false},
		{"+ab-03", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, ok := parseOracleIntervalYM(tc.in)
			if ok != tc.wantOK {
				t.Fatalf("parseOracleIntervalYM(%q) ok = %v, want %v (got %v)", tc.in, ok, tc.wantOK, got)
			}
			if ok && got != tc.want {
				t.Errorf("parseOracleIntervalYM(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// The conversion from Go's nanosecond duration into the column's unit has to
// match that unit. Emitted columns are always microseconds (go-ora caps the
// interval fraction at 6 digits, so nanoseconds could never be filled), but
// the converter is unit-driven and both branches are exercised here so the
// arithmetic is pinned if that ever changes.
func TestAppendIntervalDSUnits(t *testing.T) {
	cases := []struct {
		unit arrow.TimeUnit
		in   string
		want int64
	}{
		{arrow.Microsecond, "+00 00:00:01.500000", 1500000},
		{arrow.Nanosecond, "+00 00:00:01.500000", 1500000000},
		// Sub-microsecond digits survive only under nanoseconds, which is the
		// whole reason the unit is selectable.
		{arrow.Microsecond, "+00 00:00:00.000000123", 0},
		{arrow.Nanosecond, "+00 00:00:00.000000123", 123},
	}
	for _, tc := range cases {
		b := array.NewDurationBuilder(memory.DefaultAllocator, &arrow.DurationType{Unit: tc.unit})
		appendIntervalDS(b, tc.in)
		arr := b.NewDurationArray()
		if arr.Len() != 1 || arr.IsNull(0) {
			t.Errorf("%v %q: no value appended", tc.unit, tc.in)
		} else if int64(arr.Value(0)) != tc.want {
			t.Errorf("%v %q = %d, want %d", tc.unit, tc.in, arr.Value(0), tc.want)
		}
		arr.Release()
		b.Release()
	}
}

// An unparseable interval must become a null, never a wrong number.
func TestAppendIntervalRejectsGarbage(t *testing.T) {
	db := array.NewDurationBuilder(memory.DefaultAllocator, &arrow.DurationType{Unit: arrow.Microsecond})
	defer db.Release()
	appendIntervalDS(db, "not an interval")
	appendIntervalDS(db, 42) // wrong Go type
	dArr := db.NewDurationArray()
	defer dArr.Release()
	for i := 0; i < dArr.Len(); i++ {
		if !dArr.IsNull(i) {
			t.Errorf("duration %d = %v, want null", i, dArr.Value(i))
		}
	}

	mb := array.NewMonthIntervalBuilder(memory.DefaultAllocator)
	defer mb.Release()
	appendIntervalYM(mb, "not an interval")
	appendIntervalYM(mb, 1.5)
	mArr := mb.NewMonthIntervalArray()
	defer mArr.Release()
	for i := 0; i < mArr.Len(); i++ {
		if !mArr.IsNull(i) {
			t.Errorf("month interval %d = %v, want null", i, mArr.Value(i))
		}
	}
}
