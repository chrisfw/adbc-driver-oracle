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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// decimal128 is the type numberToArrow is expected to produce for a
// representable NUMBER(p,s).
func decimal128(precision, scale int32) arrow.DataType {
	return &arrow.Decimal128Type{Precision: precision, Scale: scale}
}

func TestNumberToArrow(t *testing.T) {
	// precision/scale here are what go-ora reports through
	// sql.ColumnType.DecimalSize, not the DDL values: an unconstrained NUMBER
	// arrives as (38, 255) and NUMBER(5,-3) as (5, 253).
	tests := []struct {
		name      string
		precision int64
		scale     int64
		ok        bool
		want      map[string]arrow.DataType // mapping mode → expected type
	}{
		{
			name: "NUMBER(10,0) fits int64", precision: 10, scale: 0, ok: true,
			want: map[string]arrow.DataType{
				NumberMappingFloat:   arrow.PrimitiveTypes.Int64,
				NumberMappingDecimal: arrow.PrimitiveTypes.Int64,
				NumberMappingString:  arrow.BinaryTypes.String,
			},
		},
		{
			name: "NUMBER(18,0) is the int64 boundary", precision: 18, scale: 0, ok: true,
			want: map[string]arrow.DataType{
				NumberMappingFloat:   arrow.PrimitiveTypes.Int64,
				NumberMappingDecimal: arrow.PrimitiveTypes.Int64,
				NumberMappingString:  arrow.BinaryTypes.String,
			},
		},
		{
			// The headline fix: integral NUMBER too wide for int64 arrived as
			// text under the float policy.
			name: "NUMBER(38,0) exceeds int64", precision: 38, scale: 0, ok: true,
			want: map[string]arrow.DataType{
				NumberMappingFloat:   arrow.BinaryTypes.String,
				NumberMappingDecimal: decimal128(38, 0),
				NumberMappingString:  arrow.BinaryTypes.String,
			},
		},
		{
			// The other headline fix: money-shaped columns were binary floats.
			name: "NUMBER(10,2) is scaled", precision: 10, scale: 2, ok: true,
			want: map[string]arrow.DataType{
				NumberMappingFloat:   arrow.PrimitiveTypes.Float64,
				NumberMappingDecimal: decimal128(10, 2),
				NumberMappingString:  arrow.BinaryTypes.String,
			},
		},
		{
			name: "NUMBER(38,38) is all fraction", precision: 38, scale: 38, ok: true,
			want: map[string]arrow.DataType{
				NumberMappingFloat:   arrow.PrimitiveTypes.Float64,
				NumberMappingDecimal: decimal128(38, 38),
				NumberMappingString:  arrow.BinaryTypes.String,
			},
		},
		{
			// go-ora rewrites unconstrained NUMBER to (38, 0xFF). Decimal128
			// has no scale to use, so the policy is the lossless String rather
			// than a silent fall-through to Float64.
			name: "unconstrained NUMBER hits the scale sentinel", precision: 38, scale: oracleScaleSentinel, ok: true,
			want: map[string]arrow.DataType{
				NumberMappingFloat:   arrow.PrimitiveTypes.Float64,
				NumberMappingDecimal: arrow.BinaryTypes.String,
				NumberMappingString:  arrow.BinaryTypes.String,
			},
		},
		{
			// NUMBER(5,-3): the declared scale wraps through go-ora's uint8 to
			// 253, which must not be read as a huge positive scale.
			name: "negative declared scale wraps", precision: 5, scale: 253, ok: true,
			want: map[string]arrow.DataType{
				NumberMappingFloat:   arrow.PrimitiveTypes.Float64,
				NumberMappingDecimal: arrow.BinaryTypes.String,
				NumberMappingString:  arrow.BinaryTypes.String,
			},
		},
		{
			name: "precision unavailable", precision: 0, scale: 0, ok: false,
			want: map[string]arrow.DataType{
				NumberMappingFloat:   arrow.PrimitiveTypes.Float64,
				NumberMappingDecimal: arrow.BinaryTypes.String,
				NumberMappingString:  arrow.BinaryTypes.String,
			},
		},
		{
			name: "precision beyond decimal128", precision: 40, scale: 2, ok: true,
			want: map[string]arrow.DataType{
				NumberMappingFloat:   arrow.PrimitiveTypes.Float64,
				NumberMappingDecimal: arrow.BinaryTypes.String,
				NumberMappingString:  arrow.BinaryTypes.String,
			},
		},
		{
			// Not producible by Oracle, but Parquet rejects scale > precision,
			// so it must not become a decimal that fails to serialise later.
			name: "scale exceeds precision", precision: 4, scale: 6, ok: true,
			want: map[string]arrow.DataType{
				NumberMappingFloat:   arrow.PrimitiveTypes.Float64,
				NumberMappingDecimal: arrow.BinaryTypes.String,
				NumberMappingString:  arrow.BinaryTypes.String,
			},
		},
	}

	for _, tt := range tests {
		for mode, want := range tt.want {
			t.Run(tt.name+"/"+mode, func(t *testing.T) {
				got := numberToArrow(tt.precision, tt.scale, tt.ok, mode)
				if !arrow.TypeEqual(got, want) {
					t.Errorf("numberToArrow(%d, %d, %v, %q) = %s, want %s",
						tt.precision, tt.scale, tt.ok, mode, got, want)
				}
			})
		}
	}
}

// The empty mapping is what a database that never saw the option produces;
// it must behave exactly as the documented default.
func TestNumberToArrow_UnsetMappingMatchesFloat(t *testing.T) {
	cases := []struct{ precision, scale int64 }{
		{10, 0}, {38, 0}, {10, 2}, {38, oracleScaleSentinel},
	}
	for _, c := range cases {
		unset := numberToArrow(c.precision, c.scale, true, "")
		explicit := numberToArrow(c.precision, c.scale, true, NumberMappingFloat)
		if !arrow.TypeEqual(unset, explicit) {
			t.Errorf("NUMBER(%d,%d): unset mapping = %s, %q = %s",
				c.precision, c.scale, unset, NumberMappingFloat, explicit)
		}
	}
}

func TestAppendValue_Decimal128(t *testing.T) {
	dt := &arrow.Decimal128Type{Precision: 20, Scale: 4}
	b := array.NewDecimal128Builder(memory.DefaultAllocator, dt)
	defer b.Release()

	// The string case is the one that runs for a real NUMBER column: go-ora
	// hands over a full-precision decimal string. 1234567890123456.7891 has 20
	// significant digits — more than float64 can hold, so a Float64 round-trip
	// would corrupt it.
	appendValue(b, "1234567890123456.7891", nil)
	appendValue(b, int64(42), nil)
	appendValue(b, 3.5, nil)
	appendValue(b, []byte("-0.0001"), nil)
	appendValue(b, nil, nil)
	appendValue(b, "not-a-number", nil)

	arr := b.NewDecimal128Array()
	defer arr.Release()

	if arr.Len() != 6 {
		t.Fatalf("expected 6 values, got %d", arr.Len())
	}

	// ValueStr renders the decimal without padding to the full scale.
	want := []string{"1234567890123456.7891", "42", "3.5", "-0.0001"}
	for i, w := range want {
		if arr.IsNull(i) {
			t.Errorf("index %d: unexpected null", i)
			continue
		}
		if got := arr.ValueStr(i); got != w {
			t.Errorf("index %d: got %s, want %s", i, got, w)
		}
	}
	if !arr.IsNull(4) {
		t.Error("nil should append null")
	}
	// AppendValueFromString appends the null itself on a parse failure; the
	// converter must not append a second one.
	if !arr.IsNull(5) {
		t.Error("unparseable string should append null")
	}
}
