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

import dec128 "github.com/apache/arrow-go/v18/arrow/decimal128"

// go-ora renders every NUMBER as a decimal string, so a Decimal128 column is
// built by parsing text back into an integer. Arrow's AppendValueFromString
// does that through math/big, which on a profile of a twenty column
// NUMBER(12,2) read was 22% of CPU and most of the seventeen allocations per
// value. Almost all real NUMBER columns are far narrower than Decimal128
// allows, so the common case can be parsed into an int64 with no allocation at
// all.
//
// parseDecimal128 handles exactly that case and refuses everything else, so
// the caller falls back to the general path rather than this growing into a
// second, subtly different decimal parser.

// maxFastDecimalDigits is the most significant digits that fit an int64. 19
// digits can overflow, 18 never do.
const maxFastDecimalDigits = 18

// parseDecimal128 parses a plain decimal string into a Decimal128 at the given
// scale, without math/big.
//
// Accepts an optional sign, digits, and at most one decimal point. Returns
// false for anything else, including exponents, a fraction longer than the
// column's scale (which would require rounding, and silently rounding is worse
// than being slow), and values needing more than 18 significant digits.
func parseDecimal128(s string, scale int32) (dec128.Num, bool) {
	if s == "" || scale < 0 {
		return dec128.Num{}, false
	}

	neg := false
	switch s[0] {
	case '-':
		neg = true
		s = s[1:]
	case '+':
		s = s[1:]
	}
	if s == "" {
		return dec128.Num{}, false
	}

	// Split on the single permitted decimal point, rejecting a second one.
	intPart, fracPart := s, ""
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '.' {
			intPart, fracPart = s[:i], s[i+1:]
			for j := 0; j < len(fracPart); j++ {
				if fracPart[j] < '0' || fracPart[j] > '9' {
					return dec128.Num{}, false
				}
			}
			break
		}
		if c < '0' || c > '9' {
			// Covers 'e', 'E', 'x', spaces and anything else unexpected.
			return dec128.Num{}, false
		}
	}

	// A fraction longer than the scale cannot be represented without
	// rounding; hand it to the general path.
	if int32(len(fracPart)) > scale {
		return dec128.Num{}, false
	}

	// Leading zeros do not count against the digit budget.
	for len(intPart) > 1 && intPart[0] == '0' {
		intPart = intPart[1:]
	}
	if intPart == "0" {
		intPart = ""
	}

	// The unscaled value is the integer digits, the fraction digits, then
	// enough zeros to reach the column's scale.
	pad := int(scale) - len(fracPart)
	if len(intPart)+len(fracPart)+pad > maxFastDecimalDigits {
		return dec128.Num{}, false
	}

	var v int64
	for i := 0; i < len(intPart); i++ {
		v = v*10 + int64(intPart[i]-'0')
	}
	for i := 0; i < len(fracPart); i++ {
		v = v*10 + int64(fracPart[i]-'0')
	}
	for i := 0; i < pad; i++ {
		v *= 10
	}

	if neg {
		v = -v
	}
	return dec128.FromI64(v), true
}
