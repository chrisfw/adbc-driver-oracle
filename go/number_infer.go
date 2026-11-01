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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
)

// An Oracle NUMBER declared without precision and scale has a per-value
// scale, and no Arrow decimal can hold that: all four of Decimal32, Decimal64,
// Decimal128 and Decimal256 carry one fixed (precision, scale) for the whole
// column, because the columnar format requires every value in a column to
// share a scale. So such a column used to come back as text, which is
// lossless and useless.
//
// What can be done instead is look at the data. go-ora renders NUMBER as a
// plain decimal string with no exponent, so sampling the first rows of a
// result gives the digits actually present, and a real decimal type can be
// chosen from them. This is the same bargain the ingest path already strikes
// when it sizes VARCHAR2 columns from the data it is about to write.
//
// The sample cannot promise anything about rows beyond it. Arrow emits one
// schema per stream and every batch must match it, and driverbase's own
// schema-update hook validates that an updated schema differs only in
// metadata, so the column type is frozen once the first batch is handed over.
// Promotion mid-stream is therefore not available at any price, and the
// mitigation is to infer conservatively rather than tightly: take the scale
// from the sample and the precision from the type's maximum, so overflow needs
// a value far outside anything the sample suggested. What remains is handled
// by numberOverflow.

// numberDigits is what a sample says about one unconstrained NUMBER column.
type numberDigits struct {
	// maxIntDigits and maxFracDigits are the widest integer and fraction
	// parts seen, ignoring sign and leading zeros.
	maxIntDigits  int
	maxFracDigits int
	// seen counts non-null values examined. Zero means the sample said
	// nothing, which is different from saying everything was small.
	seen int
	// unparseable records a value that was not a plain decimal. Inference
	// gives up on the column rather than guess around it.
	unparseable bool
}

// observe folds one rendered NUMBER into the running digit counts.
func (d *numberDigits) observe(s string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return
	}
	if s[0] == '-' || s[0] == '+' {
		s = s[1:]
	}
	if s == "" {
		d.unparseable = true
		return
	}

	intPart, fracPart, hasPoint := strings.Cut(s, ".")
	if !allDigits(intPart) || (hasPoint && !allDigits(fracPart)) {
		// An exponent, a NaN, anything unexpected: stop trusting the column.
		d.unparseable = true
		return
	}

	// Leading zeros carry no information about the magnitude.
	trimmed := strings.TrimLeft(intPart, "0")
	if n := len(trimmed); n > d.maxIntDigits {
		d.maxIntDigits = n
	}
	// Trailing zeros in the fraction do not need to be preserved, but a
	// non-zero digit at position k means the scale must reach k.
	frac := strings.TrimRight(fracPart, "0")
	if n := len(frac); n > d.maxFracDigits {
		d.maxFracDigits = n
	}
	d.seen++
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Arrow's decimal widths. Decimal32 and Decimal64 are only reachable through
// the compact-decimals option, since the C data interface spells a non-128
// decimal as "d:p,s,bits" and a consumer that only parses 128-bit decimals
// rejects it.
const (
	maxDecimal32Precision  = 9
	maxDecimal64Precision  = 18
	maxDecimal256Precision = 76
)

// inferredNumberType picks the Arrow type for an unconstrained NUMBER column
// from a sample, or nil when the sample cannot support a decimal and the
// column should stay text.
//
// Precision is the type's maximum rather than a tight fit. A tight fit would
// save sixteen bytes a value against Decimal256 and would make overflow far
// more likely, which is the wrong way round: the value of inference here is
// getting a number at all, not getting the narrowest one.
func inferredNumberType(d numberDigits) arrow.DataType {
	if d.unparseable || d.seen == 0 {
		return nil
	}

	scale := d.maxFracDigits
	// Every digit has to fit: the integer part, plus the scale.
	needed := d.maxIntDigits + scale
	switch {
	case needed <= maxDecimal128Precision:
		return &arrow.Decimal128Type{Precision: maxDecimal128Precision, Scale: int32(scale)}
	case needed <= maxDecimal256Precision:
		return &arrow.Decimal256Type{Precision: maxDecimal256Precision, Scale: int32(scale)}
	default:
		// Oracle NUMBER reaches 126 integer digits, past anything Arrow has.
		return nil
	}
}

// compactDecimalType narrows a declared NUMBER(p,s) to the smallest Arrow
// decimal that holds it. Only used when oracle.compact_decimals is on.
func compactDecimalType(precision, scale int32) arrow.DataType {
	switch {
	case precision <= maxDecimal32Precision:
		return &arrow.Decimal32Type{Precision: precision, Scale: scale}
	case precision <= maxDecimal64Precision:
		return &arrow.Decimal64Type{Precision: precision, Scale: scale}
	default:
		return &arrow.Decimal128Type{Precision: precision, Scale: scale}
	}
}

// opaqueNumberType labels a NUMBER that stays text, so the column says what it
// is instead of being an anonymous string.
//
// arrow.opaque is the canonical extension for a type from an external system
// that could not be interpreted, which is exactly the case here. A consumer
// that does not know the extension sees its utf8 storage, so this is additive.
func opaqueNumberType() arrow.DataType {
	return extensions.NewOpaqueType(arrow.BinaryTypes.String, "NUMBER", "Oracle")
}

// decimalFits reports whether a rendered NUMBER can be stored in a decimal of
// the given precision and scale without losing a digit.
//
// This has to be checked here rather than left to Arrow. Arrow's
// AppendValueFromString silently rounds a fraction longer than the scale:
// 1.2345678901234567890123456789 into a decimal128(38,2) stores 1.23 and
// returns no error, and 1.239 stores 1.24. It only reports an error when the
// integer part exceeds the precision. Rounding is the one outcome the
// overflow policy must never produce, since a rounded value looks correct, so
// the fit is decided before anything is appended.
func decimalFits(text string, precision, scale int32) bool {
	s := strings.TrimSpace(text)
	if s == "" {
		return false
	}
	if s[0] == '-' || s[0] == '+' {
		s = s[1:]
	}
	intPart, fracPart, _ := strings.Cut(s, ".")
	if !allDigits(intPart) || !allDigits(fracPart) {
		// Not the plain decimal this was inferred from; let the general
		// converter decide rather than calling it an overflow.
		return true
	}

	// A fraction longer than the scale would be rounded away.
	if int32(len(strings.TrimRight(fracPart, "0"))) > scale {
		return false
	}
	// Every significant digit has to fit, integer part plus scale.
	intDigits := int32(len(strings.TrimLeft(intPart, "0")))
	return intDigits+scale <= precision
}

// appendInferredDecimal writes one value into a column whose decimal type came
// from a sample rather than a declaration.
//
// The sample cannot promise anything about later rows, so this is where that
// shows up. A value that does not fit is either an error or a null, per
// oracle.number_overflow; it is never rounded into the inferred scale, because
// a rounded value looks correct and is not, which is a worse outcome than
// either failing or admitting the gap.
func (r *oracleRecordReader) appendInferredDecimal(b array.Builder, val interface{}, column string) (int64, error) {
	if val == nil {
		b.AppendNull()
		return 0, nil
	}

	var text string
	switch v := val.(type) {
	case string:
		text = v
	case []byte:
		text = string(v)
	default:
		// Not the decimal text inference was based on. Let the ordinary
		// converters try, since they handle the numeric cases.
		return appendValue(b, val, nil), nil
	}

	dec, isDec := b.Type().(arrow.DecimalType)
	if !isDec {
		return appendValue(b, val, nil), nil
	}

	// Decide the fit first: Arrow would round a long fraction away without
	// saying so, and a rounded value is worse than either policy outcome.
	if !decimalFits(text, dec.GetPrecision(), dec.GetScale()) {
		if r.overflowPolicy() == NumberOverflowNull {
			b.AppendNull()
			if !r.overflowLogged && r.logger != nil {
				r.overflowLogged = true
				r.logger.Warn("oracle: NUMBER value did not fit the inferred decimal type, nulled",
					"column", column, "value", text, "type", b.Type().String(),
					"option", OptionNumberOverflow)
			}
			return 0, nil
		}
		return 0, fmt.Errorf(
			"column %s: value %s does not fit %s, which was inferred from the first %d rows; "+
				"set %s=%s to null such values instead, or %s=%s to keep the column as text",
			column, text, b.Type(), r.policy.numberScanRows,
			OptionNumberOverflow, NumberOverflowNull,
			OptionNumberMapping, NumberMappingString)
	}

	width := int64(16)
	var err error
	switch bld := b.(type) {
	case *array.Decimal128Builder:
		if n, ok := parseDecimal128(text, dec.GetScale()); ok {
			bld.Append(n)
			return width, nil
		}
		err = bld.AppendValueFromString(text)
	case *array.Decimal256Builder:
		width = 32
		err = bld.AppendValueFromString(text)
	default:
		return appendValue(b, val, nil), nil
	}
	if err == nil {
		return width, nil
	}

	// It fit by digit count but Arrow still refused it. Treat it the same
	// way; AppendValueFromString appends its own null on failure.
	if r.overflowPolicy() == NumberOverflowNull {
		if !r.overflowLogged && r.logger != nil {
			r.overflowLogged = true
			r.logger.Warn("oracle: NUMBER value did not fit the inferred decimal type, nulled",
				"column", column, "value", text, "type", b.Type().String(),
				"option", OptionNumberOverflow)
		}
		return 0, nil
	}
	return 0, fmt.Errorf(
		"column %s: value %s does not fit %s, which was inferred from the first %d rows; "+
			"set %s=%s to null such values instead, or %s=%s to keep the column as text",
		column, text, b.Type(), r.policy.numberScanRows,
		OptionNumberOverflow, NumberOverflowNull,
		OptionNumberMapping, NumberMappingString)
}

func (r *oracleRecordReader) overflowPolicy() string {
	if r.policy.numberOverflow == "" {
		return defaultNumberOverflow
	}
	return r.policy.numberOverflow
}
