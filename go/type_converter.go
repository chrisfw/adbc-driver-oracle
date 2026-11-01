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
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func appendValue(fieldBuilder array.Builder, val interface{}, ct *sql.ColumnType) int64 {
	if val == nil {
		fieldBuilder.AppendNull()
		return 0
	}

	switch b := fieldBuilder.(type) {
	case *array.Int64Builder:
		return appendInt64(b, val)
	case *array.Float32Builder:
		return appendFloat32(b, val)
	case *array.Float64Builder:
		return appendFloat64(b, val)
	case *array.Decimal128Builder:
		return appendDecimal128(b, val)
	case *array.StringBuilder:
		return appendString(b, val)
	case *array.LargeStringBuilder:
		return appendLargeString(b, val)
	case *array.TimestampBuilder:
		return appendTimestamp(b, val)
	case *array.DurationBuilder:
		return appendIntervalDS(b, val)
	case *array.MonthIntervalBuilder:
		return appendIntervalYM(b, val)
	// Covers LargeBinary too: arrow-go builds both through BinaryBuilder and
	// tells them apart by the data type.
	case *array.BinaryBuilder:
		return appendBinary(b, val)
	case *array.BooleanBuilder:
		return appendBoolean(b, val)
	default:
		// Fallback: convert to string
		s := fmt.Sprintf("%v", val)
		if sb, ok := fieldBuilder.(*array.StringBuilder); ok {
			sb.Append(s)
			return int64(len(s))
		}
		fieldBuilder.AppendNull()
		return 0
	}
}

func appendInt64(b *array.Int64Builder, val interface{}) int64 {
	switch v := val.(type) {
	case int64:
		b.Append(v)
	case int32:
		b.Append(int64(v))
	case int:
		b.Append(int64(v))
	case float64:
		b.Append(int64(v))
	case float32:
		b.Append(int64(v))
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			// Try parsing as float then truncate
			f, ferr := strconv.ParseFloat(v, 64)
			if ferr != nil {
				b.AppendNull()
			} else {
				b.Append(int64(f))
			}
		} else {
			b.Append(i)
		}
	default:
		b.AppendNull()
	}
	return 8
}

// appendFloat32 fills a BINARY_FLOAT column. go-ora decodes the column to a
// Go float32, so that case is the one that runs; the others cover values
// reaching this builder by another route.
func appendFloat32(b *array.Float32Builder, val interface{}) int64 {
	switch v := val.(type) {
	case float32:
		b.Append(v)
	case float64:
		b.Append(float32(v))
	case int64:
		b.Append(float32(v))
	case int32:
		b.Append(float32(v))
	case int:
		b.Append(float32(v))
	case string:
		f, err := strconv.ParseFloat(v, 32)
		if err != nil {
			b.AppendNull()
		} else {
			b.Append(float32(f))
		}
	default:
		b.AppendNull()
	}
	return 4
}

func appendFloat64(b *array.Float64Builder, val interface{}) int64 {
	switch v := val.(type) {
	case float64:
		b.Append(v)
	case float32:
		b.Append(float64(v))
	case int64:
		b.Append(float64(v))
	case int32:
		b.Append(float64(v))
	case int:
		b.Append(float64(v))
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			b.AppendNull()
		} else {
			b.Append(f)
		}
	default:
		b.AppendNull()
	}
	return 8
}

// appendDecimal128 preserves Oracle NUMBER precision. go-ora decodes NUMBER to
// a full-precision decimal string (parameter.go: `num.String()`), so the string
// case is the one that actually runs for a NUMBER column; the numeric cases
// cover values reaching this builder by another route.
//
// AppendValueFromString appends a null itself when parsing fails, so failures
// must not append again.
func appendDecimal128(b *array.Decimal128Builder, val interface{}) int64 {
	scale := b.Type().(*arrow.Decimal128Type).Scale
	switch v := val.(type) {
	case string:
		// Fast path first: go-ora hands over a plain decimal string and
		// almost every real NUMBER fits an int64, so this avoids the
		// math/big round trip AppendValueFromString performs.
		if n, ok := parseDecimal128(v, scale); ok {
			b.Append(n)
		} else {
			_ = b.AppendValueFromString(v)
		}
	case []byte:
		if n, ok := parseDecimal128(string(v), scale); ok {
			b.Append(n)
		} else {
			_ = b.AppendValueFromString(string(v))
		}
	case int64:
		_ = b.AppendValueFromString(strconv.FormatInt(v, 10))
	case int32:
		_ = b.AppendValueFromString(strconv.FormatInt(int64(v), 10))
	case int:
		_ = b.AppendValueFromString(strconv.FormatInt(int64(v), 10))
	case float64:
		_ = b.AppendValueFromString(strconv.FormatFloat(v, 'f', -1, 64))
	case float32:
		_ = b.AppendValueFromString(strconv.FormatFloat(float64(v), 'f', -1, 32))
	default:
		b.AppendNull()
	}
	return 16
}

func appendString(b *array.StringBuilder, val interface{}) int64 {
	switch v := val.(type) {
	case string:
		b.Append(v)
		return int64(len(v))
	case []byte:
		s := string(v)
		b.Append(s)
		return int64(len(s))
	default:
		s := fmt.Sprintf("%v", v)
		b.Append(s)
		return int64(len(s))
	}
}

// appendLargeString fills a CLOB column, which uses the 64-bit offset layout.
func appendLargeString(b *array.LargeStringBuilder, val interface{}) int64 {
	switch v := val.(type) {
	case string:
		b.Append(v)
		return int64(len(v))
	case []byte:
		s := string(v)
		b.Append(s)
		return int64(len(s))
	default:
		s := fmt.Sprintf("%v", v)
		b.Append(s)
		return int64(len(s))
	}
}

func appendTimestamp(b *array.TimestampBuilder, val interface{}) int64 {
	tsType := b.Type().(*arrow.TimestampType)
	appendTime := func(t time.Time) {
		if tsType.TimeZone == "" {
			// Tz-naive column: store the wall clock. go-ora returns tz-less
			// DATE/TIMESTAMP values as wall-clock time.Time relocated into the
			// DB server's zone (UTC only when the server runs UTC), so an
			// epoch-based conversion would shift the value by the server's
			// offset. Rebuild the fields as UTC instead.
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
		}
		ts, err := arrow.TimestampFromTime(t, tsType.Unit)
		if err != nil {
			b.AppendNull()
			return
		}
		b.Append(ts)
	}
	switch v := val.(type) {
	case time.Time:
		appendTime(v)
	case string:
		// Try common Oracle timestamp formats
		for _, layout := range []string{
			"2006-01-02T15:04:05.999999999Z07:00",
			"2006-01-02 15:04:05.999999999 Z07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, v); err == nil {
				appendTime(t)
				return 8
			}
		}
		b.AppendNull()
	default:
		b.AppendNull()
	}
	return 8
}

// appendIntervalDS fills an INTERVAL DAY TO SECOND column. go-ora decodes the
// column to Oracle's text form, so the string case is the one that runs.
func appendIntervalDS(b *array.DurationBuilder, val interface{}) int64 {
	unit := b.Type().(*arrow.DurationType).Unit
	switch v := val.(type) {
	case string:
		d, ok := parseOracleIntervalDS(v)
		if !ok {
			b.AppendNull()
			return 0
		}
		b.Append(arrow.Duration(durationInUnit(d, unit)))
	case time.Duration:
		b.Append(arrow.Duration(durationInUnit(v, unit)))
	default:
		b.AppendNull()
	}
	return 8
}

// durationInUnit converts a Go duration, which counts nanoseconds, into the
// column's unit.
func durationInUnit(d time.Duration, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Nanosecond:
		return int64(d)
	case arrow.Microsecond:
		return int64(d / time.Microsecond)
	case arrow.Millisecond:
		return int64(d / time.Millisecond)
	default:
		return int64(d / time.Second)
	}
}

// appendIntervalYM fills an INTERVAL YEAR TO MONTH column as a count of
// months, which is what Arrow's month_interval carries.
func appendIntervalYM(b *array.MonthIntervalBuilder, val interface{}) int64 {
	switch v := val.(type) {
	case string:
		months, ok := parseOracleIntervalYM(v)
		if !ok {
			b.AppendNull()
			return 0
		}
		b.Append(arrow.MonthInterval(months))
	case int64:
		b.Append(arrow.MonthInterval(int32(v)))
	case int32:
		b.Append(arrow.MonthInterval(v))
	default:
		b.AppendNull()
	}
	return 4
}

func appendBinary(b *array.BinaryBuilder, val interface{}) int64 {
	switch v := val.(type) {
	case []byte:
		b.Append(v)
		return int64(len(v))
	default:
		b.AppendNull()
		return 0
	}
}

func appendBoolean(b *array.BooleanBuilder, val interface{}) int64 {
	switch v := val.(type) {
	case bool:
		b.Append(v)
	case int64:
		b.Append(v != 0)
	case float64:
		b.Append(v != 0)
	case string:
		b.Append(v == "1" || v == "true" || v == "TRUE" || v == "Y")
	default:
		b.AppendNull()
	}
	return 1
}
