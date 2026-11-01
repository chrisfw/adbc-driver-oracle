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
	"math"
	"strconv"
	"strings"
	"time"
)

// Oracle intervals arrive as text. go-ora decodes INTERVAL DAY TO SECOND to
// "[+|-]D HH:MM:SS[.fraction]" and INTERVAL YEAR TO MONTH to "[+|-]Y-M", with
// the field widths following the declared precision and the sign applying to
// the whole interval rather than to the leading field alone.

// parseOracleIntervalDS parses an INTERVAL DAY TO SECOND into a duration.
//
// Returns false rather than a partial value for anything unrecognised, so a
// caller appends a null instead of a plausible wrong number.
func parseOracleIntervalDS(s string) (time.Duration, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}

	neg := false
	switch s[0] {
	case '-':
		neg = true
		s = s[1:]
	case '+':
		s = s[1:]
	}

	// "D HH:MM:SS.fff": the day field is separated by a space, the rest by
	// colons, and the fraction by a period.
	days, rest, ok := strings.Cut(s, " ")
	if !ok {
		return 0, false
	}
	d, err := strconv.ParseInt(strings.TrimSpace(days), 10, 64)
	if err != nil || d < 0 {
		return 0, false
	}

	parts := strings.Split(strings.TrimSpace(rest), ":")
	if len(parts) != 3 {
		return 0, false
	}
	hours, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || hours < 0 || hours > 23 {
		return 0, false
	}
	minutes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || minutes < 0 || minutes > 59 {
		return 0, false
	}

	secText, fracText, hasFrac := strings.Cut(parts[2], ".")
	seconds, err := strconv.ParseInt(secText, 10, 64)
	if err != nil || seconds < 0 || seconds > 59 {
		return 0, false
	}
	var nanos int64
	if hasFrac {
		nanos, ok = parseFractionNanos(fracText)
		if !ok {
			return 0, false
		}
	}

	// INTERVAL DAY(9) TO SECOND can hold 999999999 days, far more than a
	// time.Duration's 106751-day span. Refuse what will not fit rather than
	// wrap into a plausible wrong value, the same choice the nanosecond
	// timestamp unit forces.
	if d > maxIntervalDays {
		return 0, false
	}
	dayPart := time.Duration(d) * 24 * time.Hour
	timePart := time.Duration(hours)*time.Hour +
		time.Duration(minutes)*time.Minute +
		time.Duration(seconds)*time.Second +
		time.Duration(nanos)
	if math.MaxInt64-int64(dayPart) < int64(timePart) {
		return 0, false
	}

	total := dayPart + timePart
	if neg {
		total = -total
	}
	return total, true
}

// maxIntervalDays is the largest whole day count a time.Duration holds.
const maxIntervalDays = int64(math.MaxInt64) / int64(24*time.Hour)

// parseFractionNanos reads Oracle's fractional-seconds digits as nanoseconds,
// padding or truncating to the 9 digits a duration holds.
func parseFractionNanos(frac string) (int64, bool) {
	// More than 9 digits is not something Oracle produces, and truncating
	// them would be the wrong kind of lenient.
	if frac == "" || len(frac) > 9 {
		return 0, false
	}
	for i := 0; i < len(frac); i++ {
		if frac[i] < '0' || frac[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, false
	}
	for i := len(frac); i < 9; i++ {
		n *= 10
	}
	return n, true
}

// parseOracleIntervalYM parses an INTERVAL YEAR TO MONTH into a count of
// months, which is what Arrow's month_interval carries.
func parseOracleIntervalYM(s string) (int32, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}

	neg := false
	switch s[0] {
	case '-':
		neg = true
		s = s[1:]
	case '+':
		s = s[1:]
	}

	yearText, monthText, ok := strings.Cut(s, "-")
	if !ok {
		return 0, false
	}
	years, err := strconv.ParseInt(strings.TrimSpace(yearText), 10, 64)
	if err != nil || years < 0 {
		return 0, false
	}
	months, err := strconv.ParseInt(strings.TrimSpace(monthText), 10, 64)
	if err != nil || months < 0 || months > 11 {
		return 0, false
	}

	total := years*12 + months
	if total > 1<<31-1 {
		return 0, false
	}
	if neg {
		total = -total
	}
	return int32(total), true
}
