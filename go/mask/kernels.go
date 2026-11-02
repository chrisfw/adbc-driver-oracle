package mask

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"
)

// kernel masks one non-NULL value. The kernel for a column is chosen once
// (kernelFor) and then run over every value of the array.
type kernel func(v string, key []byte) string

// tokenLen is the hex length of a token: 64 bits, so collisions stay unlikely
// across millions of distinct values.
const tokenLen = 16

func token(v string, key []byte) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(v))
	return hex.EncodeToString(m.Sum(nil))[:tokenLen]
}

func digits(v string) string {
	var b strings.Builder
	for _, r := range v {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// lastFour returns the value's last four digits, or "" when it has fewer than
// five (then even four would reveal most of it).
func lastFour(v string) string {
	d := digits(v)
	if len(d) < 5 {
		return ""
	}
	return d[len(d)-4:]
}

func formatEmail(v string) string {
	at := strings.LastIndexByte(v, '@')
	if at <= 0 {
		return "***@***"
	}
	first := []rune(v[:at])[0]
	tld := ""
	if dot := strings.LastIndexByte(v[at+1:], '.'); dot >= 0 {
		tld = v[at+1+dot:]
	}
	return string(first) + "***@***" + tld
}

func formatName(v string) string {
	var parts []string
	for _, w := range strings.FieldsFunc(v, func(r rune) bool { return unicode.IsSpace(r) || r == ',' }) {
		parts = append(parts, string([]rune(w)[0])+"***")
	}
	if len(parts) == 0 {
		return "***"
	}
	return strings.Join(parts, " ")
}

// kernelFor picks the kernel for a masked column, or refuses a combination
// this build does not implement.
func kernelFor(c ColumnPlan) (kernel, error) {
	red := c.redaction()
	redact := func(string, []byte) string { return red }
	switch c.Style {
	case StyleToken:
		switch c.Kind {
		case KindSSN, KindEmail, KindPhone, KindName, KindAddress, KindDOB, KindGovID, KindCard, KindGeneric:
			return token, nil
		}
	case StyleRedact:
		switch c.Kind {
		case KindSSN, KindEmail, KindPhone, KindName, KindAddress, KindDOB, KindGovID, KindCard, KindGeneric:
			return redact, nil
		}
	case StyleFormat:
		switch c.Kind {
		case KindEmail:
			return func(v string, _ []byte) string { return formatEmail(v) }, nil
		case KindPhone:
			return func(v string, _ []byte) string {
				if l := lastFour(v); l != "" {
					return "***-***-" + l
				}
				return red
			}, nil
		case KindSSN:
			return func(v string, _ []byte) string {
				if l := lastFour(v); l != "" {
					return "***-**-" + l
				}
				return red
			}, nil
		case KindCard:
			return func(v string, _ []byte) string {
				if l := lastFour(v); l != "" {
					return "****-****-****-" + l
				}
				return red
			}, nil
		case KindName:
			return func(v string, _ []byte) string { return formatName(v) }, nil
		case KindDOB:
			return func(string, []byte) string { return "****-**-**" }, nil
		case KindAddress:
			return func(string, []byte) string { return "[ADDRESS]" }, nil
		case KindGovID, KindGeneric:
			return redact, nil
		}
	}
	return nil, fmt.Errorf("no masking kernel for kind %q with style %q in this build", c.Kind, c.Style)
}
