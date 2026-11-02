// Package mask is the driver's masking hook: it wraps an Arrow record reader
// so every batch is masked column by column before anyone reads it.
//
// Off (no wrap), a caller sees the data as Oracle returned it. On (Wrap), each
// column follows its ColumnPlan:
//
//	PassRaw   the column unchanged
//	Mask      a kernel chosen once for the column by (Kind, Style), run over the
//	          whole array; NULL stays NULL
//	Withhold  every value replaced by the withheld marker (NULL stays NULL)
//
// Masked and withheld columns become string columns. The kernels are compiled
// in, never loaded as data or plugins; a plan that names a kind or style this
// build does not implement is refused (fail closed), never passed raw.
//
// Masked values follow what ora-mt-mcp's SQL masking produced, so users see the
// same shapes: email j***@***.tld, phone/SSN/card keep the last four digits,
// name initials, dates of birth and addresses fully hidden. Tokens are
// HMAC-SHA256 of the value under a caller-supplied key (keyed pseudonymization,
// ISO/IEC 20889; GDPR Art. 4(5)): deterministic for one key, so equal values
// join, and meaningless without it. Card numbers never show more than the last
// four digits (within PCI DSS's first-six/last-four display limit).
package mask

import (
	"fmt"
	"strings"
)

// Disposition is what happens to one output column.
type Disposition string

const (
	PassRaw  Disposition = "pass_raw"
	Mask     Disposition = "mask"
	Withhold Disposition = "withhold"
)

// Kind is the kind of personal data in a column.
type Kind string

const (
	KindSSN     Kind = "ssn"
	KindEmail   Kind = "email"
	KindPhone   Kind = "phone"
	KindName    Kind = "name"
	KindAddress Kind = "address"
	KindDOB     Kind = "dob"
	KindGovID   Kind = "gov_id"
	KindCard    Kind = "card"
	KindGeneric Kind = "generic"
)

// Style is how a masked column is rendered.
type Style string

const (
	StyleToken  Style = "token"  // keyed pseudonym; equal values give equal tokens
	StyleFormat Style = "format" // kind-specific partial reveal
	StyleRedact Style = "redact" // the redaction text
)

// DefaultRedaction and WithheldText are the replacement texts.
const (
	DefaultRedaction = "[REDACTED]"
	WithheldText     = "[WITHHELD]"
)

// ColumnPlan says how to treat one output column.
type ColumnPlan struct {
	Disposition Disposition
	Kind        Kind
	Style       Style
	// Redaction replaces values under StyleRedact (and KindGeneric/KindAddress
	// formats); empty means DefaultRedaction.
	Redaction string
}

// Plan has one entry per output column, in output order.
type Plan []ColumnPlan

// defaultStyle is the style a kind gets when its rule names none (as datamask:
// identifiers tokenized, contact details formatted).
var defaultStyle = map[Kind]Style{
	KindSSN: StyleToken, KindGovID: StyleToken, KindCard: StyleToken,
	KindEmail: StyleFormat, KindPhone: StyleFormat, KindName: StyleFormat,
	KindAddress: StyleFormat, KindDOB: StyleFormat, KindGeneric: StyleToken,
}

// MaskColumn builds the plan entry for a masked column from a classification
// rule, applying the same rules as datamask's SQL masking: a missing style
// takes the kind's default, and SSNs, government IDs and card numbers are
// redacted rather than tokenized unless the column is needed to join
// (joinCritical), since a token of a national identifier is still a stable
// identifier.
func MaskColumn(kind Kind, style Style, joinCritical bool, redaction string) ColumnPlan {
	if kind == "" {
		kind = KindGeneric
	}
	if style == "" {
		style = defaultStyle[kind]
	}
	if style == StyleToken && !joinCritical && (kind == KindSSN || kind == KindGovID || kind == KindCard) {
		style = StyleRedact
	}
	return ColumnPlan{Disposition: Mask, Kind: kind, Style: style, Redaction: redaction}
}

// Validate refuses a plan this build cannot carry out exactly.
func (p Plan) Validate() error {
	for i, c := range p {
		switch c.Disposition {
		case PassRaw, Withhold:
		case Mask:
			if _, err := kernelFor(c); err != nil {
				return fmt.Errorf("mask: column %d: %w", i, err)
			}
		default:
			return fmt.Errorf("mask: column %d: unknown disposition %q", i, c.Disposition)
		}
	}
	return nil
}

func (c ColumnPlan) redaction() string {
	if strings.TrimSpace(c.Redaction) != "" {
		return c.Redaction
	}
	return DefaultRedaction
}
