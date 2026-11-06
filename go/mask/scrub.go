package mask

// Free-text scrubbing (StyleScrub): the personal data inside a text value is
// masked and the rest stays readable.
//
//	Spoke w/ caller, callback 802-555-0142, re acct 123456789301
//	  -> Spoke w/ caller, callback <phone token>, re acct <masked account>
//
// Per value: every detector reports spans (UTF-8 byte offsets, a kind and a
// score); span edges are trimmed of whitespace and punctuation (never of
// @ - _ . / +, which belong to e-mail addresses and identifiers); overlaps are
// resolved by score, then length, keeping a span only if it overlaps nothing
// kept already; the kept spans are replaced right to left. Any error or panic
// on a value gives the column's redaction text for that value and is counted
// (fail closed), never the value itself.
//
// The detectors are compiled in: regular expressions with validators for
// e-mail, US SSN, card numbers (Luhn), US phone numbers and ABA routing
// numbers, plus identifier families in their display forms (IdentifierForm,
// supplied by the caller as code, masked with the caller's Identifier so a
// number in a note becomes exactly what the same number becomes in an
// identifier column). Nothing about detection is loaded from data or rules;
// the rules only name the style.
//
// Tokens: a detected e-mail, SSN, card, phone or routing number is replaced by
// token(normalized value, key), the same function as the token column kernel.
// The scrubber normalizes before tokenizing (e-mail lower-cased; SSN, card,
// phone and routing number reduced to digits, a phone's leading US country
// code 1 dropped), so a phone in a note gets the token a phone column stored
// as its ten digits gets. The column kernels do not normalize: a column value
// stored with punctuation or capitals tokenizes differently from the same
// value in a note.

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Span is a stretch of a text value that holds personal data.
type Span struct {
	Start, End int    // UTF-8 byte offsets: text[Start:End]
	Kind       string // e.g. "phone", "email", "account"
	Score      float64
	// Replace returns the replacement for the span's text (after trimming).
	Replace func(text string) (string, error)
}

// Detector finds personal data in a text value. It must be safe for
// concurrent use. A context-dictionary detector (names, addresses) can be
// added here later; none is built in.
type Detector interface {
	Detect(text string) []Span
}

// ScrubCounts tallies what scrubbing did, for the run report: never values.
type ScrubCounts struct {
	Values       int64            // non-NULL values read
	Changed      int64            // values with at least one span replaced
	Spans        int64            // spans replaced
	ByKind       map[string]int64 // spans replaced, per kind
	FailedClosed int64            // values replaced by the redaction text after an error
	Nulls        int64
}

// Add accumulates o into c.
func (c *ScrubCounts) Add(o ScrubCounts) {
	c.Values += o.Values
	c.Changed += o.Changed
	c.Spans += o.Spans
	c.FailedClosed += o.FailedClosed
	c.Nulls += o.Nulls
	for k, n := range o.ByKind {
		if c.ByKind == nil {
			c.ByKind = map[string]int64{}
		}
		c.ByKind[k] += n
	}
}

// Scrubber scrubs text values. It is safe for concurrent use.
type Scrubber struct {
	detectors []Detector
	redaction string
	failures  atomic.Int64

	// fault, in tests only, makes every value fail (fail-closed tests).
	fault func(string) error
}

// NewScrubber builds a scrubber with the built-in detectors keyed by key (at
// least 16 bytes) plus extra detectors (identifier families). redaction is
// the text a value becomes when scrubbing it fails ("" means
// DefaultRedaction).
func NewScrubber(key []byte, redaction string, extra ...Detector) (*Scrubber, error) {
	if len(key) < 16 {
		return nil, errors.New("mask: scrubbing tokenizes but the key is shorter than 16 bytes")
	}
	if strings.TrimSpace(redaction) == "" {
		redaction = DefaultRedaction
	}
	k := append([]byte(nil), key...)
	ds := []Detector{regexDetector{k}}
	for _, d := range extra {
		if d == nil {
			return nil, errors.New("mask: a nil detector")
		}
		ds = append(ds, d)
	}
	return &Scrubber{detectors: ds, redaction: redaction}, nil
}

// Failures is how many values this scrubber has failed closed on so far.
func (s *Scrubber) Failures() int64 { return s.failures.Load() }

// Scrub returns v with its personal data masked, and what it did. On any
// error or panic it returns the redaction text and counts a failure.
func (s *Scrubber) Scrub(v string) (out string, c ScrubCounts) {
	c.Values = 1
	defer func() {
		if r := recover(); r != nil {
			out, c = s.failed()
		}
	}()
	res, spans, err := s.scrub(v)
	if err != nil {
		return s.failed()
	}
	if len(spans) > 0 {
		c.Changed = 1
		c.Spans = int64(len(spans))
		c.ByKind = map[string]int64{}
		for _, sp := range spans {
			c.ByKind[sp.Kind]++
		}
	}
	return res, c
}

func (s *Scrubber) failed() (string, ScrubCounts) {
	s.failures.Add(1)
	return s.redaction, ScrubCounts{Values: 1, FailedClosed: 1}
}

func (s *Scrubber) scrub(v string) (string, []Span, error) {
	if s.fault != nil {
		if err := s.fault(v); err != nil {
			return "", nil, err
		}
	}
	var spans []Span
	for _, d := range s.detectors {
		for _, sp := range d.Detect(v) {
			if sp.Start < 0 || sp.End > len(v) || sp.Start > sp.End || sp.Replace == nil {
				return "", nil, fmt.Errorf("mask: a detector returned a bad %s span", sp.Kind)
			}
			if sp, ok := trimSpan(v, sp); ok {
				spans = append(spans, sp)
			}
		}
	}
	kept := resolve(spans)
	// Right to left, so earlier offsets stay valid.
	out := v
	for i := len(kept) - 1; i >= 0; i-- {
		sp := kept[i]
		rep, err := sp.Replace(v[sp.Start:sp.End])
		if err != nil {
			return "", nil, err
		}
		out = out[:sp.Start] + rep + out[sp.End:]
	}
	return out, kept, nil
}

// trimCut are the characters trimmed from span edges. @ - _ . / + are kept:
// they belong to e-mail addresses and identifiers.
const trimCut = " \t\n\r\"'`,;:!?()[]{}<>"

func trimSpan(v string, sp Span) (Span, bool) {
	for sp.Start < sp.End && strings.IndexByte(trimCut, v[sp.Start]) >= 0 {
		sp.Start++
	}
	for sp.End > sp.Start && strings.IndexByte(trimCut, v[sp.End-1]) >= 0 {
		sp.End--
	}
	return sp, sp.Start < sp.End
}

// resolve keeps the best non-overlapping spans: by score, then length (then
// position, so the result is deterministic), and returns them in text order.
func resolve(spans []Span) []Span {
	slices.SortStableFunc(spans, func(a, b Span) int {
		switch {
		case a.Score != b.Score:
			if a.Score > b.Score {
				return -1
			}
			return 1
		case a.End-a.Start != b.End-b.Start:
			return (b.End - b.Start) - (a.End - a.Start)
		}
		return a.Start - b.Start
	})
	var kept []Span
	for _, sp := range spans {
		overlaps := false
		for _, k := range kept {
			if sp.Start < k.End && k.Start < sp.End {
				overlaps = true
				break
			}
		}
		if !overlaps {
			kept = append(kept, sp)
		}
	}
	slices.SortFunc(kept, func(a, b Span) int { return a.Start - b.Start })
	return kept
}

// ScrubArray scrubs a string column (string or large string; anything else
// is refused) into a string column. NULL stays NULL. The detectors are chosen
// once for the column and run over every value.
func ScrubArray(col arrow.Array, s *Scrubber, alloc memory.Allocator) (arrow.Array, ScrubCounts, error) {
	var n ScrubCounts
	if s == nil {
		return nil, n, errors.New("mask: a scrub column needs a scrubber")
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	type strArr interface {
		arrow.Array
		Value(int) string
	}
	sa, ok := col.(strArr)
	if !ok || (col.DataType().ID() != arrow.STRING && col.DataType().ID() != arrow.LARGE_STRING) {
		return nil, n, fmt.Errorf("mask: only text can be scrubbed, not %s", col.DataType())
	}
	b := array.NewStringBuilder(alloc)
	defer b.Release()
	b.Reserve(sa.Len())
	for i := 0; i < sa.Len(); i++ {
		if sa.IsNull(i) {
			n.Nulls++
			b.AppendNull()
			continue
		}
		out, c := s.Scrub(sa.Value(i))
		n.Add(c)
		b.Append(out)
	}
	return b.NewArray(), n, nil
}

// --- built-in detectors ---

// The patterns are compiled once (RE2: linear time, no backtracking).
var (
	emailRe = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)*\.[A-Za-z]{2,}`)
	ssnRe   = regexp.MustCompile(`\b\d{3}[-\s]\d{2}[-\s]\d{4}\b`)
	cardRe  = regexp.MustCompile(`\b\d(?:[ -]?\d){12,18}\b`)
	phoneRe = regexp.MustCompile(`(?:\+?\b1[-.\s]?)?(?:\(\d{3}\)\s?|\b\d{3}[-.\s])\d{3}[-.\s]\d{4}\b`)
	abaRe   = regexp.MustCompile(`\b\d{9}\b`)
)

type regexDetector struct{ key []byte }

func (d regexDetector) tokenOf(norm func(string) string) func(string) (string, error) {
	return func(text string) (string, error) {
		v := norm(text)
		if v == "" {
			return "", errors.New("mask: nothing to tokenize")
		}
		return token(v, d.key), nil
	}
}

func (d regexDetector) Detect(text string) []Span {
	var out []Span
	add := func(win [2]int, re *regexp.Regexp, kind string, ok func(string) bool, norm func(string) string) {
		for _, m := range re.FindAllStringIndex(text[win[0]:win[1]], -1) {
			a, b := win[0]+m[0], win[0]+m[1]
			if ok == nil || ok(text[a:b]) {
				out = append(out, Span{Start: a, End: b, Kind: kind, Score: 1, Replace: d.tokenOf(norm)})
			}
		}
	}
	// The patterns run only over the few windows that can hold a match (see
	// digitWindows, atWindows): scanning a whole note with five regular
	// expressions costs far more than one pass to find them.
	for _, w := range atWindows(text) {
		add(w, emailRe, "email", nil, strings.ToLower)
	}
	for _, w := range digitWindows(text) {
		add(w, ssnRe, "ssn", nil, digits)
		add(w, cardRe, "card", func(s string) bool { return luhn(digits(s)) }, digits)
		add(w, phoneRe, "phone", nil, phoneDigits)
		add(w, abaRe, "routing", func(s string) bool { return abaValid(s) }, digits)
	}
	return out
}

// digitWindows returns the stretches of text where a number pattern can
// match: maximal runs of digits joined by at most two separators (space - . /
// ( ) +), with a leading ( or + and one byte of context on each side, so
// \b at a window's edge sees what it sees in the whole text. (RE2's \b is
// ASCII-only: a byte cut from a multi-byte rune reads as a non-word
// character, as the whole rune does.)
func digitWindows(text string) [][2]int {
	isDigit := func(c byte) bool { return c >= '0' && c <= '9' }
	isSep := func(c byte) bool { return c == ' ' || c == '-' || c == '.' || c == '/' || c == '(' || c == ')' || c == '+' }
	var out [][2]int
	for i := 0; i < len(text); {
		if !isDigit(text[i]) {
			i++
			continue
		}
		s := i
		for s > 0 && s > i-2 && (text[s-1] == '(' || text[s-1] == '+' || (text[s-1] == ' ' && s-2 >= 0 && text[s-2] == '+')) {
			s--
		}
		e := i
		for e < len(text) {
			if isDigit(text[e]) {
				e++
				continue
			}
			j := e
			for j < len(text) && j-e < 2 && isSep(text[j]) {
				j++
			}
			if j > e && j < len(text) && isDigit(text[j]) {
				e = j
				continue
			}
			break
		}
		out = append(out, [2]int{max(s-1, 0), min(e+1, len(text))})
		i = e
	}
	return out
}

// atWindows returns, for each '@', the run of non-space text around it: an
// e-mail address holds no whitespace.
func atWindows(text string) [][2]int {
	var out [][2]int
	for from := 0; ; {
		i := strings.IndexByte(text[from:], '@')
		if i < 0 {
			return out
		}
		at := from + i
		s := at
		for s > 0 && !isSpace(text[s-1]) {
			s--
		}
		e := at + 1
		for e < len(text) && !isSpace(text[e]) {
			e++
		}
		out = append(out, [2]int{s, e})
		from = e
		if from >= len(text) {
			return out
		}
	}
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v' }

// phoneDigits is a phone's ten digits: a leading US country code is dropped.
func phoneDigits(s string) string {
	d := digits(s)
	if len(d) == 11 && d[0] == '1' {
		d = d[1:]
	}
	return d
}

// luhn reports whether a 13-19 digit string passes the Luhn check.
func luhn(d string) bool {
	if len(d) < 13 || len(d) > 19 {
		return false
	}
	sum := 0
	for i := 0; i < len(d); i++ {
		x := int(d[len(d)-1-i] - '0')
		if i%2 == 1 {
			x *= 2
			if x > 9 {
				x -= 9
			}
		}
		sum += x
	}
	return sum%10 == 0
}

// abaValid checks an ABA routing number: nine digits, a valid Federal Reserve
// prefix, and the 3-7-1 checksum.
func abaValid(s string) bool {
	if len(s) != 9 {
		return false
	}
	p := int(s[0]-'0')*10 + int(s[1]-'0')
	if !(p <= 12 || (p >= 21 && p <= 32) || (p >= 61 && p <= 72) || p == 80) {
		return false
	}
	w := [9]int{3, 7, 1, 3, 7, 1, 3, 7, 1}
	sum := 0
	for i := range 9 {
		sum += w[i] * int(s[i]-'0')
	}
	return sum%10 == 0
}

// --- identifier families in text ---

// IdentifierForm is one display form of an identifier family in text, as
// code: Pattern finds it, Value reads the identifier from the match's
// submatches (ok false rejects the match: not this family), and Render writes
// the masked value back in the same form (same separators, same padding).
type IdentifierForm struct {
	Family  string
	Pattern *regexp.Regexp
	Value   func(sub []string) (uint64, bool)
	Render  func(sub []string, masked uint64) string
}

type identifierDetector struct {
	forms []identifierForm
}

type identifierForm struct {
	IdentifierForm
	whole *regexp.Regexp // ^(?:Pattern)$, to re-read a span's (trimmed) text
	id    *Identifier
}

// NewIdentifierDetector finds forms in text and masks each with its family's
// identifier mask from ids: the same masks (key, scope, zones) the identifier
// columns use, so a number in a note masks to exactly what it masks to in a
// column. A form whose family has no mask is an error.
func NewIdentifierDetector(forms []IdentifierForm, ids map[string]*Identifier) (Detector, error) {
	d := &identifierDetector{}
	for _, f := range forms {
		id := ids[f.Family]
		if id == nil {
			return nil, fmt.Errorf("mask: no %s identifier mask for its text form", f.Family)
		}
		if f.Pattern == nil || f.Value == nil || f.Render == nil {
			return nil, fmt.Errorf("mask: the %s text form is incomplete", f.Family)
		}
		d.forms = append(d.forms, identifierForm{IdentifierForm: f, whole: regexp.MustCompile(`^(?:` + f.Pattern.String() + `)$`), id: id})
	}
	return d, nil
}

// Detect runs each form over the digit windows only (see digitWindows): a
// form's pattern must consist of digits and the separators digitWindows
// joins (space - . / ( ) +).
func (d *identifierDetector) Detect(text string) []Span {
	var out []Span
	for _, w := range digitWindows(text) {
		win := text[w[0]:w[1]]
		for _, f := range d.forms {
			for _, m := range f.Pattern.FindAllStringSubmatchIndex(win, -1) {
				if _, ok := f.Value(submatches(win, m)); !ok {
					continue
				}
				out = append(out, Span{Start: w[0] + m[0], End: w[0] + m[1], Kind: f.Family, Score: 1, Replace: f.replace})
			}
		}
	}
	return out
}

func (f identifierForm) replace(text string) (string, error) {
	sub := f.whole.FindStringSubmatch(text)
	if sub == nil {
		return "", fmt.Errorf("mask: a %s span no longer matches its form", f.Family)
	}
	v, ok := f.Value(sub)
	if !ok {
		return "", fmt.Errorf("mask: a %s span is not a %s", f.Family, f.Family)
	}
	m, _ := f.id.Mask(v)
	return f.Render(sub, m), nil
}

func submatches(text string, m []int) []string {
	sub := make([]string, len(m)/2)
	for i := range sub {
		if m[2*i] >= 0 {
			sub[i] = text[m[2*i]:m[2*i+1]]
		}
	}
	return sub
}
