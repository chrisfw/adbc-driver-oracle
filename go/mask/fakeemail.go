package mask

// Fake email addresses for seeding a test database. The local part's words
// go through FakeNames, which replaces each word by a keyed fake name word, so
// "john.smith77@" becomes the same fake first and last name the customer's
// name columns got, with no join; its digit runs become keyed digits of the
// same length; separators and case are kept. The domain becomes one of the
// reserved example.com, example.net or example.org (RFC 2606), keyed by the
// real domain, so mail can never be delivered and a domain's users stay
// together. Text around the addresses (display names, lists separated by ;
// or ,) has its name words faked too.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FakeEmails makes fake email addresses. It is safe for concurrent use.
type FakeEmails struct {
	key    []byte
	names  *FakeNames
	digits *FakeCodes
}

// NewFakeEmails keys fake addresses with key (at least 16 bytes); names must
// be the export's fake-name generator (the same key), so local parts match
// the fake names.
func NewFakeEmails(key []byte, names *FakeNames) (*FakeEmails, error) {
	if len(key) < 16 {
		return nil, errors.New("mask: the fake-email key is shorter than 16 bytes")
	}
	if names == nil {
		return nil, errors.New("mask: fake emails need the fake-name generator")
	}
	digits, err := NewFakeCodes(derive(key, "xdb-fake/email-digits/v1"))
	if err != nil {
		return nil, err
	}
	return &FakeEmails{key: derive(key, "xdb-fake/email/v1"), names: names, digits: digits}, nil
}

var (
	emailAddrRe = regexp.MustCompile(`[A-Za-z0-9._%+'-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)
	digitRuns   = regexp.MustCompile(`[0-9]+`)
)

var exampleDomains = [...]string{"example.com", "example.net", "example.org"}

// Domain is the reserved domain a real one becomes.
func (f *FakeEmails) Domain(domain string) string {
	m := hmac.New(sha256.New, f.key)
	m.Write([]byte(strings.ToLower(domain)))
	d := exampleDomains[binary.BigEndian.Uint64(m.Sum(nil))%uint64(len(exampleDomains))]
	if domain == strings.ToUpper(domain) && domain != strings.ToLower(domain) {
		d = strings.ToUpper(d)
	}
	return d
}

// Local is a local part's fake: name words faked, digit runs keyed.
func (f *FakeEmails) Local(local string) string {
	out := f.names.Fake(local)
	return digitRuns.ReplaceAllStringFunc(out, f.digits.Fake)
}

// Address fakes one address.
func (f *FakeEmails) Address(addr string) string {
	at := strings.LastIndexByte(addr, '@')
	if at < 0 {
		return f.names.Fake(addr)
	}
	return f.Local(addr[:at]) + "@" + f.Domain(addr[at+1:])
}

// Fake is v with every address faked, and the name words around them.
func (f *FakeEmails) Fake(v string) string {
	if strings.TrimSpace(v) == "" {
		return v
	}
	var b strings.Builder
	last := 0
	for _, m := range emailAddrRe.FindAllStringIndex(v, -1) {
		b.WriteString(f.names.Fake(v[last:m[0]]))
		b.WriteString(f.Address(v[m[0]:m[1]]))
		last = m[1]
	}
	b.WriteString(f.names.Fake(v[last:]))
	return b.String()
}

// Fit is Fake cut to maxChars characters (0: no limit) without breaking a
// lone address: its local part is shortened, never its domain.
func (f *FakeEmails) Fit(v string, maxChars int) string {
	out := f.Fake(v)
	if maxChars <= 0 || utf8.RuneCountInString(out) <= maxChars {
		return out
	}
	if loc := emailAddrRe.FindStringIndex(out); loc != nil && loc[0] == 0 && loc[1] == len(out) {
		at := strings.LastIndexByte(out, '@')
		local, domain := []rune(out[:at]), out[at:]
		keep := maxChars - utf8.RuneCountInString(domain)
		if keep >= 1 {
			return string(local[:min(keep, len(local))]) + domain
		}
	}
	r := []rune(out)
	return string(r[:maxChars])
}

// FakeEmailArray fakes every address in a string column, cut to maxChars
// (0: no limit). NULL stays NULL; changed counts the values that changed.
func FakeEmailArray(col arrow.Array, f *FakeEmails, maxChars int, alloc memory.Allocator) (arrow.Array, int64, error) {
	if f == nil {
		return nil, 0, errors.New("mask: a fake-email column needs its generator")
	}
	return mapStrings(col, func(s string) string { return f.Fit(s, maxChars) }, alloc)
}
