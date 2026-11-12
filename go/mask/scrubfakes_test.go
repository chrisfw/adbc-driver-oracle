package mask

import (
	"strings"
	"testing"
)

// Synthetic note text only.
func TestScrubberFakes(t *testing.T) {
	phones, _ := NewFakePhones(fakeTestKey)
	names, _ := NewFakeNames(fakeTestKey)
	emails, _ := NewFakeEmails(fakeTestKey, names)
	cards, _ := NewFakeCards(fakeTestKey)
	s, err := NewScrubberFakes(fakeTestKey, "", ScrubFakes{
		Phone: func(v string) (string, error) {
			out, ok := phones.Text(v)
			if !ok {
				return "", errErr
			}
			return out, nil
		},
		Email: func(v string) (string, error) { return emails.Address(v), nil },
		Card:  cards.Text,
	})
	if err != nil {
		t.Fatal(err)
	}
	in := "Spoke w/ caller, callback 999-867-5309, mail jo.ann@mail.test, card 4111 1111 1111 1111 on file"
	out, c := s.Scrub(in)
	if strings.Contains(out, "867-5309") || !strings.Contains(out, "999-555-01") {
		t.Fatalf("phone: %q", out)
	}
	if strings.Contains(out, "mail.test") || !strings.Contains(out, "@example.") {
		t.Fatalf("email: %q", out)
	}
	card := out[strings.Index(out, "card ")+5 : strings.Index(out, " on file")]
	if card == "4111 1111 1111 1111" || len(card) != 19 || !luhn(digits(card)) || card[0] != '4' {
		t.Fatalf("card %q", card)
	}
	if !strings.HasPrefix(out, "Spoke w/ caller, callback ") || c.Values != 1 {
		t.Fatalf("%q %+v", out, c)
	}
	// Kinds without a fake are still tokenized: an SSN here.
	o2, _ := s.Scrub("ssn 123-45-6789")
	if strings.Contains(o2, "123-45-6789") {
		t.Fatalf("ssn kept: %q", o2)
	}
}

var errErr = &scrubErr{}

type scrubErr struct{}

func (*scrubErr) Error() string { return "no" }

// A fake is not a token of anything: ScrubSpans reports no Value for it, and
// still reports one for a kind left tokenized.
func TestScrubSpansFakesHaveNoValue(t *testing.T) {
	phones, _ := NewFakePhones(fakeTestKey)
	s, err := NewScrubberFakes(fakeTestKey, "", ScrubFakes{Phone: func(v string) (string, error) {
		out, ok := phones.Text(v)
		if !ok {
			return "", errErr
		}
		return out, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, spans, _ := s.ScrubSpans("callback 999-867-5309, ssn 123-45-6789")
	if len(spans) != 2 || spans[0].Kind != "phone" || spans[0].Value != "" || spans[1].Kind != "ssn" || spans[1].Value != "123456789" {
		t.Fatalf("%+v", spans)
	}
}
