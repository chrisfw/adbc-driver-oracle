package mask

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var testKey = []byte("0123456789abcdef0123456789abcdef")

func run(t *testing.T, c ColumnPlan, v string) string {
	t.Helper()
	k, err := kernelFor(c)
	if err != nil {
		t.Fatal(err)
	}
	return k(v, testKey)
}

func TestFormatKernels(t *testing.T) {
	for _, c := range []struct {
		kind     Kind
		in, want string
	}{
		{KindEmail, "jane.doe@example.com", "j***@***.com"},
		{KindEmail, "no-at-sign", "***@***"},
		{KindPhone, "(806) 555-1234", "***-***-1234"},
		{KindPhone, "1234", DefaultRedaction},
		{KindSSN, "123-45-6789", "***-**-6789"},
		{KindCard, "4111 1111 1111 1111", "****-****-****-1111"},
		{KindName, "Mary Ann Smith", "M*** A*** S***"},
		{KindName, "Smith, John", "S*** J***"},
		{KindDOB, "1980-02-03", "****-**-**"},
		{KindAddress, "1 Main St", "[ADDRESS]"},
		{KindGeneric, "anything", DefaultRedaction},
	} {
		if got := run(t, ColumnPlan{Disposition: Mask, Kind: c.kind, Style: StyleFormat}, c.in); got != c.want {
			t.Errorf("%s %q: got %q want %q", c.kind, c.in, got, c.want)
		}
	}
}

func TestTokens(t *testing.T) {
	c := ColumnPlan{Disposition: Mask, Kind: KindGeneric, Style: StyleToken}
	a, b := run(t, c, "100234"), run(t, c, "100234")
	if a != b || len(a) != tokenLen {
		t.Fatalf("tokens not deterministic or wrong length: %q %q", a, b)
	}
	if run(t, c, "100235") == a {
		t.Fatal("different values gave the same token")
	}
	k, _ := kernelFor(c)
	if k("100234", []byte("another-key-another-key-xx")) == a {
		t.Fatal("token does not depend on the key")
	}
	if strings.Contains(a, "100234") {
		t.Fatal("token contains the value")
	}
}

func TestRedactionText(t *testing.T) {
	if got := run(t, ColumnPlan{Disposition: Mask, Kind: KindGeneric, Style: StyleRedact, Redaction: "[HIDDEN]"}, "x"); got != "[HIDDEN]" {
		t.Fatalf("got %q", got)
	}
}

func TestMaskColumnRules(t *testing.T) {
	if c := MaskColumn(KindSSN, StyleToken, false, ""); c.Style != StyleRedact {
		t.Errorf("SSN token without join need should redact, got %s", c.Style)
	}
	if c := MaskColumn(KindSSN, StyleToken, true, ""); c.Style != StyleToken {
		t.Errorf("join-critical SSN should stay a token, got %s", c.Style)
	}
	if c := MaskColumn(KindEmail, "", false, ""); c.Style != StyleFormat {
		t.Errorf("email default style: got %s", c.Style)
	}
	if c := MaskColumn("", "", false, ""); c.Kind != KindGeneric {
		t.Errorf("empty kind: got %s", c.Kind)
	}
}

func TestValidateFailsClosed(t *testing.T) {
	for _, p := range []Plan{
		{{Disposition: Mask, Kind: "retina_scan", Style: StyleToken}},
		{{Disposition: Mask, Kind: KindEmail, Style: "scramble"}},
		{{Disposition: "maybe"}},
	} {
		if err := p.Validate(); err == nil {
			t.Errorf("plan %+v accepted", p)
		}
	}
}

func source(alloc memory.Allocator) array.RecordReader {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ID", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "EMAIL", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "NOTES", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ACCT", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"a@x.org", "", "c@y.net"}, []bool{true, false, true})
	b.Field(2).(*array.StringBuilder).AppendValues([]string{"free text", "more", ""}, []bool{true, true, false})
	b.Field(3).(*array.Int64Builder).AppendValues([]int64{100234, 100234, 7}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, _ := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	return rdr
}

func TestWrap(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	plan := Plan{
		{Disposition: PassRaw},
		MaskColumn(KindEmail, StyleFormat, false, ""),
		{Disposition: Withhold},
		MaskColumn(KindGeneric, StyleToken, true, ""),
	}
	rdr, err := Wrap(source(alloc), plan, testKey, alloc)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	if rdr.Schema().Field(0).Type.ID() != arrow.INT64 || rdr.Schema().Field(1).Type.ID() != arrow.STRING || rdr.Schema().Field(3).Type.ID() != arrow.STRING {
		t.Fatalf("schema: %v", rdr.Schema())
	}
	if !rdr.Next() {
		t.Fatal(rdr.Err())
	}
	rec := rdr.RecordBatch()
	ids := rec.Column(0).(*array.Int64)
	emails := rec.Column(1).(*array.String)
	notes := rec.Column(2).(*array.String)
	accts := rec.Column(3).(*array.String)
	if ids.Value(2) != 3 {
		t.Error("pass_raw column changed")
	}
	if emails.Value(0) != "a***@***.org" || !emails.IsNull(1) || emails.Value(2) != "c***@***.net" {
		t.Errorf("emails: %v", emails)
	}
	if notes.Value(0) != WithheldText || notes.Value(1) != WithheldText || !notes.IsNull(2) {
		t.Errorf("withheld: %v", notes)
	}
	if accts.Value(0) != accts.Value(1) || accts.Value(0) == accts.Value(2) || accts.Value(0) == "100234" {
		t.Errorf("tokens: %v", accts)
	}
	if rdr.Next() {
		t.Fatal("extra batch")
	}
	if rdr.Err() != nil {
		t.Fatal(rdr.Err())
	}
}

func TestWrapRefusesBadPlans(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	if _, err := Wrap(source(alloc), Plan{{Disposition: PassRaw}}, testKey, alloc); err == nil {
		t.Error("short plan accepted")
	}
	bad := Plan{{Disposition: PassRaw}, {Disposition: Mask, Kind: "retina_scan", Style: StyleToken}, {Disposition: PassRaw}, {Disposition: PassRaw}}
	if _, err := Wrap(source(alloc), bad, testKey, alloc); err == nil {
		t.Error("unknown kind accepted")
	}
	tok := Plan{{Disposition: PassRaw}, {Disposition: PassRaw}, {Disposition: PassRaw}, MaskColumn(KindGeneric, StyleToken, true, "")}
	if _, err := Wrap(source(alloc), tok, nil, alloc); err == nil {
		t.Error("tokenizing without a key accepted")
	}
}
