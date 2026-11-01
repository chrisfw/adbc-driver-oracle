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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// A rewrite that misreads a query returns wrong data rather than failing, so
// the parser has to decline everything it cannot account for. These cases are
// the boundary: what is safe to split, and what must not be.
func TestParseSingleTableQuery(t *testing.T) {
	cases := []struct {
		name    string
		query   string
		wantErr bool
		table   string
		where   string
		columns string
	}{
		{name: "star", query: "SELECT * FROM T1", table: "T1", columns: "*"},
		{name: "column list", query: "SELECT A, B FROM T1", table: "T1", columns: "A, B"},
		{name: "with where", query: "SELECT A FROM T1 WHERE A > 5", table: "T1", columns: "A", where: "A > 5"},
		{name: "qualified", query: "SELECT * FROM OWNER.T1", table: "OWNER.T1", columns: "*"},
		{name: "quoted", query: `SELECT * FROM "MixedCase"`, table: `"MixedCase"`, columns: "*"},
		{name: "lowercase keywords", query: "select a from t1 where a = 1", table: "t1", columns: "a", where: "a = 1"},
		{name: "trailing semicolon", query: "SELECT * FROM T1;", table: "T1", columns: "*"},
		{name: "alias", query: "SELECT t.A FROM T1 t WHERE t.A > 1", table: "T1", columns: "t.A", where: "t.A > 1"},
		{name: "newlines", query: "SELECT A,\n  B\nFROM T1\nWHERE A IS NOT NULL",
			table: "T1", columns: "A,\n  B", where: "A IS NOT NULL"},

		// Everything below would be split wrongly, so it must be refused.
		{name: "join", query: "SELECT * FROM T1 JOIN T2 ON T1.A = T2.A", wantErr: true},
		{name: "implicit join", query: "SELECT * FROM T1, T2 WHERE T1.A = T2.A", wantErr: true},
		{name: "union", query: "SELECT A FROM T1 UNION ALL SELECT A FROM T2", wantErr: true},
		{name: "minus", query: "SELECT A FROM T1 MINUS SELECT A FROM T2", wantErr: true},
		{name: "intersect", query: "SELECT A FROM T1 INTERSECT SELECT A FROM T2", wantErr: true},
		{name: "group by", query: "SELECT A, COUNT(*) FROM T1 GROUP BY A", wantErr: true},
		{name: "having", query: "SELECT A FROM T1 GROUP BY A HAVING COUNT(*) > 1", wantErr: true},
		{name: "connect by", query: "SELECT A FROM T1 CONNECT BY PRIOR A = B", wantErr: true},
		{name: "subquery in from", query: "SELECT * FROM (SELECT A FROM T1)", wantErr: true},
		{name: "not a select", query: "UPDATE T1 SET A = 1", wantErr: true},
		{name: "unbalanced parens", query: "SELECT * FROM T1 WHERE A IN (1, 2", wantErr: true},
		{name: "empty", query: "", wantErr: true},

		// These return a different answer when run several times rather than
		// once, which is the subtle half of the problem: the SQL stays valid
		// and the data comes back wrong.
		{name: "order by", query: "SELECT A FROM T1 ORDER BY A", wantErr: true},
		{name: "distinct", query: "SELECT DISTINCT A FROM T1", wantErr: true},
		{name: "rownum limit", query: "SELECT A FROM T1 WHERE ROWNUM <= 10", wantErr: true},
		{name: "fetch first", query: "SELECT A FROM T1 FETCH FIRST 10 ROWS ONLY", wantErr: true},
		{name: "offset", query: "SELECT A FROM T1 OFFSET 5 ROWS", wantErr: true},
		{name: "bare aggregate", query: "SELECT COUNT(*) FROM T1", wantErr: true},
		{name: "sum", query: "SELECT SUM(A) FROM T1", wantErr: true},
		{name: "analytic window", query: "SELECT ROW_NUMBER() OVER (ORDER BY A) FROM T1", wantErr: true},
		{name: "for update", query: "SELECT A FROM T1 FOR UPDATE", wantErr: true},

		// A comment can hide any of the above, and this rewriter does not
		// parse comments.
		{name: "line comment", query: "SELECT A FROM T1 -- ORDER BY A", wantErr: true},
		{name: "block comment", query: "SELECT A /* DISTINCT */ FROM T1", wantErr: true},

		// A subquery is valid to rewrite but not safe to split: AS OF SCN
		// pins only the table in the FROM clause, so the inner table would be
		// read at a different moment by each slice.
		{name: "subquery in where", query: "SELECT A FROM T1 WHERE A IN (SELECT B FROM T2)", wantErr: true},
		{name: "scalar subquery in select list", query: "SELECT A, (SELECT B FROM T2) FROM T1", wantErr: true},
		{name: "extra clause", query: "SELECT A FROM T1 SAMPLE (10)", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSingleTableQuery(tc.query)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseSingleTableQuery(%q) accepted a query it cannot split safely, giving table %q where %q",
						tc.query, got.table, got.where)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSingleTableQuery(%q): %v", tc.query, err)
			}
			if got.table != tc.table {
				t.Errorf("table = %q, want %q", got.table, tc.table)
			}
			if got.where != tc.where {
				t.Errorf("where = %q, want %q", got.where, tc.where)
			}
			if got.columns != tc.columns {
				t.Errorf("columns = %q, want %q", got.columns, tc.columns)
			}
		})
	}
}

// A keyword inside a string literal is data, not syntax. Mistaking one for a
// clause boundary would slice the query at the wrong place.
func TestParseIgnoresKeywordsInsideLiterals(t *testing.T) {
	got, err := parseSingleTableQuery("SELECT A FROM T1 WHERE B = 'FROM WHERE ORDER BY'")
	if err != nil {
		t.Fatalf("a literal containing keywords was treated as syntax: %v", err)
	}
	if got.table != "T1" {
		t.Errorf("table = %q, want T1", got.table)
	}
	if want := "B = 'FROM WHERE ORDER BY'"; got.where != want {
		t.Errorf("where = %q, want %q", got.where, want)
	}
}

// A subquery whose WHERE mentions a column named like a keyword must not be
// mistaken for a disqualifying construct, and a predicate must never be lost:
// a slice missing its WHERE returns rows the caller did not ask for.
func TestSliceQueryKeepsThePredicate(t *testing.T) {
	pq, err := parseSingleTableQuery("SELECT A FROM T1 WHERE A > 5 OR B IS NULL")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	got := pq.sliceQuery("4242", "", "ORA_HASH(ROWID, 3) = 1")

	// The original predicate is an OR, so appending AND without parenthesising
	// it would widen the slice to every row where A > 5, regardless of hash.
	// That is the bug this asserts against.
	want := "SELECT A FROM T1 AS OF SCN 4242 WHERE (A > 5 OR B IS NULL) AND (ORA_HASH(ROWID, 3) = 1)"
	if got != want {
		t.Errorf("sliceQuery:\n got %s\nwant %s", got, want)
	}
}

// PARTITION has to come before AS OF SCN. The other order is ORA-03048, so
// this ordering is a syntax requirement rather than a style choice.
func TestSliceQueryPartitionPrecedesFlashback(t *testing.T) {
	pq := parsedQuery{columns: "*", table: "T1"}
	got := pq.sliceQuery("99", "P1", "")

	if want := "SELECT * FROM T1 PARTITION (P1) AS OF SCN 99"; got != want {
		t.Fatalf("sliceQuery = %q, want %q", got, want)
	}
	if strings.Index(got, "AS OF SCN") < strings.Index(got, "PARTITION") {
		t.Error("AS OF SCN precedes PARTITION, which Oracle rejects with ORA-03048")
	}
}

func TestSliceQueryCombinations(t *testing.T) {
	pq := parsedQuery{columns: "A", table: "T1"}
	withWhere := parsedQuery{columns: "A", table: "T1", where: "A > 1"}

	cases := []struct {
		name string
		got  string
		want string
	}{
		{"no scn no predicate", pq.sliceQuery("", "", ""), "SELECT A FROM T1"},
		{"predicate only", pq.sliceQuery("", "", "ROWID IS NOT NULL"), "SELECT A FROM T1 WHERE ROWID IS NOT NULL"},
		{"where only", withWhere.sliceQuery("", "", ""), "SELECT A FROM T1 WHERE A > 1"},
		{"scn only", pq.sliceQuery("7", "", ""), "SELECT A FROM T1 AS OF SCN 7"},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", tc.name, tc.got, tc.want)
		}
	}
}

// Every row has to land in exactly one slice. Coalescing is where that could
// break: a dropped or duplicated chunk would silently change the answer.
func TestCoalesceRangesCoversEveryChunkOnce(t *testing.T) {
	for _, total := range []int{1, 2, 3, 4, 7, 51, 100} {
		for _, want := range []int{1, 2, 3, 4, 8} {
			ranges := make([][2]string, total)
			for i := range ranges {
				ranges[i] = [2]string{itoa(i), itoa(i)}
			}

			groups := coalesceRanges(ranges, want)

			if len(groups) > want && total > want {
				t.Fatalf("total=%d want=%d: produced %d groups, more than requested", total, want, len(groups))
			}
			if total < want && len(groups) != total {
				t.Fatalf("total=%d want=%d: produced %d groups, want one per chunk", total, want, len(groups))
			}

			seen := make(map[string]int)
			for _, g := range groups {
				if len(g) == 0 {
					t.Fatalf("total=%d want=%d: an empty group would open a session that reads nothing", total, want)
				}
				for _, r := range g {
					seen[r[0]]++
				}
			}
			if len(seen) != total {
				t.Fatalf("total=%d want=%d: %d of %d chunks survived coalescing", total, want, len(seen), total)
			}
			for id, n := range seen {
				if n != 1 {
					t.Fatalf("total=%d want=%d: chunk %s appears in %d groups, so its rows would be returned twice",
						total, want, id, n)
				}
			}
		}
	}
}

// The remainder is spread over the leading groups rather than piled onto one,
// so no slice does several times the work of its peers.
func TestCoalesceRangesBalances(t *testing.T) {
	ranges := make([][2]string, 10)
	for i := range ranges {
		ranges[i] = [2]string{itoa(i), itoa(i)}
	}
	groups := coalesceRanges(ranges, 4)

	sizes := make([]int, len(groups))
	for i, g := range groups {
		sizes[i] = len(g)
	}
	min, max := sizes[0], sizes[0]
	for _, n := range sizes {
		if n < min {
			min = n
		}
		if n > max {
			max = n
		}
	}
	if max-min > 1 {
		t.Errorf("group sizes %v differ by more than one chunk", sizes)
	}
}

// ROWID's character form is base-64 encoded, and its string ordering is not
// its ROWID ordering. Comparing the text directly brackets a fraction of the
// table and silently loses the rest, so the endpoints must go through
// CHARTOROWID.
func TestRowidPredicateConvertsEndpoints(t *testing.T) {
	one := rowidPredicate([][2]string{{"AAASuGAAMAAAACFAAA", "AAASuGAAMAAABEHCcf"}})
	if want := "ROWID BETWEEN CHARTOROWID('AAASuGAAMAAAACFAAA') AND CHARTOROWID('AAASuGAAMAAABEHCcf')"; one != want {
		t.Errorf("single range:\n got %s\nwant %s", one, want)
	}
	if strings.Count(one, "CHARTOROWID") != 2 {
		t.Error("an endpoint is being compared as text, which drops rows")
	}

	two := rowidPredicate([][2]string{{"a", "b"}, {"c", "d"}})
	want := "(ROWID BETWEEN CHARTOROWID('a') AND CHARTOROWID('b')) OR " +
		"(ROWID BETWEEN CHARTOROWID('c') AND CHARTOROWID('d'))"
	if two != want {
		t.Errorf("two ranges:\n got %s\nwant %s", two, want)
	}
}

// A descriptor crosses from one connection to another, so the pinned types
// have to survive the trip exactly. A type that came back wider or narrower
// would make the slice disagree with the schema it was promised.
func TestNumberTypesRoundTrip(t *testing.T) {
	in := map[int]arrow.DataType{
		0: &arrow.Decimal128Type{Precision: 38, Scale: 2},
		1: &arrow.Decimal256Type{Precision: 76, Scale: 0},
		3: &arrow.Decimal32Type{Precision: 9, Scale: 4},
		4: &arrow.Decimal64Type{Precision: 18, Scale: 7},
	}

	encoded := encodeNumberTypes(in)
	// Through JSON, since that is how it actually travels.
	blob, err := json.Marshal(encoded)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decodedSpec map[int]string
	if err := json.Unmarshal(blob, &decodedSpec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	out, err := decodeNumberTypes(decodedSpec)
	if err != nil {
		t.Fatalf("decodeNumberTypes: %v", err)
	}

	if len(out) != len(in) {
		t.Fatalf("round trip returned %d types, want %d", len(out), len(in))
	}
	for i, want := range in {
		if got := out[i]; !arrow.TypeEqual(got, want) {
			t.Errorf("column %d: round trip gave %s, want %s", i, got, want)
		}
	}
}

// A type that was not derived from data needs no pinning, because the
// receiving slice reaches the same conclusion from the catalog alone.
func TestEncodeNumberTypesSkipsNonDecimals(t *testing.T) {
	got := encodeNumberTypes(map[int]arrow.DataType{
		0: arrow.BinaryTypes.String,
		1: arrow.PrimitiveTypes.Int64,
	})
	if got != nil {
		t.Errorf("encodeNumberTypes carried %v for types that need no pinning", got)
	}
	if encodeNumberTypes(nil) != nil {
		t.Error("encodeNumberTypes(nil) should carry nothing")
	}
}

func TestDecodeNumberTypesRejectsGarbage(t *testing.T) {
	for _, spec := range []string{"utf8", "decimal128", "decimal128(38)", "decimal99(1,1)",
		"decimal128(38,2) ", "DECIMAL128(38,2)", ""} {
		if _, err := decodeNumberTypes(map[int]string{0: spec}); err == nil {
			t.Errorf("decodeNumberTypes accepted %q", spec)
		}
	}
}

// The descriptor is an opaque blob a caller may store and replay, so a version
// this driver does not write has to be refused rather than guessed at.
func TestDecodePartitionDescriptorValidates(t *testing.T) {
	good := partitionDescriptor{Query: "SELECT 1 FROM DUAL"}
	blob, err := good.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if d, err := decodePartitionDescriptor(blob); err != nil {
		t.Fatalf("decode of own output: %v", err)
	} else if d.Query != good.Query {
		t.Errorf("query = %q, want %q", d.Query, good.Query)
	}

	for _, bad := range []string{
		`not json`,
		`{"version": 99, "query": "SELECT 1 FROM DUAL"}`,
		`{"version": 1, "query": ""}`,
		`{"version": 1, "query": "   "}`,
		`{}`,
	} {
		if _, err := decodePartitionDescriptor([]byte(bad)); err == nil {
			t.Errorf("decodePartitionDescriptor accepted %q", bad)
		}
	}
}

func TestDescribeQueryReturnsNoRows(t *testing.T) {
	got := describeQuery("SELECT A FROM T1 WHERE A > 1;")
	if want := "SELECT * FROM (SELECT A FROM T1 WHERE A > 1) WHERE 1 = 0"; got != want {
		t.Errorf("describeQuery:\n got %s\nwant %s", got, want)
	}
}

// The statement's own setting wins over the connection's, which wins over the
// built-in default.
func TestPartitionCountResolution(t *testing.T) {
	cnxn := &connectionImpl{}
	stmt := &statementImpl{cnxn: cnxn}

	if got := stmt.partitionCount(); got != defaultPartitionCount {
		t.Errorf("unset = %d, want the default %d", got, defaultPartitionCount)
	}

	cnxn.partitionCount = 7
	if got := stmt.partitionCount(); got != 7 {
		t.Errorf("connection setting = %d, want 7", got)
	}

	stmt.partitionCountOpt = 3
	if got := stmt.partitionCount(); got != 3 {
		t.Errorf("statement override = %d, want 3", got)
	}

	// A statement with no connection must still answer, since GetOption can
	// be called before anything is executed.
	if got := (&statementImpl{}).partitionCount(); got != defaultPartitionCount {
		t.Errorf("no connection = %d, want the default %d", got, defaultPartitionCount)
	}
}

// A flashback failure has to be told apart from a problem with the query
// itself: the first means read the query unsplit, the second has to reach the
// caller.
func TestFlashbackUnusable(t *testing.T) {
	unusable := []struct {
		code int
		why  string
	}{
		{1466, "the SCN predates the table's last DDL"},
		{8180, "no snapshot found for the given time"},
		{8181, "not a valid system change number"},
		{1555, "snapshot too old"},
		{30052, "invalid lower limit snapshot expression"},
		{439, "flashback is not enabled in this edition"},
		{41900, "no FLASHBACK privilege on the object"},
		{1031, "insufficient privileges"},
	}
	for _, tc := range unusable {
		err := fmt.Errorf("ORA-%05d: %s", tc.code, tc.why)
		if !flashbackUnusable(err) {
			t.Errorf("ORA-%05d (%s) is not recognised, so the read would fail instead of falling back",
				tc.code, tc.why)
		}
	}

	// These are the query's problem, not the snapshot's, and hiding them
	// behind a silent fallback would turn a clear error into a slow one.
	for _, code := range []int{904, 942, 1, 60} {
		err := fmt.Errorf("ORA-%05d: something else", code)
		if flashbackUnusable(err) {
			t.Errorf("ORA-%05d was treated as a flashback problem", code)
		}
	}
	if flashbackUnusable(nil) {
		t.Error("flashbackUnusable(nil) is true")
	}
	if flashbackUnusable(errors.New("connection reset by peer")) {
		t.Error("a network error was treated as a flashback problem")
	}
}
