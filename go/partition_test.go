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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// A rewrite that misreads a query returns wrong data rather than failing, so
// the classifier has to decline everything it cannot account for. These cases
// are the boundary: what is safe to split, and what must not be.
//
// These run the embedded SQL parser, so the first of them pays the one time
// cost of compiling the WebAssembly module (several seconds). No database is
// involved.
func TestClassifyQuery(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name    string
		query   string
		wantErr bool
		owner   string
		table   string
		alias   string
	}{
		{name: "star", query: "SELECT * FROM T1", table: "T1"},
		{name: "column list", query: "SELECT A, B FROM T1", table: "T1"},
		{name: "with where", query: "SELECT A FROM T1 WHERE A > 5", table: "T1"},
		{name: "qualified", query: "SELECT * FROM OWNER.T1", owner: "OWNER", table: "T1"},
		{name: "lowercase keywords", query: "select a from t1 where a = 1", table: "T1"},
		{name: "trailing semicolon", query: "SELECT * FROM T1;", table: "T1"},
		{name: "alias", query: "SELECT t.A FROM T1 t WHERE t.A > 1", table: "T1", alias: "t"},
		{name: "newlines", query: "SELECT A,\n  B\nFROM T1\nWHERE A IS NOT NULL", table: "T1"},
		{name: "expression columns", query: "SELECT A + 1, UPPER(B) FROM T1", table: "T1"},
		{name: "case expression", query: "SELECT CASE WHEN A > 1 THEN 1 ELSE 0 END FROM T1", table: "T1"},

		// Parsing rather than text matching is what makes these splittable.
		// The scanner this replaced rejected every one of them.
		{name: "optimizer hint", query: "SELECT /*+ PARALLEL(4) */ A FROM T1", table: "T1"},
		{name: "line comment", query: "SELECT A FROM T1 -- extract for the warehouse", table: "T1"},
		{name: "block comment", query: "SELECT A /* the key */ FROM T1", table: "T1"},
		{name: "keyword in a literal", query: "SELECT A FROM T1 WHERE B = 'ORDER BY DISTINCT'", table: "T1"},
		{name: "column named like a clause", query: "SELECT ORDER_TOTAL FROM T1 WHERE GROUPING_LEVEL = 1", table: "T1"},

		// Everything below would be split wrongly, so it must be refused.
		{name: "join", query: "SELECT * FROM T1 JOIN T2 ON T1.A = T2.A", wantErr: true},
		{name: "implicit join", query: "SELECT * FROM T1, T2 WHERE T1.A = T2.A", wantErr: true},
		{name: "union", query: "SELECT A FROM T1 UNION ALL SELECT A FROM T2", wantErr: true},
		{name: "minus", query: "SELECT A FROM T1 MINUS SELECT A FROM T2", wantErr: true},
		{name: "intersect", query: "SELECT A FROM T1 INTERSECT SELECT A FROM T2", wantErr: true},
		{name: "group by", query: "SELECT A, COUNT(*) FROM T1 GROUP BY A", wantErr: true},
		{name: "having", query: "SELECT A FROM T1 GROUP BY A HAVING COUNT(*) > 1", wantErr: true},
		{name: "connect by", query: "SELECT A FROM T1 CONNECT BY PRIOR A = B", wantErr: true},
		{name: "subquery in from", query: "SELECT * FROM (SELECT A FROM T1) X", wantErr: true},
		{name: "not a select", query: "UPDATE T1 SET A = 1", wantErr: true},
		{name: "empty", query: "", wantErr: true},

		// These return a different answer when run several times rather than
		// once, which is the subtle half of the problem: the SQL stays valid
		// and the data comes back wrong.
		{name: "order by", query: "SELECT A FROM T1 ORDER BY A", wantErr: true},
		{name: "distinct", query: "SELECT DISTINCT A FROM T1", wantErr: true},
		{name: "rownum limit", query: "SELECT A FROM T1 WHERE ROWNUM <= 10", wantErr: true},
		{name: "fetch first", query: "SELECT A FROM T1 FETCH FIRST 10 ROWS ONLY", wantErr: true},
		{name: "offset", query: "SELECT A FROM T1 OFFSET 5 ROWS", wantErr: true},
		{name: "bare count", query: "SELECT COUNT(*) FROM T1", wantErr: true},
		{name: "bare sum", query: "SELECT SUM(A) FROM T1", wantErr: true},
		{name: "bare max", query: "SELECT MAX(A) FROM T1", wantErr: true},
		{name: "listagg", query: "SELECT LISTAGG(A, ',') FROM T1", wantErr: true},
		{name: "aggregate in where", query: "SELECT A FROM T1 WHERE A > ALL (SELECT MAX(B) FROM T2)", wantErr: true},
		{name: "analytic window", query: "SELECT ROW_NUMBER() OVER (ORDER BY A) FROM T1", wantErr: true},
		{name: "analytic sum", query: "SELECT SUM(A) OVER () FROM T1", wantErr: true},
		{name: "for update", query: "SELECT A FROM T1 FOR UPDATE", wantErr: true},
		{name: "sample", query: "SELECT A FROM T1 SAMPLE (10)", wantErr: true},

		// A subquery is valid to rewrite but not safe to split: a flashback
		// clause pins only the table in the FROM clause, so the inner table
		// would be read at a different moment by each slice.
		{name: "subquery in where", query: "SELECT A FROM T1 WHERE A IN (SELECT B FROM T2)", wantErr: true},
		{name: "scalar subquery", query: "SELECT A, (SELECT MIN(B) FROM T2) FROM T1", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := classifyQuery(ctx, tc.query)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("classifyQuery(%q) accepted a query it cannot split safely, giving table %s.%s",
						tc.query, got.owner, got.table)
				}
				// A refusal has to be distinguishable from the parser being
				// broken, because the two call for different handling.
				var refusal splitRefusal
				if !errors.As(err, &refusal) {
					t.Fatalf("classifyQuery(%q) failed rather than refusing: %v", tc.query, err)
				}
				if refusal.reason == "" {
					t.Error("the refusal carries no reason")
				}
				return
			}
			if err != nil {
				t.Fatalf("classifyQuery(%q): %v", tc.query, err)
			}
			if got.table != tc.table {
				t.Errorf("table = %q, want %q", got.table, tc.table)
			}
			if got.owner != tc.owner {
				t.Errorf("owner = %q, want %q", got.owner, tc.owner)
			}
			if got.alias != tc.alias {
				t.Errorf("alias = %q, want %q", got.alias, tc.alias)
			}
		})
	}
}

// A predicate must never be lost and must never be widened: a slice missing
// its WHERE returns rows the caller did not ask for, and one that loses the
// caller's parentheses returns rows from outside its own slice.
func TestBuildSliceKeepsThePredicate(t *testing.T) {
	ctx := context.Background()
	q, err := classifyQuery(ctx, "SELECT A FROM T1 WHERE A > 5 OR B IS NULL")
	if err != nil {
		t.Fatalf("classify: %v", err)
	}

	got, err := buildSlice(ctx, q, sliceSpec{scn: "4242", predicate: "ORA_HASH(ROWID, 3) = 1"})
	if err != nil {
		t.Fatalf("buildSlice: %v", err)
	}

	// The original predicate is an OR, so ANDing without parenthesising it
	// would widen the slice to every row where A > 5, regardless of hash.
	// Generating from the AST is what makes the parentheses the generator's
	// job rather than this driver's.
	want := "SELECT A FROM T1 AS OF SCN 4242 WHERE (A > 5 OR B IS NULL) AND ORA_HASH(ROWID, 3) = 1"
	if got != want {
		t.Errorf("buildSlice:\n got %s\nwant %s", got, want)
	}
}

// Oracle requires PARTITION before AS OF SCN and rejects the reverse with
// ORA-03048, so the generated order is a correctness requirement.
func TestBuildSlicePartitionPrecedesFlashback(t *testing.T) {
	ctx := context.Background()
	q, err := classifyQuery(ctx, "SELECT * FROM T1")
	if err != nil {
		t.Fatalf("classify: %v", err)
	}

	got, err := buildSlice(ctx, q, sliceSpec{scn: "99", partition: "P1"})
	if err != nil {
		t.Fatalf("buildSlice: %v", err)
	}
	if want := "SELECT * FROM T1 PARTITION(P1) AS OF SCN 99"; got != want {
		t.Fatalf("buildSlice = %q, want %q", got, want)
	}
	if strings.Index(got, "AS OF SCN") < strings.Index(got, "PARTITION") {
		t.Error("AS OF SCN precedes PARTITION, which Oracle rejects with ORA-03048")
	}
}

func TestBuildSliceCombinations(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name  string
		query string
		spec  sliceSpec
		want  string
	}{
		{"no scn no predicate", "SELECT A FROM T1", sliceSpec{}, "SELECT A FROM T1"},
		{"predicate only", "SELECT A FROM T1", sliceSpec{predicate: "ROWID IS NOT NULL"},
			"SELECT A FROM T1 WHERE ROWID IS NOT NULL"},
		{"where only", "SELECT A FROM T1 WHERE A > 1", sliceSpec{}, "SELECT A FROM T1 WHERE A > 1"},
		{"scn only", "SELECT A FROM T1", sliceSpec{scn: "7"}, "SELECT A FROM T1 AS OF SCN 7"},
		{"where and predicate", "SELECT A FROM T1 WHERE A > 1", sliceSpec{predicate: "B = 2"},
			"SELECT A FROM T1 WHERE A > 1 AND B = 2"},
		// A hint has to survive the rewrite, since the caller put it there to
		// control the plan the slice runs with.
		{"hint survives", "SELECT /*+ FULL(T1) */ A FROM T1", sliceSpec{scn: "5"},
			"SELECT /*+ FULL(T1) */ A FROM T1 AS OF SCN 5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := classifyQuery(ctx, tc.query)
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			got, err := buildSlice(ctx, q, tc.spec)
			if err != nil {
				t.Fatalf("buildSlice: %v", err)
			}
			if got != tc.want {
				t.Errorf("\n got %s\nwant %s", got, tc.want)
			}
		})
	}
}

// Once a table is aliased, Oracle rejects a bare reference to it, so a ROWID
// predicate has to be qualified with the alias.
func TestBuildSliceQualifiesRowidWithTheAlias(t *testing.T) {
	ctx := context.Background()

	aliased, err := classifyQuery(ctx, "SELECT x.A FROM T1 x WHERE x.A > 1")
	if err != nil {
		t.Fatalf("classify aliased: %v", err)
	}
	if got := aliased.qualifiedRowid(); got != `"x".ROWID` {
		t.Errorf("aliased ROWID = %q, want %q", got, `"x".ROWID`)
	}

	bare, err := classifyQuery(ctx, "SELECT A FROM T1")
	if err != nil {
		t.Fatalf("classify bare: %v", err)
	}
	if got := bare.qualifiedRowid(); got != "ROWID" {
		t.Errorf("unaliased ROWID = %q, want ROWID", got)
	}

	// And the qualified form has to survive into the generated slice.
	got, err := buildSlice(ctx, aliased, sliceSpec{
		predicate: rowidPredicate(aliased.qualifiedRowid(), [][2]string{{"AAA", "BBB"}}),
	})
	if err != nil {
		t.Fatalf("buildSlice: %v", err)
	}
	if !strings.Contains(got, `"x".ROWID BETWEEN`) {
		t.Errorf("the alias was lost from the ROWID predicate: %s", got)
	}
}

// A query that already pins itself to a snapshot keeps its own clause: the
// caller chose that SCN and the slices should share it rather than be moved to
// a different one.
func TestBuildSliceRespectsAnExistingFlashback(t *testing.T) {
	ctx := context.Background()
	q, err := classifyQuery(ctx, "SELECT A FROM T1 AS OF SCN 1000")
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if !q.pinnedAlready {
		t.Fatal("a query with AS OF SCN was not recognised as already pinned")
	}

	got, err := buildSlice(ctx, q, sliceSpec{scn: "9999", predicate: "ORA_HASH(ROWID, 1) = 0"})
	if err != nil {
		t.Fatalf("buildSlice: %v", err)
	}
	if !strings.Contains(got, "AS OF SCN 1000") {
		t.Errorf("the caller's SCN was replaced: %s", got)
	}
	if strings.Contains(got, "9999") {
		t.Errorf("the driver's SCN overrode the caller's: %s", got)
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
	one := rowidPredicate("ROWID", [][2]string{{"AAASuGAAMAAAACFAAA", "AAASuGAAMAAABEHCcf"}})
	if want := "ROWID BETWEEN CHARTOROWID('AAASuGAAMAAAACFAAA') AND CHARTOROWID('AAASuGAAMAAABEHCcf')"; one != want {
		t.Errorf("single range:\n got %s\nwant %s", one, want)
	}
	if strings.Count(one, "CHARTOROWID") != 2 {
		t.Error("an endpoint is being compared as text, which drops rows")
	}

	two := rowidPredicate("ROWID", [][2]string{{"a", "b"}, {"c", "d"}})
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
	if want := "SELECT * FROM (SELECT A FROM T1 WHERE A > 1\n) WHERE 1 = 0"; got != want {
		t.Errorf("describeQuery:\n got %q\nwant %q", got, want)
	}
}

// A query ending in a line comment must still be wrappable: without a newline
// the closing parenthesis lands inside the comment and Oracle reports
// ORA-00907, missing right parenthesis.
func TestDescribeQuerySurvivesALineComment(t *testing.T) {
	got := describeQuery("SELECT A FROM T1 -- nightly extract")
	if !strings.HasSuffix(got, ") WHERE 1 = 0") {
		t.Fatalf("describeQuery produced %q", got)
	}
	comment := strings.Index(got, "--")
	newline := strings.Index(got, "\n")
	if newline < comment {
		t.Errorf("the newline does not end the comment: %q", got)
	}
	if strings.Contains(got[comment:newline], ")") {
		t.Errorf("the closing parenthesis is inside the comment: %q", got)
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

// A query whose expressions nest even moderately deeply used to defeat the
// embedded parser. The WASI module was linked with the default 1 MiB stack, so
// a sum of forty columns overflowed it while parsing or generating, and in
// WASM that reaches the host as "out of bounds memory access" with no message.
// The driver could only read it as the parser being unavailable and fall back
// to an unsplit read, so the cost was silent: correct data, no parallelism, no
// explanation.
//
// The module now links a 64 MiB stack and bounds request depth explicitly.
// This test is what would catch a rebuild from an older or differently
// configured source.
func TestClassifyAndBuildSurviveDeepExpressions(t *testing.T) {
	ctx := context.Background()

	for _, terms := range []int{10, 40, 61, 120, 250} {
		columns := make([]string, terms)
		for i := range columns {
			columns[i] = "c" + itoa(i)
		}
		sql := "SELECT " + strings.Join(columns, " + ") + " AS total FROM T1 WHERE c0 > 0"

		q, err := classifyQuery(ctx, sql)
		if err != nil {
			t.Errorf("%d terms: classify: %v", terms, err)
			continue
		}
		got, err := buildSlice(ctx, q, sliceSpec{scn: "123", predicate: "ORA_HASH(ROWID, 3) = 1"})
		if err != nil {
			t.Errorf("%d terms: buildSlice: %v", terms, err)
			continue
		}
		// The slice still has to carry everything it was given.
		for _, want := range []string{"AS OF SCN 123", "ORA_HASH(ROWID, 3) = 1", "c0 > 0", "AS total"} {
			if !strings.Contains(got, want) {
				t.Errorf("%d terms: the slice lost %q: %s", terms, want, got)
			}
		}
	}
}

// Past the depth the module supports, the answer has to be a reported error
// rather than a trap, so the planner can fall back to an unsplit read with a
// reason instead of reporting the parser as broken.
func TestClassifyReportsRatherThanTrappingOnAbsurdNesting(t *testing.T) {
	ctx := context.Background()

	// Deep enough that the parser's own guard refuses it well before the
	// protocol's depth bound comes into play.
	sql := "SELECT " + strings.Repeat("(", 2000) + "1" + strings.Repeat(")", 2000) + " FROM T1"
	_, err := classifyQuery(ctx, sql)
	if err == nil {
		t.Fatal("a query nested 2000 levels deep was accepted")
	}
	if strings.Contains(err.Error(), "out of bounds memory access") {
		t.Errorf("the parser trapped rather than reporting a reason: %v", err)
	}
	t.Logf("reported: %v", err)
}
