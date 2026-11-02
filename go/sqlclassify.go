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
	"fmt"
	"strings"
)

// Splitting a query means running it several times with different predicates,
// so a query is only splittable when its result is a row by row mapping of one
// table's rows. Anything whose answer depends on seeing all the rows at once
// would be computed per slice, and the union would be wrong rather than merely
// unordered.
//
// The subtle half of that list is the constructs that stay valid SQL while
// returning wrong data: ORDER BY, DISTINCT, ROWNUM, FETCH, OFFSET, a bare
// aggregate, an analytic window. Each is checked below against the parsed
// query rather than against its text, which is the point of parsing: a column
// named ORDER_TOTAL, a string literal containing the word DISTINCT and a
// comment mentioning ROWNUM are all harmless, and text matching cannot tell
// them from the real thing.

// splitRefusal explains why a query cannot be split. It is not an error: the
// caller falls back to reading the query whole, which is always correct.
type splitRefusal struct {
	reason string
}

func (r splitRefusal) Error() string { return r.reason }

func refuse(format string, args ...interface{}) error {
	return splitRefusal{reason: fmt.Sprintf(format, args...)}
}

// selectClausesThatBlockSplitting names the SELECT fields whose presence means
// the query is not a row by row mapping.
//
// ORDER BY is here because splitting cannot preserve an order across slices,
// and silently returning the right rows in the wrong order is still the wrong
// answer to the query that was asked.
var selectClausesThatBlockSplitting = map[string]string{
	"distinct":      "DISTINCT, which only removes duplicates within a slice",
	"distinct_on":   "DISTINCT ON, which only removes duplicates within a slice",
	"group_by":      "GROUP BY, which aggregates across rows",
	"having":        "HAVING, which filters aggregated groups",
	"order_by":      "ORDER BY, and splitting cannot preserve an order across slices",
	"sort_by":       "SORT BY, and splitting cannot preserve an order across slices",
	"cluster_by":    "CLUSTER BY",
	"distribute_by": "DISTRIBUTE BY",
	"limit":         "a row limit, which every slice would apply to itself",
	"offset":        "OFFSET, which every slice would apply to itself",
	"fetch":         "FETCH, which every slice would apply to itself",
	"top":           "TOP, which every slice would apply to itself",
	"qualify":       "QUALIFY, which filters on analytic results",
	"windows":       "a named window, whose frame would be computed per slice",
	"connect":       "CONNECT BY, which generates rows from other rows",
	"locks":         "FOR UPDATE, and a lock taken per slice is not the same lock",
	"into":          "INTO",
	"with":          "a WITH clause",
	"lateral_views": "a lateral view",
	"joins":         "a join, so more than one table would have to be split",
	"sample":        "a sample, which every slice would take independently",
}

// aggregateKinds are the node kinds polyglot uses for aggregates. An aggregate
// with no GROUP BY returns one row for the whole query, so each slice would
// return its own result rather than part of one.
//
// aggregate_function is polyglot's catch all for an aggregate with no
// dedicated node, which is what keeps this list from having to be exhaustive.
var aggregateKinds = map[string]bool{
	"count":                 true,
	"sum":                   true,
	"avg":                   true,
	"min":                   true,
	"max":                   true,
	"stddev":                true,
	"stddev_pop":            true,
	"stddev_samp":           true,
	"variance":              true,
	"var_pop":               true,
	"var_samp":              true,
	"median":                true,
	"list_agg":              true,
	"approx_count_distinct": true,
	"j_s_o_n_array_agg":     true,
	"j_s_o_n_object_agg":    true,
	"array_agg":             true,
	"group_concat":          true,
	"aggregate_function":    true,

	// Every analytic function, since a window would be computed per slice: a
	// ROW_NUMBER would restart in each one.
	"window_function": true,
}

// aggregateFunctionNames catches aggregates that parse as a generic function
// node rather than a dedicated one, such as COLLECT and GROUPING.
var aggregateFunctionNames = map[string]bool{
	"COLLECT":         true,
	"GROUPING":        true,
	"GROUPING_ID":     true,
	"XMLAGG":          true,
	"CORR":            true,
	"COVAR_POP":       true,
	"COVAR_SAMP":      true,
	"DENSE_RANK":      true,
	"PERCENTILE_CONT": true,
	"PERCENTILE_DISC": true,
	"RANK":            true,
}

// splittableQuery is what the planner needs to know about a query it can split.
type splittableQuery struct {
	// statements is the parsed query. Slices are built by copying it, so it is
	// never modified in place.
	statements []astNode

	// owner and table name the one table the query reads, uppercased and
	// unquoted, for looking up partitions and ROWID ranges.
	owner string
	table string

	// alias is the table's alias if it has one, which a ROWID predicate has to
	// qualify with: once a table is aliased, Oracle rejects a bare reference
	// to it.
	alias string

	// pinnedAlready is true when the query already carries its own flashback
	// clause. The slices then share the caller's snapshot and this driver must
	// not substitute its own.
	pinnedAlready bool
}

// classifyQuery parses a query and reports whether it can be split, and if so
// which table to split by.
func classifyQuery(ctx context.Context, sql string) (*splittableQuery, error) {
	a, err := sharedAnalyzer(ctx)
	if err != nil {
		return nil, err
	}

	statements, err := a.parseStatements(ctx, sql)
	if err != nil {
		return nil, refuse("the query could not be parsed as Oracle SQL: %s", err)
	}
	if len(statements) != 1 {
		return nil, refuse("the request contains %d statements, and only a single query can be split",
			len(statements))
	}

	root := statements[0]
	kind := nodeKind(root)
	if kind != "select" {
		// A set operator parses as its own node rather than as a SELECT, so
		// UNION, MINUS and INTERSECT land here along with DML.
		return nil, refuse("the statement is %s rather than a plain SELECT, so it cannot be split by table",
			describeStatementKind(kind))
	}
	sel := child(root, "select")

	for field, description := range selectClausesThatBlockSplitting {
		if isSet(sel, field) {
			return nil, refuse("the query uses %s", description)
		}
	}

	// An aggregate or analytic anywhere in the projection or predicate.
	if found, ok := containsKind(sel["expressions"], aggregateKinds); ok {
		return nil, refuse("the query aggregates (%s), so each slice would return its own result "+
			"rather than part of one", strings.ToUpper(found))
	}
	if found, ok := containsKind(sel["where_clause"], aggregateKinds); ok {
		return nil, refuse("the query's WHERE clause aggregates (%s)", strings.ToUpper(found))
	}
	if name, ok := aggregateCallByName(sel); ok {
		return nil, refuse("the query aggregates with %s, so each slice would return its own result "+
			"rather than part of one", name)
	}

	// ROWNUM is assigned as rows are produced, so each slice would number its
	// own rows from one. A query filtering on it would return up to its limit
	// per slice.
	if hasRownum(root) {
		return nil, refuse("the query references ROWNUM, which every slice would assign from one")
	}

	// A nested query means another table, and AS OF SCN attaches to one table
	// reference: the inner table would be read at whatever moment each slice
	// happened to run, so the slices would not share a snapshot.
	if hasNestedQuery(sel) {
		return nil, refuse("the query contains a subquery, whose table would not be pinned to the " +
			"partition snapshot because a flashback clause applies to one table reference")
	}

	from := child(sel, "from")
	tables := list(from, "expressions")
	if len(tables) != 1 {
		return nil, refuse("the query reads %d tables, and only a single table query can be split",
			len(tables))
	}
	if kind := nodeKind(tables[0]); kind != "table" {
		return nil, refuse("the query selects from %s rather than a table", describeFromKind(kind))
	}
	tableRef := child(tables[0].(map[string]interface{}), "table")

	// A sample is a fraction of the table chosen per query, so slicing it
	// would sample each slice independently.
	if isSet(tableRef, "table_sample") {
		return nil, refuse("the query samples the table, which every slice would do independently")
	}

	name := identifierName(child(tableRef, "name"))
	if name == "" {
		return nil, refuse("the query's table has no plain name, so its partitions and ROWID " +
			"ranges cannot be looked up")
	}
	if isSet(tableRef, "identifier_func") {
		return nil, refuse("the query names its table dynamically")
	}

	return &splittableQuery{
		statements:    statements,
		owner:         strings.ToUpper(identifierName(child(tableRef, "schema"))),
		table:         strings.ToUpper(name),
		alias:         identifierName(child(tableRef, "alias")),
		pinnedAlready: isSet(tableRef, "version") || isSet(tableRef, "when"),
	}, nil
}

// describeStatementKind turns a node kind into something a caller can read.
func describeStatementKind(kind string) string {
	switch kind {
	case "union":
		return "a UNION, MINUS or INTERSECT"
	case "":
		return "an unrecognised statement"
	default:
		return "a " + strings.ToUpper(strings.ReplaceAll(kind, "_", " ")) + " statement"
	}
}

func describeFromKind(kind string) string {
	switch kind {
	case "subquery", "select", "union":
		return "a subquery"
	case "function", "table_function", "lateral":
		return "a function"
	case "values":
		return "a VALUES list"
	case "":
		return "an unrecognised expression"
	default:
		return "a " + strings.ReplaceAll(kind, "_", " ")
	}
}

// identifierName reads an identifier node's name.
func identifierName(node astNode) string {
	if node == nil {
		return ""
	}
	// An identifier appears either bare or wrapped, depending on the field.
	if name := text(node, "name"); name != "" {
		return name
	}
	return text(child(node, "identifier"), "name")
}

// aggregateCallByName finds an aggregate that parsed as a generic function.
func aggregateCallByName(sel astNode) (string, bool) {
	found := ""
	walkAST(sel, func(kind string, node astNode) bool {
		if kind != "function" {
			return true
		}
		name := strings.ToUpper(text(child(node, "function"), "name"))
		if aggregateFunctionNames[name] {
			found = name
			return false
		}
		return true
	})
	return found, found != ""
}

// hasRownum reports whether the query references the ROWNUM pseudocolumn.
func hasRownum(root astNode) bool {
	found := false
	walkAST(root, func(kind string, node astNode) bool {
		if kind != "pseudocolumn" {
			return true
		}
		if strings.EqualFold(text(child(node, "pseudocolumn"), "kind"), "rownum") {
			found = true
			return false
		}
		return true
	})
	return found
}

// hasNestedQuery reports whether anything below the top level is itself a
// query.
func hasNestedQuery(sel astNode) bool {
	nested := false
	for field, value := range sel {
		// The FROM clause is checked separately, since a single table there is
		// the whole point and a subquery there gets its own message.
		if field == "from" {
			continue
		}
		walkAST(value, func(kind string, _ astNode) bool {
			if kind == "select" || kind == "union" || kind == "subquery" {
				nested = true
				return false
			}
			return true
		})
		if nested {
			return true
		}
	}
	// A subquery in the FROM clause.
	for _, table := range list(child(sel, "from"), "expressions") {
		if kind := nodeKind(table); kind == "subquery" || kind == "select" || kind == "union" {
			return true
		}
	}
	return false
}

// --- Building a slice ---

// sliceSpec is one slice's modifications to the query.
type sliceSpec struct {
	// scn pins the slice to a snapshot. Empty leaves the query unpinned, which
	// is right for a single unsplit read.
	scn string

	// partition names a single partition to read, if the strategy is to split
	// by partition.
	partition string

	// predicate is SQL that narrows the slice, such as a ROWID range or a hash
	// bucket. It is parsed rather than spliced, so the generator decides the
	// parenthesising.
	predicate string
}

// buildSlice renders one slice's SQL from the parsed query.
//
// Everything happens on a copy of the AST: the predicate is ANDed into the
// WHERE clause, the flashback clause and partition are set on the table
// reference, and the generator turns it back into SQL. Operator precedence is
// the generator's problem, which is why a caller's `WHERE a > 5 OR b IS NULL`
// comes back as `WHERE (a > 5 OR b IS NULL) AND <predicate>` rather than
// silently widening to every row where a > 5.
func buildSlice(ctx context.Context, q *splittableQuery, spec sliceSpec) (string, error) {
	a, err := sharedAnalyzer(ctx)
	if err != nil {
		return "", err
	}

	statements, err := copyAST(q.statements)
	if err != nil {
		return "", err
	}
	sel := child(statements[0], "select")
	tableRef := child(list(child(sel, "from"), "expressions")[0].(map[string]interface{}), "table")

	if spec.partition != "" {
		tableRef["partitions"] = []interface{}{
			map[string]interface{}{"name": spec.partition, "quoted": false},
		}
	}

	// A query that already pins itself keeps its own clause, so the caller's
	// snapshot is honoured rather than replaced.
	if spec.scn != "" && !q.pinnedAlready {
		tableRef["version"] = map[string]interface{}{
			"this": map[string]interface{}{
				"identifier": map[string]interface{}{"name": "SCN", "quoted": false},
			},
			"kind": "AS OF",
			"expression": map[string]interface{}{
				"literal": map[string]interface{}{"literal_type": "number", "value": spec.scn},
			},
		}
	}

	if spec.predicate != "" {
		predicate, err := a.parsePredicate(ctx, spec.predicate)
		if err != nil {
			return "", err
		}
		existing := child(sel, "where_clause")
		if isSet(existing, "this") {
			sel["where_clause"] = map[string]interface{}{
				"this": map[string]interface{}{
					"and": map[string]interface{}{
						"left":              existing["this"],
						"right":             predicate,
						"left_comments":     []interface{}{},
						"operator_comments": []interface{}{},
						"trailing_comments": []interface{}{},
					},
				},
			}
		} else {
			sel["where_clause"] = map[string]interface{}{"this": predicate}
		}
	}

	return a.generate(ctx, statements)
}

// parsePredicate turns predicate SQL into an expression node by parsing it as
// a WHERE clause and taking the result.
//
// Building the node by hand would mean this driver encoding polyglot's node
// shapes for every operator it wants to emit. Parsing hands that job to the
// parser, which also means a predicate is validated before it reaches the
// database.
func (a *sqlAnalyzer) parsePredicate(ctx context.Context, predicate string) (interface{}, error) {
	statements, err := a.parseStatements(ctx, "SELECT 1 FROM DUAL WHERE "+predicate)
	if err != nil {
		return nil, fmt.Errorf("the partition predicate %q is not valid SQL: %w", predicate, err)
	}
	where := child(child(statements[0], "select"), "where_clause")
	if !isSet(where, "this") {
		return nil, fmt.Errorf("the partition predicate %q did not parse as a condition", predicate)
	}
	return where["this"], nil
}

// copyAST deep copies a parsed query so slices never share nodes.
//
// Through JSON rather than by hand: the tree is decoded JSON of a type this
// driver deliberately does not model, so a hand written copy would have to
// know every node shape.
func copyAST(statements []astNode) ([]astNode, error) {
	encoded, err := json.Marshal(statements)
	if err != nil {
		return nil, err
	}
	var copied []astNode
	if err := json.Unmarshal(encoded, &copied); err != nil {
		return nil, err
	}
	return copied, nil
}

// qualifiedRowid returns the ROWID reference for the query's table, qualified
// by the alias when there is one.
//
// Oracle rejects a bare table reference once the table is aliased, so a
// predicate on `T.ROWID` has to become `alias.ROWID`. For an unaliased table a
// bare ROWID is right, since the predicate is unambiguous with one table.
func (q *splittableQuery) qualifiedRowid() string {
	if q.alias != "" {
		return quoteOracleIdentifier(q.alias) + ".ROWID"
	}
	return "ROWID"
}
