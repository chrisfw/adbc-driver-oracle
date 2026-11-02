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
	"log/slog"
	"regexp"
	"strconv"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// itoa is strconv.Itoa, named short because it appears inside SQL building.
func itoa(n int) string { return strconv.Itoa(n) }

// ExecutePartitions splits one query into slices that different connections
// can read at the same time. The read path is roughly three quarters fetch
// wait, so overlapping whole queries attacks the dominant cost in a way a
// faster decoder cannot.
//
// Three things decide whether this is worth doing, and they are not equal:
//
//   - A partitioned table splits by partition. Each slice reads a disjoint set
//     of segments, so the work is divided rather than duplicated.
//   - A heap table splits by ROWID range, via DBMS_PARALLEL_EXECUTE's chunker.
//     Also disjoint: each slice scans its own blocks.
//   - Failing both, slices are separated by ORA_HASH of the ROWID. This always
//     works and every slice scans the whole table, so total I/O is multiplied
//     by the slice count. It can still win on wall clock when the server has
//     spare I/O, and it can lose badly when it does not, so it is last and
//     is reported in the descriptor rather than hidden.
//
// Every slice reads AS OF the same SCN, captured once here, so the union is a
// consistent snapshot rather than a set of reads drifting against concurrent
// writes.

// partitionStrategy names how a query was split, so a caller can tell a
// divided scan from a duplicated one.
type partitionStrategy string

const (
	partitionByPartition  partitionStrategy = "partition"
	partitionByRowidRange partitionStrategy = "rowid_range"
	partitionByHash       partitionStrategy = "ora_hash"
	partitionSingle       partitionStrategy = "single"
)

// partitionDescriptor is the opaque bytes ADBC passes from ExecutePartitions
// to ReadPartition. It carries the finished SQL rather than the pieces to
// rebuild it, so ReadPartition stays a plain execute and cannot reach a
// different conclusion than the planner did.
type partitionDescriptor struct {
	// Version guards against reading a descriptor produced by a future
	// driver whose fields mean something else.
	Version int `json:"version"`

	// Query is the complete statement for this slice, already carrying the
	// AS OF SCN and whatever predicate separates it from the others.
	Query string `json:"query"`

	// Strategy and Index are informational, for logs and tests.
	Strategy partitionStrategy `json:"strategy"`
	Index    int               `json:"index"`
	Count    int               `json:"count"`

	// SCN is the snapshot every slice reads at.
	SCN string `json:"scn,omitempty"`

	// NumberTypes pins the Arrow type of any column whose type would
	// otherwise be inferred from data. Slices see different rows, so left to
	// infer they could disagree with each other and with the schema
	// ExecutePartitions returned. Keyed by column index.
	NumberTypes map[int]string `json:"number_types,omitempty"`
}

const partitionDescriptorVersion = 1

func (d *partitionDescriptor) encode() ([]byte, error) {
	d.Version = partitionDescriptorVersion
	return json.Marshal(d)
}

func decodePartitionDescriptor(b []byte) (*partitionDescriptor, error) {
	var d partitionDescriptor
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("partition descriptor is not valid: %w", err)
	}
	if d.Version != partitionDescriptorVersion {
		return nil, fmt.Errorf(
			"partition descriptor version %d is not supported by this driver, which writes version %d",
			d.Version, partitionDescriptorVersion)
	}
	if strings.TrimSpace(d.Query) == "" {
		return nil, fmt.Errorf("partition descriptor carries no query")
	}
	return &d, nil
}

// Splitting requires knowing the table, because every strategy needs either
// its partitions, its ROWID ranges, or a predicate attached to its rows. That
// means recognising the query shape, and recognising it conservatively: a
// rewrite that silently drops a predicate or misreads a join would return
// wrong data, which is far worse than declining to parallelise.
//
// So only the shape below is accepted, and anything else is reported as not
// implemented with the reason. The accepted shape is a single table, no join,
// no set operator, no subquery, and an optional WHERE.
// Recognising which queries can be split, and rewriting them into slices,
// both live in sqlclassify.go. They work on the parsed query rather than its
// text: see the comment there for why that distinction decides correctness
// rather than tidiness.

// describeQuery wraps a query so it returns its own column metadata and no
// rows, which is how the partitioned schema is obtained without reading
// anything.
func describeQuery(query string) string {
	return "SELECT * FROM (" + wrappable(query) + ") WHERE 1 = 0"
}

// wrappable prepares a query to be surrounded by parentheses.
//
// The newline is load bearing. A query ending in a line comment swallows
// whatever follows it on the same line, so `SELECT a FROM t -- nightly` becomes
// `SELECT * FROM (SELECT a FROM t -- nightly) WHERE 1 = 0` and Oracle reports
// a missing right parenthesis, because the parenthesis is inside the comment.
// Ending the line first puts the parenthesis back in the statement.
func wrappable(query string) string {
	return strings.TrimSuffix(strings.TrimSpace(query), ";") + "\n"
}

// --- Planning ---

// defaultPartitionCount is used when oracle.partition_count is unset. Four
// overlapping readers is enough to show whether parallel extract helps on a
// given link without opening a session per CPU on a shared database.
const defaultPartitionCount = 4

// currentSCN captures the snapshot every slice will read at.
//
// Three sources, best first, because they differ in precision and in what
// they require:
//
//   - DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER is exact and current, and needs
//     EXECUTE on the package.
//   - V$DATABASE.CURRENT_SCN is exact and current, and needs SELECT on the
//     view.
//   - TIMESTAMP_TO_SCN needs no grant at all and is what an ordinary user
//     gets, but it resolves through Oracle's timestamp-to-SCN mapping, whose
//     granularity means the answer can be a few seconds old.
//
// That staleness matters: an SCN from before the table's last DDL cannot be
// flashed back to, which is ORA-01466. So the SCN is probed before any
// descriptor is handed out, and a query that cannot be read at it falls back
// to a single unpartitioned read rather than failing. See ExecutePartitions.
func (c *connectionImpl) currentSCN(ctx context.Context) (string, error) {
	sess, err := c.session(ctx)
	if err != nil {
		return "", err
	}

	sources := []string{
		"SELECT TO_CHAR(DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER) FROM DUAL",
		"SELECT TO_CHAR(CURRENT_SCN) FROM V$DATABASE",
		"SELECT TO_CHAR(TIMESTAMP_TO_SCN(SYSTIMESTAMP)) FROM DUAL",
	}
	var lastErr error
	for _, q := range sources {
		var scn string
		if err := sess.QueryRowContext(ctx, q).Scan(&scn); err != nil {
			lastErr = err
			continue
		}
		if strings.TrimSpace(scn) != "" {
			return scn, nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no SCN source returned a value")
	}
	return "", lastErr
}

// flashbackUnusable reports whether an error means this SCN cannot be read,
// as opposed to something wrong with the query itself.
//
// These are the ways a flashback target can be rejected:
//
//	ORA-01466  the SCN predates the table's last DDL
//	ORA-08180  no snapshot found for the given time
//	ORA-08181  not a valid system change number
//	ORA-01555  snapshot too old: undo for that SCN is gone
//	ORA-30052  invalid lower limit snapshot expression
//	ORA-00439  flashback is not enabled in this edition
//	ORA-41900  no FLASHBACK privilege on the object
//	ORA-01031  insufficient privileges
//
// The last two are privilege errors rather than snapshot errors, and they
// belong here because reading a table is not the same right as reading it as
// of an earlier SCN: a user who can SELECT their own table may well have no
// FLASHBACK privilege on it, and SYS.DUAL is one such table for everyone.
// Treating that as "cannot use this SCN" degrades to an ordinary read, which
// is the useful answer. Nothing is swallowed by doing so: the fallback runs
// the same query without flashback, so a genuine permission problem still
// surfaces from there.
func flashbackUnusable(err error) bool {
	for _, code := range []int{1466, 8180, 8181, 1555, 30052, 439, 41900, 1031} {
		if isOraCode(err, code) {
			return true
		}
	}
	return false
}

// tablePartitions lists a table's partition names, or nothing when the table
// is not partitioned.
func (c *connectionImpl) tablePartitions(ctx context.Context, owner, table string) ([]string, error) {
	sess, err := c.session(ctx)
	if err != nil {
		return nil, err
	}
	const q = `SELECT partition_name FROM ALL_TAB_PARTITIONS
		WHERE TABLE_OWNER = NVL(:1, SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) AND TABLE_NAME = :2
		ORDER BY partition_position`
	var ownerArg interface{}
	if owner != "" {
		ownerArg = owner
	}
	rows, err := sess.QueryContext(ctx, q, ownerArg, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

// rowidRanges asks DBMS_PARALLEL_EXECUTE to chunk a heap table into disjoint
// ROWID ranges, which is the one way to divide a heap scan rather than repeat
// it: each range is a block range Oracle can read on its own.
//
// The chunker is stateful, so this creates a task, reads the chunks and drops
// the task. A caller without EXECUTE on the package, or without the quota for
// the chunk tables, gets an error and the planner falls back.
func (c *connectionImpl) rowidRanges(ctx context.Context, owner, table string, want int) ([][2]string, error) {
	sess, err := c.session(ctx)
	if err != nil {
		return nil, err
	}

	// The task name has to be unique per planning call, since two concurrent
	// ones would read each other's chunks.
	var suffix string
	if err := sess.QueryRowContext(ctx,
		"SELECT TO_CHAR(SYS_CONTEXT('USERENV','SID')) || '_' || TO_CHAR(SYSTIMESTAMP, 'SSSSSFF3') FROM DUAL").
		Scan(&suffix); err != nil {
		return nil, err
	}
	task := "ADBC_PART_" + suffix
	if len(task) > 30 {
		task = task[:30]
	}

	ownerExpr := "USER"
	args := []interface{}{task, table}
	if owner != "" {
		ownerExpr = ":3"
		args = append(args, owner)
	}

	// Chunk by block count rather than row count: CREATE_CHUNKS_BY_ROWID
	// takes a chunk size in blocks when by_row is false, which divides the
	// segment evenly without first counting rows.
	plsql := `
		DECLARE
		  blocks NUMBER;
		  chunk  NUMBER;
		BEGIN
		  BEGIN DBMS_PARALLEL_EXECUTE.DROP_TASK(:1); EXCEPTION WHEN OTHERS THEN NULL; END;
		  SELECT NVL(SUM(blocks), 0) INTO blocks FROM USER_EXTENTS WHERE segment_name = :2;
		  chunk := GREATEST(TRUNC(blocks / ` + itoa(want) + `), 1);
		  DBMS_PARALLEL_EXECUTE.CREATE_TASK(:1);
		  DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID(:1, ` + ownerExpr + `, :2, false, chunk);
		END;`
	if _, err := sess.ExecContext(ctx, plsql, args...); err != nil {
		return nil, fmt.Errorf("DBMS_PARALLEL_EXECUTE chunking unavailable: %w", err)
	}
	// Always drop the task, including when reading the chunks fails.
	defer func() {
		_, _ = sess.ExecContext(ctx,
			`BEGIN DBMS_PARALLEL_EXECUTE.DROP_TASK(:1); EXCEPTION WHEN OTHERS THEN NULL; END;`, task)
	}()

	rows, err := sess.QueryContext(ctx,
		`SELECT start_rowid, end_rowid FROM USER_PARALLEL_EXECUTE_CHUNKS
		 WHERE task_name = :1 ORDER BY chunk_id`, task)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ranges [][2]string
	for rows.Next() {
		var lo, hi string
		if err := rows.Scan(&lo, &hi); err != nil {
			return nil, err
		}
		if lo == "" || hi == "" {
			continue
		}
		ranges = append(ranges, [2]string{lo, hi})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		return nil, fmt.Errorf("the chunker produced no ROWID ranges")
	}
	return ranges, nil
}

// coalesceRanges groups a chunk list into at most want slices, each covering a
// run of consecutive chunks. The chunker returns whatever the segment's extent
// layout implies, which is usually but not always the requested count.
//
// Grouping keeps the ranges disjoint, so the union still covers every row
// exactly once.
func coalesceRanges(ranges [][2]string, want int) [][][2]string {
	if want < 1 {
		want = 1
	}
	if len(ranges) <= want {
		out := make([][][2]string, len(ranges))
		for i, r := range ranges {
			out[i] = [][2]string{r}
		}
		return out
	}
	groups := make([][][2]string, want)
	// Spread the remainder over the leading groups rather than piling it onto
	// the last one, which would leave one slice doing several times the work.
	base, extra := len(ranges)/want, len(ranges)%want
	pos := 0
	for i := 0; i < want; i++ {
		n := base
		if i < extra {
			n++
		}
		groups[i] = ranges[pos : pos+n]
		pos += n
	}
	return groups
}

// rowidPredicate renders one slice's ROWID ranges.
//
// The endpoints are passed as text through CHARTOROWID rather than compared as
// text. ROWID's character form is a base-64 encoding whose string ordering is
// not its ROWID ordering, so comparing the text directly silently drops rows:
// MIN and MAX of the text form over a 500,000 row table bracket about 15% of
// it. CHARTOROWID restores the exact value, so a chunk endpoint round-trips
// losslessly.
func rowidPredicate(rowid string, ranges [][2]string) string {
	parts := make([]string, 0, len(ranges))
	for _, r := range ranges {
		parts = append(parts,
			rowid+" BETWEEN CHARTOROWID('"+r[0]+"') AND CHARTOROWID('"+r[1]+"')")
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return "(" + strings.Join(parts, ") OR (") + ")"
}

// objectIsTable reports whether a name refers to a table in this schema, which
// every splitting strategy below assumes: views and synonyms have no
// partitions, no extents, and in general no usable ROWID.
func (c *connectionImpl) objectIsTable(ctx context.Context, owner, name string) (bool, error) {
	sess, err := c.session(ctx)
	if err != nil {
		return false, err
	}
	const q = `SELECT COUNT(*) FROM ALL_TABLES
		WHERE OWNER = NVL(:1, SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) AND TABLE_NAME = :2`
	var ownerArg interface{}
	if owner != "" {
		ownerArg = owner
	}
	var n int
	if err := sess.QueryRowContext(ctx, q, ownerArg, name).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// planPartitions walks the strategy ladder and returns one slice query per
// partition, plus the strategy that produced them and the reason any better
// strategy was skipped.
func (c *connectionImpl) planPartitions(ctx context.Context, q *splittableQuery, scn string, want int) (queries []string, strategy partitionStrategy, reason string) {
	isTable, err := c.objectIsTable(ctx, q.owner, q.table)
	if err != nil || !isTable {
		why := "it is not a table in this schema, so it has no partitions or ROWID ranges to split"
		if err != nil {
			why = "the table could not be looked up: " + err.Error()
		}
		return nil, partitionSingle, why
	}

	// render builds one slice and reports a failure as a reason to fall back,
	// since a slice that cannot be built is not a slice that should be run.
	var failed error
	render := func(spec sliceSpec) string {
		if failed != nil {
			return ""
		}
		sql, err := buildSlice(ctx, q, spec)
		if err != nil {
			failed = err
		}
		return sql
	}

	// Best: one slice per partition. Disjoint segments, so the scan is divided
	// rather than repeated.
	if parts, err := c.tablePartitions(ctx, q.owner, q.table); err == nil && len(parts) > 1 {
		for _, name := range parts {
			queries = append(queries, render(sliceSpec{scn: scn, partition: name}))
		}
		if failed != nil {
			return nil, partitionSingle, "the partition slices could not be built: " + failed.Error()
		}
		// The partition count is what the table has, not what was asked for:
		// a PARTITION clause names one partition, and covering several in one
		// slice would need a UNION ALL this rewriter will not build.
		return queries, partitionByPartition, ""
	}

	// Next: disjoint ROWID ranges over the heap.
	rowid := q.qualifiedRowid()
	ranges, rangeErr := c.rowidRanges(ctx, q.owner, q.table, want)
	if rangeErr == nil && len(ranges) > 0 {
		for _, group := range coalesceRanges(ranges, want) {
			queries = append(queries, render(sliceSpec{scn: scn, predicate: rowidPredicate(rowid, group)}))
		}
		if failed != nil {
			return nil, partitionSingle, "the ROWID range slices could not be built: " + failed.Error()
		}
		return queries, partitionByRowidRange, ""
	}

	// Last: separate the slices by a hash of the ROWID. Correct, and always
	// available, but every slice scans the whole table, so total server I/O is
	// multiplied by the slice count. The reason the better strategy was
	// unavailable travels with the result so this is a visible tradeoff rather
	// than a silent one.
	for k := 0; k < want; k++ {
		pred := "ORA_HASH(" + rowid + ", " + itoa(want-1) + ") = " + itoa(k)
		queries = append(queries, render(sliceSpec{scn: scn, predicate: pred}))
	}
	if failed != nil {
		return nil, partitionSingle, "the hash slices could not be built: " + failed.Error()
	}
	reason = "ROWID range chunking was unavailable, so each slice filters a full scan by ORA_HASH(ROWID): " +
		"total server I/O is multiplied by the partition count"
	if rangeErr != nil {
		reason += " (" + rangeErr.Error() + ")"
	}
	return queries, partitionByHash, reason
}

// --- ADBC entry points ---

// partitionSchema runs the query for metadata only and returns the schema
// every slice must produce, plus the Arrow type of any column whose type came
// from sampled data.
//
// The sample is the reason this cannot just describe with WHERE 1 = 0. An
// unconstrained NUMBER gets its precision and scale from the rows, and slices
// see different rows: left to decide for themselves, one slice would return
// decimal128(7,2) where another returned decimal128(5,0), and neither would
// match the schema returned here. So the types are decided once, against the
// whole table at the snapshot, and pinned into every descriptor.
func (s *statementImpl) partitionSchema(ctx context.Context, query, scn string) (*arrow.Schema, map[int]arrow.DataType, error) {
	pol := s.cnxn.typePolicy()

	probe := query
	if scn != "" {
		// Sample at the same snapshot the slices read, so the types describe
		// the data being returned rather than the data as it is now.
		probe = "SELECT * FROM (" + wrappable(query) + ") AS OF SCN " + scn
	}
	if n := pol.numberScanRows; n > 0 {
		probe = "SELECT * FROM (" + wrappable(probe) + ") WHERE ROWNUM <= " + itoa(n)
	} else {
		probe = describeQuery(probe)
	}

	sess, err := s.cnxn.session(ctx)
	if err != nil {
		return nil, nil, err
	}
	rows, err := sess.QueryContext(ctx, probe)
	if err != nil {
		return nil, nil, s.ErrorHelper.WrapIO(err, "could not describe the query for partitioning")
	}
	defer rows.Close()

	probeReader := &oracleRecordReader{rows: rows, policy: pol, batchRows: readerBatchRows}
	schema, err := probeReader.NextResultSet(ctx, nil, 0)
	if err != nil {
		return nil, nil, err
	}
	return schema, probeReader.inferredTypes, nil
}

// ExecutePartitions splits the query into slices that can be read
// concurrently, by different connections, and returns the schema they all
// produce along with one opaque descriptor each.
//
// A query that cannot be split safely comes back as a single partition rather
// than an error, so a caller can always use this path and get correct data.
func (s *statementImpl) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	none := adbc.Partitions{}
	if s.query == "" {
		return nil, none, -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}
	// Bound parameters are not carried in a descriptor: ReadPartition
	// receives bytes and no bindings, so a slice would run with the
	// placeholders unfilled. Refusing is the only honest answer.
	if s.bindParams != nil && s.bindParams.NumCols() > 0 {
		return nil, none, -1, s.ErrorHelper.Errorf(adbc.StatusNotImplemented,
			"ExecutePartitions does not support bound parameters: a partition descriptor carries only SQL, "+
				"so the slices would execute with unbound placeholders. Inline the values in the query")
	}

	want := s.partitionCount()

	// One slice means nothing to split, so skip the planning work entirely.
	if want <= 1 {
		return s.singlePartition(ctx, "oracle.partition_count is 1")
	}

	// Reading the slices is the caller's job, and the usual way is one
	// connection per slice on this same database. That needs a pool big
	// enough to hold them: with more slices than connections, opening the
	// last one blocks until an earlier reader finishes, which turns a
	// parallel read into a serial one or a deadlock if the caller waits for
	// all of them before closing any.
	//
	// The driver cannot tell how the partitions will be read, since ADBC
	// allows them to go to other processes entirely, so this is a warning
	// rather than a cap.
	if pool := s.cnxn.poolSize; pool > 0 && want > pool && s.cnxn.Logger != nil {
		s.cnxn.Logger.Warn(
			"more partitions were requested than the connection pool can hold, "+
				"so reading them all at once on this database will block",
			"partitions", want, OptionPoolSize, pool)
	}

	// Every slice reads at this SCN, which is what makes the union a
	// consistent snapshot instead of several reads drifting against
	// concurrent writes. Without it, splitting would change the answer.
	scn, err := s.cnxn.currentSCN(ctx)
	if err != nil {
		return s.singlePartition(ctx,
			"the current SCN could not be read, so the slices could not be pinned to one snapshot: "+err.Error())
	}

	q, err := classifyQuery(ctx, s.query)
	if err != nil {
		var refusal splitRefusal
		if errors.As(err, &refusal) {
			return s.singlePartition(ctx, refusal.reason)
		}
		// The parser itself is unavailable, which is not a property of the
		// query, so say so rather than reporting the query as unsplittable.
		return s.singlePartition(ctx, "the SQL parser is unavailable: "+err.Error())
	}

	// Describe the query at the captured SCN before planning anything. This
	// does two jobs: it decides the column types every slice must produce,
	// and it proves the SCN can actually be read. An SCN that predates the
	// table's last DDL cannot be, and finding that out here means falling
	// back to one correct read rather than handing out descriptors that all
	// fail at ReadPartition time.
	schema, inferred, err := s.partitionSchema(ctx, s.query, scn)
	if err != nil {
		if flashbackUnusable(err) {
			return s.singlePartition(ctx,
				"the query cannot be read at the captured SCN, so the slices could not share a snapshot: "+err.Error())
		}
		return nil, none, -1, err
	}
	pinned := encodeNumberTypes(inferred)

	queries, strategy, reason := s.cnxn.planPartitions(ctx, q, scn, want)
	if len(queries) == 0 {
		return s.singlePartition(ctx, reason)
	}
	if reason != "" && s.cnxn.Logger != nil {
		s.cnxn.Logger.Warn("partitioned read is using a fallback strategy",
			"strategy", string(strategy), "partitions", len(queries), "reason", reason)
	}

	parts := adbc.Partitions{
		NumPartitions: uint64(len(queries)),
		PartitionIDs:  make([][]byte, 0, len(queries)),
	}
	for i, q := range queries {
		d := partitionDescriptor{
			Query:       q,
			Strategy:    strategy,
			Index:       i,
			Count:       len(queries),
			SCN:         scn,
			NumberTypes: pinned,
		}
		b, err := d.encode()
		if err != nil {
			return nil, none, -1, s.ErrorHelper.Errorf(adbc.StatusInternal,
				"could not serialise partition %d: %s", i, err)
		}
		parts.PartitionIDs = append(parts.PartitionIDs, b)
	}

	// Oracle has no row count for a query it has not run, and guessing from
	// statistics would be a number the caller could not rely on.
	return schema, parts, -1, nil
}

// singlePartition returns the whole query as one slice, which is what happens
// whenever splitting is not safe or not possible. The reason is logged rather
// than discarded, since a caller seeing one partition deserves to know why.
func (s *statementImpl) singlePartition(ctx context.Context, reason string) (*arrow.Schema, adbc.Partitions, int64, error) {
	if reason != "" && s.cnxn.Logger != nil {
		s.cnxn.Logger.Info("query was not split, returning a single partition", "reason", reason)
	}

	// No SCN: one reader is a single query, and a single query in Oracle is
	// already consistent as of the moment it starts.
	schema, inferred, err := s.partitionSchema(ctx, s.query, "")
	if err != nil {
		return nil, adbc.Partitions{}, -1, err
	}
	d := partitionDescriptor{
		Query:       s.query,
		Strategy:    partitionSingle,
		Index:       0,
		Count:       1,
		NumberTypes: encodeNumberTypes(inferred),
	}
	b, err := d.encode()
	if err != nil {
		return nil, adbc.Partitions{}, -1, s.ErrorHelper.Errorf(adbc.StatusInternal,
			"could not serialise the partition: %s", err)
	}
	return schema, adbc.Partitions{NumPartitions: 1, PartitionIDs: [][]byte{b}}, -1, nil
}

// ReadPartition executes one slice and returns its rows.
//
// The descriptor carries finished SQL, so this cannot reach a different
// conclusion than the planner did, and it carries the pinned column types, so
// this slice's schema matches what ExecutePartitions promised even though it
// sees only part of the data.
func (c *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	d, err := decodePartitionDescriptor(serializedPartition)
	if err != nil {
		return nil, c.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "%s", err)
	}

	forced, err := decodeNumberTypes(d.NumberTypes)
	if err != nil {
		return nil, c.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
			"partition descriptor pins a column type this driver cannot build: %s", err)
	}

	sess, err := c.session(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := sess.QueryContext(ctx, d.Query)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "partition read failed")
	}

	impl := &oracleRecordReader{
		rows:        rows,
		policy:      c.typePolicy(),
		batchRows:   readerBatchRows,
		logger:      c.Logger,
		forcedTypes: forced,
	}

	var rr driverbase.BaseRecordReader
	if err := rr.Init(ctx, c.Alloc, slog.Default(), nil,
		driverbase.BaseRecordReaderOptions{BatchRowLimit: readerBatchRows}, impl); err != nil {
		rows.Close()
		return nil, err
	}
	return &rr, nil
}

// --- Pinned column types ---

// A partition descriptor has to survive being handed to another connection, so
// the pinned types are written as text rather than as Go values. Only decimals
// appear here: they are the one family whose parameters come from data rather
// than from the catalog, which is exactly why they need pinning.
func encodeNumberTypes(types map[int]arrow.DataType) map[int]string {
	if len(types) == 0 {
		return nil
	}
	out := make(map[int]string, len(types))
	for i, dt := range types {
		switch t := dt.(type) {
		case *arrow.Decimal32Type:
			out[i] = fmt.Sprintf("decimal32(%d,%d)", t.Precision, t.Scale)
		case *arrow.Decimal64Type:
			out[i] = fmt.Sprintf("decimal64(%d,%d)", t.Precision, t.Scale)
		case *arrow.Decimal128Type:
			out[i] = fmt.Sprintf("decimal128(%d,%d)", t.Precision, t.Scale)
		case *arrow.Decimal256Type:
			out[i] = fmt.Sprintf("decimal256(%d,%d)", t.Precision, t.Scale)
		default:
			// Anything else was not derived from the data, so the receiving
			// slice will reach the same conclusion on its own and nothing
			// needs to be carried.
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

var decimalSpec = regexp.MustCompile(`^decimal(32|64|128|256)\((\d+),(\d+)\)$`)

func decodeNumberTypes(spec map[int]string) (map[int]arrow.DataType, error) {
	if len(spec) == 0 {
		return nil, nil
	}
	out := make(map[int]arrow.DataType, len(spec))
	for i, text := range spec {
		m := decimalSpec.FindStringSubmatch(text)
		if m == nil {
			return nil, fmt.Errorf("column %d: %q is not a decimal type this driver writes", i, text)
		}
		prec, err := strconv.ParseInt(m[2], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("column %d: precision in %q is out of range", i, text)
		}
		scale, err := strconv.ParseInt(m[3], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("column %d: scale in %q is out of range", i, text)
		}
		p, sc := int32(prec), int32(scale)
		switch m[1] {
		case "32":
			out[i] = &arrow.Decimal32Type{Precision: p, Scale: sc}
		case "64":
			out[i] = &arrow.Decimal64Type{Precision: p, Scale: sc}
		case "128":
			out[i] = &arrow.Decimal128Type{Precision: p, Scale: sc}
		case "256":
			out[i] = &arrow.Decimal256Type{Precision: p, Scale: sc}
		}
	}
	return out, nil
}

// partitionCount resolves how many slices to aim for: the statement's own
// setting if it has one, else the connection's.
func (s *statementImpl) partitionCount() int {
	if s.partitionCountOpt > 0 {
		return s.partitionCountOpt
	}
	if s.cnxn != nil && s.cnxn.partitionCount > 0 {
		return s.cnxn.partitionCount
	}
	return defaultPartitionCount
}
