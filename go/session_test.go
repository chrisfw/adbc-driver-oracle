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
	"database/sql"
	"database/sql/driver"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Both *sql.Conn and *sql.Tx must satisfy sessionExecutor: that is what lets
// session() swap an open transaction in for the bare session without touching
// a single call site. Checked at compile time, because a signature drift in
// database/sql would otherwise surface as a confusing failure much later.
var (
	_ sessionExecutor = (*sql.Conn)(nil)
	_ sessionExecutor = (*sql.Tx)(nil)
)

// --- Unit: ingest parallelism is bounded by the transaction mode ---

func TestIngestConcurrency(t *testing.T) {
	cases := []struct {
		name       string
		autocommit bool
		requested  int
		want       int
	}{
		// Autocommit on: workers take their own pooled sessions, so the
		// caller's request stands. This is the ingest fast path.
		{"autocommit keeps requested workers", true, 4, 4},
		{"autocommit keeps a single worker", true, 1, 1},
		{"autocommit floors a bogus count", true, 0, 1},
		{"autocommit floors a negative count", true, -3, 1},
		// Autocommit off: every worker shares the one owned session, so
		// parallelism buys nothing and the rows must stay in the caller's tx.
		{"transaction forces serial", false, 8, 1},
		{"transaction forces serial from 1", false, 1, 1},
		{"transaction floors a bogus count", false, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &connectionImpl{}
			c.Autocommit = tc.autocommit
			if got := c.ingestConcurrency(tc.requested); got != tc.want {
				t.Errorf("ingestConcurrency(%d) with autocommit=%v: got %d, want %d",
					tc.requested, tc.autocommit, got, tc.want)
			}
		})
	}
}

// --- Unit: ingest sessions only commit and close what they own ---

// With autocommit off the worker must join the caller's transaction on the
// connection's own session, never take one from the pool. This is the core of
// 1.1 and 1.2 together: no ingest worker may slip onto a second Oracle session
// and commit outside the transaction the caller opened.
func TestNewIngestSessionInTransactionJoinsTransaction(t *testing.T) {
	ctx := context.Background()
	c, _, log := newStubConnection(t)
	c.Autocommit = false
	defer closeStubConnection(t, c)

	sess, err := c.newIngestSession(ctx)
	if err != nil {
		t.Fatalf("newIngestSession: %v", err)
	}
	if sess.pooled {
		t.Error("took a pooled session; ingest would escape the caller's transaction")
	}
	if sess.conn != nil {
		t.Error("captured a pool connection while in a transaction")
	}
	if c.tx == nil {
		t.Fatal("no transaction was begun")
	}
	if sess.exec != sessionExecutor(c.tx) {
		t.Error("ingest did not run inside the caller's transaction")
	}

	// Committing here would end the caller's transaction behind its back.
	log.reset()
	if err := sess.commit(ctx); err != nil {
		t.Errorf("commit on a transaction-bound session: got %v, want nil", err)
	}
	if got := log.statements(); len(got) != 0 {
		t.Errorf("transaction-bound session issued %v, want no statements", got)
	}

	// Closing must leave the transaction open for the next batch and for the
	// caller's eventual Commit.
	sess.close()
	if c.tx == nil {
		t.Error("ingest close ended the caller's transaction")
	}
}

// With autocommit on the worker gets a session of its own, commits on it, and
// hands it back. That is what makes parallel ingest safe and fast.
func TestNewIngestSessionAutocommitUsesPooledSession(t *testing.T) {
	c, _, log := newStubConnection(t)
	c.Autocommit = true
	defer closeStubConnection(t, c)

	sess, err := c.newIngestSession(context.Background())
	if err != nil {
		t.Fatalf("newIngestSession: %v", err)
	}
	if !sess.pooled {
		t.Fatal("autocommit returned a non-pooled session")
	}
	if sess.conn == nil {
		t.Fatal("pooled session carries no connection")
	}

	log.reset()
	if err := sess.commit(context.Background()); err != nil {
		t.Fatalf("commit on a pooled session: %v", err)
	}
	if got := log.statements(); !slices.Contains(got, "COMMIT") {
		t.Errorf("pooled session issued %v, want a COMMIT", got)
	}

	sess.close()
	if sess.conn != nil {
		t.Error("close left the pool connection referenced")
	}
	// Called again on error paths and then via defer; must not panic.
	sess.close()
}

// --- Integration: one ADBC connection is one Oracle session ---

// Session settings must not leak between ADBC connections. Before 1.1 both
// connections shared the database's *sql.DB, so an ALTER SESSION on one landed
// on whichever pooled session was free and could show up on the other.
func TestIntegration_SessionIsolationBetweenConnections(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxnA, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	defer cnxnA.Close()

	cnxnB, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	defer cnxnB.Close()

	before := querySingleString(ctx, t, cnxnB, currentSchemaSQL)

	// Switch A to a schema that is not B's, so the change is observable.
	target := "SYS"
	if strings.EqualFold(target, before) {
		target = "SYSTEM"
	}
	setter, ok := cnxnA.(adbc.GetSetOptions)
	if !ok {
		t.Fatal("connection does not implement adbc.GetSetOptions")
	}
	if err := setter.SetOption(adbc.OptionKeyCurrentDbSchema, target); err != nil {
		t.Fatalf("SetOption(CurrentDbSchema=%q) on A: %v", target, err)
	}

	if got := querySingleString(ctx, t, cnxnA, currentSchemaSQL); !strings.EqualFold(got, target) {
		t.Errorf("connection A schema: got %q, want %q", got, target)
	}
	if got := querySingleString(ctx, t, cnxnB, currentSchemaSQL); !strings.EqualFold(got, before) {
		t.Errorf("connection B schema leaked: got %q, want the untouched %q", got, before)
	}
}

// Every statement on one ADBC connection must run on the same Oracle session,
// and two connections must never share one.
//
// Worth knowing which half does the work. The 50-statement SID loop is the
// weaker assertion: it passes even on the pre-1.1 shared-pool driver, because
// database/sql reuses the most recently idle connection, so a sequential
// caller keeps getting the same physical session handed back (it still passed
// with four other connections hammering the pool concurrently). The real
// detector is the distinct-SID check at the end: before 1.1 two ADBC
// connections demonstrably landed on the same Oracle session. The loop stays
// as a guard against statements wandering if the pool is ever handed out
// again.
func TestIntegration_StatementsShareOneSession(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	// A pool wider than one is the point: if statements could wander between
	// sessions, this is where they would.
	db, err := drv.NewDatabase(map[string]string{
		OptionDSN:      dsn,
		OptionPoolSize: "8",
	})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	const statements = 50
	first := querySingleString(ctx, t, cnxn, sessionSIDSQL)
	if first == "" {
		t.Fatal("empty SID")
	}
	for i := 1; i < statements; i++ {
		if got := querySingleString(ctx, t, cnxn, sessionSIDSQL); got != first {
			t.Fatalf("statement %d ran on SID %q, want %q: the connection does not own one session",
				i, got, first)
		}
	}

	// A second connection must be a different session, or the loop above
	// would pass trivially on a pool that only ever had one session.
	other, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open second: %v", err)
	}
	defer other.Close()
	if got := querySingleString(ctx, t, other, sessionSIDSQL); got == first {
		t.Errorf("second connection reused SID %q; connections are not getting their own sessions", got)
	}
}

// Closing an ADBC connection must hand its session back to the pool, not close
// the pool. On a pool of one, a session that was never returned makes the next
// Open block until the context dies.
func TestIntegration_ConnectionCloseReturnsSessionToPool(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{
		OptionDSN:      dsn,
		OptionPoolSize: "1",
	})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	for i := 0; i < 3; i++ {
		cnxn, err := db.Open(ctx)
		if err != nil {
			t.Fatalf("Open %d (session not returned to the pool?): %v", i, err)
		}
		if got := querySingleString(ctx, t, cnxn, "SELECT 1 FROM DUAL"); got != "1" {
			t.Fatalf("Open %d: query returned %q", i, got)
		}
		if err := cnxn.Close(); err != nil {
			t.Fatalf("Close %d: %v", i, err)
		}
	}
}

// Ingest must keep working on the owned session, including the parallel-worker
// path, since workers now go through newIngestSession rather than straight to
// the pool.
func TestIntegration_IngestOnOwnedSessionWithWorkers(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{
		OptionDSN:           dsn,
		OptionIngestWorkers: "4",
		OptionPoolSize:      "8",
	})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	const table = "ADBC_T11_WORKERS"
	const rows = 2000
	dropTable(ctx, t, cnxn, table)
	defer dropTable(ctx, t, cnxn, table)

	rec := intRecord(t, "ID", rows)
	defer rec.Release()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, table); err != nil {
		t.Fatalf("SetOption(target table): %v", err)
	}
	if err := stmt.Bind(ctx, rec); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	n, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		t.Fatalf("ExecuteUpdate: %v", err)
	}
	if n != rows {
		t.Errorf("ExecuteUpdate reported %d rows, want %d", n, rows)
	}

	// Read the count back on the same connection: the ingest sessions must
	// have committed, so the owned session sees every row.
	if got := querySingleString(ctx, t, cnxn, "SELECT COUNT(*) FROM "+table); got != "2000" {
		t.Errorf("row count after ingest: got %q, want 2000", got)
	}
}

// --- Integration helpers ---

const (
	currentSchemaSQL = "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL"
	sessionSIDSQL    = "SELECT TO_CHAR(SYS_CONTEXT('USERENV','SID')) FROM DUAL"
)

// querySingleString runs a one-row, one-column query through the ADBC stack
// and returns the value as a string, so callers can compare SIDs, schema names
// and counts without caring which Arrow type the driver chose.
func querySingleString(ctx context.Context, t *testing.T, cnxn adbc.Connection, query string) string {
	t.Helper()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		t.Fatalf("SetSqlQuery(%q): %v", query, err)
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("ExecuteQuery(%q): %v", query, err)
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		return strings.TrimSpace(rec.Column(0).ValueStr(0))
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader error for %q: %v", query, err)
	}
	t.Fatalf("no rows for %q", query)
	return ""
}

// execIgnoreError runs a statement for its side effect, tolerating failure.
// Used for the DROP in test setup, where "table does not exist" is expected.
func execIgnoreError(ctx context.Context, t *testing.T, cnxn adbc.Connection, query string) {
	t.Helper()
	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(query); err != nil {
		t.Fatalf("SetSqlQuery(%q): %v", query, err)
	}
	_, _ = stmt.ExecuteUpdate(ctx)
}

func dropTable(ctx context.Context, t *testing.T, cnxn adbc.Connection, table string) {
	t.Helper()
	execIgnoreError(ctx, t, cnxn, "DROP TABLE "+table+" PURGE")
}

// --- Stub driver: exercises the ingest-session plumbing without a database ---

// stubLog records the statements a stub session was asked to run, so a test
// can assert what committed and what did not.
type stubLog struct {
	mu   sync.Mutex
	sqls []string
}

func (l *stubLog) record(q string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sqls = append(l.sqls, q)
}

func (l *stubLog) statements() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.sqls)
}

func (l *stubLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sqls = nil
}

type stubDriver struct{ log *stubLog }

func (d stubDriver) Open(string) (driver.Conn, error) { return stubConn{log: d.log}, nil }

type stubConn struct{ log *stubLog }

func (c stubConn) Prepare(q string) (driver.Stmt, error) { return stubStmt{log: c.log, sql: q}, nil }
func (c stubConn) Close() error                          { return nil }
func (c stubConn) Begin() (driver.Tx, error)             { return stubTx{}, nil }

// ExecContext captures the statement directly, so a COMMIT shows up in the log
// as itself rather than as a prepared statement.
func (c stubConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	c.log.record(q)
	return driver.RowsAffected(0), nil
}

// go-ora implements both of these, and database/sql only keeps a transaction's
// connection on rollback when it sees them, so the stub mirrors that shape.
func (c stubConn) ResetSession(context.Context) error { return nil }
func (c stubConn) IsValid() bool                      { return true }

type stubTx struct{}

func (stubTx) Commit() error   { return nil }
func (stubTx) Rollback() error { return nil }

type stubStmt struct {
	log *stubLog
	sql string
}

func (s stubStmt) Close() error  { return nil }
func (s stubStmt) NumInput() int { return -1 }
func (s stubStmt) Exec([]driver.Value) (driver.Result, error) {
	s.log.record(s.sql)
	return driver.RowsAffected(0), nil
}
func (s stubStmt) Query([]driver.Value) (driver.Rows, error) {
	s.log.record(s.sql)
	return stubRows{}, nil
}

type stubRows struct{}

func (stubRows) Columns() []string         { return []string{"x"} }
func (stubRows) Close() error              { return nil }
func (stubRows) Next([]driver.Value) error { return io.EOF }

var (
	stubRegisterOnce sync.Once
	// The driver closes over its log and sql.Register panics on a duplicate
	// name, so there is one registration and one log, reset per test. These
	// tests do not run in parallel, so the shared log stays deterministic.
	stubSharedLog = &stubLog{}
)

// newStubConnection builds a connectionImpl over the stub driver, wired the
// way databaseImpl.Open wires a real one: its own session out of the pool and
// a connection-scoped transaction context.
func newStubConnection(t *testing.T) (*connectionImpl, *sql.DB, *stubLog) {
	t.Helper()
	pool, log := newStubPool(t)
	owned, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatalf("checkout owned session: %v", err)
	}
	c := &connectionImpl{db: pool, conn: owned}
	c.ErrorHelper = driverbase.ErrorHelper{DriverName: "stub"}
	c.Autocommit = true
	c.txCtx, c.txCancel = context.WithCancel(context.Background())
	return c, pool, log
}

// closeStubConnection closes through connectionImpl.Close, which is where the
// open-transaction rollback lives. Calling owned.Close() directly instead
// would block forever on any test that left a transaction open.
func closeStubConnection(t *testing.T, c *connectionImpl) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close blocked: an open transaction was not resolved before returning the session")
	}
}

// newStubPool returns a *sql.DB backed by the stub driver, plus the log of
// statements run against it.
func newStubPool(t *testing.T) (*sql.DB, *stubLog) {
	t.Helper()
	stubRegisterOnce.Do(func() { sql.Register("adbc-oracle-stub", stubDriver{log: stubSharedLog}) })
	stubSharedLog.reset()

	pool, err := sql.Open("adbc-oracle-stub", "")
	if err != nil {
		t.Fatalf("open stub pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	return pool, stubSharedLog
}

// intRecord builds a single-column Int64 record batch of n rows, for ingest
// tests that only care about row counts.
func intRecord(t *testing.T, name string, n int) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	ib := b.Field(0).(*array.Int64Builder)
	ib.Reserve(n)
	for i := 0; i < n; i++ {
		ib.Append(int64(i))
	}
	return b.NewRecordBatch()
}
