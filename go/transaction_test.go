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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// --- Unit: isolation level mapping ---

func TestOracleIsolationClause(t *testing.T) {
	cases := []struct {
		in     string
		want   string
		wantOK bool
	}{
		// Oracle's two real levels, as canonical ADBC values and short names.
		{string(adbc.LevelReadCommitted), isolationReadCommitted, true},
		{"read_committed", isolationReadCommitted, true},
		{"READ_COMMITTED", isolationReadCommitted, true},
		{string(adbc.LevelSerializable), isolationSerializable, true},
		{"serializable", isolationSerializable, true},
		{"  Serializable  ", isolationSerializable, true},
		// Default means "leave the server alone", so no clause.
		{string(adbc.LevelDefault), "", true},
		{"default", "", true},
		{"", "", true},
		// Oracle has no equivalent for these, so they must be refused rather
		// than silently downgraded to something weaker or stronger.
		{string(adbc.LevelReadUncommitted), "", false},
		{string(adbc.LevelRepeatableRead), "", false},
		{string(adbc.LevelSnapshot), "", false},
		{string(adbc.LevelLinearizable), "", false},
		{"nonsense", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, ok := oracleIsolationClause(tc.in)
			if ok != tc.wantOK {
				t.Fatalf("oracleIsolationClause(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
			}
			if got != tc.want {
				t.Errorf("oracleIsolationClause(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSetOptionIsolationLevel(t *testing.T) {
	c, _, _ := newStubConnection(t)
	defer closeStubConnection(t, c)

	if err := c.SetOption(adbc.OptionKeyIsolationLevel, string(adbc.LevelSerializable)); err != nil {
		t.Fatalf("SetOption(serializable): %v", err)
	}
	if c.isolation != isolationSerializable {
		t.Errorf("isolation = %q, want %q", c.isolation, isolationSerializable)
	}
	if got, err := c.GetOption(adbc.OptionKeyIsolationLevel); err != nil {
		t.Errorf("GetOption: %v", err)
	} else if got != string(adbc.LevelSerializable) {
		t.Errorf("GetOption = %q, want %q", got, adbc.LevelSerializable)
	}

	// An unsupported level must come back as StatusNotImplemented and leave
	// the configured level untouched.
	err := c.SetOption(adbc.OptionKeyIsolationLevel, string(adbc.LevelSnapshot))
	if err == nil {
		t.Fatal("SetOption(snapshot) succeeded, want StatusNotImplemented")
	}
	var adbcErr adbc.Error
	if !errors.As(err, &adbcErr) {
		t.Fatalf("SetOption(snapshot) returned %T, want adbc.Error", err)
	}
	if adbcErr.Code != adbc.StatusNotImplemented {
		t.Errorf("SetOption(snapshot) code = %v, want StatusNotImplemented", adbcErr.Code)
	}
	if c.isolation != isolationSerializable {
		t.Errorf("a rejected level changed isolation to %q", c.isolation)
	}
}

// --- Unit: transaction lifecycle on the owned session ---

// With autocommit on, statements run straight on the session and no
// transaction is ever begun: go-ora commits each DML execute itself.
func TestSessionAutocommitBeginsNoTransaction(t *testing.T) {
	ctx := context.Background()
	c, _, _ := newStubConnection(t)
	c.Autocommit = true
	defer closeStubConnection(t, c)

	sess, err := c.session(ctx)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	if sess != sessionExecutor(c.conn) {
		t.Error("autocommit did not run on the bare session")
	}
	if c.tx != nil {
		t.Error("autocommit began a transaction")
	}
}

// With autocommit off the transaction starts at the first statement and every
// later statement joins the same one.
func TestSessionOffBeginsOneTransactionLazily(t *testing.T) {
	ctx := context.Background()
	c, _, _ := newStubConnection(t)
	c.Autocommit = false
	defer closeStubConnection(t, c)

	if c.tx != nil {
		t.Fatal("a transaction existed before any statement ran")
	}
	first, err := c.session(ctx)
	if err != nil {
		t.Fatalf("first session: %v", err)
	}
	if c.tx == nil {
		t.Fatal("first statement did not begin a transaction")
	}
	second, err := c.session(ctx)
	if err != nil {
		t.Fatalf("second session: %v", err)
	}
	if first != second {
		t.Error("second statement did not join the first transaction")
	}
}

// Commit ends the transaction, and the next statement begins a fresh one.
func TestCommitEndsTransactionAndNextStatementBeginsAnother(t *testing.T) {
	ctx := context.Background()
	c, _, _ := newStubConnection(t)
	c.Autocommit = false
	defer closeStubConnection(t, c)

	if _, err := c.session(ctx); err != nil {
		t.Fatalf("session: %v", err)
	}
	firstTx := c.tx
	if err := c.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if c.tx != nil {
		t.Error("Commit left the transaction attached")
	}
	if _, err := c.session(ctx); err != nil {
		t.Fatalf("session after commit: %v", err)
	}
	if c.tx == nil {
		t.Fatal("no transaction after commit")
	}
	if c.tx == firstTx {
		t.Error("the committed transaction was reused instead of a fresh one")
	}
}

// Rollback behaves the same way.
func TestRollbackEndsTransaction(t *testing.T) {
	ctx := context.Background()
	c, _, _ := newStubConnection(t)
	c.Autocommit = false
	defer closeStubConnection(t, c)

	if _, err := c.session(ctx); err != nil {
		t.Fatalf("session: %v", err)
	}
	if err := c.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if c.tx != nil {
		t.Error("Rollback left the transaction attached")
	}
}

// Autocommit off with nothing executed yet means there is no transaction, and
// ending one that never started is not an error.
func TestCommitRollbackWithNoTransactionAreNoOps(t *testing.T) {
	ctx := context.Background()
	c, _, _ := newStubConnection(t)
	c.Autocommit = false
	defer closeStubConnection(t, c)

	if err := c.Commit(ctx); err != nil {
		t.Errorf("Commit with no transaction: %v", err)
	}
	if err := c.Rollback(ctx); err != nil {
		t.Errorf("Rollback with no transaction: %v", err)
	}
}

// ADBC requires switching autocommit back on to commit whatever is pending.
func TestSetAutocommitOnCommitsPendingTransaction(t *testing.T) {
	ctx := context.Background()
	c, _, _ := newStubConnection(t)
	c.Autocommit = false
	defer closeStubConnection(t, c)

	if _, err := c.session(ctx); err != nil {
		t.Fatalf("session: %v", err)
	}
	if c.tx == nil {
		t.Fatal("no transaction to commit")
	}
	if err := c.SetAutocommit(true); err != nil {
		t.Fatalf("SetAutocommit(true): %v", err)
	}
	if c.tx != nil {
		t.Error("enabling autocommit left the transaction open")
	}
}

// Turning autocommit off must not touch the network: it only arms session(),
// which begins the transaction at the next statement.
func TestSetAutocommitOffDefersTheTransaction(t *testing.T) {
	c, _, _ := newStubConnection(t)
	c.Autocommit = true
	defer closeStubConnection(t, c)

	if err := c.SetAutocommit(false); err != nil {
		t.Fatalf("SetAutocommit(false): %v", err)
	}
	if c.tx != nil {
		t.Error("disabling autocommit began a transaction eagerly")
	}
}

// Close has to end an open transaction before handing the session back.
// database/sql's Conn.Close blocks until the transaction resolves rather than
// rolling it back, so a Close that forgets this hangs the caller outright.
// This is the regression closeStubConnection's timeout guards everywhere else.
func TestCloseRollsBackOpenTransaction(t *testing.T) {
	ctx := context.Background()
	c, _, _ := newStubConnection(t)
	c.Autocommit = false

	if _, err := c.session(ctx); err != nil {
		t.Fatalf("session: %v", err)
	}
	tx := c.tx
	if tx == nil {
		t.Fatal("no transaction to roll back")
	}

	closeStubConnection(t, c) // fails the test if Close blocks

	if c.tx != nil {
		t.Error("Close left the transaction attached")
	}
	// The transaction must be finished, not merely detached: rolling it back
	// again has to report it is already done.
	if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
		t.Errorf("Close did not resolve the transaction: second Rollback = %v, want sql.ErrTxDone", err)
	}
}

// Closing twice must not panic or block; driverbase guards the ADBC-level
// double close, but Close itself should be safe.
func TestCloseIsSafeWithoutTransaction(t *testing.T) {
	c, _, _ := newStubConnection(t)
	c.Autocommit = true
	closeStubConnection(t, c)
	if err := c.Close(); err != nil && !strings.Contains(err.Error(), "already closed") {
		t.Errorf("second Close: %v", err)
	}
}

// --- Integration: transactions against a real Oracle ---

// Autocommit off: an inserted row must disappear on rollback and survive a
// commit, and a second connection must not see it until then.
func TestIntegration_TransactionRollbackAndCommit(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	writer, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open writer: %v", err)
	}
	defer writer.Close()
	reader, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open reader: %v", err)
	}
	defer reader.Close()

	const table = "ADBC_T12_TX"
	// DDL commits implicitly in Oracle, so create the table before the
	// transaction under test rather than inside it.
	dropTable(ctx, t, writer, table)
	execUpdate(ctx, t, writer, "CREATE TABLE "+table+" (ID NUMBER(10))")
	defer dropTable(ctx, t, writer, table)

	setAutocommit(t, writer, false)

	// Rollback: the row must vanish.
	execUpdate(ctx, t, writer, "INSERT INTO "+table+" VALUES (1)")
	if got := countRows(ctx, t, writer, table); got != "1" {
		t.Errorf("writer sees %q rows before rollback, want 1", got)
	}
	if got := countRows(ctx, t, reader, table); got != "0" {
		t.Errorf("reader sees %q uncommitted rows, want 0", got)
	}
	if err := writer.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if got := countRows(ctx, t, writer, table); got != "0" {
		t.Errorf("writer sees %q rows after rollback, want 0", got)
	}

	// Commit: the row must survive, and the second connection must see it.
	execUpdate(ctx, t, writer, "INSERT INTO "+table+" VALUES (2)")
	if got := countRows(ctx, t, reader, table); got != "0" {
		t.Errorf("reader sees %q uncommitted rows, want 0", got)
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := countRows(ctx, t, reader, table); got != "1" {
		t.Errorf("reader sees %q rows after commit, want 1", got)
	}
}

// Commit and Rollback with autocommit on are a caller error per ADBC.
func TestIntegration_CommitWithAutocommitIsInvalidState(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	// Autocommit defaults to on per the ADBC spec.
	if got := getOption(t, cnxn, adbc.OptionKeyAutoCommit); got != adbc.OptionValueEnabled {
		t.Errorf("default autocommit = %q, want %q", got, adbc.OptionValueEnabled)
	}

	for name, call := range map[string]func(context.Context) error{
		"Commit":   cnxn.Commit,
		"Rollback": cnxn.Rollback,
	} {
		err := call(ctx)
		if err == nil {
			t.Errorf("%s with autocommit on succeeded, want StatusInvalidState", name)
			continue
		}
		var adbcErr adbc.Error
		if !errors.As(err, &adbcErr) {
			t.Errorf("%s returned %T, want adbc.Error", name, err)
			continue
		}
		if adbcErr.Code != adbc.StatusInvalidState {
			t.Errorf("%s code = %v, want StatusInvalidState", name, adbcErr.Code)
		}
	}
}

// Switching autocommit from off to on commits the pending transaction.
func TestIntegration_AutocommitToggleCommitsPendingWork(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	writer, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open writer: %v", err)
	}
	defer writer.Close()
	reader, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open reader: %v", err)
	}
	defer reader.Close()

	const table = "ADBC_T12_TOGGLE"
	dropTable(ctx, t, writer, table)
	execUpdate(ctx, t, writer, "CREATE TABLE "+table+" (ID NUMBER(10))")
	defer dropTable(ctx, t, writer, table)

	setAutocommit(t, writer, false)
	execUpdate(ctx, t, writer, "INSERT INTO "+table+" VALUES (7)")
	if got := countRows(ctx, t, reader, table); got != "0" {
		t.Fatalf("reader sees %q uncommitted rows, want 0", got)
	}

	setAutocommit(t, writer, true)
	if got := countRows(ctx, t, reader, table); got != "1" {
		t.Errorf("reader sees %q rows after enabling autocommit, want 1 (the pending insert should have committed)", got)
	}
}

// Closing a connection with an open transaction must discard the work and
// return promptly, not block on database/sql's Conn.Close.
func TestIntegration_CloseWithOpenTransactionDiscardsWork(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	reader, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open reader: %v", err)
	}
	defer reader.Close()

	const table = "ADBC_T12_CLOSE"
	dropTable(ctx, t, reader, table)
	execUpdate(ctx, t, reader, "CREATE TABLE "+table+" (ID NUMBER(10))")
	defer dropTable(ctx, t, reader, table)

	writer, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open writer: %v", err)
	}
	setAutocommit(t, writer, false)
	execUpdate(ctx, t, writer, "INSERT INTO "+table+" VALUES (3)")

	done := make(chan error, 1)
	go func() { done <- writer.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close with an open transaction: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Close blocked with an open transaction")
	}

	if got := countRows(ctx, t, reader, table); got != "0" {
		t.Errorf("uncommitted rows survived Close: got %q, want 0", got)
	}
}

// Both isolation levels Oracle implements must be accepted and actually take
// effect on the transaction; everything else must be refused.
func TestIntegration_IsolationLevels(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	for _, level := range []adbc.OptionIsolationLevel{adbc.LevelReadCommitted, adbc.LevelSerializable} {
		t.Run(string(level), func(t *testing.T) {
			cnxn, err := db.Open(ctx)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer cnxn.Close()

			setter := cnxn.(adbc.GetSetOptions)
			if err := setter.SetOption(adbc.OptionKeyIsolationLevel, string(level)); err != nil {
				t.Fatalf("SetOption(%s): %v", level, err)
			}
			setAutocommit(t, cnxn, false)

			// A statement begins the transaction, which is where the
			// SET TRANSACTION clause is issued; a bad clause fails here.
			if got := querySingleString(ctx, t, cnxn, "SELECT 1 FROM DUAL"); got != "1" {
				t.Fatalf("query under %s returned %q", level, got)
			}
			if err := cnxn.Commit(ctx); err != nil {
				t.Fatalf("Commit under %s: %v", level, err)
			}
			if got := getOption(t, cnxn, adbc.OptionKeyIsolationLevel); got != string(level) {
				t.Errorf("GetOption = %q, want %q", got, level)
			}
		})
	}

	t.Run("unsupported", func(t *testing.T) {
		cnxn, err := db.Open(ctx)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer cnxn.Close()

		err = cnxn.(adbc.GetSetOptions).SetOption(
			adbc.OptionKeyIsolationLevel, string(adbc.LevelRepeatableRead))
		if err == nil {
			t.Fatal("repeatable_read was accepted, want StatusNotImplemented")
		}
		var adbcErr adbc.Error
		if !errors.As(err, &adbcErr) {
			t.Fatalf("got %T, want adbc.Error", err)
		}
		if adbcErr.Code != adbc.StatusNotImplemented {
			t.Errorf("code = %v, want StatusNotImplemented", adbcErr.Code)
		}
	})
}

// Ingest with autocommit off must land inside the caller's transaction, so a
// rollback discards every row even with several workers configured.
func TestIntegration_IngestInsideTransactionRollsBack(t *testing.T) {
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

	const table = "ADBC_T12_INGEST_TX"
	const rows = 2000
	dropTable(ctx, t, cnxn, table)
	// Create up front: the ingest runs in append mode so its own DDL cannot
	// commit the transaction out from under the test.
	execUpdate(ctx, t, cnxn, "CREATE TABLE "+table+" (\"ID\" NUMBER(19))")
	defer dropTable(ctx, t, cnxn, table)

	setAutocommit(t, cnxn, false)

	rec := intRecord(t, "ID", rows)
	defer rec.Release()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, table); err != nil {
		t.Fatalf("SetOption(target table): %v", err)
	}
	if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend); err != nil {
		t.Fatalf("SetOption(append): %v", err)
	}
	if err := stmt.Bind(ctx, rec); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	n, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		stmt.Close()
		t.Fatalf("ExecuteUpdate: %v", err)
	}
	stmt.Close()
	if n != rows {
		t.Errorf("ExecuteUpdate reported %d rows, want %d", n, rows)
	}
	if got := countRows(ctx, t, cnxn, table); got != "2000" {
		t.Fatalf("writer sees %q rows inside the transaction, want 2000", got)
	}

	if err := cnxn.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if got := countRows(ctx, t, cnxn, table); got != "0" {
		t.Errorf("ingest escaped the transaction: %q rows survived rollback, want 0", got)
	}
}

// --- helpers ---

func setAutocommit(t *testing.T, cnxn adbc.Connection, enabled bool) {
	t.Helper()
	val := adbc.OptionValueDisabled
	if enabled {
		val = adbc.OptionValueEnabled
	}
	setter, ok := cnxn.(adbc.GetSetOptions)
	if !ok {
		t.Fatal("connection does not implement adbc.GetSetOptions")
	}
	if err := setter.SetOption(adbc.OptionKeyAutoCommit, val); err != nil {
		t.Fatalf("SetOption(autocommit=%v): %v", enabled, err)
	}
}

func getOption(t *testing.T, cnxn adbc.Connection, key string) string {
	t.Helper()
	getter, ok := cnxn.(adbc.GetSetOptions)
	if !ok {
		t.Fatal("connection does not implement adbc.GetSetOptions")
	}
	val, err := getter.GetOption(key)
	if err != nil {
		t.Fatalf("GetOption(%s): %v", key, err)
	}
	return val
}

func execUpdate(ctx context.Context, t *testing.T, cnxn adbc.Connection, query string) {
	t.Helper()
	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(query); err != nil {
		t.Fatalf("SetSqlQuery(%q): %v", query, err)
	}
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		t.Fatalf("ExecuteUpdate(%q): %v", query, err)
	}
}

func countRows(ctx context.Context, t *testing.T, cnxn adbc.Connection, table string) string {
	t.Helper()
	return querySingleString(ctx, t, cnxn, "SELECT COUNT(*) FROM "+table)
}
