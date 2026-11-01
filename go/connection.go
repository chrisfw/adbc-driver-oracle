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
	"sync"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	// db is the database's shared pool, kept only as a session factory for
	// ingest workers. Statements must never run on it: a pool hands out
	// whichever session is free, so a query, an ALTER SESSION and a COMMIT
	// issued back to back could each land on a different Oracle session.
	db *sql.DB

	// conn is this ADBC connection's own Oracle session, checked out of the
	// pool for the connection's whole life. Every statement, session setting
	// and transaction runs here, which is what makes ALTER SESSION,
	// CURRENT_SCHEMA and transactions mean anything on this connection.
	conn *sql.Conn

	// tx is the open transaction, or nil. Only ever non-nil with autocommit
	// off, where session() begins it at the first statement and Commit,
	// Rollback or Close ends it.
	tx *sql.Tx

	// txCtx is the context transactions are begun with, and txCancel ends it.
	// It must outlive any single statement: database/sql rolls a transaction
	// back when the context it was begun with is cancelled, so beginning one
	// with a statement's context would kill the transaction as soon as that
	// statement's caller moved on. Close cancels it, which also stops the
	// goroutine database/sql starts per transaction to watch for that.
	txCtx    context.Context
	txCancel context.CancelFunc

	// isolation is the Oracle isolation level clause applied to each
	// transaction this connection begins, or "" for the server default.
	// Set through adbc.OptionKeyIsolationLevel.
	isolation string

	ingestWorkers int // parallel insert workers (from database config)

	// numberMapping is the NUMBER→Arrow policy for result sets, resolved to a
	// concrete value by databaseImpl.effectiveNumberMapping.
	numberMapping string

	varcharOnce  sync.Once
	varcharLimit int // cached inline VARCHAR2 byte cap; see inlineStringLimit
}

// sessionExecutor is the subset of *sql.Conn and *sql.Tx that statements and
// metadata queries use. Both types satisfy it, so routing this connection's
// work through an open transaction becomes a change to session() alone rather
// than to every call site.
type sessionExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// session returns the executor this connection's work must run on.
//
// With autocommit on that is the owned session directly: go-ora sets the TTC
// commit-on-success flag on every DML execute, so each statement commits
// itself and no transaction is involved.
//
// With autocommit off the first caller here begins a transaction on the owned
// session and every later statement joins it, until Commit or Rollback ends
// it; the statement after that begins a fresh one. go-ora's Begin is local
// state (it only clears the commit-on-success flag), so this costs no round
// trip beyond the isolation level when one is set.
//
// After Close the owned *sql.Conn is spent rather than nil, so a statement
// used past its connection reports sql.ErrConnDone instead of panicking.
func (c *connectionImpl) session(ctx context.Context) (sessionExecutor, error) {
	if c.Autocommit {
		return c.conn, nil
	}
	if c.tx != nil {
		return c.tx, nil
	}

	txCtx := c.txCtx
	if txCtx == nil {
		txCtx = context.Background()
	}
	// Isolation goes in as SQL, not TxOptions: go-ora's BeginTx rejects any
	// non-default level outright ("only support default value for isolation").
	tx, err := c.conn.BeginTx(txCtx, nil)
	if err != nil {
		return nil, c.ErrorHelper.Errorf(adbc.StatusIO, "failed to begin transaction: %s", err)
	}
	if c.isolation != "" {
		// SET TRANSACTION has to be the transaction's first statement, which
		// is exactly where we are.
		if _, err := tx.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL "+c.isolation); err != nil {
			_ = tx.Rollback()
			return nil, c.ErrorHelper.Errorf(adbc.StatusIO,
				"failed to set isolation level %s: %s", c.isolation, err)
		}
	}
	c.tx = tx
	return c.tx, nil
}

// takeTx detaches the open transaction so a caller can end it exactly once.
func (c *connectionImpl) takeTx() *sql.Tx {
	tx := c.tx
	c.tx = nil
	return tx
}

// SetAutocommit implements driverbase.AutocommitSetter. driverbase routes
// adbc.OptionKeyAutoCommit here and records the new state only when this
// returns nil.
func (c *connectionImpl) SetAutocommit(enabled bool) error {
	if enabled == c.Autocommit {
		return nil
	}
	if !enabled {
		// Turning autocommit off only arms session(); the transaction begins
		// at the next statement, so there is nothing here that can fail.
		return nil
	}
	// ADBC requires enabling autocommit to commit whatever is pending.
	tx := c.takeTx()
	if tx == nil {
		return nil
	}
	if err := tx.Commit(); err != nil {
		return c.ErrorHelper.Errorf(adbc.StatusIO,
			"failed to commit pending transaction while enabling autocommit: %s", err)
	}
	return nil
}

// SetOption handles the connection options this driver owns. driverbase has
// already dealt with autocommit, catalog and schema by the time a key reaches
// here.
func (c *connectionImpl) SetOption(key, val string) error {
	switch key {
	case adbc.OptionKeyIsolationLevel:
		clause, ok := oracleIsolationClause(val)
		if !ok {
			return c.ErrorHelper.Errorf(adbc.StatusNotImplemented,
				"unsupported %s %q: Oracle offers read_committed and serializable",
				adbc.OptionKeyIsolationLevel, val)
		}
		// Applied when the next transaction begins; an open one keeps the
		// level it started with, since SET TRANSACTION only works first.
		c.isolation = clause
		return nil
	default:
		return c.ConnectionImplBase.SetOption(key, val)
	}
}

func (c *connectionImpl) GetOption(key string) (string, error) {
	switch key {
	case adbc.OptionKeyIsolationLevel:
		switch c.isolation {
		case isolationReadCommitted:
			return string(adbc.LevelReadCommitted), nil
		case isolationSerializable:
			return string(adbc.LevelSerializable), nil
		default:
			return string(adbc.LevelDefault), nil
		}
	default:
		return c.ConnectionImplBase.GetOption(key)
	}
}

// Oracle implements exactly two of the SQL isolation levels: READ COMMITTED
// (its default) and SERIALIZABLE. READ ONLY exists too but is not an ADBC
// level. Anything else has no Oracle equivalent and must be refused rather
// than silently downgraded.
const (
	isolationReadCommitted = "READ COMMITTED"
	isolationSerializable  = "SERIALIZABLE"
)

// oracleIsolationClause maps an ADBC isolation level to its Oracle SET
// TRANSACTION clause. It accepts the canonical ADBC option values and the
// bare short names, since callers reach for both.
func oracleIsolationClause(val string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case string(adbc.LevelDefault), "default", "":
		return "", true
	case string(adbc.LevelReadCommitted), "read_committed":
		return isolationReadCommitted, true
	case string(adbc.LevelSerializable), "serializable":
		return isolationSerializable, true
	default:
		return "", false
	}
}

// ingestSession is the session one ingest worker inserts on.
//
// With autocommit on, each worker takes its own pooled session: ingest batches
// commit themselves and are order-independent, nothing is inside a caller
// transaction, and the round-trip latency of one worker overlaps another's.
// Recycling such a session also sidesteps the ORA-00600 [kopi2_readlen083]
// that go-ora hits after many SDO_GEOMETRY UDT binds on one session.
//
// With autocommit off the rows have to land in the caller's transaction, and
// only the connection's owned session is in that transaction. Workers then
// share that one session, committing is the caller's job, and closing is a
// no-op because the session outlives the ingest.
type ingestSession struct {
	exec sessionExecutor

	// pooled distinguishes a session taken from the pool for this worker
	// alone, which is safe to COMMIT on and to close, from the connection's
	// owned session, which is neither.
	pooled bool
	conn   *sql.Conn
}

func (c *connectionImpl) newIngestSession(ctx context.Context) (*ingestSession, error) {
	if !c.Autocommit {
		// Join the caller's transaction rather than take a session that would
		// commit outside it.
		exec, err := c.session(ctx)
		if err != nil {
			return nil, err
		}
		return &ingestSession{exec: exec}, nil
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &ingestSession{exec: conn, pooled: true, conn: conn}, nil
}

// commit ends this worker's unit of work. Only a pooled session commits; on
// the owned session the caller's Commit decides the fate of the whole ingest.
func (is *ingestSession) commit(ctx context.Context) error {
	if !is.pooled {
		return nil
	}
	_, err := is.conn.ExecContext(ctx, "COMMIT")
	return err
}

// close returns a pooled session to the pool. The owned session outlives the
// ingest, so closing it here would take the whole ADBC connection down.
func (is *ingestSession) close() {
	if is.pooled && is.conn != nil {
		is.conn.Close()
		is.conn = nil
		is.exec = nil
	}
}

// ingestConcurrency caps ingest parallelism at what the current transaction
// mode allows. With autocommit off every worker shares the connection's single
// owned session so the rows stay inside the caller's transaction; parallel
// workers would only interleave on that one session, so ingest goes serial.
func (c *connectionImpl) ingestConcurrency(requested int) int {
	if !c.Autocommit {
		return 1
	}
	if requested < 1 {
		return 1
	}
	return requested
}

// inlineStringLimit is the max byte length a value can carry as a plain
// VARCHAR2 bind (vs. a temporary-LOB CLOB bind). Oracle allows 32767-byte
// VARCHAR2 when MAX_STRING_SIZE=EXTENDED (the Autonomous DB default), else 4000.
//
// This matters for ingest throughput, not storage: on EXTENDED Oracle already
// stores VARCHAR2 >4000 out-of-line, but a VARCHAR2 column binds the whole batch
// as one plain-string array (a single array execute), whereas a CLOB column
// allocates a temporary LOB locator per row (per-value round-trips). Sizing
// wide-but-<=32767 columns as VARCHAR2 keeps the fast batch path; only values
// beyond 32767 bytes need CLOB.
//
// Detected once and cached. Any probe failure (e.g. no v$parameter privilege)
// falls back to the safe 4000 — correct everywhere, just slower for wide text.
func (c *connectionImpl) inlineStringLimit(ctx context.Context) int {
	c.varcharOnce.Do(func() {
		c.varcharLimit = maxInlineStringBytes
		sess, err := c.session(ctx)
		if err != nil {
			return
		}
		var v string
		if err := sess.QueryRowContext(ctx,
			"SELECT value FROM v$parameter WHERE name = 'max_string_size'").Scan(&v); err == nil {
			if strings.EqualFold(strings.TrimSpace(v), "EXTENDED") {
				c.varcharLimit = maxExtendedStringBytes
			}
		}
	})
	return c.varcharLimit
}

func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	stBase := driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper)
	st := &statementImpl{
		StatementImplBase: stBase,
		cnxn:              c,
		alloc:             c.Alloc,
	}
	return driverbase.NewStatement(st), nil
}

// --- Metadata: DbObjectsEnumerator ---

func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	// Oracle doesn't have catalogs in the same way. Return the current database name.
	var name string
	sess, err := c.session(ctx)
	if err != nil {
		return nil, err
	}
	if err := sess.QueryRowContext(ctx, "SELECT ORA_DATABASE_NAME FROM DUAL").Scan(&name); err != nil {
		return nil, err
	}
	if catalogFilter != nil && !matchLike(name, *catalogFilter) {
		return nil, nil
	}
	return []string{name}, nil
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	query := "SELECT USERNAME FROM ALL_USERS ORDER BY USERNAME"
	sess, err := c.session(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := sess.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if schemaFilter != nil && !matchLike(name, *schemaFilter) {
			continue
		}
		schemas = append(schemas, name)
	}
	return schemas, rows.Err()
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	query := `SELECT TABLE_NAME, TABLE_TYPE FROM (
		SELECT TABLE_NAME, 'TABLE' AS TABLE_TYPE FROM ALL_TABLES WHERE OWNER = :1
		UNION ALL
		SELECT VIEW_NAME, 'VIEW' AS TABLE_TYPE FROM ALL_VIEWS WHERE OWNER = :2
	) ORDER BY TABLE_NAME`

	sess, err := c.session(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := sess.QueryContext(ctx, query, strings.ToUpper(schema), strings.ToUpper(schema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []driverbase.TableInfo
	for rows.Next() {
		var name, tableType string
		if err := rows.Scan(&name, &tableType); err != nil {
			return nil, err
		}
		if tableFilter != nil && !matchLike(name, *tableFilter) {
			continue
		}

		info := driverbase.TableInfo{
			TableName: name,
			TableType: tableType,
		}

		if includeColumns {
			cols, err := c.getColumnsForTable(ctx, schema, name, columnFilter)
			if err != nil {
				return nil, err
			}
			info.TableColumns = cols
		}

		tables = append(tables, info)
	}
	return tables, rows.Err()
}

func (c *connectionImpl) getColumnsForTable(ctx context.Context, schema, table string, columnFilter *string) ([]driverbase.ColumnInfo, error) {
	query := `SELECT COLUMN_NAME, COLUMN_ID, DATA_TYPE, NULLABLE, DATA_DEFAULT, DATA_PRECISION, DATA_SCALE
		FROM ALL_TAB_COLUMNS WHERE OWNER = :1 AND TABLE_NAME = :2 ORDER BY COLUMN_ID`
	sess, err := c.session(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := sess.QueryContext(ctx, query, strings.ToUpper(schema), strings.ToUpper(table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []driverbase.ColumnInfo
	for rows.Next() {
		var (
			name      string
			ordinal   sql.NullInt32
			dataType  sql.NullString
			nullable  sql.NullString
			colDef    sql.NullString
			precision sql.NullInt16
			scale     sql.NullInt16
		)
		if err := rows.Scan(&name, &ordinal, &dataType, &nullable, &colDef, &precision, &scale); err != nil {
			return nil, err
		}
		if columnFilter != nil && !matchLike(name, *columnFilter) {
			continue
		}

		xdbcNullable := driverbase.XdbcColumnNullableUnknown
		if nullable.Valid && nullable.String == "Y" {
			xdbcNullable = driverbase.XdbcColumnNullable
		} else if nullable.Valid && nullable.String == "N" {
			xdbcNullable = driverbase.XdbcColumnNoNulls
		}

		col := driverbase.ColumnInfo{
			ColumnName:      name,
			OrdinalPosition: driverbase.NullInt32ToPtr(ordinal),
			XdbcTypeName:    driverbase.NullStringToPtr(dataType),
			XdbcNullable:    &xdbcNullable,
			XdbcColumnDef:   driverbase.NullStringToPtr(colDef),
		}
		if precision.Valid {
			col.XdbcColumnSize = driverbase.ToPtr[int32](int32(precision.Int16))
		}
		if scale.Valid {
			col.XdbcDecimalDigits = driverbase.NullInt16ToPtr(scale)
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// --- Metadata: TableTypeLister ---

func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"TABLE", "VIEW", "MATERIALIZED VIEW", "SYNONYM"}, nil
}

// --- Metadata: GetTableSchema ---

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	var schema string
	if dbSchema != nil && *dbSchema != "" {
		schema = strings.ToUpper(*dbSchema)
	} else if s, err := c.GetCurrentDbSchema(); err == nil {
		schema = s
	} else {
		return nil, c.ErrorHelper.Errorf(adbc.StatusIO, "GetTableSchema: cannot resolve current schema: %s", err)
	}

	query := `SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_PRECISION, DATA_SCALE
		FROM ALL_TAB_COLUMNS WHERE OWNER = :1 AND TABLE_NAME = :2 ORDER BY COLUMN_ID`
	sess, err := c.session(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := sess.QueryContext(ctx, query, schema, strings.ToUpper(tableName))
	if err != nil {
		return nil, c.ErrorHelper.Errorf(adbc.StatusIO, "GetTableSchema: %s", err)
	}
	defer rows.Close()

	var fields []arrow.Field
	for rows.Next() {
		var (
			name     string
			dataType string
			nullable string
			prec     sql.NullInt64
			scale    sql.NullInt64
		)
		if err := rows.Scan(&name, &dataType, &nullable, &prec, &scale); err != nil {
			return nil, err
		}

		arrowType := oracleDataTypeToArrow(dataType, prec, scale)
		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     arrowType,
			Nullable: nullable == "Y",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, c.ErrorHelper.Errorf(adbc.StatusNotFound, "table %s.%s not found", schema, tableName)
	}

	return arrow.NewSchema(fields, nil), nil
}

// --- Namespace: CurrentNamespacer ---

func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	// CurrentNamespacer carries no context; the session is what matters here,
	// so pass Background rather than reach for the pool's context-free API.
	ctx := context.Background()
	sess, err := c.session(ctx)
	if err != nil {
		return "", err
	}
	var name string
	err = sess.QueryRowContext(ctx, "SELECT ORA_DATABASE_NAME FROM DUAL").Scan(&name)
	return name, err
}

func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	ctx := context.Background()
	sess, err := c.session(ctx)
	if err != nil {
		return "", err
	}
	var name string
	err = sess.QueryRowContext(ctx,
		"SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL").Scan(&name)
	return name, err
}

func (c *connectionImpl) SetCurrentCatalog(catalog string) error {
	// Oracle doesn't support switching catalogs
	return c.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Oracle does not support switching catalogs")
}

func (c *connectionImpl) SetCurrentDbSchema(schema string) error {
	ctx := context.Background()
	sess, err := c.session(ctx)
	if err != nil {
		return err
	}
	if _, err := sess.ExecContext(ctx,
		"ALTER SESSION SET CURRENT_SCHEMA = "+strings.ToUpper(schema)); err != nil {
		return c.ErrorHelper.Errorf(adbc.StatusIO, "failed to set schema: %s", err)
	}
	return nil
}

// --- Transactions ---

// Commit ends the open transaction. driverbase rejects this with
// StatusInvalidState while autocommit is on, so by here autocommit is off.
func (c *connectionImpl) Commit(ctx context.Context) error {
	tx := c.takeTx()
	if tx == nil {
		// Autocommit is off but no statement has run, so there is nothing to
		// end. Committing an empty transaction is not an error.
		return nil
	}
	if err := tx.Commit(); err != nil {
		return c.ErrorHelper.Errorf(adbc.StatusIO, "commit failed: %s", err)
	}
	return nil
}

func (c *connectionImpl) Rollback(ctx context.Context) error {
	tx := c.takeTx()
	if tx == nil {
		return nil
	}
	if err := tx.Rollback(); err != nil {
		return c.ErrorHelper.Errorf(adbc.StatusIO, "rollback failed: %s", err)
	}
	return nil
}

func (c *connectionImpl) Close() error {
	if c.conn == nil {
		return nil
	}

	// An open transaction has to end before the session goes back to the pool.
	// database/sql's Conn.Close blocks until the transaction resolves rather
	// than rolling it back, so skipping this hangs the caller outright. ADBC
	// discards uncommitted work on close, so roll back.
	var firstErr error
	if tx := c.takeTx(); tx != nil {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			firstErr = c.ErrorHelper.Errorf(adbc.StatusIO, "rollback on close failed: %s", err)
		}
	}
	if c.txCancel != nil {
		c.txCancel()
	}

	// The pool belongs to the database, so this returns only the session this
	// connection owned. The *sql.Conn stays referenced on purpose: database/sql
	// then reports sql.ErrConnDone for any later use instead of dereferencing
	// a nil pointer.
	if err := c.conn.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// --- Helpers ---

// oracleDataTypeToArrow converts an Oracle data type name (from ALL_TAB_COLUMNS) to an Arrow type.
func oracleDataTypeToArrow(dataType string, precision, scale sql.NullInt64) arrow.DataType {
	dt := strings.ToUpper(dataType)
	switch {
	case dt == "NUMBER":
		if scale.Valid && scale.Int64 == 0 && precision.Valid && precision.Int64 > 0 && precision.Int64 <= 18 {
			return arrow.PrimitiveTypes.Int64
		}
		if scale.Valid && scale.Int64 == 0 && precision.Valid && precision.Int64 > 18 {
			return arrow.BinaryTypes.String
		}
		return arrow.PrimitiveTypes.Float64
	case dt == "FLOAT" || dt == "BINARY_FLOAT" || dt == "BINARY_DOUBLE":
		return arrow.PrimitiveTypes.Float64
	case dt == "VARCHAR2" || dt == "VARCHAR" || dt == "NVARCHAR2" ||
		dt == "CHAR" || dt == "NCHAR" || dt == "CLOB" || dt == "NCLOB" ||
		dt == "LONG" || dt == "ROWID":
		return arrow.BinaryTypes.String
	case dt == "DATE" || dt == "TIMESTAMP":
		return timestampNoTZ
	case strings.HasPrefix(dt, "TIMESTAMP"):
		return arrow.FixedWidthTypes.Timestamp_us
	case strings.HasPrefix(dt, "INTERVAL"):
		return arrow.BinaryTypes.String
	case dt == "RAW" || dt == "LONG RAW" || dt == "BLOB":
		return arrow.BinaryTypes.Binary
	case dt == "SDO_GEOMETRY":
		return arrow.BinaryTypes.Binary
	case dt == "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean
	default:
		return arrow.BinaryTypes.String
	}
}

// matchLike performs SQL LIKE-style matching (% wildcard only).
func matchLike(value, pattern string) bool {
	v := strings.ToUpper(value)
	p := strings.ToUpper(pattern)

	if p == "%" {
		return true
	}
	if !strings.Contains(p, "%") {
		return v == p
	}

	parts := strings.Split(p, "%")
	pos := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.Index(v[pos:], part)
		if idx < 0 {
			return false
		}
		if i == 0 && idx != 0 {
			return false // must match start if pattern doesn't start with %
		}
		pos += idx + len(part)
	}
	// If pattern doesn't end with %, must match end
	if !strings.HasSuffix(p, "%") && pos != len(v) {
		return false
	}
	return true
}
