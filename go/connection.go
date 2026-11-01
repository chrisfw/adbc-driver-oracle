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
	"fmt"
	"strconv"
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

	// timestampUnit is the Arrow unit for TIMESTAMP columns, resolved by
	// databaseImpl.effectiveTimestampUnit.
	timestampUnit arrow.TimeUnit

	// tzOffsetColumn emits the "<col>__tz" sibling columns. See
	// OptionTzOffsetColumn.
	tzOffsetColumn bool

	// numberScanRows samples rows to infer a type for unconstrained NUMBER;
	// 0 disables it. See OptionNumberScanRows.
	numberScanRows int

	// numberOverflow is the policy for a value that does not fit the inferred
	// decimal. See OptionNumberOverflow.
	numberOverflow string

	// compactDecimals narrows declared NUMBER to Decimal32/64 where it fits.
	compactDecimals bool

	// timestampWithOffset carries TIMESTAMP WITH TIME ZONE as the canonical
	// arrow extension rather than as a UTC timestamp plus a sibling column.
	timestampWithOffset bool

	// opaqueNumber labels an uninferred NUMBER as arrow.opaque. See
	// OptionOpaqueNumber.
	opaqueNumber bool

	varcharOnce  sync.Once
	varcharLimit int // cached inline VARCHAR2 byte cap; see inlineStringLimit
}

// typePolicy returns the resolved type-mapping configuration both schema
// paths must use.
func (c *connectionImpl) typePolicy() typePolicy {
	return typePolicy{
		numberMapping:       c.numberMapping,
		timestampUnit:       c.timestampUnit,
		tzOffsetColumn:      c.tzOffsetColumn,
		numberScanRows:      c.numberScanRows,
		numberOverflow:      c.numberOverflow,
		compactDecimals:     c.compactDecimals,
		timestampWithOffset: c.timestampWithOffset,
		opaqueNumber:        c.opaqueNumber,
	}
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
		return nil, c.ErrorHelper.WrapIO(err, "failed to begin transaction")
	}
	if c.isolation != "" {
		// SET TRANSACTION has to be the transaction's first statement, which
		// is exactly where we are.
		if _, err := tx.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL "+c.isolation); err != nil {
			_ = tx.Rollback()
			return nil, c.ErrorHelper.WrapIO(err,
				"failed to set isolation level %s", c.isolation)
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
		return c.ErrorHelper.WrapIO(err,
			"failed to commit pending transaction while enabling autocommit")
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
		return nil, c.ErrorHelper.WrapIO(err, "GetTableSchema: cannot resolve current schema")
	}

	query := `SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_PRECISION, DATA_SCALE
		FROM ALL_TAB_COLUMNS WHERE OWNER = :1 AND TABLE_NAME = :2 ORDER BY COLUMN_ID`
	sess, err := c.session(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := sess.QueryContext(ctx, query, schema, strings.ToUpper(tableName))
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "GetTableSchema")
	}
	defer rows.Close()

	var fields []arrow.Field
	// Columns whose type has to come from data rather than from the
	// dictionary, resolved after this loop so the cursor is free.
	var inferNumber []string
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

		pol := c.typePolicy()
		colType := oracleDataTypeToArrow(dataType, prec, scale, pol)
		// An unconstrained NUMBER gets its type from the data in the query
		// path, so the dictionary has to sample too or the two schemas
		// describe the same column differently, which is the defect 2.1
		// exists to prevent.
		if p, s, ok := decimalSizeFromMetadata(prec, scale); numberNeedsInference(p, s, ok, pol) &&
			strings.ToUpper(dataType) == "NUMBER" {
			inferNumber = append(inferNumber, name)
		}
		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     colType,
			Nullable: nullable == "Y",
		})
		// The sibling zone column has to appear here too, or this schema
		// stops describing the batches ExecuteQuery returns.
		if pol.tzOffsetColumn && ddlHasRecoverableZone(dataType) {
			fields = append(fields, arrow.Field{
				Name:     name + tzColumnSuffix,
				Type:     arrow.BinaryTypes.String,
				Nullable: true,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, c.ErrorHelper.Errorf(adbc.StatusNotFound, "table %s.%s not found", schema, tableName)
	}

	if len(inferNumber) > 0 {
		c.inferTableNumberTypes(ctx, schema, tableName, inferNumber, fields)
	}

	return arrow.NewSchema(fields, nil), nil
}

// inferTableNumberTypes samples the table to type the unconstrained NUMBER
// columns the dictionary could not, updating fields in place.
//
// This is what keeps GetTableSchema agreeing with ExecuteQuery: the query path
// samples the result, so the metadata path samples the table. A failure here
// is not fatal, since the columns already hold the text fallback, which is
// what the query path also falls back to.
func (c *connectionImpl) inferTableNumberTypes(ctx context.Context, schema, tableName string, cols []string, fields []arrow.Field) {
	n := c.numberScanRows
	if n < 1 {
		return
	}

	quoted := make([]string, len(cols))
	for i, name := range cols {
		quoted[i] = quoteOracleIdentifier(name)
	}
	query := "SELECT " + strings.Join(quoted, ", ") +
		" FROM " + quoteOracleIdentifier(schema) + "." + quoteOracleIdentifier(strings.ToUpper(tableName)) +
		" WHERE ROWNUM <= " + strconv.Itoa(n)

	sess, err := c.session(ctx)
	if err != nil {
		return
	}
	rows, err := sess.QueryContext(ctx, query)
	if err != nil {
		// No SELECT on the table, or it is a view that cannot be sampled.
		// The text fallback already in fields stands.
		return
	}
	defer rows.Close()

	digits := make([]numberDigits, len(cols))
	dest := make([]interface{}, len(cols))
	vals := make([]interface{}, len(cols))
	for i := range dest {
		dest[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return
		}
		for i := range cols {
			switch v := vals[i].(type) {
			case string:
				digits[i].observe(v)
			case []byte:
				digits[i].observe(string(v))
			case nil:
			default:
				digits[i].unparseable = true
			}
		}
	}
	if rows.Err() != nil {
		return
	}

	byName := make(map[string]int, len(cols))
	for i, name := range cols {
		byName[name] = i
	}
	for fi := range fields {
		i, ok := byName[fields[fi].Name]
		if !ok {
			continue
		}
		if dt := inferredNumberType(digits[i]); dt != nil {
			fields[fi].Type = dt
		}
	}
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
	ident, err := normalizeSchemaIdentifier(schema)
	if err != nil {
		return c.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "invalid schema name: %s", err)
	}

	ctx := context.Background()
	sess, err := c.session(ctx)
	if err != nil {
		return err
	}
	// Quoted, so Oracle can only read the value as a single identifier.
	// ALTER SESSION takes a list of parameters, so concatenating the name raw
	// let a caller set any other session parameter: a schema of
	// `ADBC NLS_DATE_FORMAT = 'YYYY'` changed the date format too. Quoted, the
	// same string is one (nonexistent) identifier and fails with ORA-01435.
	if _, err := sess.ExecContext(ctx,
		"ALTER SESSION SET CURRENT_SCHEMA = "+quoteOracleIdentifier(ident)); err != nil {
		return c.ErrorHelper.WrapIO(err, "failed to set schema to %q", ident)
	}
	return nil
}

// maxOracleIdentifierBytes is the identifier length Oracle allows from 12.2
// onward. Older servers cap at 30 and reject a longer name themselves, so this
// bound only exists to fail obvious junk early with a clear message.
const maxOracleIdentifierBytes = 128

// quoteOracleIdentifier renders name as an Oracle delimited identifier:
// wrapped in double quotes with any embedded quote doubled. Quoting
// unconditionally is what makes the value inert: Oracle can only read it as
// one identifier, never as additional statement syntax.
func quoteOracleIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// normalizeSchemaIdentifier resolves a caller-supplied schema name to the
// identifier Oracle should see.
//
// Oracle folds an unquoted identifier to upper case and takes a quoted one
// literally, so `hr`, `HR` and `Hr` all mean the schema HR while `"hr"` means
// a genuinely lower-case one. Bare names are what callers pass almost always,
// so those are upper-cased, which is what this driver has always done; a name
// the caller already wrapped in quotes is honoured exactly.
//
// Rejecting a quote here is defence in depth rather than the actual defence:
// quoteOracleIdentifier escapes whatever gets through, so the error exists to
// say what is wrong instead of letting Oracle answer with a puzzling
// ORA-01435.
func normalizeSchemaIdentifier(name string) (string, error) {
	s := strings.TrimSpace(name)
	if s == "" {
		return "", errors.New("name is empty")
	}
	if len(s) >= 2 && strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		s = s[1 : len(s)-1] // caller asked for an exact-case identifier
	} else {
		s = strings.ToUpper(s)
	}

	switch {
	case s == "":
		return "", errors.New("name is empty")
	case strings.ContainsRune(s, 0):
		return "", errors.New("name contains a NUL byte")
	case strings.Contains(s, `"`):
		return "", fmt.Errorf("name contains a double quote: %q", name)
	case len(s) > maxOracleIdentifierBytes:
		return "", fmt.Errorf("name is %d bytes, over Oracle's %d-byte identifier limit",
			len(s), maxOracleIdentifierBytes)
	}
	return s, nil
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
		return c.ErrorHelper.WrapIO(err, "commit failed")
	}
	return nil
}

func (c *connectionImpl) Rollback(ctx context.Context) error {
	tx := c.takeTx()
	if tx == nil {
		return nil
	}
	if err := tx.Rollback(); err != nil {
		return c.ErrorHelper.WrapIO(err, "rollback failed")
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
			firstErr = c.ErrorHelper.WrapIO(err, "rollback on close failed")
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

// oracleDataTypeToArrow converts an Oracle data type name (from ALL_TAB_COLUMNS)
// to an Arrow type.
//
// numberMapping must be the connection's resolved policy. This path and the
// result-set path (oracleTypeToArrow) have to produce the same type for the
// same column, or GetTableSchema describes a table that ExecuteQuery does not
// return, so both send NUMBER through numberToArrow rather than reimplementing
// the policy.
func oracleDataTypeToArrow(dataType string, precision, scale sql.NullInt64, pol typePolicy) arrow.DataType {
	dt := strings.ToUpper(dataType)
	switch {
	// FLOAT is an ANSI synonym stored as NUMBER, and go-ora reports it as
	// NUMBER on the wire, so it has to take the NUMBER route or the two
	// schemas disagree: under the decimal policy a query returned String
	// while this said Float64. BINARY_FLOAT and BINARY_DOUBLE are genuine
	// IEEE floats, are reported as themselves, and stay Float64.
	case dt == "NUMBER" || dt == "FLOAT":
		p, s, ok := decimalSizeFromMetadata(precision, scale)
		return numberToArrow(p, s, ok, pol.numberMapping)
	case dt == "BINARY_FLOAT":
		return arrow.PrimitiveTypes.Float32
	case dt == "BINARY_DOUBLE":
		return arrow.PrimitiveTypes.Float64
	// See oracleTypeToArrow: LOBs take the 64-bit offset layout so a batch
	// over 2 GiB of LOB data cannot overflow int32 offsets.
	case dt == "CLOB" || dt == "NCLOB" || dt == "LONG":
		return arrow.BinaryTypes.LargeString
	case dt == "VARCHAR2" || dt == "VARCHAR" || dt == "NVARCHAR2" ||
		dt == "CHAR" || dt == "NCHAR" || dt == "ROWID" || dt == "UROWID":
		return arrow.BinaryTypes.String
	// Oracle DATE has second granularity, so microseconds is always lossless
	// for it and the unit policy does not apply.
	case dt == "DATE":
		return timestampNoTZ
	// ALL_TAB_COLUMNS spells the declared precision into DATA_TYPE, so the
	// name here is "TIMESTAMP(6)" or "TIMESTAMP(9) WITH LOCAL TIME ZONE" and
	// never a bare "TIMESTAMP". An exact match for the bare name therefore
	// never fired, and every naive TIMESTAMP fell through to the zoned branch:
	// GetTableSchema announced timestamp[us, tz=UTC] for a column ExecuteQuery
	// returns as naive.
	case strings.HasPrefix(dt, "TIMESTAMP"):
		return pol.oracleTimestamp(strings.Contains(dt, "TIME ZONE"))
	// ALL_TAB_COLUMNS spells these "INTERVAL DAY(2) TO SECOND(6)" and
	// "INTERVAL YEAR(4) TO MONTH".
	case strings.HasPrefix(dt, "INTERVAL DAY"):
		return pol.oracleIntervalDS()
	case strings.HasPrefix(dt, "INTERVAL YEAR"):
		return arrow.FixedWidthTypes.MonthInterval
	case strings.HasPrefix(dt, "INTERVAL"):
		return arrow.BinaryTypes.String
	case dt == "BLOB" || dt == "LONG RAW" || dt == "BFILE":
		return arrow.BinaryTypes.LargeBinary
	case dt == "RAW":
		return arrow.BinaryTypes.Binary
	case dt == "SDO_GEOMETRY":
		return arrow.BinaryTypes.Binary
	case dt == "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean
	default:
		return arrow.BinaryTypes.String
	}
}

// ddlHasRecoverableZone is hasRecoverableZone for a declared type name, which
// is how ALL_TAB_COLUMNS spells it: "TIMESTAMP(6) WITH TIME ZONE" or
// "TIMESTAMP(6) WITH LOCAL TIME ZONE". Only the former still carries the zone
// its values were written with.
func ddlHasRecoverableZone(dataType string) bool {
	dt := strings.ToUpper(dataType)
	return strings.Contains(dt, "WITH TIME ZONE") && !strings.Contains(dt, "LOCAL")
}

// decimalSizeFromMetadata translates ALL_TAB_COLUMNS precision and scale into
// the triple sql.ColumnType.DecimalSize reports for the same column, so both
// schema paths hand numberToArrow identical values.
//
// They differ only in how an unconstrained NUMBER arrives. The data dictionary
// leaves DATA_PRECISION and DATA_SCALE null; go-ora substitutes Oracle's -127
// "floating" scale with 0xFF, which surfaces as 255 (see oracleScaleSentinel).
// A null scale therefore has to become that sentinel and not zero, which would
// read as an integral column and map to Int64.
//
// ok is always true: reaching here means the data dictionary described a
// NUMBER, which is exactly what DecimalSize reporting true means.
func decimalSizeFromMetadata(precision, scale sql.NullInt64) (int64, int64, bool) {
	var p int64
	if precision.Valid {
		p = precision.Int64
	}
	s := int64(oracleScaleSentinel)
	if scale.Valid {
		s = scale.Int64
	}
	return p, s, true
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
