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
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	go_ora "github.com/sijms/go-ora/v2"
)

type statementImpl struct {
	driverbase.StatementImplBase
	cnxn  *connectionImpl
	alloc memory.Allocator
	query string

	// Prepared statement support
	prepared *sql.Stmt
	// preparedTx is the transaction `prepared` was created on, or nil when it
	// was prepared on the bare session. database/sql closes a transaction's
	// prepared statements when that transaction ends, so once the connection
	// moves to a different transaction the statement has to be prepared
	// again. Always nil under autocommit, where there is no transaction.
	preparedTx *sql.Tx

	// preparedBinds is the bind shape `prepared` was created for, one entry
	// per parameter. A parameter cannot change bind type between executes of
	// one prepared statement without desynchronizing go-ora's TTC stream, so
	// a batch needing a different shape forces a re-prepare. See
	// resolveStringBinds for what goes wrong otherwise.
	preparedBinds []bool

	// Bind parameters (single batch)
	bindParams arrow.RecordBatch
	// Bind stream (multiple batches for bulk ingest)
	bindStream array.RecordReader

	// Ingest options
	ingestTargetTable string
	ingestMode        string
	ingestGeomCols    map[int]int64 // col index → srid for geometry columns

	// String-column DDL sizing (see resolveColumnTypes)
	ingestStringType    string          // "auto" (default), "varchar2", "clob"
	ingestClobColumns   map[string]bool // uppercased column names forced to CLOB
	ingestScanLimitByte int64           // max bytes buffered for the auto type scan (0 = scan disabled)
	ingestScanLimitSet  bool            // distinguishes "unset, use default" from an explicit 0

	// explicitlyPrepared records that the caller called Prepare, so the
	// statement is re-prepared after a transaction ends rather than quietly
	// falling back to an ad-hoc execute.
	explicitlyPrepared bool

	// clobBinds[i] says whether column i binds as a CLOB for every batch of
	// this ingest. Resolved once by resolveStringBinds; read via bindsAsClob.
	clobBinds []bool

	// inlineLimit is the VARCHAR2/CLOB byte boundary for this ingest, resolved
	// from the server's MAX_STRING_SIZE (4000 STANDARD / 32767 EXTENDED) once
	// executeBulkIngest starts. It decides the DDL type in resolveColumnTypes;
	// the bind type then follows that column type rather than this limit, via
	// resolveStringBinds.
	inlineLimit int
	// partitionCountOpt overrides the connection's partition count for this
	// statement. Zero means unset.
	partitionCountOpt int
}

func (s *statementImpl) Base() *driverbase.StatementImplBase {
	return &s.StatementImplBase
}

func (s *statementImpl) SetSqlQuery(query string) error {
	s.query = query
	s.closePrepared()
	// A new query discards the caller's Prepare along with the statement it
	// produced; executing it again should not silently re-prepare.
	s.explicitlyPrepared = false
	return nil
}

func (s *statementImpl) SetOption(key, value string) error {
	switch key {
	case adbc.OptionKeyIngestTargetTable:
		s.ingestTargetTable = value
		return nil
	case adbc.OptionKeyIngestMode:
		s.ingestMode = value
		return nil
	case OptionIngestStringType:
		v := strings.ToLower(strings.TrimSpace(value))
		switch v {
		case "", "auto", "varchar2", "clob":
			s.ingestStringType = v
			return nil
		}
		return s.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
			"%s must be one of auto, varchar2, clob (got %q)", OptionIngestStringType, value)
	case OptionIngestClobColumns:
		s.ingestClobColumns = make(map[string]bool)
		for _, name := range strings.Split(value, ",") {
			if name = strings.TrimSpace(name); name != "" {
				s.ingestClobColumns[strings.ToUpper(name)] = true
			}
		}
		return nil
	case OptionPartitionCount:
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 {
			return s.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"%s must be a positive integer (1 means do not split) (got %q)", OptionPartitionCount, value)
		}
		s.partitionCountOpt = n
		return nil
	case OptionIngestTypeScanLimit:
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < 0 {
			return s.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"%s must be a non-negative integer (got %q; 0 disables the scan)", OptionIngestTypeScanLimit, value)
		}
		s.ingestScanLimitByte = n
		s.ingestScanLimitSet = true
		return nil
	default:
		return s.StatementImplBase.SetOption(key, value)
	}
}

func (s *statementImpl) GetOption(key string) (string, error) {
	switch key {
	case adbc.OptionKeyIngestTargetTable:
		return s.ingestTargetTable, nil
	case adbc.OptionKeyIngestMode:
		return s.ingestMode, nil
	case OptionIngestStringType:
		if s.ingestStringType == "" {
			return "auto", nil
		}
		return s.ingestStringType, nil
	case OptionIngestClobColumns:
		cols := make([]string, 0, len(s.ingestClobColumns))
		for name := range s.ingestClobColumns {
			cols = append(cols, name)
		}
		sort.Strings(cols)
		return strings.Join(cols, ","), nil
	case OptionPartitionCount:
		return strconv.Itoa(s.partitionCount()), nil
	case OptionIngestTypeScanLimit:
		if !s.ingestScanLimitSet {
			return strconv.FormatInt(defaultTypeScanLimitBytes, 10), nil
		}
		return strconv.FormatInt(s.ingestScanLimitByte, 10), nil
	default:
		return s.StatementImplBase.GetOption(key)
	}
}

func (s *statementImpl) Prepare(ctx context.Context) error {
	if s.query == "" {
		return s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}
	s.closePrepared()
	// Preparing on the open transaction when there is one: a *sql.Stmt is
	// bound to whatever began it, and one prepared on the bare session would
	// execute outside the caller's transaction.
	_, err := s.preparedStmt(ctx, nil)
	return err
}

// preparedStmt returns the statement to execute, preparing it if needed and
// re-preparing it when the transaction it belonged to has ended.
//
// ADBC lets a caller prepare once and execute many times, including either
// side of a Commit, but database/sql closes a transaction's prepared
// statements along with the transaction. Tracking which transaction the
// statement belongs to catches that: same transaction (or none, under
// autocommit) reuses it, a different one prepares again. Returns nil when
// nothing has been prepared, which is how the execute paths tell the
// prepared and ad-hoc cases apart.
// preparedStmt returns the statement to execute for the given bind shape,
// preparing or re-preparing it as needed. A nil shape means the caller binds
// no arrays and only the transaction matters.
func (s *statementImpl) preparedStmt(ctx context.Context, shape []bool) (*sql.Stmt, error) {
	if s.prepared != nil && s.preparedTx == s.cnxn.tx && slices.Equal(s.preparedBinds, shape) {
		return s.prepared, nil
	}
	if s.prepared == nil && !s.explicitlyPrepared {
		return nil, nil
	}
	if s.query == "" {
		return nil, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}

	sess, err := s.cnxn.session(ctx)
	if err != nil {
		return nil, err
	}
	s.closePrepared()
	stmt, err := sess.PrepareContext(ctx, s.query)
	if err != nil {
		return nil, s.ErrorHelper.WrapIO(err, "prepare failed")
	}
	s.prepared = stmt
	s.preparedTx = s.cnxn.tx
	s.preparedBinds = shape
	s.explicitlyPrepared = true
	return s.prepared, nil
}

func (s *statementImpl) Bind(ctx context.Context, values arrow.RecordBatch) error {
	if values != nil {
		values.Retain()
	}
	if s.bindParams != nil {
		s.bindParams.Release()
	}
	s.bindParams = values
	return nil
}

func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	if s.bindStream != nil {
		s.bindStream.Release()
	}
	if stream != nil {
		stream.Retain()
	}
	s.bindStream = stream
	return nil
}

func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.query == "" {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}

	// ADBC specifies one result set per bound row. This driver can return
	// only one, and silently answering for row 0 while discarding the rest is
	// worse than refusing: the caller gets plausible data for the wrong
	// parameters and no indication of it.
	if s.bindParams != nil && s.bindParams.NumCols() > 0 && s.bindParams.NumRows() > 1 {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusNotImplemented,
			"ExecuteQuery with %d bound rows is not supported: ADBC expects one result set per row. "+
				"Bind a single row to query, or use ExecuteUpdate to run the statement once per row",
			s.bindParams.NumRows())
	}

	// Build query args from bind parameters
	args := s.extractBindArgs(0)

	prepared, err := s.preparedStmt(ctx, nil)
	if err != nil {
		return nil, -1, err
	}

	var rows *sql.Rows
	if prepared != nil {
		rows, err = prepared.QueryContext(ctx, args...)
	} else {
		sess, sessErr := s.cnxn.session(ctx)
		if sessErr != nil {
			return nil, -1, sessErr
		}
		rows, err = sess.QueryContext(ctx, s.query, args...)
	}
	if err != nil {
		return nil, -1, s.ErrorHelper.WrapIO(err, "query execution failed")
	}

	impl := &oracleRecordReader{
		rows:      rows,
		policy:    s.cnxn.typePolicy(),
		batchRows: readerBatchRows,
		logger:    s.cnxn.Logger,
	}

	var rr driverbase.BaseRecordReader
	// driverbase-go v0.0.0-20260409+ added a *slog.Logger as the 3rd
	// argument to BaseRecordReader.Init (previously just (ctx, alloc,
	// params, options, impl)). Pass slog.Default() — driverbase rejects
	// a nil logger.
	if err := rr.Init(ctx, s.alloc, slog.Default(), nil,
		driverbase.BaseRecordReaderOptions{BatchRowLimit: readerBatchRows}, impl); err != nil {
		rows.Close()
		return nil, -1, err
	}
	return &rr, -1, nil
}

func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	// Bulk ingest path
	if s.ingestTargetTable != "" {
		return s.executeBulkIngest(ctx)
	}

	if s.query == "" {
		return -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}

	exec := func(shape []bool, args []interface{}) (int64, error) {
		prepared, err := s.preparedStmt(ctx, shape)
		if err != nil {
			return -1, err
		}
		nrow := 1
		if s.bindParams != nil && args != nil {
			nrow = int(s.bindParams.NumRows())
		}
		var ex execer
		if prepared != nil {
			ex = prepared
		} else {
			sess, sessErr := s.cnxn.session(ctx)
			if sessErr != nil {
				return -1, sessErr
			}
			ex = execerFunc(func(ctx context.Context, a ...any) (sql.Result, error) {
				return sess.ExecContext(ctx, s.query, a...)
			})
		}
		if nrow <= 1 {
			result, err := ex.ExecContext(ctx, args...)
			if err != nil {
				return -1, s.ErrorHelper.WrapIO(err, "execute failed")
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return -1, s.ErrorHelper.WrapIO(err, "failed to get rows affected")
			}
			return affected, nil
		}
		// An array execute: in chunks go-ora can count (arrayexec.go).
		affected, err := execArray(ctx, ex, args, nrow)
		if err != nil {
			return -1, s.ErrorHelper.WrapIO(err, "execute failed")
		}
		return affected, nil
	}

	// No parameters bound: execute once.
	if s.bindParams == nil || s.bindParams.NumCols() == 0 {
		return exec(nil, nil)
	}
	// A bound batch with zero rows executes zero times.
	if s.bindParams.NumRows() == 0 {
		return 0, nil
	}

	// ADBC semantics are one execution per bound row, which Oracle gives us
	// as a single array execute rather than a round trip each. go-ora binds a
	// slice per parameter and reports the total rows affected.
	shape := s.boundBindShape(ctx, s.bindParams)
	return exec(shape, s.arrayBindArgs(s.bindParams, shape))
}

// boundBindShape decides, per parameter, whether a bound column goes in as a
// CLOB, and so also what bind shape the prepared statement must have.
//
// Only string width forces it. A plain string bind rides go-ora's negotiated
// varchar limit and a wider value is rejected client-side with ORA-01461, so a
// column holding one must use the Clob bind, which Oracle accepts into VARCHAR2
// and CLOB columns alike at any width. Deciding before binding rather than
// retrying after a failure is deliberate: Oracle applies the rows ahead of a
// mid-array error, so a retry would insert them twice.
func (s *statementImpl) boundBindShape(ctx context.Context, rec arrow.RecordBatch) []bool {
	shape := make([]bool, rec.NumCols())

	hasStrings := false
	for col := 0; col < int(rec.NumCols()); col++ {
		switch rec.Column(col).DataType().ID() {
		case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
			hasStrings = true
		}
	}
	if !hasStrings {
		// Nothing to measure, and no reason to probe the server for its limit.
		return shape
	}

	limit := s.cnxn.inlineStringLimit(ctx)
	widths := make([]int, rec.NumCols())
	scanStringWidths(rec, widths)
	for col := range shape {
		shape[col] = widths[col] > limit
	}
	return shape
}

// arrayBindArgs converts a bound batch into one go-ora array bind per column.
//
// These are the ingest path's converters, which bind values in their native
// Oracle form. That matters beyond speed: the per-row path this replaces sent
// a naive Arrow timestamp as time.Time, which go-ora encodes as TIMESTAMP WITH
// TIME ZONE and so shifts the wall clock by the session zone, and sent a
// boolean as a Go bool rather than the 0 or 1 a NUMBER(1) column expects.
func (s *statementImpl) arrayBindArgs(rec arrow.RecordBatch, shape []bool) []interface{} {
	nrow := int(rec.NumRows())
	args := make([]interface{}, rec.NumCols())
	for col := range args {
		asClob := col < len(shape) && shape[col]
		args[col] = arrowColumnToSliceRange(rec.Column(col), 0, nrow, asClob)
	}
	return args
}

// extractBindArgs extracts parameter values from the bind RecordBatch at the given row index.
// Oracle uses :1, :2, ... as bind placeholders.
func (s *statementImpl) extractBindArgs(rowIdx int) []interface{} {
	if s.bindParams == nil || s.bindParams.NumCols() == 0 {
		return nil
	}
	if int64(rowIdx) >= s.bindParams.NumRows() {
		return nil
	}

	args := make([]interface{}, s.bindParams.NumCols())
	for i := 0; i < int(s.bindParams.NumCols()); i++ {
		col := s.bindParams.Column(i)
		if col.IsNull(rowIdx) {
			args[i] = nil
			continue
		}
		args[i] = extractArrowValue(col, rowIdx)
	}
	return args
}

// extractArrowValue reads a single value from an Arrow array at the given index.
func extractArrowValue(arr arrow.Array, idx int) interface{} {
	switch a := arr.(type) {
	case *array.Int8:
		return int64(a.Value(idx))
	case *array.Int16:
		return int64(a.Value(idx))
	case *array.Int32:
		return int64(a.Value(idx))
	case *array.Int64:
		return a.Value(idx)
	case *array.Uint8:
		return int64(a.Value(idx))
	case *array.Uint16:
		return int64(a.Value(idx))
	case *array.Uint32:
		return int64(a.Value(idx))
	case *array.Uint64:
		// Route through decimal text — uint64 can exceed int64.
		return strconv.FormatUint(a.Value(idx), 10)
	case *array.Float32:
		return float64(a.Value(idx))
	case *array.Float64:
		return a.Value(idx)
	case *array.Decimal128:
		return a.Value(idx).ToString(a.DataType().(*arrow.Decimal128Type).Scale)
	case *array.Decimal256:
		return a.Value(idx).ToString(a.DataType().(*arrow.Decimal256Type).Scale)
	case *array.String:
		return a.Value(idx)
	case *array.LargeString:
		return a.Value(idx)
	case *array.Binary:
		return a.Value(idx)
	case *array.LargeBinary:
		return a.Value(idx)
	case *array.Boolean:
		return a.Value(idx)
	case *array.Timestamp:
		dt := a.DataType().(*arrow.TimestampType)
		if toTime, err := dt.GetToTimeFunc(); err == nil {
			return toTime(a.Value(idx))
		}
		return a.Value(idx).ToTime(dt.Unit)
	case *array.Date32:
		return a.Value(idx).ToTime()
	case *array.Date64:
		return a.Value(idx).ToTime()
	default:
		// Fallback: use the string representation
		return arr.ValueStr(idx)
	}
}

// --- Bulk Ingest ---

func (s *statementImpl) executeBulkIngest(ctx context.Context) (int64, error) {
	tableName := strings.ToUpper(s.ingestTargetTable)

	// Resolve the VARCHAR2/CLOB boundary from the server's MAX_STRING_SIZE
	// once, up front: it is what decides whether a string column is declared
	// VARCHAR2 or CLOB, and a column declared wider than the server allows
	// would fail at insert time.
	s.inlineLimit = s.cnxn.inlineStringLimit(ctx)

	// Determine the schema from bindStream or bindParams
	var schema *arrow.Schema
	if s.bindStream != nil {
		schema = s.bindStream.Schema()
	} else if s.bindParams != nil {
		schema = s.bindParams.Schema()
	} else {
		return -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no data bound for ingest")
	}

	mode := s.ingestMode
	if mode == "" {
		mode = adbc.OptionValueIngestModeCreate
	}

	// Resolve per-column Oracle DDL types. In "auto" mode, sizing string
	// columns requires looking at the data: VARCHAR2 caps at 4000 bytes, so
	// wider values must land in CLOB (ORA-12899 otherwise). For a bound
	// stream, buffer records up to the scan limit while scanning widths;
	// buffered records are inserted first, then the rest of the stream.
	var buffered []arrow.RecordBatch
	bufferedOwned := true // until handed to the insert pipeline
	defer func() {
		if bufferedOwned {
			for _, rec := range buffered {
				rec.Release()
			}
		}
	}()

	var colTypes []string
	if mode != adbc.OptionValueIngestModeAppend {
		var widths []int
		if s.needsStringScan(schema) {
			widths = make([]int, schema.NumFields())
			limit := s.ingestScanLimitByte
			if !s.ingestScanLimitSet {
				limit = defaultTypeScanLimitBytes
			}
			// Same source precedence as the insert loop below: a bound stream
			// wins over bound params, so we always size from the data we insert.
			switch {
			case s.bindStream == nil:
				scanStringWidths(s.bindParams, widths)
			case limit == 0:
				// Scan explicitly disabled — fall back to data-independent
				// defaults (LargeString → CLOB, String → VARCHAR2).
				widths = nil
			default:
				var seen int64
				for seen < limit && s.bindStream.Next() {
					rec := s.bindStream.Record()
					rec.Retain()
					buffered = append(buffered, rec)
					seen += recordSizeEstimate(rec)
					scanStringWidths(rec, widths)
				}
				// Only io errors are visible here; end-of-stream is fine
				// (small streams fit entirely inside the scan window).
				if err := s.bindStream.Err(); err != nil {
					return -1, s.ErrorHelper.WrapIO(err, "bind stream error")
				}
			}
		}
		colTypes = resolveColumnTypes(schema, widths, s.ingestStringType, s.ingestClobColumns, s.inlineLimit)
		for i, f := range schema.Fields() {
			if colTypes[i] == "CLOB" && s.cnxn.Logger != nil {
				s.cnxn.Logger.Info("oracle ingest: string column mapped to CLOB",
					"table", tableName, "column", strings.ToUpper(f.Name))
			}
		}
	}

	// Handle table creation/replacement based on mode
	if err := s.prepareIngestTable(ctx, tableName, schema, mode, colTypes); err != nil {
		return -1, err
	}

	// Fix the string bind per column now that the table has its final shape,
	// and before anything is bound. This must not be decided per batch.
	s.clobBinds = s.resolveStringBinds(ctx, tableName, schema, mode, colTypes)

	// Build INSERT statement.
	// Geometry columns are converted client-side from WKB to SdoGeometry UDT
	// structs and sent via go-ora's Object encoding (see wkbColumnToSdoSliceRange).
	colNames := make([]string, schema.NumFields())
	placeholders := make([]string, schema.NumFields())
	geomCols := make(map[int]int64) // col index → srid
	for i, f := range schema.Fields() {
		colNames[i] = fmt.Sprintf(`"%s"`, strings.ToUpper(f.Name))
		placeholders[i] = fmt.Sprintf(":%d", i+1)
		if isGeometryColumn(f) {
			geomCols[i] = extractSRIDFromField(f)
		}
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
	)

	s.ingestGeomCols = geomCols

	// Enable NOLOGGING on the table to reduce redo log overhead during bulk insert.
	// Note: APPEND_VALUES hint is not compatible with go-ora's array binding protocol
	// (Oracle interprets al8i4[1]>1 as batch execution, triggering ORA-38910).
	// NOLOGGING alone still reduces redo I/O for conventional inserts.
	//
	// Only with autocommit on. ALTER TABLE is DDL, and Oracle commits
	// implicitly around DDL, so the restoring ALTER would commit the rows this
	// ingest just inserted and the caller's Rollback would find nothing to
	// undo. Giving up a redo optimisation is the right trade against silently
	// breaking transactional ingest.
	if s.cnxn.Autocommit {
		// Best-effort tuning: ignore failures, including a session we cannot get.
		if sess, err := s.cnxn.session(ctx); err == nil {
			sess.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s NOLOGGING", tableName))
			// Restore logging on every exit path, not just success.
			defer sess.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s LOGGING", tableName))
		}
	}

	var totalRows int64

	if s.bindStream != nil {
		// Pipeline: prepare next batch while current one is inserting.
		// This overlaps CPU-bound column conversion with network-bound Oracle insert.
		convert := func(rec arrow.RecordBatch) preparedBatchT {
			nrow := int(rec.NumRows())
			args := make([]interface{}, int(rec.NumCols()))
			for col := 0; col < int(rec.NumCols()); col++ {
				arr := rec.Column(col)
				if srid, ok := s.ingestGeomCols[col]; ok {
					// Convert WKB to SdoGeometry UDT objects client-side.
					// This also copies the data, so Arrow buffer reuse is safe.
					args[col] = wkbColumnToSdoSliceRange(arr, 0, nrow, srid)
				} else {
					args[col] = arrowColumnToSliceRange(arr, 0, nrow, s.bindsAsClob(col))
				}
			}
			return preparedBatchT{args: args, nrow: nrow}
		}

		workers := s.streamWorkerCount()
		ahead := make(chan preparedBatchT, workers*2)
		done := make(chan struct{})
		var doneOnce sync.Once
		closeDone := func() { doneOnce.Do(func() { close(done) }) }
		prodExited := make(chan struct{})
		// Join the producer before returning (defers run LIFO: closeDone
		// unblocks it first). Otherwise an early error return would leave the
		// goroutine using bindStream while the caller Closes or re-executes
		// the statement.
		defer func() { <-prodExited }()
		defer closeDone()
		bufferedOwned = false // producer goroutine releases buffered records
		go func() {
			defer close(prodExited)
			defer close(ahead)
			send := func(rec arrow.RecordBatch) bool {
				if rec.NumRows() == 0 {
					return true
				}
				pb := convert(rec)
				select {
				case ahead <- pb:
					return true
				case <-done:
					// Consumer bailed out (insert error); stop converting so
					// this goroutine doesn't block forever on a full channel.
					return false
				}
			}
			for i, rec := range buffered {
				ok := send(rec)
				rec.Release()
				buffered[i] = nil
				if !ok {
					for _, r := range buffered[i+1:] {
						r.Release()
					}
					return
				}
			}
			for s.bindStream.Next() {
				if !send(s.bindStream.Record()) {
					return
				}
			}
		}()

		// Consumers: fan the stream across `workers` connections. Inserts are
		// order-independent; the round-trip latency (prepare/exec/commit) of one
		// worker overlaps another's, which is where the wall-clock win comes
		// from on WAN links. Errors close `done` (stops the producer) and the
		// remaining workers drain ahead via the closed channel.
		var (
			wg       sync.WaitGroup
			rowsIns  int64
			firstErr error
			errOnce  sync.Once
		)
		stopOnErr := func(err error) {
			errOnce.Do(func() {
				firstErr = err
				// Producer watches `done`; closing it here (not just via the
				// defer) stops conversion as soon as any worker fails.
				closeDone()
			})
		}
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				n, err := s.streamInsertWorker(ctx, insertSQL, ahead, done)
				atomic.AddInt64(&rowsIns, n)
				if err != nil {
					stopOnErr(err)
				}
			}()
		}
		wg.Wait()
		totalRows = atomic.LoadInt64(&rowsIns)
		if firstErr != nil {
			return totalRows, firstErr
		}
		if err := s.bindStream.Err(); err != nil {
			return totalRows, s.ErrorHelper.WrapIO(err, "bind stream error")
		}
	} else if s.bindParams != nil {
		n, err := s.insertBatchParallel(ctx, insertSQL, s.bindParams)
		if err != nil {
			return 0, err
		}
		totalRows = n
	}

	return totalRows, nil
}

// insertBatchParallel splits a record batch into chunks and inserts them
// in parallel across multiple database connections for higher throughput.
func (s *statementImpl) insertBatchParallel(ctx context.Context, insertSQL string, rec arrow.RecordBatch) (int64, error) {
	numRows := int(rec.NumRows())
	if numRows == 0 {
		return 0, nil
	}

	// Determine parallelism — default 1 worker (single big batch is fastest on small instances).
	// Configure via oracle.ingest_workers option or ORACLE_INGEST_WORKERS env var.
	numWorkers := s.cnxn.ingestWorkers
	if numWorkers <= 0 {
		numWorkers = 1
	}
	if w := os.Getenv("ORACLE_INGEST_WORKERS"); w != "" {
		fmt.Sscanf(w, "%d", &numWorkers)
	}
	// Autocommit off forces this to 1: the rows must stay inside the caller's
	// transaction, which only the connection's owned session is in.
	numWorkers = s.cnxn.ingestConcurrency(numWorkers)
	chunkSize := (numRows + numWorkers - 1) / numWorkers
	if chunkSize < 500 || numWorkers <= 1 {
		// For small batches, just use single connection
		return s.insertChunk(ctx, insertSQL, rec, 0, numRows)
	}

	type result struct {
		rows int64
		err  error
	}

	results := make(chan result, numWorkers)
	for w := 0; w < numWorkers; w++ {
		startRow := w * chunkSize
		endRow := startRow + chunkSize
		if endRow > numRows {
			endRow = numRows
		}
		if startRow >= numRows {
			break
		}

		go func(start, end int) {
			n, err := s.insertChunk(ctx, insertSQL, rec, start, end)
			results <- result{n, err}
		}(startRow, endRow)
	}

	var totalRows int64
	var firstErr error
	activeWorkers := numWorkers
	if numRows < chunkSize*numWorkers {
		activeWorkers = (numRows + chunkSize - 1) / chunkSize
	}
	for i := 0; i < activeWorkers; i++ {
		r := <-results
		totalRows += r.rows
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
	}

	if firstErr != nil {
		return totalRows, firstErr
	}
	return totalRows, nil
}

// insertChunk inserts a slice of rows [startRow, endRow) from a record batch.
// The session comes from newIngestSession: a fresh pooled one under autocommit
// (parallel inserts, and it avoids the ORA-00600 that long-running UDT sessions
// hit), or the connection's owned session when a caller transaction is in play.
func (s *statementImpl) insertChunk(ctx context.Context, insertSQL string, rec arrow.RecordBatch, startRow, endRow int) (int64, error) {
	numRows := endRow - startRow
	numCols := int(rec.NumCols())

	sess, err := s.cnxn.newIngestSession(ctx)
	if err != nil {
		return 0, s.ErrorHelper.WrapIO(err, "get connection failed")
	}
	defer sess.close()

	stmt, err := sess.exec.PrepareContext(ctx, insertSQL)
	if err != nil {
		return 0, s.ErrorHelper.WrapIO(err, "prepare insert failed")
	}
	defer stmt.Close()

	// Build columnar arrays for the chunk.
	// Geometry columns are converted client-side from WKB to SdoGeometry UDT objects.
	args := make([]interface{}, numCols)
	for col := 0; col < numCols; col++ {
		arr := rec.Column(col)
		if srid, ok := s.ingestGeomCols[col]; ok {
			args[col] = wkbColumnToSdoSliceRange(arr, startRow, endRow, srid)
		} else {
			args[col] = arrowColumnToSliceRange(arr, startRow, endRow, s.bindsAsClob(col))
		}
	}

	_, err = execArray(ctx, stmt, args, endRow-startRow)
	if err != nil {
		return 0, s.ErrorHelper.WrapIO(err, "batch insert failed")
	}

	if err := sess.commit(ctx); err != nil {
		return int64(numRows), s.ErrorHelper.WrapIO(err, "commit failed")
	}

	return int64(numRows), nil
}

// insertPreparedBatch inserts pre-converted column args into Oracle.
// Used by the pipeline path where column conversion happens in a background goroutine.
// preparedBatchT is one converted record batch queued for insert: columnar
// go-ora bind args plus the row count they carry.
type preparedBatchT struct {
	args []interface{}
	nrow int
}

// streamWorkerCount resolves the stream-consumer parallelism: the
// oracle.ingest_workers option / ORACLE_INGEST_WORKERS env when set, else 4.
// Stream inserts are round-trip-bound (prepare/exec/commit to a remote
// listener), so overlapping a few connections is a wall-clock win even on
// small instances; 4 keeps contention modest on 1-ECPU Autonomous tiers.
func (s *statementImpl) streamWorkerCount() int {
	n := s.cnxn.ingestWorkers
	if w := os.Getenv("ORACLE_INGEST_WORKERS"); w != "" {
		fmt.Sscanf(w, "%d", &n)
	}
	if n <= 0 {
		n = 4
	}
	// Autocommit off forces this to 1, see ingestConcurrency.
	return s.cnxn.ingestConcurrency(n)
}

// Stream-consumer tuning. Commits are batched (an Oracle round trip each) and
// sessions are recycled periodically: go-ora sessions that bind many
// SDO_GEOMETRY UDTs eventually hit ORA-00600 [kopi2_readlen083] (see
// insertChunk), so a worker re-connects every streamRecycleBatches batches
// instead of holding one session for the whole stream.
const (
	streamCommitEveryBatches = 16
	streamRecycleBatches     = 64
)

// streamInsertWorker drains prepared batches from `in` over one ingest
// session: prepare once per session, exec per batch, COMMIT every
// streamCommitEveryBatches, recycle the session every streamRecycleBatches.
//
// Returns the rows durably written by this worker. Under autocommit that means
// inserted and committed on its own pooled session. With autocommit off the
// session is the connection's own, commit is a no-op here, and the count is
// the rows added to the caller's transaction for the caller's Commit to keep.
func (s *statementImpl) streamInsertWorker(ctx context.Context, insertSQL string, in <-chan preparedBatchT, done <-chan struct{}) (int64, error) {
	var (
		sess         *ingestSession
		stmt         *sql.Stmt
		committed    int64
		uncommitted  int64
		sinceCommit  int
		sinceRecycle int
	)
	cleanup := func() {
		if stmt != nil {
			stmt.Close()
			stmt = nil
		}
		if sess != nil {
			sess.close()
			sess = nil
		}
	}
	defer cleanup()

	open := func() error {
		var err error
		if sess, err = s.cnxn.newIngestSession(ctx); err != nil {
			return s.ErrorHelper.WrapIO(err, "get connection failed")
		}
		if stmt, err = sess.exec.PrepareContext(ctx, insertSQL); err != nil {
			cleanup()
			return s.ErrorHelper.WrapIO(err, "prepare insert failed")
		}
		sinceRecycle = 0
		return nil
	}
	commit := func() error {
		if sess == nil || uncommitted == 0 {
			return nil
		}
		if err := sess.commit(ctx); err != nil {
			return s.ErrorHelper.WrapIO(err, "commit failed")
		}
		committed += uncommitted
		uncommitted = 0
		sinceCommit = 0
		return nil
	}

	for {
		var (
			pb preparedBatchT
			ok bool
		)
		select {
		case pb, ok = <-in:
		case <-done:
			// Another worker failed; commit what this worker already wrote
			// (matches the previous per-batch-commit semantics) and stop.
			err := commit()
			return committed, err
		}
		if !ok {
			err := commit()
			return committed, err
		}
		if sess == nil {
			if err := open(); err != nil {
				return committed, err
			}
		}
		if _, err := execArray(ctx, stmt, pb.args, pb.nrow); err != nil {
			commitErr := commit() // keep completed batches, like the old path
			_ = commitErr
			if isOraCode(err, 12899) {
				// A string wider than the scan window predicted slipped past
				// the auto sizing; tell the caller how to force CLOB.
				return committed, s.ErrorHelper.WrapIO(err,
					"batch insert failed (hint: value exceeds the column width chosen by the "+
						"auto type scan, so set %s for the affected column, or %s=clob)",
					OptionIngestClobColumns, OptionIngestStringType)
			}
			return committed, s.ErrorHelper.WrapIO(err, "batch insert failed")
		}
		uncommitted += int64(pb.nrow)
		sinceCommit++
		sinceRecycle++
		if sinceCommit >= streamCommitEveryBatches {
			if err := commit(); err != nil {
				return committed, err
			}
		}
		if sinceRecycle >= streamRecycleBatches && sess.pooled {
			// Recycling is only meaningful for a pooled session: a *fresh*
			// session is what dodges go-ora's ORA-00600 after many UDT binds.
			// The owned session cannot be swapped without leaving the caller's
			// transaction, so autocommit-off ingest keeps the one session.
			if err := commit(); err != nil {
				return committed, err
			}
			cleanup()
		}
	}
}

func (s *statementImpl) insertPreparedBatch(ctx context.Context, insertSQL string, args []interface{}, numRows int) (int64, error) {
	sess, err := s.cnxn.newIngestSession(ctx)
	if err != nil {
		return 0, s.ErrorHelper.WrapIO(err, "get connection failed")
	}
	defer sess.close()

	stmt, err := sess.exec.PrepareContext(ctx, insertSQL)
	if err != nil {
		return 0, s.ErrorHelper.WrapIO(err, "prepare insert failed")
	}
	defer stmt.Close()

	_, err = execArray(ctx, stmt, args, numRows)
	if err != nil {
		if isOraCode(err, 12899) {
			// A string wider than the scan window predicted slipped past the
			// auto sizing; tell the caller how to force CLOB.
			return 0, s.ErrorHelper.WrapIO(err,
				"batch insert failed (hint: value exceeds the column width chosen by the "+
					"auto type scan, so set %s for the affected column, or %s=clob)",
				OptionIngestClobColumns, OptionIngestStringType)
		}
		return 0, s.ErrorHelper.WrapIO(err, "batch insert failed")
	}

	if err := sess.commit(ctx); err != nil {
		return int64(numRows), s.ErrorHelper.WrapIO(err, "commit failed")
	}

	return int64(numRows), nil
}

// arrowColumnToSliceRange extracts a range [start, end) from an Arrow column
// into a Go slice for go-ora's bulk array bind.
//
// Values must be bound in the driver-native representation, not stringified:
// stringified timestamps hit Oracle's implicit TO_DATE with an NLS format mask
// and fail on the ISO-8601 'T' separator (ORA-01858); stringified booleans
// ("true") fail NUMBER(1) conversion (ORA-01722).
//
// asClob says whether string columns take the CLOB bind; it is fixed per
// column for the whole ingest by resolveStringBinds.
//
// Everything variable-length is deep-copied: Arrow Value() calls alias the
// underlying buffer, which is released when the next stream record arrives.
// Holding the alias past that point produces use-after-free corruption
// (manifests as garbled VARCHAR2 columns through Oracle's bulk array bind).
func arrowColumnToSliceRange(arr arrow.Array, start, end int, asClob bool) interface{} {
	numRows := end - start
	switch a := arr.(type) {
	case *array.Int64:
		return intColumnToSlice(a, start, end, func(v int64) int64 { return v })
	case *array.Int32:
		return intColumnToSlice(a, start, end, func(v int32) int64 { return int64(v) })
	case *array.Int16:
		return intColumnToSlice(a, start, end, func(v int16) int64 { return int64(v) })
	case *array.Int8:
		return intColumnToSlice(a, start, end, func(v int8) int64 { return int64(v) })
	case *array.Uint32:
		return intColumnToSlice(a, start, end, func(v uint32) int64 { return int64(v) })
	case *array.Uint16:
		return intColumnToSlice(a, start, end, func(v uint16) int64 { return int64(v) })
	case *array.Uint8:
		return intColumnToSlice(a, start, end, func(v uint8) int64 { return int64(v) })
	case *array.Uint64:
		// uint64 can exceed int64; bind the decimal text and let NUMBER(20)
		// take it exactly.
		vals := make([]sql.NullString, numRows)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = sql.NullString{String: strconv.FormatUint(a.Value(i), 10), Valid: true}
			}
		}
		return vals
	case *array.Float64:
		vals := make([]sql.NullFloat64, numRows)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = sql.NullFloat64{Float64: a.Value(i), Valid: true}
			}
		}
		return vals
	case *array.Float32:
		vals := make([]sql.NullFloat64, numRows)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = sql.NullFloat64{Float64: float64(a.Value(i)), Valid: true}
			}
		}
		return vals
	case *array.Boolean:
		// NUMBER(1) column, bound as explicit 0/1 (stringified "true"/"false"
		// would fail the implicit NUMBER conversion with ORA-01722).
		vals := make([]sql.NullInt64, numRows)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				var v int64
				if a.Value(i) {
					v = 1
				}
				vals[i-start] = sql.NullInt64{Int64: v, Valid: true}
			}
		}
		return vals
	case *array.String:
		return stringColumnToSlice(a, start, end, asClob)
	case *array.LargeString:
		return stringColumnToSlice(a, start, end, asClob)
	case *array.Binary:
		return binaryColumnToSlice(a, start, end, asClob)
	case *array.LargeBinary:
		return binaryColumnToSlice(a, start, end, asClob)
	case *array.FixedSizeBinary:
		return binaryColumnToSlice(a, start, end, asClob)
	case *array.BinaryView:
		return binaryColumnToSlice(a, start, end, asClob)
	case *array.Decimal128:
		vals := make([]sql.NullString, numRows)
		scale := a.DataType().(*arrow.Decimal128Type).Scale
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = sql.NullString{String: a.Value(i).ToString(scale), Valid: true}
			}
		}
		return vals
	case *array.Decimal256:
		vals := make([]sql.NullString, numRows)
		scale := a.DataType().(*arrow.Decimal256Type).Scale
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = sql.NullString{String: a.Value(i).ToString(scale), Valid: true}
			}
		}
		return vals
	case *array.Timestamp:
		return timestampColumnToSlice(a, start, end)
	case *array.Date32:
		vals := make([]go_ora.NullTimeStamp, numRows)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = go_ora.NullTimeStamp{TimeStamp: go_ora.TimeStamp(a.Value(i).ToTime()), Valid: true}
			}
		}
		return vals
	case *array.Date64:
		vals := make([]go_ora.NullTimeStamp, numRows)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = go_ora.NullTimeStamp{TimeStamp: go_ora.TimeStamp(a.Value(i).ToTime()), Valid: true}
			}
		}
		return vals
	case *array.StringView:
		return stringColumnToSlice(a, start, end, asClob)
	default:
		// Remaining types (Time32/64, Duration, Interval, Dictionary, nested…)
		// have no natural Oracle mapping; bind their textual form into the
		// VARCHAR2 fallback column. Clone defensively: some ValueStr
		// implementations (Dictionary, RunEndEncoded) alias the Arrow buffer.
		vals := make([]sql.NullString, numRows)
		for i := start; i < end; i++ {
			if !arr.IsNull(i) {
				vals[i-start] = sql.NullString{String: strings.Clone(arr.ValueStr(i)), Valid: true}
			}
		}
		return vals
	}
}

// bindsAsClob reports whether column col binds as a CLOB. Defaults to false
// when the decision was never resolved, which is the plain-string bind the
// non-ingest paths have always used.
func (s *statementImpl) bindsAsClob(col int) bool {
	return col < len(s.clobBinds) && s.clobBinds[col]
}

// resolveStringBinds fixes, per column, how string values are bound for the
// whole ingest.
//
// This cannot be decided per batch. The INSERT is prepared once and reused for
// every batch, and switching a parameter from a plain string bind to a
// go_ora.Clob bind between two executes of one prepared statement
// desynchronizes go-ora's TTC stream: the next execute fails with ORA-03106
// "fatal two-task communication protocol error", or ORA-03146 "invalid buffer
// length for TTC field". Varying the Clob width across executes is fine, and
// so is going from Clob to string; only string to Clob breaks. Picking from
// each batch's widest value, as this used to, flips exactly that way the first
// time a narrow batch is followed by a wide one.
//
// The destination column type is the input, since that is what the bind has to
// satisfy. For a table this ingest just created, colTypes is that type. Append
// and create-append can land on a table that already exists and disagrees, so
// there the table itself is asked. Neither available means no information, and
// the conservative answer is the CLOB bind: Oracle accepts it into VARCHAR2
// and CLOB columns alike and at any width, where a string bind fails outright
// on a value past the negotiated varchar limit.
func (s *statementImpl) resolveStringBinds(ctx context.Context, tableName string, schema *arrow.Schema, mode string, colTypes []string) []bool {
	binds := make([]bool, schema.NumFields())

	stringCols := false
	for _, f := range schema.Fields() {
		switch f.Type.ID() {
		case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW,
			arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
			// binary columns too: one bound for a BLOB column goes in as a
			// go_ora.Blob (arrowColumnToSliceRange)
			stringCols = true
		}
	}
	if !stringCols {
		return binds
	}

	// A table created moments ago by this ingest is exactly colTypes, so skip
	// the round trip. The other modes may be appending to someone else's
	// table, where only the server knows.
	if mode == adbc.OptionValueIngestModeCreate || mode == adbc.OptionValueIngestModeReplace {
		if lobs := clobBindsFromColTypes(schema, colTypes); lobs != nil {
			return lobs
		}
	}

	if lobs, err := s.lobColumnsOfTable(ctx, tableName, schema); err == nil {
		return lobs
	} else if lobs := clobBindsFromColTypes(schema, colTypes); lobs != nil {
		return lobs
	} else {
		// No information. Bind every string column as a CLOB: slower, because
		// Oracle allocates a temporary LOB per row, but it cannot corrupt the
		// stream or reject a wide value.
		if s.cnxn.Logger != nil {
			s.cnxn.Logger.Warn("oracle ingest: cannot read destination column types, binding all string columns as CLOB",
				"table", tableName, "error", err)
		}
		for i, f := range schema.Fields() {
			switch f.Type.ID() {
			case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
				binds[i] = true
			}
		}
		return binds
	}
}

// clobBindsFromColTypes reads the decision out of resolved DDL types, or
// returns nil when there are none to read.
func clobBindsFromColTypes(schema *arrow.Schema, colTypes []string) []bool {
	if len(colTypes) != schema.NumFields() {
		return nil
	}
	binds := make([]bool, schema.NumFields())
	for i := range colTypes {
		binds[i] = lobBind(schema.Field(i).Type, colTypes[i])
	}
	return binds
}

// isClobDDLType reports whether a resolved DDL type is a character LOB.
func isClobDDLType(ddl string) bool {
	switch strings.ToUpper(strings.TrimSpace(ddl)) {
	case "CLOB", "NCLOB":
		return true
	}
	return false
}

// lobBind reports whether a column of Arrow type dt bound for an Oracle
// column of type ddl goes in as a LOB: a string for a CLOB/NCLOB (a
// go_ora.Clob), binary data for a BLOB (a go_ora.Blob). Bound as plain bytes,
// a BLOB value past the RAW limit travels as a LONG RAW, and Oracle refuses
// it beside another long value (ORA-01461).
func lobBind(dt arrow.DataType, ddl string) bool {
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return isClobDDLType(ddl)
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
		return strings.ToUpper(strings.TrimSpace(ddl)) == "BLOB"
	}
	return false
}

// lobColumnsOfTable asks the data dictionary which of the target table's
// columns are character LOBs, matched by name to the bound schema.
//
// ALL_TAB_COLUMNS, not a no-row SELECT on the table: go-ora's wire type names
// do not survive that round trip reliably. A CLOB comes back as LongVarChar, a
// VARCHAR2 as NCHAR and an NCHAR as CHAR, so a decision made on them reads a
// CLOB column as plain text and binds strings into it, which then fails on the
// first wide value. DATA_TYPE is the declared type and says CLOB.
func (s *statementImpl) lobColumnsOfTable(ctx context.Context, tableName string, schema *arrow.Schema) ([]bool, error) {
	sess, err := s.cnxn.session(ctx)
	if err != nil {
		return nil, err
	}

	owner, table := splitQualifiedName(tableName)
	const query = `SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS
		WHERE OWNER = NVL(:1, SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) AND TABLE_NAME = :2`
	var ownerArg interface{}
	if owner != "" {
		ownerArg = owner
	}
	rows, err := sess.QueryContext(ctx, query, ownerArg, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	lob := make(map[string]string) // column -> declared type
	for rows.Next() {
		var name, dataType string
		if err := rows.Scan(&name, &dataType); err != nil {
			return nil, err
		}
		lob[strings.ToUpper(name)] = dataType
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(lob) == 0 {
		return nil, fmt.Errorf("no columns found for %s", tableName)
	}

	binds := make([]bool, schema.NumFields())
	for i, f := range schema.Fields() {
		binds[i] = lobBind(f.Type, lob[strings.ToUpper(f.Name)])
	}
	return binds, nil
}

// splitQualifiedName separates an optional SCHEMA. prefix from a table name.
// Returns an empty owner for an unqualified name, which the caller resolves to
// the session's current schema.
func splitQualifiedName(name string) (owner, table string) {
	name = strings.TrimSpace(name)
	if i := strings.LastIndex(name, "."); i >= 0 {
		return strings.Trim(name[:i], `"`), strings.Trim(name[i+1:], `"`)
	}
	return "", strings.Trim(name, `"`)
}

// intColumnToSlice converts any fixed-width integer Arrow array to NullInt64s.
func intColumnToSlice[T int64 | int32 | int16 | int8 | uint32 | uint16 | uint8](
	a interface {
		arrow.Array
		Value(int) T
	}, start, end int, conv func(T) int64,
) []sql.NullInt64 {
	vals := make([]sql.NullInt64, end-start)
	for i := start; i < end; i++ {
		if !a.IsNull(i) {
			vals[i-start] = sql.NullInt64{Int64: conv(a.Value(i)), Valid: true}
		}
	}
	return vals
}

// stringColumnToSlice deep-copies string values (see aliasing note above).
//
// asClob picks the bind, and it comes from the destination column type rather
// than from this batch's data: see resolveStringBinds for why the choice has
// to be the same for every batch of an ingest.
//
// A CLOB column needs the go_ora.Clob bind because a plain string bind rides
// go-ora's negotiated varchar limit (4000 bytes on MAX_STRING_SIZE=STANDARD
// servers, 32767 on EXTENDED) and a wider value fails at bind time. A VARCHAR2
// column never needs it: resolveColumnTypes clamps the declared width to that
// same limit, so any value the column can hold is a value the string bind can
// carry, and the string bind is the cheap array one.
func stringColumnToSlice(a interface {
	arrow.Array
	Value(int) string
}, start, end int, asClob bool) interface{} {
	if asClob {
		lobs := make([]go_ora.Clob, end-start)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				lobs[i-start] = go_ora.Clob{String: strings.Clone(a.Value(i)), Valid: true}
			}
		}
		return lobs
	}
	vals := make([]sql.NullString, end-start)
	for i := start; i < end; i++ {
		if !a.IsNull(i) {
			vals[i-start] = sql.NullString{String: strings.Clone(a.Value(i)), Valid: true}
		}
	}
	return vals
}

// binaryColumnToSlice deep-copies binary values (see aliasing note above).
func binaryColumnToSlice(a interface {
	arrow.Array
	Value(int) []byte
}, start, end int, asBlob bool) interface{} {
	if asBlob {
		// A BLOB column: a go_ora.Blob per row (NULL: Valid false).
		lobs := make([]go_ora.Blob, end-start)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				src := a.Value(i)
				dst := make([]byte, len(src))
				copy(dst, src)
				lobs[i-start] = go_ora.Blob{Data: dst, Valid: true}
			}
		}
		return lobs
	}
	vals := make([][]byte, end-start)
	for i := start; i < end; i++ {
		if !a.IsNull(i) {
			src := a.Value(i)
			dst := make([]byte, len(src))
			copy(dst, src)
			vals[i-start] = dst
		}
	}
	return vals
}

// timestampColumnToSlice binds timestamps in the driver's native temporal form.
//
// Naive (no time zone) columns bind as go_ora.TimeStamp — wire-level Oracle
// TIMESTAMP carrying the wall-clock fields — so the value lands verbatim with
// no session-time-zone conversion. Zone-aware columns bind as time.Time, which
// go-ora encodes as TIMESTAMP WITH TIME ZONE, preserving the absolute instant.
func timestampColumnToSlice(a *array.Timestamp, start, end int) interface{} {
	dt := a.DataType().(*arrow.TimestampType)
	toTime, err := dt.GetToTimeFunc()
	if err != nil {
		// Unloadable time zone — fall back to UTC-based conversion.
		unit := dt.Unit
		toTime = func(ts arrow.Timestamp) time.Time { return ts.ToTime(unit) }
	}

	if dt.TimeZone == "" {
		vals := make([]go_ora.NullTimeStamp, end-start)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = go_ora.NullTimeStamp{TimeStamp: go_ora.TimeStamp(toTime(a.Value(i))), Valid: true}
			}
		}
		return vals
	}

	vals := make([]sql.NullTime, end-start)
	for i := start; i < end; i++ {
		if !a.IsNull(i) {
			vals[i-start] = sql.NullTime{Time: toTime(a.Value(i)), Valid: true}
		}
	}
	return vals
}

// wkbColumnToSdoSliceRange converts a range [start, end) of WKB binary values to SdoGeometry UDT objects.
func wkbColumnToSdoSliceRange(arr arrow.Array, start, end int, srid int64) interface{} {
	binArr, ok := arr.(*array.Binary)
	if !ok {
		return make([]*go_ora.Object, end-start)
	}

	vals := make([]*go_ora.Object, end-start)
	for i := start; i < end; i++ {
		if binArr.IsNull(i) {
			vals[i-start] = nil
			continue
		}
		wkb := binArr.Value(i)
		geom, err := WKBToSdo(wkb, srid)
		if err != nil {
			vals[i-start] = nil
			continue
		}
		vals[i-start] = go_ora.NewObject("MDSYS", "SDO_GEOMETRY", *geom)
	}
	return vals
}

// prepareIngestTable runs the ingest DDL on the connection's session.
//
// Oracle commits implicitly around DDL, so with autocommit off a CREATE or
// DROP here also commits whatever the caller had pending. That is Oracle's
// behaviour rather than this driver's, and append mode is the way to avoid it.
func (s *statementImpl) prepareIngestTable(ctx context.Context, tableName string, schema *arrow.Schema, mode string, colTypes []string) error {
	if mode == adbc.OptionValueIngestModeAppend {
		// Table must exist, so there is no DDL and no session is needed.
		return nil
	}
	sess, err := s.cnxn.session(ctx)
	if err != nil {
		return err
	}

	switch mode {
	case adbc.OptionValueIngestModeCreate:
		// Create table — fail if exists
		ddl := buildCreateTableDDL(tableName, schema, colTypes)
		if _, err := sess.ExecContext(ctx, ddl); err != nil {
			return s.ErrorHelper.WrapIO(err, "create table failed")
		}
	case adbc.OptionValueIngestModeReplace:
		// Drop and recreate. A missing table (ORA-00942) is fine; any other
		// drop failure would surface as a confusing ORA-00955 on the CREATE,
		// so report it directly.
		if _, err := sess.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s PURGE", tableName)); err != nil && !isOraCode(err, 942) {
			return s.ErrorHelper.WrapIO(err, "drop table failed")
		}
		ddl := buildCreateTableDDL(tableName, schema, colTypes)
		if _, err := sess.ExecContext(ctx, ddl); err != nil {
			return s.ErrorHelper.WrapIO(err, "create table failed")
		}
	case adbc.OptionValueIngestModeCreateAppend:
		// Create if not exists, append if exists. Only ORA-00955 ("name is
		// already used") means "exists" — anything else is a real failure.
		ddl := buildCreateTableDDL(tableName, schema, colTypes)
		if _, err := sess.ExecContext(ctx, ddl); err != nil && !isOraCode(err, 955) {
			return s.ErrorHelper.WrapIO(err, "create table failed")
		}
	default:
		return s.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "unknown ingest mode: %s", mode)
	}
	return nil
}

// isGeometryColumn checks if an Arrow field is a geometry column.
// Detects via geoarrow.wkb extension metadata (from Arrow/ADBC) or
// GeoParquet column naming convention (GEOM, GEOMETRY, geom, geometry).
func isGeometryColumn(f arrow.Field) bool {
	if f.Type.ID() != arrow.BINARY {
		return false
	}
	// Check geoarrow.wkb extension metadata
	extName, ok := f.Metadata.GetValue("ARROW:extension:name")
	if ok && extName == "geoarrow.wkb" {
		return true
	}
	// Check common geometry column names (GeoParquet convention)
	name := strings.ToUpper(f.Name)
	return name == "GEOM" || name == "GEOMETRY" || name == "GEOM_WKB" || name == "WKB_GEOMETRY"
}

// extractSRIDFromField extracts the SRID from geoarrow extension metadata.
//
// DuckDB exposes CRS metadata as the compact GeoArrow form
//
//	{"crs":"EPSG:3857"}
//
// while other producers send PROJJSON
//
//	{"crs":{"id":{"authority":"EPSG","code":3857}}}
//
// Both are accepted. Returns 4326 when no usable SRID is found so that
// callers always have a concrete value to bind into SDO_GEOMETRY.
func extractSRIDFromField(f arrow.Field) int64 {
	meta, ok := f.Metadata.GetValue("ARROW:extension:metadata")
	if !ok || meta == "" || meta == "{}" {
		return 4326
	}
	var holder struct {
		CRS json.RawMessage `json:"crs"`
	}
	if err := json.Unmarshal([]byte(meta), &holder); err != nil || len(holder.CRS) == 0 || string(holder.CRS) == "null" {
		return 4326
	}
	// Compact GeoArrow CRS: a JSON string like "EPSG:3857".
	var crsStr string
	if err := json.Unmarshal(holder.CRS, &crsStr); err == nil {
		if strings.HasPrefix(strings.ToUpper(crsStr), "EPSG:") {
			if code, err := strconv.ParseInt(crsStr[5:], 10, 64); err == nil && code != 0 {
				return code
			}
		}
		return 4326
	}
	// PROJJSON: {"id":{"authority":"EPSG","code":N}}.
	var crs struct {
		ID struct {
			Authority string `json:"authority"`
			Code      int64  `json:"code"`
		} `json:"id"`
	}
	if err := json.Unmarshal(holder.CRS, &crs); err == nil {
		if strings.EqualFold(crs.ID.Authority, "EPSG") && crs.ID.Code != 0 {
			return crs.ID.Code
		}
	}
	return 4326
}

// maxInlineStringBytes is the VARCHAR2 byte cap with MAX_STRING_SIZE=STANDARD.
// String values wider than this must be stored as CLOB on a STANDARD server.
const maxInlineStringBytes = 4000

// maxExtendedStringBytes is the VARCHAR2 byte cap with MAX_STRING_SIZE=EXTENDED
// (the Autonomous DB default). Values up to this size bind as plain VARCHAR2
// (fast batch array bind) instead of per-row temporary-LOB CLOB binds.
const maxExtendedStringBytes = 32767

// defaultTypeScanLimitBytes bounds how much of a bound stream executeBulkIngest
// buffers while sizing string columns in "auto" mode (~64 MiB keeps a couple of
// wide DuckDB batches without holding the whole table in memory).
const defaultTypeScanLimitBytes = 64 << 20

// needsStringScan reports whether the schema has string columns whose DDL type
// depends on observed data widths (i.e. "auto" mode with nothing forcing CLOB).
func (s *statementImpl) needsStringScan(schema *arrow.Schema) bool {
	if s.ingestStringType == "varchar2" || s.ingestStringType == "clob" {
		return false
	}
	for _, f := range schema.Fields() {
		id := f.Type.ID()
		if (id == arrow.STRING || id == arrow.STRING_VIEW || id == arrow.LARGE_STRING) &&
			!s.ingestClobColumns[strings.ToUpper(f.Name)] {
			return true
		}
	}
	return false
}

// scanStringWidths updates widths[i] with the maximum value byte-length seen
// in rec for each string column. len(Value(i)) reads the offsets only — no
// value bytes are copied.
func scanStringWidths(rec arrow.RecordBatch, widths []int) {
	for col := 0; col < int(rec.NumCols()) && col < len(widths); col++ {
		switch a := rec.Column(col).(type) {
		case *array.String:
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					if n := len(a.Value(i)); n > widths[col] {
						widths[col] = n
					}
				}
			}
		case *array.LargeString:
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					if n := len(a.Value(i)); n > widths[col] {
						widths[col] = n
					}
				}
			}
		case *array.StringView:
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					if n := len(a.Value(i)); n > widths[col] {
						widths[col] = n
					}
				}
			}
		}
	}
}

// recordSizeEstimate approximates the in-memory size of a record batch by
// summing its Arrow buffer lengths, recursing into child data (list/struct
// values, dictionaries) — top-level buffers alone are just validity+offsets
// for nested types and would undercount by orders of magnitude.
func recordSizeEstimate(rec arrow.RecordBatch) int64 {
	var total int64
	for col := 0; col < int(rec.NumCols()); col++ {
		total += arrayDataSize(rec.Column(col).Data())
	}
	return total
}

func arrayDataSize(data arrow.ArrayData) int64 {
	// Dictionary() and children can surface a typed-nil *array.Data (seen on
	// records imported through the C data interface) — the interface itself
	// is non-nil, so check the concrete pointer too.
	if data == nil {
		return 0
	}
	if d, ok := data.(*array.Data); ok && d == nil {
		return 0
	}
	var total int64
	for _, buf := range data.Buffers() {
		if buf != nil {
			total += int64(buf.Len())
		}
	}
	for _, child := range data.Children() {
		total += arrayDataSize(child)
	}
	total += arrayDataSize(data.Dictionary())
	return total
}

// resolveColumnTypes decides the Oracle DDL type for every column.
//
// String columns are the interesting case: VARCHAR2 caps at 4000 bytes, so
//   - stringType "clob" / "varchar2" forces that type for all string columns;
//   - clobColumns (uppercased names) forces CLOB for specific columns;
//   - otherwise ("auto") a column becomes CLOB when the scanned data contains
//     a value wider than 4000 bytes (widths[i], from scanStringWidths). With
//     no scan data at all, LargeString columns fall back to CLOB — producers
//     use the large layout precisely when values are big — while regular
//     strings stay VARCHAR2.
//
// widths may be nil when no scan ran (forced types, or scan disabled).
// inlineLimit is the VARCHAR2 byte cap for this server (maxInlineStringBytes on
// STANDARD, maxExtendedStringBytes on EXTENDED); columns wider than it become
// CLOB. Keeping the declared width within that cap is also what lets the bind
// follow the column type: any value a VARCHAR2 column accepts is one the plain
// string bind can carry. See resolveStringBinds.
// varcharDeclWidth picks a VARCHAR2 declared width from the observed max byte
// width of a column's data. It adds headroom (values beyond the type-scan
// window aren't measured — same trust the CLOB threshold already relies on)
// and rounds up to a 64-byte bucket for stability across similar batches,
// clamped to [64, inlineLimit]. Kept tight because go-ora's read buffer scales
// with the declared width, not the actual value length.
func varcharDeclWidth(observed, inlineLimit int) int {
	if observed < 1 {
		observed = 1
	}
	n := ((observed*2 + 16 + 63) / 64) * 64 // 2x headroom, rounded to a 64-byte bucket
	// A column whose data fits inline (<= 4000 bytes) must stay declared inline
	// even after headroom — VARCHAR2 > 4000 stores out-of-line on EXTENDED
	// servers and reads slowly, the very thing this sizing avoids.
	if observed <= maxInlineStringBytes && n > maxInlineStringBytes {
		n = maxInlineStringBytes
	}
	if n < 64 {
		n = 64
	}
	if n > inlineLimit {
		n = inlineLimit
	}
	return n
}

func resolveColumnTypes(schema *arrow.Schema, widths []int, stringType string, clobColumns map[string]bool, inlineLimit int) []string {
	types := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		if isGeometryColumn(f) {
			types[i] = "MDSYS.SDO_GEOMETRY"
			continue
		}
		id := f.Type.ID()
		if id == arrow.STRING || id == arrow.LARGE_STRING || id == arrow.STRING_VIEW {
			switch {
			case clobColumns[strings.ToUpper(f.Name)] || stringType == "clob":
				types[i] = "CLOB"
			case stringType == "varchar2":
				types[i] = fmt.Sprintf("VARCHAR2(%d)", inlineLimit)
			case widths != nil && i < len(widths) && widths[i] > inlineLimit:
				types[i] = "CLOB"
			case widths != nil && i < len(widths):
				// Size to the observed max width, not the server inline limit.
				// go-ora array-fetch defines a per-row buffer sized to the
				// DECLARED VARCHAR2 width, so declaring VARCHAR2(32767) for a
				// column of short strings makes reads O(declared width)/row —
				// a full-table read of such a column is 30x+ slower and can
				// exceed query timeouts (Oracle-as-source transfers, exports).
				// Reads scale linearly with the declared width, so keep it tight.
				types[i] = fmt.Sprintf("VARCHAR2(%d)", varcharDeclWidth(widths[i], inlineLimit))
			case id == arrow.LARGE_STRING:
				types[i] = "CLOB"
			default:
				types[i] = fmt.Sprintf("VARCHAR2(%d)", inlineLimit)
			}
			continue
		}
		types[i] = arrowToOracleType(f.Type)
	}
	return types
}

// buildCreateTableDDL renders CREATE TABLE DDL. colTypes comes from
// resolveColumnTypes; pass nil to use data-independent defaults.
func buildCreateTableDDL(tableName string, schema *arrow.Schema, colTypes []string) string {
	if colTypes == nil {
		colTypes = resolveColumnTypes(schema, nil, "", nil, maxInlineStringBytes)
	}
	var cols []string
	for i, f := range schema.Fields() {
		nullable := ""
		// Oracle stores '' as NULL, so a NOT NULL string column would reject
		// legitimate empty strings — skip the constraint for string types.
		id := f.Type.ID()
		isString := id == arrow.STRING || id == arrow.LARGE_STRING || id == arrow.STRING_VIEW
		if !f.Nullable && !isString {
			nullable = " NOT NULL"
		}
		cols = append(cols, fmt.Sprintf(`"%s" %s%s`, strings.ToUpper(f.Name), colTypes[i], nullable))
	}
	return fmt.Sprintf("CREATE TABLE %s (%s) NOLOGGING", tableName, strings.Join(cols, ", "))
}

// arrowToOracleType maps a non-string, non-geometry Arrow type to Oracle DDL.
// String columns are resolved by resolveColumnTypes, which sizes them from
// the data; the mapping here must stay bind-compatible with
// arrowColumnToSliceRange.
func arrowToOracleType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return "NUMBER(19)"
	case arrow.UINT8, arrow.UINT16, arrow.UINT32:
		return "NUMBER(19)"
	case arrow.UINT64:
		// uint64 max is 1.8e19 — one digit more than NUMBER(19) holds.
		return "NUMBER(20)"
	case arrow.FLOAT16, arrow.FLOAT32:
		return "BINARY_FLOAT"
	case arrow.FLOAT64:
		return "BINARY_DOUBLE"
	case arrow.DECIMAL128, arrow.DECIMAL256:
		if dec, ok := dt.(arrow.DecimalType); ok {
			prec, scale := dec.GetPrecision(), dec.GetScale()
			// Oracle NUMBER(p,s) caps p at 38. Clamping would reject values
			// that legitimately fit the Arrow type (ORA-01438), so fall back
			// to unconstrained NUMBER for anything wider.
			if prec >= 1 && prec <= 38 && scale >= 0 && scale <= prec {
				return fmt.Sprintf("NUMBER(%d,%d)", prec, scale)
			}
		}
		return "NUMBER"
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return fmt.Sprintf("VARCHAR2(%d)", maxInlineStringBytes)
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY, arrow.BINARY_VIEW:
		return "BLOB"
	case arrow.BOOL:
		return "NUMBER(1)"
	case arrow.TIMESTAMP:
		ts, ok := dt.(*arrow.TimestampType)
		if !ok {
			return "TIMESTAMP(6)"
		}
		prec := 6
		if ts.Unit == arrow.Nanosecond {
			prec = 9
		}
		if ts.TimeZone != "" {
			return fmt.Sprintf("TIMESTAMP(%d) WITH TIME ZONE", prec)
		}
		return fmt.Sprintf("TIMESTAMP(%d)", prec)
	case arrow.DATE32, arrow.DATE64:
		return "DATE"
	default:
		return fmt.Sprintf("VARCHAR2(%d)", maxInlineStringBytes)
	}
}

// --- Unimplemented ---

func (s *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecuteSchema")
}

func (s *statementImpl) SetSubstraitPlan(plan []byte) error {
	return s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "SetSubstraitPlan")
}

func (s *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	return nil, s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "GetParameterSchema")
}

func (s *statementImpl) Close() error {
	s.closePrepared()
	if s.bindParams != nil {
		s.bindParams.Release()
		s.bindParams = nil
	}
	if s.bindStream != nil {
		s.bindStream.Release()
		s.bindStream = nil
	}
	return nil
}

func (s *statementImpl) closePrepared() {
	if s.prepared != nil {
		s.prepared.Close()
		s.prepared = nil
	}
	s.preparedTx = nil
	s.preparedBinds = nil
}

// =============================================================================
// oracleRecordReader — implements driverbase.RecordReaderImpl
// =============================================================================

// outputKind says what an emitted Arrow column carries.
type outputKind uint8

const (
	outputValue outputKind = iota
	outputGeometry
	outputZone
	// outputInferredDecimal is a NUMBER whose type came from sampling rather
	// than from a declaration, so a later value may not fit it and the
	// overflow policy applies.
	outputInferredDecimal
	// outputTimestampOffset is a TIMESTAMP WITH TIME ZONE carried as the
	// canonical arrow.timestamp_with_offset extension, which holds the UTC
	// instant and the original offset in one column.
	outputTimestampOffset
)

// outputField links one emitted Arrow column to the Oracle column it reads.
type outputField struct {
	col  int
	kind outputKind
}

type oracleRecordReader struct {
	rows     *sql.Rows
	colTypes []*sql.ColumnType
	outputs  []outputField

	// batchRows is how many rows one AppendRows call fills. It matches the
	// batch row limit given to driverbase, so a full batch costs one call
	// rather than one per row.
	batchRows   int64
	scanDest    []interface{}
	scanVals    []interface{}
	geomIndices []int

	// peeked holds rows read before the schema could be declared, and
	// peekPos how many of them AppendRows has replayed. Both the geometry
	// SRID and the inferred NUMBER types need to look at data before naming
	// a type, and those rows still have to reach the consumer.
	peeked  [][]interface{}
	peekPos int

	// policy is the connection's resolved type-mapping configuration.
	policy typePolicy

	// overflowErr is set when a value did not fit an inferred decimal and
	// the policy is to fail. AppendRows returns it rather than appending.
	overflowErr error

	// overflowLogged keeps the warning for a nulled value to one per read
	// instead of one per value.
	overflowLogged bool

	// logger is used for the overflow warning. May be nil.
	logger *slog.Logger

	// forcedTypes pins the Arrow type of a column that would otherwise be
	// inferred from data, keyed by Oracle column index. A partitioned read
	// sets it: each slice sees only part of the data, so slices left to infer
	// would disagree with each other and with the schema ExecutePartitions
	// returned.
	forcedTypes map[int]arrow.DataType

	// inferredTypes records what inference decided, so a caller that built
	// this reader to sample the data can pin the same types elsewhere.
	inferredTypes map[int]arrow.DataType
}

// peekRows reads up to n more rows into the peek buffer, stopping early at end
// of data. Returns how many rows the buffer holds.
func (r *oracleRecordReader) peekRows(n int) int {
	width := len(r.colTypes)
	for len(r.peeked) < n {
		if !r.rows.Next() {
			break
		}
		dest := make([]interface{}, width)
		vals := make([]interface{}, width)
		for i := range dest {
			dest[i] = &vals[i]
		}
		if err := r.rows.Scan(dest...); err != nil {
			break
		}
		r.peeked = append(r.peeked, vals)
	}
	return len(r.peeked)
}

func (r *oracleRecordReader) NextResultSet(ctx context.Context, rec arrow.RecordBatch, rowIdx int) (*arrow.Schema, error) {
	colTypes, err := r.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	r.colTypes = colTypes

	r.geomIndices = nil

	// Detect geometry columns. go-ora reports SDO_GEOMETRY natively for the
	// MDSYS UDT registered in databaseImpl.getPool; XMLTYPE is also accepted
	// for the SDO_UTIL.TO_GMLGEOMETRY-style server-side conversion path.
	for i, ct := range colTypes {
		dbType := strings.ToUpper(ct.DatabaseTypeName())
		if dbType == "SDO_GEOMETRY" || dbType == "MDSYS.SDO_GEOMETRY" || dbType == "XMLTYPE" {
			r.geomIndices = append(r.geomIndices, i)
		}
	}

	// Decide which NUMBER columns have no declared precision and scale, so a
	// type has to come from the data instead.
	inferring := make(map[int]bool)
	if n := r.policy.numberScanRows; n > 0 {
		for i, ct := range colTypes {
			if strings.ToUpper(ct.DatabaseTypeName()) != "NUMBER" {
				continue
			}
			// A pinned type is not re-derived. Sampling this slice could
			// reach a different answer, and the promised schema wins.
			if _, pinned := r.forcedTypes[i]; pinned {
				continue
			}
			p, s, ok := ct.DecimalSize()
			if numberNeedsInference(p, s, ok, r.policy) {
				inferring[i] = true
			}
		}
	}

	// Both the geometry SRID and the inferred types need to see data before a
	// type can be named, so take one sample that serves both. The rows are
	// buffered and replayed, so nothing is consumed from the caller.
	sample := 0
	if len(r.geomIndices) > 0 {
		sample = 1
	}
	if len(inferring) > 0 && r.policy.numberScanRows > sample {
		sample = r.policy.numberScanRows
	}
	if sample > 0 {
		r.peekRows(sample)
	}

	var srid int64
	if len(r.geomIndices) > 0 {
		srid = r.peekSRID()
	}

	// Fold the sample into digit counts per inferred column.
	inferred := make(map[int]arrow.DataType, len(inferring))
	if len(inferring) > 0 {
		digits := make(map[int]*numberDigits, len(inferring))
		for i := range inferring {
			digits[i] = &numberDigits{}
		}
		for _, vals := range r.peeked {
			for i, d := range digits {
				switch v := vals[i].(type) {
				case string:
					d.observe(v)
				case []byte:
					d.observe(string(v))
				case nil:
					// A null says nothing about the scale.
				default:
					// Not the decimal text this relies on.
					d.unparseable = true
				}
			}
		}
		for i, d := range digits {
			if dt := inferredNumberType(*d); dt != nil {
				inferred[i] = dt
			}
		}
	}
	for i, dt := range r.forcedTypes {
		if i >= 0 && i < len(colTypes) {
			inferred[i] = dt
		}
	}
	r.inferredTypes = inferred

	// Build the emitted schema. It is not always one Arrow column per Oracle
	// column: with oracle.tz_offset_column on, a TIMESTAMP WITH TIME ZONE
	// contributes a second column carrying the zone it arrived in. r.outputs
	// records what each emitted column reads, so AppendRows does not have to
	// rediscover it per row.
	geomCols := make(map[int]struct{}, len(r.geomIndices))
	for _, idx := range r.geomIndices {
		geomCols[idx] = struct{}{}
	}

	fields := make([]arrow.Field, 0, len(colTypes))
	r.outputs = make([]outputField, 0, len(colTypes))
	for i, ct := range colTypes {
		nullable, _ := ct.Nullable()

		if _, ok := geomCols[i]; ok {
			fields = append(fields, GeoArrowWKBField(ct.Name(), srid, nullable))
			r.outputs = append(r.outputs, outputField{col: i, kind: outputGeometry})
			continue
		}

		colType := oracleTypeToArrow(ct, r.policy)
		kind := outputValue
		if dt, ok := inferred[i]; ok {
			colType = dt
			kind = outputInferredDecimal
		}
		// The canonical extension carries the instant and its offset in one
		// column, so it replaces both the UTC timestamp and the sibling that
		// oracle.tz_offset_column would add.
		if r.policy.timestampWithOffset && hasRecoverableZone(ct.DatabaseTypeName()) {
			colType = extensions.NewTimestampWithOffsetType(r.policy.effectiveTimeUnit())
			kind = outputTimestampOffset
		}
		fields = append(fields, arrow.Field{
			Name:     ct.Name(),
			Type:     colType,
			Nullable: nullable,
		})
		r.outputs = append(r.outputs, outputField{col: i, kind: kind})

		if r.policy.tzOffsetColumn && !r.policy.timestampWithOffset &&
			hasRecoverableZone(ct.DatabaseTypeName()) {
			fields = append(fields, arrow.Field{
				Name:     ct.Name() + tzColumnSuffix,
				Type:     arrow.BinaryTypes.String,
				Nullable: true,
			})
			r.outputs = append(r.outputs, outputField{col: i, kind: outputZone})
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// tzColumnSuffix names the sibling column oracle.tz_offset_column emits.
const tzColumnSuffix = "__tz"

// hasRecoverableZone reports whether a column's values still carry the zone
// they were written with.
//
// TIMESTAMP WITH TIME ZONE does: Oracle stores the region or offset alongside
// the instant, and go-ora hands it back in the time.Time's location. WITH
// LOCAL TIME ZONE does not: Oracle normalises it to the database zone on the
// way in and renders it in the session zone on the way out, so the original is
// gone before the driver sees it and a sibling column would only repeat the
// session's own offset.
func hasRecoverableZone(dbTypeName string) bool {
	return strings.ToUpper(dbTypeName) == "TIMESTAMPTZ_DTY"
}

func (r *oracleRecordReader) BeginAppending(builder *array.RecordBuilder) error {
	n := len(r.colTypes)
	r.scanDest = make([]interface{}, n)
	r.scanVals = make([]interface{}, n)
	for i := range r.scanDest {
		r.scanDest[i] = &r.scanVals[i]
	}
	return nil
}

// AppendRows fills up to batchRows rows per call.
//
// It used to append exactly one. driverbase calls this until the batch row
// limit is reached, so one row per call meant its loop, and everything it does
// per call, ran once per row: five million times for a five million row read.
// That was most of the fixed per-row cost, which showed up as eight
// allocations to read a single int64 column.
//
// Returning early with rows already appended is fine and is how end of data is
// reported: driverbase keeps the rows and calls again, and the next call
// returns io.EOF with nothing appended.
func (r *oracleRecordReader) AppendRows(builder *array.RecordBuilder) (int64, int64, error) {
	var (
		rows    int64
		rowSize int64
		target  = r.batchRows
	)
	if target < 1 {
		target = 1
	}

	if r.overflowErr != nil {
		return 0, 0, r.overflowErr
	}

	for rows < target {
		if r.peekPos < len(r.peeked) {
			// NextResultSet read these before it could name the types.
			copy(r.scanVals, r.peeked[r.peekPos])
			r.peeked[r.peekPos] = nil
			r.peekPos++
		} else {
			if !r.rows.Next() {
				if err := r.rows.Err(); err != nil {
					return rows, rowSize, err
				}
				if rows == 0 {
					return 0, 0, io.EOF
				}
				// Hand back what was appended; the next call reports EOF.
				return rows, rowSize, nil
			}
			if err := r.rows.Scan(r.scanDest...); err != nil {
				return rows, rowSize, fmt.Errorf("scan error: %w", err)
			}
		}

		for field, o := range r.outputs {
			val := r.scanVals[o.col]
			switch o.kind {
			case outputGeometry:
				rowSize += r.appendGeometry(builder.Field(field), val)
			case outputZone:
				rowSize += appendZone(builder.Field(field), val)
			case outputTimestampOffset:
				rowSize += appendTimestampWithOffset(builder.Field(field), val)
			case outputInferredDecimal:
				size, err := r.appendInferredDecimal(builder.Field(field), val, r.colTypes[o.col].Name())
				if err != nil {
					r.overflowErr = err
					return rows, rowSize, err
				}
				rowSize += size
			default:
				rowSize += appendValue(builder.Field(field), val, r.colTypes[o.col])
			}
		}
		rows++
	}

	return rows, rowSize, nil
}

// appendTimestampWithOffset fills a column carried as the canonical
// arrow.timestamp_with_offset extension. The builder derives both the UTC
// instant and the offset in minutes from the value's location, which is where
// go-ora puts the zone Oracle stored.
func appendTimestampWithOffset(fieldBuilder array.Builder, val interface{}) int64 {
	b, ok := fieldBuilder.(*extensions.TimestampWithOffsetBuilder)
	if !ok {
		fieldBuilder.AppendNull()
		return 0
	}
	t, ok := val.(time.Time)
	if !ok {
		b.AppendNull()
		return 0
	}
	b.Append(t)
	// Eight bytes of timestamp plus two of offset.
	return 10
}

// appendZone writes the zone a timestamp arrived in: the IANA region name when
// Oracle stored one ("America/New_York"), otherwise the fixed offset
// ("+05:30"), which is exactly what go-ora puts in the value's location.
func appendZone(fieldBuilder array.Builder, val interface{}) int64 {
	b, ok := fieldBuilder.(*array.StringBuilder)
	if !ok {
		fieldBuilder.AppendNull()
		return 0
	}
	t, ok := val.(time.Time)
	if !ok {
		b.AppendNull()
		return 0
	}
	name := t.Location().String()
	b.Append(name)
	return int64(len(name))
}

func (r *oracleRecordReader) appendGeometry(fieldBuilder array.Builder, val interface{}) int64 {
	if val == nil {
		fieldBuilder.AppendNull()
		return 0
	}

	geom, ok := val.(SdoGeometry)
	if !ok {
		fieldBuilder.AppendNull()
		return 0
	}

	wkb, err := SdoToWKB(&geom)
	if err != nil {
		fieldBuilder.AppendNull()
		return 0
	}

	fieldBuilder.(*array.BinaryBuilder).Append(wkb)
	return int64(len(wkb))
}

// peekSRID finds the SRID of the first geometry value in the peek buffer.
// Reads from the buffer rather than consuming a row of its own, so it can
// share the sample that NUMBER inference also needs.
func (r *oracleRecordReader) peekSRID() int64 {
	for _, vals := range r.peeked {
		for _, idx := range r.geomIndices {
			if geom, ok := vals[idx].(SdoGeometry); ok && geom.SRID != 0 {
				return geom.SRID
			}
		}
	}
	return 0
}

func (r *oracleRecordReader) Close() error {
	if r.rows != nil {
		return r.rows.Close()
	}
	return nil
}

// =============================================================================
// Type mapping
// =============================================================================

// readerBatchRows is the rows per Arrow batch, and so also the rows one
// AppendRows call fills. Set on both sides so they cannot drift: driverbase
// ends a batch once this many rows are in, and the reader fills exactly that
// many per call, which makes a batch cost one call instead of one per row.
//
// 65536 is driverbase's own default, kept so batch sizes do not change.
const readerBatchRows = 65536

var timestampNoTZ = &arrow.TimestampType{Unit: arrow.Microsecond}

// typePolicy is the connection's resolved type-mapping configuration.
//
// Both schema paths read the same value, which is what keeps GetTableSchema
// and ExecuteQuery describing a column identically. Passed as one struct
// rather than as separate arguments because every policy added here has to
// reach both, and a missed argument is exactly how they drift apart.
type typePolicy struct {
	numberMapping string
	timestampUnit arrow.TimeUnit

	// tzOffsetColumn emits a sibling String column next to every TIMESTAMP
	// WITH TIME ZONE, carrying the zone the value arrived in. See
	// OptionTzOffsetColumn.
	tzOffsetColumn bool

	// opaqueNumber labels an uninferred NUMBER as arrow.opaque rather than
	// leaving it plain utf8. See OptionOpaqueNumber.
	opaqueNumber bool

	// numberScanRows samples rows to infer a type for unconstrained NUMBER;
	// 0 disables it and such columns stay text. See OptionNumberScanRows.
	numberScanRows int

	// numberOverflow is the policy for a value that does not fit the inferred
	// decimal. See OptionNumberOverflow.
	numberOverflow string

	// compactDecimals narrows declared NUMBER to Decimal32/64 where it fits.
	// See OptionCompactDecimals.
	compactDecimals bool

	// timestampWithOffset carries TIMESTAMP WITH TIME ZONE as the canonical
	// arrow.timestamp_with_offset extension. See OptionTimestampWithOffset.
	timestampWithOffset bool
}

// effectiveTimeUnit is timestampUnit with the zero value resolved, so a
// policy built without one does not produce a seconds-resolution column.
func (p typePolicy) effectiveTimeUnit() arrow.TimeUnit {
	if p.timestampUnit == 0 {
		return arrow.Microsecond
	}
	return p.timestampUnit
}

// oracleIntervalDS builds the Arrow type for an INTERVAL DAY TO SECOND.
//
// Always microseconds, and deliberately not following oracle.timestamp_unit.
// go-ora decodes an interval to Oracle's text form and renders at most 6
// fractional digits whatever the column declares: an INTERVAL DAY(2) TO
// SECOND(9) holding .123456789, which Oracle's own TO_CHAR shows in full,
// reaches this driver as ".123456". A nanosecond duration would therefore
// advertise precision the wire cannot carry, so the unit is fixed at what is
// actually deliverable.
func (p typePolicy) oracleIntervalDS() arrow.DataType {
	return arrow.FixedWidthTypes.Duration_us
}

// oracleTimestamp builds the Arrow type for an Oracle TIMESTAMP under this
// policy. zoned covers both WITH TIME ZONE and WITH LOCAL TIME ZONE, which are
// normalised to UTC on the way out.
func (p typePolicy) oracleTimestamp(zoned bool) arrow.DataType {
	unit := p.timestampUnit
	if unit == 0 {
		unit = arrow.Microsecond
	}
	if zoned {
		return &arrow.TimestampType{Unit: unit, TimeZone: "UTC"}
	}
	return &arrow.TimestampType{Unit: unit}
}

// oracleScaleSentinel is what go-ora reports for a NUMBER whose scale arrived
// as Oracle's -127 "floating" sentinel — that is, an unconstrained NUMBER, or
// the result of an arithmetic expression. go-ora stores scale in a uint8 and
// substitutes 0xFF (parameter.go: `par.Scale = 0xFF`), so it surfaces through
// database/sql's DecimalSize as 255 rather than as 0 or -127.
//
// A declared negative scale — Oracle permits NUMBER(5,-3) — takes the same
// route and wraps to 256+scale, so it also lands in the high end of the uint8
// range (-3 → 253). numberDecimalType therefore rejects the whole band above
// arrow's maximum precision rather than testing for 255 alone; NUMBER(p,-1)
// would be indistinguishable from the sentinel anyway.
const oracleScaleSentinel = 255

// maxDecimal128Precision is both Decimal128's limit and Oracle NUMBER's
// maximum precision, so every *constrained* NUMBER fits.
const maxDecimal128Precision = 38

func oracleTypeToArrow(ct *sql.ColumnType, pol typePolicy) arrow.DataType {
	dbType := strings.ToUpper(ct.DatabaseTypeName())

	// Note: go-ora's ColumnType.DatabaseTypeName returns TNS wire-type names
	// (NCHAR, IBDouble, OCIBlobLocator, TimeStampDTY, …), not the DDL names
	// from ALL_TAB_COLUMNS — match both here.
	switch {
	case dbType == "NUMBER":
		precision, scale, ok := ct.DecimalSize()
		return numberToArrowWith(precision, scale, ok, pol)
	// BINARY_FLOAT is an IEEE single and go-ora decodes it to a Go float32,
	// so widening it to Float64 only invented precision the column never had
	// and doubled the width of every value.
	case dbType == "BINARY_FLOAT" || dbType == "IBFLOAT" || dbType == "BFLOAT":
		return arrow.PrimitiveTypes.Float32
	// FLOAT is listed for completeness; go-ora reports a FLOAT column as
	// NUMBER on the wire, so it is handled by the NUMBER case above.
	case dbType == "FLOAT" || dbType == "BINARY_DOUBLE" ||
		dbType == "IBDOUBLE" || dbType == "BDOUBLE":
		return arrow.PrimitiveTypes.Float64

	// Character LOBs use the 64-bit offset layout. A String column's offsets
	// are int32, so a batch carrying more than 2 GiB of text overflows them,
	// and a CLOB column is exactly where that much text comes from. go-ora
	// reports a CLOB as LongVarChar and Oracle's deprecated LONG the same way;
	// both are wide text and both belong here.
	case dbType == "CLOB" || dbType == "NCLOB" || dbType == "OCICLOBLOCATOR" ||
		dbType == "LONG" || dbType == "LONGVARCHAR":
		return arrow.BinaryTypes.LargeString

	case dbType == "VARCHAR2" || dbType == "VARCHAR" || dbType == "NVARCHAR2" ||
		dbType == "CHAR" || dbType == "NCHAR" ||
		dbType == "ROWID" || dbType == "UROWID":
		return arrow.BinaryTypes.String

	// Oracle DATE has second granularity, so microseconds is always lossless
	// for it and the unit policy does not apply.
	case dbType == "DATE":
		return timestampNoTZ
	case dbType == "TIMESTAMPDTY" || dbType == "TIMESTAMP":
		return pol.oracleTimestamp(false)
	case dbType == "TIMESTAMPTZ_DTY" || dbType == "TIMESTAMPLTZ_DTY" ||
		strings.HasPrefix(dbType, "TIMESTAMP"):
		return pol.oracleTimestamp(true)

	// Intervals were String, which is readable but not arithmetic. go-ora
	// delivers them as Oracle's text form, which parses exactly into the two
	// Arrow interval types.
	case dbType == "INTERVALDS_DTY" || dbType == "INTERVALDS":
		return pol.oracleIntervalDS()
	case dbType == "INTERVALYM_DTY" || dbType == "INTERVALYM":
		return arrow.FixedWidthTypes.MonthInterval
	case strings.HasPrefix(dbType, "INTERVAL"):
		return arrow.BinaryTypes.String

	// Binary LOBs, same reasoning as CLOB above. RAW is capped at 32767 bytes
	// so it keeps the 32-bit layout.
	case dbType == "BLOB" || dbType == "OCIBLOBLOCATOR" || dbType == "OCIFILELOCATOR" ||
		dbType == "LONG RAW" || dbType == "LONGRAW":
		return arrow.BinaryTypes.LargeBinary

	case dbType == "RAW":
		return arrow.BinaryTypes.Binary

	case dbType == "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean

	default:
		return arrow.BinaryTypes.String
	}
}

// numberToArrow picks the Arrow type for an Oracle NUMBER column under the
// configured mapping policy. precision, scale and ok are as reported by
// sql.ColumnType.DecimalSize. See the NumberMapping* constants.
func numberToArrow(precision, scale int64, ok bool, numberMapping string) arrow.DataType {
	return numberToArrowWith(precision, scale, ok, typePolicy{numberMapping: numberMapping})
}

// numberToArrowWith is numberToArrow under a full policy, so compact decimals
// and the opaque label are decided in the one place both schema paths share.
//
// An unconstrained NUMBER returns nil rather than a type: only the caller
// knows whether it can sample rows to infer one, so the decision of what to do
// instead belongs there. numberFallbackType gives the answer when it cannot.
func numberToArrowWith(precision, scale int64, ok bool, pol typePolicy) arrow.DataType {
	numberMapping := pol.numberMapping
	if numberMapping == "" {
		// Reached when a reader was built without a resolved policy. Keep it
		// identical to the resolved default rather than quietly differing.
		numberMapping = defaultNumberMapping
	}
	if numberMapping == NumberMappingString {
		return arrow.BinaryTypes.String
	}

	// Integral NUMBER narrow enough for int64 is exact under every policy, and
	// cheaper than a decimal, so it wins before the policies diverge.
	if ok && scale == 0 && precision > 0 && precision <= 18 {
		return arrow.PrimitiveTypes.Int64
	}

	if numberMapping == NumberMappingDecimal {
		if dt := numberDecimalType(precision, scale, ok); dt != nil {
			if pol.compactDecimals {
				if dec, isDec := dt.(arrow.DecimalType); isDec {
					return compactDecimalType(dec.GetPrecision(), dec.GetScale())
				}
			}
			return dt
		}
		// No declared precision and scale to build a decimal from: an
		// unconstrained NUMBER, a negative declared scale, or precision past
		// 38. Callers that can sample rows ask numberNeedsInference first and
		// use what they infer; this is the answer when they cannot.
		return numberFallbackType(pol)
	}

	// NumberMappingFloat: the upstream driver's historical behaviour.
	if ok && scale == 0 && precision > 18 {
		return arrow.BinaryTypes.String
	}
	return arrow.PrimitiveTypes.Float64
}

// numberNeedsInference reports whether a NUMBER column has no declared
// precision and scale to build a decimal from, so a type could instead be
// inferred by sampling rows.
//
// True only under the decimal mapping: float already has a numeric answer for
// such a column and string was asked for text, so neither has anything to gain
// from a sample.
func numberNeedsInference(precision, scale int64, ok bool, pol typePolicy) bool {
	mapping := pol.numberMapping
	if mapping == "" {
		mapping = defaultNumberMapping
	}
	if mapping != NumberMappingDecimal {
		return false
	}
	if ok && scale == 0 && precision > 0 && precision <= 18 {
		// Already Int64, which is exact and narrower than any decimal.
		return false
	}
	return numberDecimalType(precision, scale, ok) == nil
}

// numberFallbackType is what an unconstrained NUMBER becomes when no type can
// be inferred from data: text, but labelled.
//
// arrow.opaque is the canonical extension for a value from an external system
// that could not be interpreted, which is what this is. It does not make the
// column a number, but it stops the column claiming to be an ordinary string
// when it is really an Oracle NUMBER that Arrow has no type for. Consumers
// that do not know the extension see its utf8 storage.
func numberFallbackType(pol typePolicy) arrow.DataType {
	if pol.opaqueNumber && pol.numberMapping != NumberMappingString {
		// Opt-in only: see OptionOpaqueNumber for why the label is not free.
		return opaqueNumberType()
	}
	return arrow.BinaryTypes.String
}

// numberDecimalType returns the Decimal128 type for a declared precision and
// scale, or nil when the pair cannot be represented exactly.
func numberDecimalType(precision, scale int64, ok bool) arrow.DataType {
	if !ok {
		return nil
	}
	if scale == oracleScaleSentinel {
		// Unconstrained NUMBER: no declared scale to build a decimal from.
		return nil
	}
	if precision < 1 || precision > maxDecimal128Precision {
		return nil
	}
	// scale > precision is legal in Arrow but not in Parquet, and does not
	// arise from a real Oracle column; reject it rather than emit a decimal
	// that fails to serialise downstream. Every wrapped negative scale
	// (256+scale, so >= 172) is excluded by the same bound.
	if scale < 0 || scale > precision {
		return nil
	}
	return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}
}
