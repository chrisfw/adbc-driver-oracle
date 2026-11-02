package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RowsReaderOptions configures NewRowsReader. The zero value gives the same
// types a connection with no options set gives.
type RowsReaderOptions struct {
	// NumberMapping is the NUMBER→Arrow policy: NumberMappingDecimal (the
	// default when empty), NumberMappingFloat or NumberMappingString.
	NumberMapping string
	// BatchRowLimit caps rows per record batch; 0 uses the driver's default.
	BatchRowLimit int64
	// NoNumberInference turns off sampling rows to infer a type for an
	// unconstrained NUMBER (such columns then stay text), as
	// oracle.number_scan_rows=0 does.
	NoNumberInference bool
}

// NewRowsReader returns the driver's Oracle→Arrow conversion over rows the
// caller already holds, for callers that manage their own connections (for
// example to wrap the go-ora connector, or to run inside a read-only
// transaction of their own) instead of letting the driver open a pool from a
// DSN. The result is the same kind of array.RecordReader ExecuteQuery returns,
// so it can be passed through the mask package's hook. The reader owns rows
// and closes them when it is released.
func NewRowsReader(ctx context.Context, alloc memory.Allocator, rows *sql.Rows, opts RowsReaderOptions) (array.RecordReader, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	switch opts.NumberMapping {
	case "", NumberMappingDecimal, NumberMappingFloat, NumberMappingString:
	default:
		rows.Close()
		return nil, fmt.Errorf("oracle: unknown number mapping %q", opts.NumberMapping)
	}
	// Resolve the defaults exactly as a connection does (databaseImpl's
	// effective* helpers), so a reader built here types columns the same way
	// ExecuteQuery would.
	db := &databaseImpl{numberMapping: opts.NumberMapping, numberScanRows: -1}
	if opts.NoNumberInference {
		db.numberScanRows = 0
	}
	batch := opts.BatchRowLimit
	if batch <= 0 {
		batch = readerBatchRows
	}
	impl := &oracleRecordReader{
		rows: rows,
		policy: typePolicy{
			numberMapping:  db.effectiveNumberMapping(),
			timestampUnit:  db.effectiveArrowTimeUnit(),
			numberScanRows: db.effectiveNumberScanRows(),
			numberOverflow: db.effectiveNumberOverflow(),
		},
		batchRows: batch,
		logger:    slog.Default(),
	}
	var rr driverbase.BaseRecordReader
	if err := rr.Init(ctx, alloc, slog.Default(), nil, driverbase.BaseRecordReaderOptions{BatchRowLimit: batch}, impl); err != nil {
		rows.Close()
		return nil, err
	}
	return &rr, nil
}
