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
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	go_ora "github.com/sijms/go-ora/v2"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	hostname       string
	port           string
	serviceName    string
	user           string
	password       string
	walletLocation string
	walletPassword string
	walletContent  string // Inline ewallet.pem content (avoids temp dir)
	dsn            string

	// Performance tuning
	poolSize      int // Max open connections (default: 8)
	ingestWorkers int // Parallel insert workers (default: 1)

	// numberMapping selects the Arrow type for NUMBER result columns.
	// Empty means defaultNumberMapping. See OptionNumberMapping.
	numberMapping string

	// timestampUnit selects the Arrow time unit for TIMESTAMP result columns.
	// Empty means defaultTimestampUnit. See OptionTimestampUnit.
	timestampUnit string

	// tzOffsetColumn emits the "<col>__tz" sibling columns. See
	// OptionTzOffsetColumn.
	tzOffsetColumn bool

	// lobFetch selects go-ora's LOB retrieval mode. Empty means
	// defaultLobFetch. See OptionLobFetch.
	lobFetch string

	// prefetchRows is the rows per fetch round trip. Zero means
	// defaultPrefetchRows. See OptionPrefetchRows.
	prefetchRows int

	// partitionCount is how many slices ExecutePartitions aims for. Zero
	// means unset, which resolves to defaultPartitionCount.
	partitionCount int

	// numberScanRows samples rows to infer a type for unconstrained NUMBER.
	// Negative means unset; 0 disables inference. See OptionNumberScanRows.
	numberScanRows int

	// numberOverflow is the policy for a value that does not fit the inferred
	// decimal. Empty means defaultNumberOverflow. See OptionNumberOverflow.
	numberOverflow string

	// compactDecimals narrows declared NUMBER to Decimal32/64 where it fits.
	// See OptionCompactDecimals.
	compactDecimals bool

	// timestampWithOffset carries TIMESTAMP WITH TIME ZONE as the canonical
	// arrow extension. See OptionTimestampWithOffset.
	timestampWithOffset bool

	// opaqueNumber labels an uninferred NUMBER as arrow.opaque. See
	// OptionOpaqueNumber.
	opaqueNumber bool

	// Connection pool — shared across all Open() calls.
	// sql.DB is already a pool internally; we create it once and reuse.
	pool     *sql.DB
	poolOnce sync.Once
	poolErr  error
}

func (db *databaseImpl) SetOption(key string, val string) error {
	switch key {
	case OptionServerHostname:
		db.hostname = val
	case OptionPort:
		db.port = val
	case OptionServiceName:
		db.serviceName = val
	case OptionUser, adbc.OptionKeyUsername:
		db.user = val
	case OptionPassword, adbc.OptionKeyPassword:
		db.password = val
	case OptionWalletLocation:
		db.walletLocation = val
	case OptionWalletPassword:
		db.walletPassword = val
	case OptionWalletContent:
		db.walletContent = val
	case OptionDSN:
		db.dsn = val
	case OptionPoolSize:
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			db.poolSize = n
		}
	case OptionIngestWorkers:
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			db.ingestWorkers = n
		}
	case OptionNumberMapping:
		switch val {
		case NumberMappingFloat, NumberMappingDecimal, NumberMappingString:
			db.numberMapping = val
		default:
			return db.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"invalid %s %q: expected %q, %q or %q",
				OptionNumberMapping, val,
				NumberMappingFloat, NumberMappingDecimal, NumberMappingString)
		}
	case OptionNumberScanRows:
		n, err := strconv.Atoi(val)
		if err != nil || n < 0 {
			return db.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"invalid %s %q: expected a non-negative integer (0 disables inference)",
				OptionNumberScanRows, val)
		}
		db.numberScanRows = n
	case OptionPartitionCount:
		n, err := strconv.Atoi(val)
		if err != nil || n < 1 {
			return db.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"invalid %s %q: expected a positive integer (1 means do not split)",
				OptionPartitionCount, val)
		}
		db.partitionCount = n
	case OptionNumberOverflow:
		switch val {
		case NumberOverflowError, NumberOverflowNull:
			db.numberOverflow = val
		default:
			return db.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"invalid %s %q: expected %q or %q",
				OptionNumberOverflow, val, NumberOverflowError, NumberOverflowNull)
		}
	case OptionOpaqueNumber:
		b, err := parseBoolOption(val)
		if err != nil {
			return db.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"invalid %s %q: expected %q or %q",
				OptionOpaqueNumber, val, adbc.OptionValueEnabled, adbc.OptionValueDisabled)
		}
		db.opaqueNumber = b
	case OptionCompactDecimals:
		b, err := parseBoolOption(val)
		if err != nil {
			return db.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"invalid %s %q: expected %q or %q",
				OptionCompactDecimals, val, adbc.OptionValueEnabled, adbc.OptionValueDisabled)
		}
		db.compactDecimals = b
	case OptionTimestampWithOffset:
		b, err := parseBoolOption(val)
		if err != nil {
			return db.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"invalid %s %q: expected %q or %q",
				OptionTimestampWithOffset, val, adbc.OptionValueEnabled, adbc.OptionValueDisabled)
		}
		db.timestampWithOffset = b
	case OptionPrefetchRows:
		n, err := strconv.Atoi(val)
		if err != nil || n < 1 {
			return db.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"invalid %s %q: expected a positive integer", OptionPrefetchRows, val)
		}
		db.prefetchRows = n
	case OptionLobFetch:
		if _, ok := oracleLobFetchParam(val); !ok {
			return db.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"invalid %s %q: expected %q or %q",
				OptionLobFetch, val, LobFetchInline, LobFetchStream)
		}
		db.lobFetch = val
	case OptionTzOffsetColumn:
		switch val {
		case adbc.OptionValueEnabled:
			db.tzOffsetColumn = true
		case adbc.OptionValueDisabled:
			db.tzOffsetColumn = false
		default:
			return db.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"invalid %s %q: expected %q or %q",
				OptionTzOffsetColumn, val, adbc.OptionValueEnabled, adbc.OptionValueDisabled)
		}
	case OptionTimestampUnit:
		if _, ok := arrowTimeUnit(val); !ok {
			return db.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"invalid %s %q: expected %q or %q",
				OptionTimestampUnit, val, TimestampUnitMicro, TimestampUnitNano)
		}
		db.timestampUnit = val
	default:
		return db.DatabaseImplBase.SetOption(key, val)
	}
	return nil
}

func (db *databaseImpl) SetOptions(options map[string]string) error {
	for key, val := range options {
		if err := db.SetOption(key, val); err != nil {
			return err
		}
	}
	return nil
}

func (db *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionServerHostname:
		return db.hostname, nil
	case OptionPort:
		return db.port, nil
	case OptionServiceName:
		return db.serviceName, nil
	case OptionUser:
		return db.user, nil
	case OptionWalletLocation:
		return db.walletLocation, nil
	case OptionNumberMapping:
		return db.effectiveNumberMapping(), nil
	case OptionTimestampUnit:
		return db.effectiveTimestampUnit(), nil
	case OptionLobFetch:
		if db.lobFetch == "" {
			return defaultLobFetch, nil
		}
		return db.lobFetch, nil
	case OptionPrefetchRows:
		return strconv.Itoa(db.effectivePrefetchRows()), nil
	case OptionNumberScanRows:
		return strconv.Itoa(db.effectiveNumberScanRows()), nil
	case OptionNumberOverflow:
		if db.numberOverflow == "" {
			return defaultNumberOverflow, nil
		}
		return db.numberOverflow, nil
	case OptionCompactDecimals:
		return boolOptionValue(db.compactDecimals), nil
	case OptionOpaqueNumber:
		return boolOptionValue(db.opaqueNumber), nil
	case OptionTimestampWithOffset:
		return boolOptionValue(db.timestampWithOffset), nil
	case OptionTzOffsetColumn:
		if db.tzOffsetColumn {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	default:
		return db.DatabaseImplBase.GetOption(key)
	}
}

// effectiveNumberMapping resolves the unset value to the default, so callers
// never have to test for "".
func (db *databaseImpl) effectiveNumberMapping() string {
	if db.numberMapping == "" {
		return defaultNumberMapping
	}
	return db.numberMapping
}

// effectiveArrowTimeUnit resolves the configured unit to its Arrow value.
// SetOption already rejected anything unrecognised, so this cannot fail.
func (db *databaseImpl) effectiveArrowTimeUnit() arrow.TimeUnit {
	unit, _ := arrowTimeUnit(db.timestampUnit)
	return unit
}

// effectiveNumberScanRows resolves the unset value to the default. A
// configured 0 is meaningful and means "do not infer", so unset is tracked as
// a negative rather than as zero.
func (db *databaseImpl) effectiveNumberScanRows() int {
	if db.numberScanRows < 0 {
		return defaultNumberScanRows
	}
	return db.numberScanRows
}

// effectivePoolSize resolves the unset value to the default, matching what
// getPool actually configures.
func (db *databaseImpl) effectivePoolSize() int {
	if db.poolSize <= 0 {
		return 8
	}
	return db.poolSize
}

// effectivePartitionCount resolves the unset value to the default.
func (db *databaseImpl) effectivePartitionCount() int {
	if db.partitionCount < 1 {
		return defaultPartitionCount
	}
	return db.partitionCount
}

// effectiveNumberOverflow resolves the unset value to the default.
func (db *databaseImpl) effectiveNumberOverflow() string {
	if db.numberOverflow == "" {
		return defaultNumberOverflow
	}
	return db.numberOverflow
}

// effectivePrefetchRows resolves the unset value to the default.
func (db *databaseImpl) effectivePrefetchRows() int {
	if db.prefetchRows < 1 {
		return defaultPrefetchRows
	}
	return db.prefetchRows
}

// effectiveTimestampUnit resolves the unset value to the default.
func (db *databaseImpl) effectiveTimestampUnit() string {
	if db.timestampUnit == "" {
		return defaultTimestampUnit
	}
	return db.timestampUnit
}

func (db *databaseImpl) buildDSN() string {
	if db.dsn != "" {
		// If the user passed something that isn't a URL — typically a TNS
		// alias from an Autonomous Database wallet — try to resolve it
		// against tnsnames.ora in walletLocation. This lets users paste in
		// the same alias they'd use with sqlplus / sqlcl.
		if !strings.Contains(db.dsn, "://") && db.walletLocation != "" {
			if aliases, err := loadTNSAliases(db.walletLocation); err == nil {
				if entry, ok := aliases[strings.ToLower(db.dsn)]; ok {
					return assembleOracleURL(db.user, db.password, entry.Host, entry.Port,
						entry.Service, db.walletLocation, db.walletPassword, db.lobFetch,
						db.effectivePrefetchRows())
				}
			}
		}
		return db.dsn
	}

	return assembleOracleURL(db.user, db.password, db.hostname, db.port, db.serviceName,
		db.walletLocation, db.walletPassword, db.lobFetch, db.effectivePrefetchRows())
}

// assembleOracleURL builds the oracle://user:pass@host:port/service URL with
// the same query-parameter conventions used historically by buildDSN.
func assembleOracleURL(user, password, host, port, service, walletLocation, walletPassword, lobFetch string, prefetchRows int) string {
	dsn := fmt.Sprintf("oracle://%s:%s@%s:%s/%s",
		url.PathEscape(user),
		url.PathEscape(password),
		host,
		port,
		service,
	)
	params := url.Values{}
	if prefetchRows < 1 {
		prefetchRows = defaultPrefetchRows
	}
	params.Set("PREFETCH_ROWS", strconv.Itoa(prefetchRows))
	// go-ora's own default is INLINE, but setting it explicitly keeps the
	// behaviour pinned to this driver's documented default rather than to
	// whatever go-ora changes its mind about.
	if param, ok := oracleLobFetchParam(lobFetch); ok {
		params.Set("LOB FETCH", param)
	}
	if walletLocation != "" {
		// Wallet implies TCPS for Autonomous Database; switch SSL on.
		// SSL VERIFY=false matches the wallet's mTLS chain rather than the
		// hostname, which is what ATP wallets are designed for.
		params.Set("WALLET", walletLocation)
		params.Set("SSL", "enable")
		params.Set("SSL VERIFY", "false")
	}
	if walletPassword != "" {
		params.Set("WALLET PASSWORD", walletPassword)
	}
	if len(params) > 0 {
		dsn += "?" + params.Encode()
	}
	return dsn
}

// getPool returns the shared connection pool, creating it on first call.
func (db *databaseImpl) getPool(ctx context.Context) (*sql.DB, error) {
	db.poolOnce.Do(func() {
		var pool *sql.DB
		var err error

		if db.walletContent != "" {
			// Use Connector API for in-memory wallet (avoids temp files).
			// Wallet content is base64-encoded cwallet.sso binary data.
			walletBytes, decErr := base64.StdEncoding.DecodeString(db.walletContent)
			if decErr != nil {
				db.poolErr = fmt.Errorf("failed to decode wallet content (expected base64): %w", decErr)
				return
			}
			dsn := db.buildDSN()
			connector := go_ora.NewConnector(dsn).(*go_ora.OracleConnector)
			if walletErr := connector.WithWallet(bytes.NewReader(walletBytes)); walletErr != nil {
				db.poolErr = fmt.Errorf("failed to load wallet content: %w", walletErr)
				return
			}
			pool = sql.OpenDB(connector)
		} else {
			dsn := db.buildDSN()
			pool, err = sql.Open("oracle", dsn)
			if err != nil {
				db.poolErr = fmt.Errorf("failed to open Oracle connection pool: %w", err)
				return
			}
		}

		if err := pool.PingContext(ctx); err != nil {
			pool.Close()
			db.poolErr = fmt.Errorf("failed to ping Oracle: %w", err)
			return
		}

		// The pool now bounds concurrent ADBC connections (one owned session
		// each) plus the extra sessions ingest workers take under autocommit,
		// so the worst case is roughly connections * (1 + ingest_workers).
		poolSize := db.poolSize
		if poolSize <= 0 {
			poolSize = 8
		}
		pool.SetMaxOpenConns(poolSize)
		pool.SetMaxIdleConns(poolSize)

		// Register SDO_GEOMETRY UDT so go-ora can decode geometry columns
		if err := RegisterSDOTypes(pool); err != nil {
			db.Logger.Warn("SDO_GEOMETRY UDT registration failed", "error", err)
		}

		db.pool = pool
	})
	return db.pool, db.poolErr
}

func (db *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	pool, err := db.getPool(ctx)
	if err != nil {
		return nil, db.ErrorHelper.WrapIO(err, "failed to open Oracle database")
	}

	// Check one session out of the pool and keep it for this connection's
	// whole life. An ADBC connection is a session: ALTER SESSION, CURRENT_SCHEMA,
	// temp tables and transactions are all session state, and handing out the
	// pool instead would scatter them across whichever sessions happened to be
	// free. The pool stays on databaseImpl purely as the session factory.
	conn, err := pool.Conn(ctx)
	if err != nil {
		return nil, db.ErrorHelper.WrapIO(err, "failed to open Oracle session")
	}

	cnxnBase := driverbase.NewConnectionImplBase(&db.DatabaseImplBase)
	cnxn := &connectionImpl{
		ConnectionImplBase: cnxnBase,
		db:                 pool,
		conn:               conn,
		ingestWorkers:      db.ingestWorkers,
		numberMapping:      db.effectiveNumberMapping(),
		timestampUnit:      db.effectiveArrowTimeUnit(),
		tzOffsetColumn:     db.tzOffsetColumn,

		numberScanRows:      db.effectiveNumberScanRows(),
		partitionCount:      db.effectivePartitionCount(),
		poolSize:            db.effectivePoolSize(),
		numberOverflow:      db.effectiveNumberOverflow(),
		compactDecimals:     db.compactDecimals,
		timestampWithOffset: db.timestampWithOffset,
		opaqueNumber:        db.opaqueNumber,
	}
	// Transactions are begun with a context scoped to the connection, not to
	// the statement that happens to start one: database/sql rolls a
	// transaction back when its context is cancelled. Close cancels this.
	// Deliberately not derived from ctx, which only covers this Open call.
	cnxn.txCtx, cnxn.txCancel = context.WithCancel(context.Background())

	return driverbase.NewConnectionBuilder(cnxn).
		WithDbObjectsEnumerator(cnxn).
		WithTableTypeLister(cnxn).
		WithCurrentNamespacer(cnxn).
		WithAutocommitSetter(cnxn).
		Connection(), nil
}

func (db *databaseImpl) Close() error {
	if db.pool != nil {
		err := db.pool.Close()
		db.pool = nil
		return err
	}
	return db.DatabaseImplBase.Close()
}
