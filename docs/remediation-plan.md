# adbc-driver-oracle remediation plan

Baseline: commit `39d2da8` (Map Oracle NUMBER to Decimal128 behind oracle.number_mapping).
Goal: make the driver a correct, session-faithful ADBC driver that ODBC and JDBC facades can sit on, then remove the per-row read bottleneck.

Work the phases in order. Each item lists the defect, the change, and the tests that prove it. Do not mix phases in one commit.

## Ground rules

- Unit tests run with `go test ./go/...` and must pass with `-race`.
- Integration tests use `ORACLE_TEST_DSN` (see `go/driver_test.go:31`). Every behaviour change below needs an integration test, skipped when the DSN is unset.
- Target databases for integration runs: Oracle Free 23ai (container) and a 19c instance.
- Keep the public option names stable unless an item says otherwise. Document every new option in `go/driver.go` next to the existing constants and in the README.
- One logical change per commit, message explains why.

## Phase 1: connection ownership (blocks everything else)

### 1.1 Each ADBC connection owns one Oracle session

Defect: `databaseImpl.Open` (`go/database.go:244`) hands every connection the shared `*sql.DB` pool from `getPool` (`go/database.go:191`). Any statement, commit or session setting runs on whichever pooled session is free.

Change:
- Keep `*sql.DB` on `databaseImpl` as the session factory only.
- In `Open`, call `pool.Conn(ctx)` and store the `*sql.Conn` on `connectionImpl`. Replace every `c.db.*` call in `connection.go` and `statement.go` with the owned conn.
- `connectionImpl.Close` returns the conn to the pool (`conn.Close()`). Remove `ownsDB`.
- Ingest workers (`insertBatchParallel`, `streamInsertWorker`) may still take extra pool sessions, but only when autocommit is on. With autocommit off, ingest runs serially on the owned session so it is inside the caller's transaction. Document this.
- Pool size option stays; it now bounds concurrent ADBC connections plus ingest workers.

Tests:
- Two connections from one database: `ALTER SESSION SET CURRENT_SCHEMA` on one is not visible on the other (`SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual`).
- `SELECT SYS_CONTEXT('USERENV','SID')` returns the same SID for every statement on one connection, across 50 statements.

### 1.2 Autocommit and transactions

Defect: no handling of `adbc.OptionKeyAutoCommit`. `Commit`/`Rollback` (`go/connection.go:294-308`) exec `COMMIT`/`ROLLBACK` on a random pooled session.

Change:
- Implement `SetOption(adbc.OptionKeyAutoCommit, ...)` on the connection. Default `true` per ADBC spec.
- Autocommit off: begin a `*sql.Tx` on the owned conn lazily at the first statement, route statements through it, `Commit`/`Rollback` call `tx.Commit`/`tx.Rollback`, next statement starts a new tx.
- Autocommit on: no tx, and `Commit`/`Rollback` return `StatusInvalidState` per ADBC.
- Switching autocommit from off to on commits any open tx (ADBC spec).
- Expose `adbc.OptionKeyIsolationLevel`: support `read_committed` and `serializable`, reject others with `StatusNotImplemented`.
- Verify go-ora's commit behaviour under database/sql with autocommit on. If it does not commit per exec, set it explicitly.

Tests:
- Autocommit off, insert, rollback: row absent. Insert, commit: row present from a second connection.
- Autocommit off, insert, read from a second connection before commit: row absent.
- Commit with autocommit on: `StatusInvalidState`.
- Toggle off to on with pending insert: row committed.

### 1.3 Session state follows the session

Defect: `SetCurrentDbSchema` (`go/connection.go:284`) runs on a random session. Prepared statements (`statement.go` `Prepare`) prepare on the pool.

Change:
- Both now use the owned conn (falls out of 1.1). `SetCurrentDbSchema` must quote the identifier with `"` and escape embedded quotes instead of `strings.ToUpper` concatenation (SQL injection today).
- `Prepare` uses `conn.PrepareContext`, or `tx.PrepareContext` when a tx is open.

Tests:
- Set schema, then unqualified `SELECT` from a table that exists only in that schema succeeds.
- Schema name containing a quote is rejected or escaped, never executed raw.

## Phase 2: correctness bugs

### 2.1 GetTableSchema agrees with ExecuteQuery

Defect: `oracleDataTypeToArrow` (`go/connection.go:324`) has its own NUMBER logic and ignores `numberMapping`, so under `decimal` the metadata schema says Float64 while query results are Decimal128.

Change: route `oracleDataTypeToArrow`'s NUMBER case through `numberToArrow` with the connection's `numberMapping`. ALL_TAB_COLUMNS reports unconstrained NUMBER as NULL precision and NULL scale; map that to the same branch as go-ora's 255 sentinel. Better: a single mapping function with an input struct used by both paths.

Tests: for each of NUMBER, NUMBER(10), NUMBER(19), NUMBER(12,2), NUMBER(5,-2), FLOAT, under each mapping, `GetTableSchema` type equals the `ExecuteQuery` schema type for `SELECT col FROM t`.

### 2.2 TLS server verification

> **DEFERRED 2026-11-01**, to a future phase, along with the rest of the wallet
> work. `SSL VERIFY=false` is only set when `oracle.wallet_location` is given,
> so the exposure is wallet-only and leaves with it. Note that this is the one
> item in Phase 2 that is a live security hole rather than a correctness bug:
> until it lands, ATP connections are open to MITM. It should not drift
> indefinitely on the strength of "no wallet work this phase".

Defect: `assembleOracleURL` (`go/database.go:163`) always sets `SSL VERIFY=false` with a wallet (line 179). No server certificate check, so MITM is possible.

Change:
- Default to verification on.
- New option `oracle.ssl_verify` (`true` default, `false` allowed with a logged warning).
- Confirm go-ora verifies against the wallet's trusted certs for ATP. If ATP fails with verify on, find out why (hostname vs `ssl_server_cert_dn`) and support DN matching rather than turning verification off.

Tests: unit test that the assembled DSN contains no `SSL VERIFY=false` by default. Integration against ATP if a wallet is available in env (`ORACLE_TEST_WALLET`).

### 2.3 Credentials out of the DSN string

> **PARTIALLY DONE 2026-11-01.** The regression tests landed with 2.5. The
> structural change described below is not achievable against the pinned
> go-ora: `NewConnector` accepts only a connect string and exposes no config,
> and a hand-rolled `driver.Connector` calling `NewConnection("", config)`
> loses `conn.cusTyp` and `drv.init`, both unexported, which is what registers
> SDO_GEOMETRY. Deferred to the fork in 4.2, where the API can be extended.
> The premise about error messages was also already false: nothing formats the
> DSN and go-ora's parser does not echo the connect string.

Defect: password is URL-embedded in the DSN, which then lives in memory, error messages and logs.

Change: build the go-ora connector with `go_ora.NewConnector` and set credentials through its config rather than the URL where go-ora allows it. Make sure no error path formats the DSN. Add a test that error text from a failed connect does not contain the password.

### 2.4 Bound parameters

Defects:
- `ExecuteQuery` uses only row 0 of a bound batch (`go/statement.go:192`). ADBC expects one result per row; returning silently wrong data is worse than an error.
- `ExecuteUpdate` with binds loops one round trip per row (`go/statement.go:261`).

Change:
- `ExecuteQuery` with a bound batch of more than one row: return `StatusNotImplemented` with a clear message. One row: current behaviour.
- `ExecuteUpdate`: reuse the ingest path's column slicing (`arrowColumnToSliceRange`, `go/statement.go:876`) to pass array binds so go-ora does a single array execute per batch. Keep per-row fallback only for types the array path cannot bind, and log it once.
- Implement `GetParameterSchema` for prepared statements if go-ora exposes bind metadata; otherwise leave `StatusNotImplemented`.

Tests:
- 10,000-row bound batch `INSERT` completes in one batch execute (assert via timing bound or a round-trip counter if go-ora exposes one) and row count is 10,000.
- Multi-row bound `ExecuteQuery` returns `StatusNotImplemented`.

### 2.5 Error mapping

Defect: every failure is `StatusIO` with no Oracle code.

Change:
- Extract `ORA-nnnnn` from go-ora errors. Put the code in `adbc.Error.VendorCode` and a SQLSTATE in `adbc.Error.SqlState`.
- Map at least: ORA-00001 to 23000 / `StatusIntegrity`; ORA-00942, ORA-00904 to 42S02, 42S22 / `StatusNotFound`; ORA-01017 to 28000 / `StatusUnauthenticated`; ORA-01013 to HY008 / `StatusCancelled`; ORA-12899, ORA-01438 to 22001, 22003 / `StatusInvalidData`; ORA-00060 to 40001; ORA-125xx and connection resets to 08001/08S01 / `StatusIO`. Everything else HY000 / `StatusInternal` or `StatusIO`.
- Table-driven unit test for the mapper; integration tests for 00001, 00942 and 01017.

## Phase 3: type fidelity

- **Default number mapping:** change the default from `float` to `decimal`. This is a deliberate schema change in our fork; note it in the README and commit message.
- **TIMESTAMP precision:** map TIMESTAMP(p) with p > 6 to `timestamp[ns]`, p ≤ 6 to `timestamp[us]`. DATE stays `timestamp[s]` or `timestamp[us]`; pick `timestamp[s]` only if no consumer breaks, otherwise keep `us` and document.
- **TIMESTAMP WITH TIME ZONE / LOCAL TIME ZONE:** keep UTC-normalised `timestamp[*, UTC]`. Add option `oracle.tz_offset_column=true` that emits a sibling `<col>__tz` String column with the original region/offset. Verify DST-boundary values round-trip.
- **INTERVAL DAY TO SECOND** to `duration[ns]` (or `us` matching scale). **INTERVAL YEAR TO MONTH** to `month_interval`. Today both are String.
- **BINARY_FLOAT** to `float32` (currently widened to float64).
- **ROWID/UROWID:** stay String.
- **LOB handling:** confirm go-ora LOB prefetch behaviour. Expose `oracle.lob_prefetch_bytes` and use LargeString/LargeBinary for CLOB/BLOB so batches over 2 GiB of LOB data do not overflow int32 offsets.
- **Charset:** integration test against a WE8MSWIN1252 database (or a NCHAR column with non-ASCII data) proving UTF-8 output is correct.

Tests: one integration table per type family with edge values (NULL, min, max, negative scale, DST transitions, 4000/32767-byte strings, multi-MB CLOB), asserting exact Arrow values.

## Phase 4: read path performance (measure before and after)

Build a benchmark first: `go/benchmark_test.go` already exists; extend it with a fixed 5M-row table of mixed types (NUMBER(12,2), NUMBER(10), VARCHAR2(100), DATE, TIMESTAMP(6)) and record rows/s and allocations/row for the baseline at the start of the phase.

### 4.1 Cheap wins in the current reader

- `AppendRows` allocates `geomSet` every row (`go/statement.go:1625`). Build it once in `NextResultSet`.
- Return many rows per `AppendRows` call (loop until the builder reaches the target batch size) instead of one; driverbase supports it.
- Per-column appender closures chosen once from the schema, not a type switch per cell.
- `PREFETCH_ROWS` is hard-coded to 10000 (`go/database.go:172`). Expose `oracle.prefetch_rows`.

### 4.2 Columnar decode in our own go-ora fork

- Fork go-ora under codeflowvt, starting from jatorre's commit currently pinned in `go.mod`, so the UDT fixes from PR #721 come with it. Repoint the `replace`.
- Add a fetch API that hands raw column buffers for each fetched row block to a decoder interface, bypassing `driver.Value` boxing.
- Implement decoders in this repo that write straight into Arrow builders: NUMBER bytes to Decimal128/Int64 without going through a string; DATE/TIMESTAMP bytes to epoch integers; VARCHAR2 bytes appended directly to the string builder's value buffer.
- Keep the database/sql path as a fallback behind `oracle.fetch_mode=sql|columnar`, default `columnar` once it passes the Phase 3 type suite byte-for-byte.

Exit criterion: at least 3x the Phase 4 baseline rows/s on the benchmark table, with allocations per row near zero for fixed-width columns.

### 4.3 Parallel extract via ExecutePartitions

> **DONE 2026-11-01.** Measured 4.3x to 4.7x on a 500,000 row table at four
> slices, with the cache warmed and a single slice confirmed to match serial
> within noise. Eight slices are no faster than four, hence the default of 4.
> Two deviations from the plan as written, both for correctness:
>
> - A query that cannot be split safely returns a single partition rather than
>   `StatusNotImplemented`, so a caller can always use this path.
> - The SCN is validated by describing the query at it before any descriptor is
>   issued. `TIMESTAMP_TO_SCN` is the only source an ordinary user has and it
>   can name an SCN from before the table's last DDL, which is unreadable
>   (ORA-01466, about four seconds after a `CREATE TABLE`). Such a query falls
>   back to one unsplit read.
>
> See `docs/phase4-read-baseline.md` for the numbers.

- Implement `ExecutePartitions`: given a query over a single table, split by partition (ALL_TAB_PARTITIONS), else by ROWID ranges from DBMS_PARALLEL_EXECUTE-style extent chunking, else by `ORA_HASH(ROWID, n-1)`.
- Partition descriptors are serialised bytes; `ReadPartition` on any connection executes its slice.
- Option `oracle.partition_count`. Document consistency: slices are read at the same SCN using `AS OF SCN` captured at `ExecutePartitions` time.

Tests: union of all partitions equals the full query result (row count and checksum) on partitioned and heap tables.

## Phase 5: packaging

- Regenerate `pkg/oracle/driver.go` from the arrow-adbc v1.11.0 template (it was generated from v1.9.0).
- Makefile: add `build-windows` producing `adbc_driver_oracle.dll` (`GOOS=windows`, cgo with mingw-w64), and arm64 targets for macOS and Linux.
- CI: add the Oracle Free 23ai service container and run integration tests with `ORACLE_TEST_DSN`. Add `-race` to unit tests.
- Write a driver manifest TOML for ADBC driver-manager discovery.
- Update README import paths from `jatorre` to `codeflowvt`, and document all options.

## Out of scope for this plan

ODBC and JDBC facades. They start after Phase 1 and Phase 2 are merged, since both assume session affinity, transactions and SQLSTATE mapping. Separate design doc.
