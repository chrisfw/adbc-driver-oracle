# adbc-driver-oracle

An [ADBC](https://arrow.apache.org/adbc/) driver for Oracle Database, written in Go.

**Key feature:** Client-side SDO_GEOMETRY to WKB conversion via direct UDT wire decode — no server-side `Get_WKB()` or `SDO_UTIL.TO_WKBGEOMETRY()` needed.

## Architecture

```
Oracle DB ──(TNS wire)──> go-ora (UDT decode) ──> SdoGeometry struct ──> WKB ──> geoarrow.wkb Arrow column
                                │                                                         │
                                └── pure Go, no Oracle Instant Client            DuckDB adbc_scanner
```

## Features

- **Pure Go** — no Oracle Instant Client or CGO dependency (uses [go-ora](https://github.com/sijms/go-ora))
- **Oracle Autonomous Database** — wallet/TLS authentication supported
- **SDO_GEOMETRY → geoarrow.wkb** — points, lines, polygons, multi-geometries decoded from the wire protocol
- **SRID detection** — extracted from the geometry's wire data, embedded as PROJJSON CRS in Arrow metadata
- **ADBC metadata** — GetObjects, GetTableSchema, GetTableTypes, catalog/schema navigation
- **Transactions** — Commit/Rollback support

## Performance

Tested against Oracle Autonomous DB (Ashburn), FIRES_WORLDWIDE table (333K rows, 15 columns):

| Metric | Value |
|--------|-------|
| `SELECT *` with geometry | **60,000+ rows/sec** |
| Baseline (oracledb + WKT) | 11,200 rows/sec |
| Speedup | **5.4x** |

## Installing

```bash
go get github.com/chrisfw/adbc-driver-oracle@v0.3.0
```

Then add one `replace` to your `go.mod`:

```
replace github.com/sijms/go-ora/v2 => github.com/jatorre/go-ora/v2 v2.9.1-0.20260719131444-0a202851613c
```

**This is required, not optional.** The driver needs a patched go-ora, and Go
ignores `replace` directives declared by a dependency: only the main module's
replaces apply. Without it the build fails on

```
connector.WithWallet undefined (type *go_ora.OracleConnector has no field or method WithWallet)
```

which is an unhelpful way to be told a dependency is the wrong version. Two
things in the fork are needed and only one of them is visible at compile time:

- `WithWallet`, which is the error above.
- Fixes for UDT and VARRAY binds, which compile fine against upstream and then
  fail at run time. A polygon with more than about sixteen vertices raises
  ORA-00600; a bulk insert of `SDO_GEOMETRY` with mixed NULL rows raises
  ORA-03146; a SELECT including NULL geometry desyncs the result stream.

So a build that compiles without the `replace` is not a build that works.

Removing this requirement means publishing the go-ora fork under its own module
path so it can be required directly rather than substituted. That is tracked in
`docs/remediation-plan.md` and is not done yet.

### Minimal consumer

```go
import (
    "github.com/apache/arrow-go/v18/arrow/memory"
    oracle "github.com/chrisfw/adbc-driver-oracle/go"
)

drv := oracle.NewDriver(memory.DefaultAllocator)
db, err := drv.NewDatabase(map[string]string{
    oracle.OptionDSN: "oracle://user:pass@host:1521/SERVICE",
})
```

The package builds with `CGO_ENABLED=0`.

## Usage

### CLI test harness

```bash
go build -o oracle-adbc-test ./cmd/

# Direct connection
./oracle-adbc-test -dsn "oracle://user:pass@host:1521/service" -query "SELECT * FROM my_table"

# Oracle Autonomous DB with wallet
./oracle-adbc-test \
  -dsn "oracle://ADMIN:pass@adb.us-ashburn-1.oraclecloud.com:1522/service?ssl=enable&ssl verify=false&wallet=/path/to/wallet" \
  -query "SELECT * FROM my_table"

# Benchmark mode
./oracle-adbc-test -dsn "..." -benchmark -query "SELECT * FROM large_table"
```

### As a Go library

```go
import (
    oracle "github.com/chrisfw/adbc-driver-oracle/go"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

drv := oracle.NewDriver(memory.DefaultAllocator)
db, _ := drv.NewDatabase(map[string]string{
    oracle.OptionDSN: "oracle://user:pass@host:1521/service",
})
cnxn, _ := db.Open(ctx)
stmt, _ := cnxn.NewStatement()
stmt.SetSqlQuery("SELECT * FROM my_table")
reader, _, _ := stmt.ExecuteQuery(ctx)
// reader is an Arrow RecordReader with geoarrow.wkb geometry columns
```

## Bound parameters

`Bind` plus `ExecuteUpdate` runs the statement once per bound row, sent as a
single Oracle array execute rather than a round trip each. A 10,000-row bound
`INSERT` is one execution at the server, which `V$SQL.EXECUTIONS` confirms.

Values are bound in their native Oracle form, using the same converters as
bulk ingest. A string column whose widest value exceeds the server's inline
`VARCHAR2` limit binds as a CLOB instead, decided once per batch before
anything is sent; a prepared statement reused across batches of differing
widths is re-prepared rather than having a parameter change bind type
mid-stream.

`ExecuteQuery` with more than one bound row returns `StatusNotImplemented`.
ADBC specifies one result set per bound row and this driver can return only
one; it previously answered for row 0 and discarded the rest, which is wrong
data rather than a missing feature. Bind a single row to query, or use
`ExecuteUpdate` to run the statement for every row.

`GetParameterSchema` is unimplemented: go-ora describes no bind metadata.

## Errors

Failures carry the Oracle error code in `adbc.Error.VendorCode` and a SQL:2003
SQLSTATE in `adbc.Error.SqlState`, alongside the ADBC status. The SQLSTATE is
what ODBC and JDBC consumers switch on, so it is the more portable signal:

| Oracle | SQLSTATE | ADBC status |
|--------|----------|-------------|
| ORA-00001, ORA-02290 | 23000 | Integrity |
| ORA-01400 | 23502 | Integrity |
| ORA-02291, ORA-02292 | 23503 | Integrity |
| ORA-00942 | 42S02 | Not Found |
| ORA-00904 | 42S22 | Not Found |
| ORA-00955 | 42S01 | Already Exists |
| ORA-01017 | 28000 | Unauthenticated |
| ORA-01031, ORA-01045 | 42000 | Unauthorized |
| ORA-01013 | HY008 | Cancelled |
| ORA-12899 | 22001 | Invalid Data |
| ORA-01438 | 22003 | Invalid Data |
| ORA-01722 | 22018 | Invalid Data |
| ORA-01858, ORA-01861 | 22007 | Invalid Data |
| ORA-00060, ORA-08177 | 40001 | Internal |
| ORA-12000 to ORA-12999 | 08001 | I/O |
| ORA-00028, ORA-031xx | 08S01 | I/O |
| anything else | HY000 | unchanged |

ORA-00060 and ORA-08177 mean the transaction should be retried. ADBC has no
retryable status, so SQLSTATE 40001 is the signal to act on rather than the
status.

A context deadline maps to Timeout, a cancelled context to Cancelled, and a
transport failure with no Oracle code to 08S01. Those carry no vendor code,
since Oracle never saw them.

## NUMBER mapping

`oracle.number_mapping` decides how Oracle `NUMBER` columns are typed. **The
default is `decimal`**, which differs from the upstream driver, where it was
`float`.

| Value | NUMBER(p,0), p<=18 | NUMBER(p,s) | unconstrained NUMBER, negative scale |
|-------|--------------------|-------------|--------------------------------------|
| `decimal` (default) | Int64 | Decimal128(p,s) | String |
| `float` | Int64 | Float64 | Float64 (String for integral p>18) |
| `string` | String | String | String |

Oracle `NUMBER` is base-10 with up to 38 significant digits, and go-ora hands
it over as a full-precision decimal string, so `float` discarded exactness that
had already arrived: a `NUMBER(12,2)` holding a monetary amount became a
float64 and stopped summing correctly. `decimal` keeps every digit, falling
back to String only where precision and scale cannot describe a Decimal128,
which is unconstrained `NUMBER` and negative scales. Set `float` to get the
previous shape back.

### Unconstrained NUMBER

A `NUMBER` declared without precision and scale has a *per-value* scale, and
no Arrow decimal can hold that: all four widths carry one fixed precision and
scale for the whole column, because the format requires every value in a
column to share a scale. The same is true of the latest Arrow, so there is no
version to upgrade to that changes it.

Instead the driver samples the data. go-ora renders `NUMBER` as a plain
decimal string, so the first rows show the digits actually present and a real
decimal is chosen from them. `oracle.number_scan_rows` controls the sample
(default 1024; `0` disables it and such columns stay text). This matters most
for expressions: Oracle reports a literal or computed value as `NUMBER` with a
floating scale, so `SELECT 42`, `COUNT(*)`, `SUM(x)` and arithmetic are typed
from their own digits rather than arriving as text.

Precision is taken at the type maximum rather than fitted to the sample, so a
later row has room. `Decimal128` is used where 38 digits suffice, `Decimal256`
to 76, and anything wider stays text, which `NUMBER` can reach: `9.99E125`
renders as 126 integer digits.

**A sample cannot promise anything about later rows**, and Arrow cannot change
a column's type mid-stream, so a value past the inferred scale has to be
handled. `oracle.number_overflow` decides how:

| Value | Behaviour |
|-------|-----------|
| `error` (default) | Fail the read, naming the column, the value and the options that change it |
| `null` | Null that value, log once, keep streaming |

There is deliberately no option to round into the inferred scale. Arrow's own
string parse does exactly that silently (`1.239` into a `decimal(38,2)` stores
`1.24` and reports no error), and a rounded value looks correct while being
wrong, which is worse than either outcome above. The driver checks the fit
itself before appending.

`GetTableSchema` samples the table the same way, so it keeps describing the
batches queries return.

`GetTableSchema` and `ExecuteQuery` agree on the result under every mapping,
including for `FLOAT`, which Oracle stores as `NUMBER`. `BINARY_FLOAT` and
`BINARY_DOUBLE` are genuine IEEE floats and are unaffected by this option.

## TIMESTAMP precision

`oracle.timestamp_unit` selects the Arrow time unit for `TIMESTAMP` columns:
`us` (default) or `ns`. Oracle `DATE` has second granularity and is always
microseconds, whatever this is set to.

It is a connection-wide policy rather than being derived from each column's
declared `TIMESTAMP(p)`, and that is forced by go-ora: it reports one wire type
for every TIMESTAMP precision and exposes no scale for it, so only the data
dictionary knows `p`. Choosing the unit per column from the dictionary would
make `GetTableSchema` describe a column differently from the batches
`ExecuteQuery` returns for it, which is the class of bug this driver has
deliberately eliminated.

`us` is lossless for `DATE` and for `TIMESTAMP` up to `p=6`, which is Oracle's
own default precision, and it covers Oracle's whole date range. Set `ns` to
keep all nine digits of a `TIMESTAMP(7..9)`, but note that a nanosecond Arrow
timestamp spans only **1677-09-21 to 2262-04-11**, and values outside it do not
fail: Arrow wraps them silently, so a year-1500 date becomes a plausible but
wrong instant. Only choose `ns` when the data is known to fall inside that
window.

### Recovering the original time zone

`TIMESTAMP WITH TIME ZONE` is normalised to UTC on the way out, so the instant
is never ambiguous, but the zone it was written in is lost. Set
`oracle.tz_offset_column=true` to emit a sibling String column named
`<col>__tz` immediately after each such column, holding the IANA region Oracle
stored (`America/New_York`) or a fixed offset (`+05:30`) when that is what was
stored. `GetTableSchema` reports the sibling too, so it still describes the
batches queries return.

`TIMESTAMP WITH LOCAL TIME ZONE` gets no sibling. Oracle normalises it to the
database zone on the way in and renders it in the session zone on the way out,
so the original is gone before the driver sees it and the column would only
repeat the session's own offset.

## Character sets

Text comes out as UTF-8 regardless of the database character set; go-ora
converts on the way through. Verified against an AL32UTF8 database with
AL16UTF16 `NCHAR`, which is the Autonomous Database default, across
`VARCHAR2`, `NVARCHAR2` and `CLOB`, with Latin-1, Cyrillic, CJK, right-to-left,
combining-mark and 4-byte emoji content, in both directions through bound
parameters and back.

Two Oracle behaviours to expect rather than debug:

- **The empty string is NULL.** Oracle does not distinguish them for character
  types, so a round-tripped `""` comes back as a null. This is why ingest also
  skips `NOT NULL` on string columns.
- **Column widths are bytes by default.** `VARCHAR2(4000)` holds 4000 *bytes*,
  so about 1333 three-byte characters. Declare `VARCHAR2(n CHAR)` to size in
  characters.

## LOBs

`CLOB`, `NCLOB` and `BLOB` map to Arrow `LargeString` and `LargeBinary`, which
use 64-bit offsets. A `String` or `Binary` column addresses its values with
int32 offsets, so a batch carrying more than 2 GiB in one column overflows
them, and a LOB column is exactly where that much data comes from. `VARCHAR2`
and `RAW` cap well inside int32 and keep the cheaper 32-bit layout.

`oracle.lob_fetch` chooses how values are retrieved:

| Value | Behaviour |
|-------|-----------|
| `inline` (default) | LOB data arrives with its row. No extra round trip, but a batch of large LOBs is held in memory at once. |
| `stream` | Each LOB is fetched after its row. Lower peak memory for multi-megabyte values, at one round trip each. |

This is a mode rather than a byte threshold because that is what go-ora
exposes (`LOB FETCH=INLINE|STREAM`); it has no prefetch-size knob for a byte
option to set.

## Intervals

`INTERVAL YEAR TO MONTH` arrives as Arrow `MonthInterval`, a count of months,
and `INTERVAL DAY TO SECOND` as `Duration` in microseconds. Both were String
before, which reads fine and cannot be computed with.

Microseconds rather than nanoseconds is a limit of go-ora, not a choice:
it decodes an interval to Oracle's text form and renders at most 6 fractional
digits whatever the column declares. An `INTERVAL DAY(2) TO SECOND(9)` holding
`.123456789`, which Oracle's own `TO_CHAR` shows in full, reaches the driver as
`.123456`. `oracle.timestamp_unit=ns` therefore does not apply to intervals,
since nothing could fill the extra digits.

A duration also spans at most about 106751 days. An `INTERVAL DAY(9) TO SECOND`
can hold more; values beyond that come back NULL rather than wrapped into a
wrong number.

Ingest in the other direction does not yet map Arrow `Duration` or
`MonthInterval` onto Oracle interval columns; those still become
`VARCHAR2` text.

## Interop options

Three options change the Arrow types in ways a consumer may not handle. All
are off by default, and all are safe to turn on once the consumer is known to
cope.

| Option | Effect | Risk if the consumer is old |
|--------|--------|------------------------------|
| `oracle.compact_decimals` | A declared `NUMBER(p,s)` narrows to `Decimal32` (to 9 digits) or `Decimal64` (to 18) instead of always `Decimal128`. Four bytes a value instead of sixteen for a `NUMBER(4,2)`. | The C data interface spells a non-128 decimal as `d:<precision>,<scale>,<bits>`; a consumer that only parses 128-bit decimals rejects the column. |
| `oracle.timestamp_with_offset` | `TIMESTAMP WITH TIME ZONE` becomes the canonical `arrow.timestamp_with_offset` extension, carrying the UTC instant and the original offset in one column. Supersedes `oracle.tz_offset_column`. | A consumer that does not know the extension sees a bare `Struct{timestamp, offset_minutes}` rather than a timestamp. |
| `oracle.opaque_number` | An unconstrained `NUMBER` that could not be inferred is labelled `arrow.opaque` over `utf8` instead of being a plain `utf8` column, so it records that it is an Oracle `NUMBER`. | Less additive than it looks: across the C data interface an unknown extension degrades to its storage type, but in-process the concrete array becomes an extension array and arrow-go's `ExtensionArrayBase.ValueStr` **panics**. Anything doing generic value extraction breaks. |

## Connections and sessions

Each ADBC connection owns one Oracle session for its whole life, checked out of
the database's pool by `db.Open` and handed back by `cnxn.Close`. That is what
makes session state mean anything: `ALTER SESSION`, `CURRENT_SCHEMA`, temporary
tables and transactions all belong to the connection that set them and are
invisible to every other connection.

`oracle.pool_size` (default 8) therefore bounds concurrent connections plus the
extra sessions ingest workers borrow, roughly `connections * (1 +
oracle.ingest_workers)`.

Bulk ingest is the one place a connection uses more than its own session, and
only when autocommit is on: each worker inserts and commits on a session of its
own, which is what overlaps round-trip latency on remote listeners. With
autocommit off the rows must land in the caller's transaction, so ingest runs
serially on the connection's own session and the caller's `Commit` decides the
outcome.

## Transactions

Autocommit is on by default, as the ADBC spec requires. In that mode go-ora
sets Oracle's commit-on-success flag on every DML execute, so each statement
commits itself and `Commit`/`Rollback` return `StatusInvalidState`.

Setting `adbc.connection.autocommit` to `false` makes the connection
transactional. The transaction begins at the next statement on the connection's
own session, every later statement joins it, and `Commit` or `Rollback` ends
it; the statement after that begins a fresh one. Switching autocommit back on
commits whatever is pending. Closing a connection with an open transaction
discards the uncommitted work.

`adbc.connection.current_db_schema` issues `ALTER SESSION SET CURRENT_SCHEMA`
on the connection's own session, so it affects only that connection. The name
is sent as a quoted identifier: a bare name is upper-cased the way Oracle folds
an unquoted identifier (`hr` and `HR` both mean `HR`), and a name you wrap in
double quotes yourself is used verbatim for a genuinely mixed-case schema.

`adbc.connection.transaction.isolation_level` accepts `read_committed` (the
Oracle default) and `serializable`. The other ADBC levels have no Oracle
equivalent and are refused with `StatusNotImplemented` rather than silently
downgraded. The level applies to the next transaction begun, not to one already
open, since Oracle's `SET TRANSACTION` only works as a transaction's first
statement.

Two Oracle behaviours to know about in transactional mode:

- **DDL commits implicitly.** `CREATE TABLE` and `DROP TABLE` in Oracle commit
  before and after themselves, so an ingest in `create` or `replace` mode also
  commits whatever the caller had pending. Use `append` against a pre-existing
  table to keep an ingest fully inside your transaction.
- **Bulk ingest runs serially.** With autocommit on, each ingest worker inserts
  and commits on a session of its own. With autocommit off the rows must land
  in the caller's transaction and only the connection's own session is in it,
  so `oracle.ingest_workers` is ignored and the redo-log `NOLOGGING`
  optimisation is skipped (it is DDL, and would commit the rows).

## Supported Oracle Types

| Oracle Type | Arrow Type | Notes |
|------------|------------|-------|
| NUMBER (integer, p<=18) | Int64 | |
| NUMBER (p,s), FLOAT | Decimal128(p,s) | Default mapping; see NUMBER mapping above |
| NUMBER unconstrained, negative scale | String | No declared p,s to build a decimal from |
| BINARY_FLOAT | Float32 | IEEE single, not widened |
| BINARY_DOUBLE | Float64 | |
| VARCHAR2, CHAR, NVARCHAR2, NCHAR | String | |
| CLOB, NCLOB, LONG | LargeString | 64-bit offsets |
| ROWID, UROWID | String | |
| DATE | Timestamp (us, no TZ) | Oracle DATE is wall-clock time; always us |
| TIMESTAMP(p) | Timestamp (us or ns, no TZ) | Unit from `oracle.timestamp_unit` |
| TIMESTAMP(p) WITH TIME ZONE | Timestamp (us or ns, UTC) | Normalised to UTC |
| TIMESTAMP(p) WITH LOCAL TIME ZONE | Timestamp (us or ns, UTC) | Normalised to UTC |
| INTERVAL YEAR TO MONTH | MonthInterval | Count of months |
| INTERVAL DAY TO SECOND | Duration (us) | go-ora caps the fraction at 6 digits |
| RAW | Binary | Capped at 32767 bytes |
| BLOB, LONG RAW, BFILE | LargeBinary | 64-bit offsets |
| BOOLEAN (Oracle 23c+) | Boolean | |
| SDO_GEOMETRY | Binary (geoarrow.wkb) | With SRID as PROJJSON CRS |

## Bulk Ingest (Arrow → Oracle)

`adbc.ingest.target_table` + `Bind`/`BindStream` creates the destination table
from the Arrow schema:

| Arrow Type | Oracle Type | Notes |
|------------|-------------|-------|
| Int8..Int64, Uint8..Uint32 | NUMBER(19) | |
| Uint64 | NUMBER(20) | Full uint64 range |
| Float16/32 | BINARY_FLOAT | |
| Float64 | BINARY_DOUBLE | |
| Decimal128/256 | NUMBER(p,s) | p capped at 38 |
| String, LargeString | VARCHAR2(4000) or CLOB | See string sizing below |
| Binary, LargeBinary, FixedSizeBinary | BLOB | |
| Boolean | NUMBER(1) | Bound as 0/1 |
| Timestamp (no TZ) | TIMESTAMP(6) — TIMESTAMP(9) for ns | Bound as native TIMESTAMP (wall clock preserved) |
| Timestamp (with TZ) | TIMESTAMP(6/9) WITH TIME ZONE | Bound as native TSTZ (instant preserved) |
| Date32/Date64 | DATE | |
| Binary + `geoarrow.wkb` metadata | MDSYS.SDO_GEOMETRY | Client-side WKB → SDO UDT |
| Other (Time, Interval, …) | VARCHAR2(4000) | Textual form |

### String column sizing

Oracle `VARCHAR2` holds at most 4000 bytes; wider values raise `ORA-12899`.
By default (`auto`) the driver scans the bound data and creates `CLOB` for any
string column with a value over 4000 bytes. A single bound batch (`Bind`) is
scanned in full; a bound stream (`BindStream`) is scanned up to
`oracle.ingest.type_scan_limit_bytes` (64 MiB, may overshoot by one batch) —
the scanned records are buffered and inserted first, then the rest streams
through. With the scan disabled (`0`), `LargeString` columns map to `CLOB` and
regular strings to `VARCHAR2(4000)`. Statement options:

| Option | Values | Default |
|--------|--------|---------|
| `oracle.ingest.string_type` | `auto`, `varchar2`, `clob` | `auto` |
| `oracle.ingest.clob_columns` | comma-separated column names forced to CLOB | — |
| `oracle.ingest.type_scan_limit_bytes` | stream bytes the auto scan may buffer; `0` disables the scan | `67108864` |

If wide values appear only deep in a large stream (beyond the scan window),
force those columns with `oracle.ingest.clob_columns` — the ORA-12899 error
message names this option when the auto scan guesses wrong.

## How Geometry Works

1. On connection open, the driver registers `MDSYS.SDO_GEOMETRY` and its sub-types as go-ora UDTs
2. go-ora decodes Oracle's binary TNS wire format directly into `SdoGeometry` Go structs
3. The driver converts `SdoGeometry` → WKB (Well-Known Binary) client-side
4. The result is an Arrow `binary` column with `geoarrow.wkb` extension metadata and PROJJSON CRS

Supported geometry types: Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, optimized rectangles, and arc densification.

## Development

```bash
# Run tests
go test -v ./go/

# Build
go build -o oracle-adbc-test ./cmd/
```

## Credits

Developed by [CARTO](https://carto.com/).

## License

Copyright 2025 CARTO. Licensed under the Apache License 2.0.
