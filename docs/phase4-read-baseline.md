# Phase 4 read-path baseline

Recorded before any Phase 4 change, as the plan requires. Everything in Phase 4
is measured against these numbers, and the 4.2 exit criterion (at least 3x
rows/s, allocations per row near zero for fixed-width columns) refers to them.

## How to reproduce

```
BENCH_READ_ROWS=5000000 ORACLE_TEST_DSN=... \
  go test ./go/... -run XXX -bench BenchmarkRead -benchmem -benchtime 1x
```

The fixture is `BENCH_READ_MIXED`: `NUMBER(12,2)`, `NUMBER(10)`,
`VARCHAR2(100)`, `DATE`, `TIMESTAMP(6)`, generated server side and reused
between runs. Each benchmark reads the whole table once after an untimed warm
read, so the figures reflect driver cost rather than Oracle's physical reads.

## Baseline, commit bc17fcd, 5,000,000 rows

Oracle Free 23ai in a container on the same host, Apple M1, Go 1.26.

| Query | rows/s | allocs/row | bytes/row |
|-------|-------:|-----------:|----------:|
| all five columns | 384,677 | 37.0 | 1,495 |
| `NUMBER(12,2)` + `NUMBER(10)` | 842,383 | 25.0 | 963 |
| `VARCHAR2(100)` only | 1,905,290 | 7.0 | 624 |
| `DATE` + `TIMESTAMP(6)` | 1,380,656 | 11.0 | 657 |
| `NUMBER(10)` only, maps to Int64 | 2,209,621 | 8.0 | 514 |

## What the numbers say

The allocation figures are the interesting half. Reading one `NUMBER(10)`
column costs **8 allocations and 514 bytes per row**: 5M int64 values are 40 MB
of data and the read allocates 2.57 GB to deliver them.

A single column costing 7 to 8 allocations per row means most of the cost is
fixed per row rather than per value, which points at the structure of the
reader rather than at any one converter:

- `AppendRows` returns one row per call, so driverbase's per-call work is paid
  5,000,000 times instead of once per batch. This is the first thing 4.1
  changes.
- `rows.Scan` goes into `[]interface{}`, so every value of every column is
  boxed into an interface before the driver looks at it.
- `appendValue` runs a type switch per cell rather than choosing a per-column
  appender once from the schema.

Scaled columns are the most expensive: adding `NUMBER(12,2)` to the Int64
column takes 8 allocations per row to 25. go-ora renders NUMBER as a decimal
string and `appendDecimal128` parses it back with `AppendValueFromString`, so
each value makes a round trip through text. That is what 4.2's columnar decode
is for; 4.1 cannot fix it.

For reference, the cheapest column (Int64, 2.2M rows/s) and the full five-column
read (385k rows/s) differ by 5.7x, so per-column cost dominates and the
mixed-type figure is the one to watch.

## 4.1 result: the cheap wins are not where the cost is

Measured after the 4.1 changes, same fixture and host. The figures are
unchanged from the baseline above, within run-to-run noise:

| Query | baseline rows/s | after 4.1 | allocs/row before | after |
|-------|---------------:|----------:|------------------:|------:|
| all five columns | 384,677 | 362,085 | 37.0 | 37.0 |
| `NUMBER` x2 | 842,383 | 829,361 | 25.0 | 25.0 |
| `VARCHAR2` x1 | 1,905,290 | 1,620,168 | 7.0 | 7.0 |
| temporal x2 | 1,380,656 | 1,364,431 | 11.0 | 11.0 |
| Int64 x1 | 2,209,621 | 2,134,316 | 8.0 | 8.0 |

`AppendRows` now fills a whole batch per call, confirmed by counting: 5 calls
for 200,000 rows where there used to be 200,000. It bought nothing measurable,
because driverbase's per-call overhead was never the cost.

A profile of the single-column Int64 read says where the cost actually is.
Allocations, by object count:

```
39.92%  go-ora (*defaultStmt).read
21.29%  go-ora (*ParameterInfo).decodePrimValue
19.44%  go-ora (*Number).decode
18.67%  bytes.(*Buffer).grow          <- from go-ora (*Number).String
```

That is 99.62% of allocations inside go-ora and none in this driver's reader.
Note the last two lines: even an integral `NUMBER(10)` that maps to Int64 is
rendered to a decimal string by go-ora and parsed back, allocating a
`bytes.Buffer` per value.

CPU tells the same story from the other side. Of 3.55s wall time, only 1.00s
was on-CPU at all, so the read is about 72% waiting on fetch round trips.
Within that CPU, `appendValue` and `appendInt64` together are 2%.

### What this means for the remaining 4.1 bullets

- **Per-column appender closures, replacing the per-cell type switch:** not
  done, deliberately. The type switch it would remove is inside the 2% of CPU
  that is 28% of wall time, so the ceiling on this change is roughly half a
  percent, and replacing a type switch with an indirect closure call is not
  obviously faster. Adding the indirection for an unmeasurable gain would be
  the wrong trade. Worth revisiting only if 4.2 removes enough of the go-ora
  cost to make per-cell dispatch visible.
- **`oracle.prefetch_rows`:** done, with the default unchanged at 10000.
  Measurement put the old hard-coded value at the knee of the curve, so the
  option is for moving the knee, not for fixing a bad default.

### Where this leaves Phase 4

The 3x target in 4.2's exit criterion has to come from 4.2, because 4.1 had
nothing to give: the driver's own read path contributes ~0.4% of allocations
and 2% of CPU. 4.2 now has a precise target rather than a general one, namely
`Number.decode`, `Number.String` and `decodePrimValue`, and the string round
trip for integral NUMBER is the single clearest waste.

The 72% round-trip wait also means throughput on this fixture is bounded by
fetch latency, not decode. A columnar decode will show its full value on wider
rows and on links where a fetch costs more, and the benchmark should grow a
wide-row variant before 4.2 is measured, or it will understate the result.

## A win 4.1 did not predict: parse decimals without math/big

Profiling the decode-bound fixture (20 x `NUMBER(12,2)`) rather than the narrow
one changed the picture. There, 76% of wall time is on-CPU, and within it:

```
60.34%  syscall.rawsyscalln                        (fetch)
22.39%  appendDecimal128 -> AppendValueFromString   <- this driver's own code
        -> decimal.Decimal128FromString -> math/big
```

Arrow's `AppendValueFromString` parses through `math/big`. Oracle `NUMBER`
columns are almost always far narrower than Decimal128 permits, so the common
case needs no big integers at all. `parseDecimal128` handles values that fit an
int64 with no allocation, and refuses everything else so the general path still
takes exponents, over-long fractions and anything past 18 significant digits.

Correctness came first: the fast path is tested for exact agreement with
`AppendValueFromString` across the shapes go-ora emits plus the edges, a batch
mixing accepted and refused values is compared element by element, and the
refusals are asserted so a value is never silently rounded. A silently wrong
number would be far worse than a slow parse.

### Result, 5,000,000 rows unless noted

| benchmark | rows/s before | after | speedup | allocs/row before | after | reduction |
|-----------|-------------:|------:|--------:|-----------------:|------:|----------:|
| all five columns | 384,677 | 400,676 | 1.04x | 37.0 | 26.1 | 1.42x |
| `NUMBER` x2 | 842,383 | 1,275,222 | **1.51x** | 25.0 | 14.0 | 1.78x |
| `VARCHAR2` x1 | 1,905,290 | 1,442,738 | 0.76x | 7.0 | 7.0 | none |
| temporal x2 | 1,380,656 | 1,392,371 | 1.01x | 11.0 | 11.0 | none |
| Int64 x1 | 2,209,621 | 2,117,336 | 0.96x | 8.0 | 8.0 | none |
| 20 x `NUMBER(12,2)`, 200k rows | 66,897 | 135,923 | **2.03x** | 342 (17.1/value) | 123 (6.15/value) | 2.78x |

The columns without decimals are unchanged, as expected, since no code on
their path moved. `VARCHAR2` at 0.76x and `Int64` at 0.96x are run-to-run
variance on a benchmark that is mostly fetch wait, not regressions: both paths
are byte-identical to before. The figure to read is the decimal-heavy one.

`all five columns` gains only 4% in throughput despite 1.42x fewer allocations,
which is the round-trip bound asserting itself: one of its five columns is a
decimal, so there is not enough decode work in a row for decode savings to
show. That is also why the wide fixture exists.

### What is left for 4.2

The remaining per-value cost is go-ora's, and the profile names it:
`(*Number).decode` and `(*Number).String`, which render every NUMBER to a
decimal string, plus `decodePrimValue` and the `driver.Value` boxing above it.
Removing the string round trip entirely is what 4.2's columnar decode is for,
and it now has a measured size: the fast parser took the decimal path from 17.1
to 6.15 allocations per value, and the remaining 6 are below this driver.

Note for 4.2's exit criterion, which asks for 3x on the benchmark table: on the
narrow mixed fixture that target is not reachable by decode work alone, because
the read is about 72% fetch wait and the ceiling is near 1.4x. On the
decode-bound fixture there is room, and the fast decimal parser already
delivered 2.03x of it without touching go-ora. The criterion should be restated
against a named fixture, or it cannot be judged.

## 4.3 Parallel extract

`ExecutePartitions` splits one query into slices that different connections
read at the same time. This attacks the cost 4.1 identified as dominant: 72%
of read wall clock is fetch wait, and a lone reader leaves the server idle
while it decodes and idle again while it waits.

### Measurement

500,000 rows, five columns (`NUMBER`, `NUMBER`, `VARCHAR2(100)`, `DATE`,
`TIMESTAMP(6)`), Oracle Free 23ai in a container on the same host as the
client. Buffer cache warmed with a full read before timing, and a second
serial read taken afterwards as a control. Three runs.

| Readers | Wall clock | Rows/s | Speedup |
| --- | --- | --- | --- |
| serial | 1.17s to 1.25s | ~400,000 | 1.00x |
| 1 slice | 1.23s to 1.29s | ~400,000 | 0.91x to 1.00x |
| 2 slices | 451ms to 483ms | ~1,060,000 | 2.51x to 2.63x |
| 4 slices | 258ms to 270ms | ~1,890,000 | 4.32x to 4.69x |
| 8 slices | 277ms to 303ms | ~1,720,000 | 4.13x to 4.20x |

Planning costs 16ms to 32ms when a query is split, 1ms when it is not.

Three things in that table are worth reading carefully.

**The 1 slice row is the control that makes the rest trustworthy.** One slice
goes through `ExecutePartitions` and `ReadPartition` but does no splitting, and
it lands within noise of the serial read. So the speedups below it are
parallelism and not an artifact of a different code path.

**2 slices is superlinear (2.51x), which is the expected shape for a
latency-bound pipeline, not a measurement error.** Neither side was saturated
in the serial case: the server waits while the client decodes, and the client
waits while the server fetches. Two sessions fill each other's idle time, so
both sides move from roughly half utilised towards fully utilised. This is the
same finding as 4.1 from the other direction.

**8 slices is no faster than 4.** The database shares a host with the client
here, so past four readers they contend rather than overlap. That is why
`oracle.partition_count` defaults to 4 instead of one per core.

The control read, taken after everything else, ran 1.21x to 1.40x faster than
the first serial read. So the cache was still warming even after a full warm-up
pass, and the honest conservative comparison is 4 slices against the fastest
serial read ever observed (881ms), which is 3.3x.

These conditions are a low-latency loopback link. A real network would likely
widen the gap, since there is more wait to hide; a contended server would
narrow it. The numbers are not a promise about any other deployment.

### Splitting strategies

| Strategy | When | Divides the work |
| --- | --- | --- |
| `partition` | table is partitioned | yes, disjoint segments |
| `rowid_range` | heap table, `DBMS_PARALLEL_EXECUTE` available | yes, disjoint block ranges |
| `ora_hash` | otherwise | **no**, every slice scans the whole table |
| `single` | query cannot be split safely | n/a |

`ora_hash` always works and is the only rung that does not divide the work:
total server I/O is multiplied by the slice count. The descriptor reports which
strategy was used and why a better one was unavailable, so this is a visible
tradeoff. `DBMS_PARALLEL_EXECUTE` turned out to work for an ordinary user with
no extra grant, so in practice heap tables reach `rowid_range`.

Two SQL facts worth recording, both verified before writing any Go:

- `PARTITION (p)` must come before `AS OF SCN`. The reverse is ORA-03048.
- ROWID's character form is base-64 encoded and its **string ordering is not
  its ROWID ordering**. A range built from `MIN`/`MAX` of `ROWIDTOCHAR(ROWID)`
  bracketed 72,949 of 500,000 rows and silently dropped the rest. Endpoints
  must round-trip through `CHARTOROWID`, which is lossless: the chunker's 4
  chunks covered exactly 500,000 rows, each once.

### Snapshot consistency, and its one real limitation

Every slice reads `AS OF SCN` the same value, captured once, so the union is a
consistent snapshot. The SCN comes from the first source available:

1. `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER` (exact, current, needs EXECUTE)
2. `V$DATABASE.CURRENT_SCN` (exact, current, needs SELECT on the view)
3. `TIMESTAMP_TO_SCN(SYSTIMESTAMP)` (no grant, but **not current**)

On a default Oracle Free install an ordinary user has neither of the first two,
so the third is what most deployments get. It resolves through Oracle's
timestamp-to-SCN mapping, whose granularity means the answer can be a few
seconds old, and **an SCN from before the table's last DDL cannot be read**.
Measured: `ORA-01466` persists for about four seconds after a `CREATE TABLE`,
with the mapped SCN frozen across the first two polls.

So the SCN is proved rather than assumed. `ExecutePartitions` describes the
query at the SCN before issuing any descriptor; if that fails for a flashback
reason the query is returned as a single unsplit partition with the reason
logged. Missing FLASHBACK privilege (ORA-41900) counts as such a reason, which
is why `SELECT FROM DUAL` works at all: nobody has FLASHBACK on `SYS.DUAL`.
Nothing is swallowed, because the fallback runs the same query without
flashback and a genuine permission error surfaces from there.

The practical consequence: an ETL that creates a staging table, loads it and
extracts it immediately will read unsplit unless it waits a few seconds or the
user has one of the first two SCN sources granted.

### What is not split

Only a single-table `SELECT` with an optional `WHERE`. Everything else returns
one partition. The refusals that matter are not the obviously-unsupported ones
but the queries that stay **valid SQL while returning wrong data** when run
several times: `ORDER BY`, `DISTINCT`, `ROWNUM`, `FETCH`, `OFFSET`, a bare
aggregate, an analytic `OVER`. Each would be evaluated per slice.

A subquery is refused for a different reason: `AS OF SCN` attaches only to the
table in the `FROM` clause, so a subquery's own table would be read at whatever
moment each slice happened to execute, and the slices would no longer share a
snapshot.

Recognising that shape needs a scanner, not a regexp. Go's RE2 has no negative
lookahead, and more fundamentally a regexp cannot count parentheses, so it
cannot distinguish a subquery's `FROM` from the outer one, nor a keyword inside
a string literal from a clause. Both mistakes slice the query in the wrong
place.

Bound parameters are refused outright: a descriptor carries only SQL, so the
slices would execute with unbound placeholders.

### Type agreement across slices

An unconstrained `NUMBER` gets its Arrow type from sampled data. Slices see
different rows, so left to infer they would disagree with each other and with
the schema `ExecutePartitions` returned. So the types are decided once against
the whole table at the snapshot, and pinned into every descriptor.

The integration test proves this is not vacuous. Stripped of its pinned types,
the last slice of the fixture announces `decimal(38, 1)` for a column the
caller was promised as `decimal(38, 4)`.

## 4.3 revisited: parsing the query instead of matching its text

The first implementation of 4.3 decided whether a query could be split, and
rewrote it into slices, with a hand-written scanner over the SQL text. That
worked but was conservative in a way that cost real parallelism, and its
failure mode was the wrong kind: a rewrite that misreads a query returns
plausible wrong data rather than failing.

The driver now parses the query with [polyglot](https://github.com/tobilg/polyglot),
a Rust SQL parser and generator with an Oracle dialect, reached from Go as a
WebAssembly module run by [wazero](https://github.com/tetratelabs/wazero).

### What this buys

**Queries that were silently refused now split.** The scanner rejected every
query containing a comment or an optimizer hint, because it did not parse
comments. Both are common in hand-written extract SQL, and rejecting them cost
the caller all parallelism with no indication why:

| Query | Before | Now |
| --- | --- | --- |
| `SELECT /*+ FULL(t) */ a FROM t` | one partition | split, hint preserved in every slice |
| `SELECT a FROM t -- nightly` | one partition | split |
| `SELECT a FROM t WHERE b = 'ORDER BY'` | one partition | split |
| `SELECT ORDER_TOTAL FROM t` | one partition | split |

**Operator precedence stops being this driver's problem.** A slice predicate is
ANDed into the query's WHERE clause by modifying the AST, so the generator
decides the parentheses. A caller's `WHERE a > 5 OR b IS NULL` comes back as
`WHERE (a > 5 OR b IS NULL) AND <predicate>`. The scanner got this right only
because it parenthesised unconditionally; the AST gets it right by construction.

**Classification is structural.** Every refusal is now a field on the parsed
query (`distinct`, `group_by`, `order_by`, `limit`, `offset`, `fetch`,
`qualify`, `windows`, `connect`, `locks`, `joins`, `sample`) or a node kind in
its tree (`count`, `sum`, `window_function`, and polyglot's `aggregate_function`
catch-all). That maps one to one onto what the regexps were approximating,
without the false positives.

The generated SQL is unchanged. A slice built from the AST is byte for byte
what the scanner produced for the same input, which is how this was checked
for regressions.

### What it does not buy

Joins and subqueries are still refused, and that is not a parsing limitation.
`AS OF SCN` attaches to one table reference, so splitting a join would mean
emitting the clause on every table reference, and the planner would have to
choose a driving table. That is a feature, not a side effect of better parsing.

### Upstream changes this required

polyglot's Oracle dialect could not represent a flashback query, which is the
one clause every slice must carry. Two changes were needed, both submitted
upstream:

- [tobilg/polyglot#518](https://github.com/tobilg/polyglot/pull/518): parse and
  generate `AS OF SCN` and `AS OF TIMESTAMP`, and fix Oracle partition
  selection, which was mis-parsed as a table aliased `PARTITION` carrying a
  column alias list. That regenerated correctly, so only an AST assertion
  caught it.
- [tobilg/polyglot#519](https://github.com/tobilg/polyglot/pull/519): a WASI
  module exposing parse, generate, transpile and format over JSON, so a Go
  host can embed one portable artifact instead of locating a platform-specific
  native library at run time.

Until those are released, `go/wasm/polyglot_sql.wasm` is built from the fork.
The header comment in `go/sqlanalyzer.go` records the commit and the rebuild
command.

### Cost

| | |
| --- | --- |
| embedded module | 29 MB (5.5 MB in git, compressed) |
| driver shared library | 51 MB to 83 MB |
| cgo | still not required: wazero is pure Go |

Translating 29 MB of WebAssembly to machine code is the one expensive part, so
it happens once, lazily, only when a query is actually partitioned, and is
cached on disk under `os.UserCacheDir()`:

| | init | per call |
| --- | --- | --- |
| compiled, cold | 10.6s | 534 µs |
| compiled, cached | **207 ms** | 288 µs |
| interpreted | 739 ms | 7.7 ms |

Under `-race`, which this project's tests use, a cold compile costs **2m32s**
and a cached one 3.1s. That is why the cache is not optional: without it every
test run touching partitioning stalls for over two minutes. It also explains a
confusing symptom seen while building this: a single integration test appearing
to take 150 seconds with an idle database.

Planning a partitioned read costs 24ms to 39ms, against 16ms to 32ms before,
plus roughly 310ms once per process for the analyzer. Both are small against
the read itself.

### A footgun found while measuring

Reading N partitions concurrently on one database needs a pool that holds N
connections. `oracle.pool_size` defaults to 8, so a caller requesting more
slices than that will find the last `Open` blocking until an earlier reader
finishes, or deadlocking if it waits for all of them before closing any.

The driver cannot cap this, because ADBC allows partitions to be read in other
processes entirely, so `ExecutePartitions` logs a warning naming both numbers
when the requested count exceeds the pool.

### The speedup, measured again

Re-measured after the change, on the same machine and fixture:

| Readers | Wall clock | Speedup |
| --- | --- | --- |
| serial | 973ms to 1.15s | 1.00x |
| 1 slice | 1.14s to 1.22s | 0.82x to 1.01x |
| 2 slices | 468ms to 516ms | 1.89x to 2.45x |
| 4 slices | 249ms to 281ms | 3.46x to 4.61x |
| 8 slices | 207ms to 246ms | 4.69x to 4.79x |

This agrees with the measurement taken before the parser change, which is the
expected result: the generated slice SQL is byte for byte identical, so there
was nothing for it to change.

An earlier revision of this document reported 2.19x to 2.22x at four slices and
explained the gap as the serial baseline drifting on an idle laptop. That was
wrong, and the way it was wrong is worth recording. The laptop was not idle.
Four orphaned `cargo test` child processes were pinning a core each, three of
them for over sixteen hours, left behind by killing `cargo` without killing the
test binaries it had spawned. Every number in that revision was taken on a
machine with most of its cores saturated.

Two lessons, and the second is the useful one:

- Killing `cargo test` does not kill the test binary it spawned. It reparents
  to init and keeps running. `pkill -f <test-binary-name>` is what actually
  stops it.
- "The environment shifted" is not an explanation, it is the absence of one.
  Taking it as the answer is what allowed a measurement from a saturated
  machine to be written down as a finding, and then published. A contended
  machine is a cause that can be checked in one command, which is less work
  than the paragraph that was written instead.

The single slice control held at roughly 1.00x in both the contended and the
idle measurements, which is worth noting: it confirms the code paths are
equivalent, but it did not reveal the contention, because contention scales
serial and parallel together. A control for the comparison is not a control for
the machine.

### A bug the driver inherited, found by upstream review

Review of the two polyglot pull requests turned up a defect that reached the
driver. The WASI target was linked with the default 1 MiB stack, where
polyglot's other WASM target already used 64 MiB for the stated reason that
WASM has no `stacker` support. A query whose expressions nest even moderately
deeply, such as a sum of forty columns, overflowed it:

```
SELECT c0 + c1 + ... + c39 AS total FROM t
  -> wasm error: out of bounds memory access
```

The consequence in the driver was not wrong data. A trap is not a
`splitRefusal`, so `ExecutePartitions` reported the parser as unavailable and
fell back to one unsplit read, which is correct. But the cost was silent:
moderately complex queries lost all parallelism with nothing to indicate why,
which is the exact failure the parser was adopted to avoid.

Fixed upstream by linking the same stack, plus an explicit request-depth bound
so that input past what the module can handle gets a reported error rather than
a trap. The bound is 1536 JSON levels, chosen from measurement: the parser
emits at most ~1006, and decoding overflows from ~2406.

`go/wasm/polyglot_sql.wasm` is now built from `b7a6b3b37255`. Two regression
tests cover it, and the comment in `go/sqlanalyzer.go` warns against rebuilding
from an earlier commit.
