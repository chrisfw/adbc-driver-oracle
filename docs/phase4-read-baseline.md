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
