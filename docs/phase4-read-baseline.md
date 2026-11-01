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
