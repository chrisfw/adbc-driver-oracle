module github.com/chrisfw/adbc-driver-oracle

go 1.26.0

require (
	github.com/adbc-drivers/driverbase-go/driverbase v0.0.0-20260409004641-0e8d426bb483
	github.com/apache/arrow-adbc/go/adbc v1.11.0
	github.com/apache/arrow-go/v18 v18.8.0
	github.com/sijms/go-ora/v2 v2.8.24
)

// TODO: Remove this replace once go-ora PR #721 is merged upstream.
// https://github.com/sijms/go-ora/pull/721
// Fixes:
//   - VARRAY encoding in nested UDT objects (null + large >252 bytes).
//     Without this fix, SDO_GEOMETRY insert crashes with ORA-00600 for polygons >~16 vertices.
//   - NULL UDT array bind: per-row envelope drops the trailing zero so
//     bulk INSERTs of SDO_GEOMETRY with mixed-NULL rows don't fail with
//     ORA-03146 "invalid buffer length for TTC field".
//   - NULL UDT result-set decode: removes the bogus extra 0x81 0x01
//     trailer read so SELECTs that include NULL SDO_GEOMETRY rows don't
//     desync the stream and crash later rows with "invalid size for GetInt64".
replace github.com/sijms/go-ora/v2 => github.com/jatorre/go-ora/v2 v2.9.1-0.20260719131444-0a202851613c

require (
	github.com/adbc-drivers/driverbase-go/testutil v0.0.0-20251215145213-df04bfe8de4f // indirect
	github.com/andybalholm/brotli v1.2.3 // indirect
	github.com/apache/thrift v0.24.0 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.28.0 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.29 // indirect
	github.com/tetratelabs/wazero v1.11.0
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.43.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.43.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.43.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.opentelemetry.io/proto/otlp v1.10.0 // indirect
	golang.org/x/exp v0.0.0-20260312153236-7ab1446f8b90 // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/grpc v1.83.2 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
