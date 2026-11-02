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
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
)

// Partitioning has to decide whether a query can be split and then rewrite it.
// Both jobs need to understand Oracle SQL, and understanding it with string
// matching has a failure mode worth avoiding: a rewrite that misreads a query
// returns plausible wrong data rather than failing.
//
// polyglot is a SQL parser and generator with an Oracle dialect, so it answers
// both questions structurally. A query becomes an AST, the AST says what the
// query does, and a modified AST becomes SQL again. The generator handles
// operator precedence, so ANDing a predicate into a WHERE that is an OR comes
// back correctly parenthesised instead of relying on this driver to remember.
//
// It reaches Go as a WebAssembly module run by wazero. That matters for how
// this driver ships: wazero is pure Go, so there is no cgo and nothing for a
// user to install, and one embedded module covers every platform. The
// alternative binding is a native shared library that has to be built per
// platform and found at run time, which for a driver distributed as a single
// shared library means shipping and locating a second native artifact.
//
// The cost is paid in two places and both are bounded. The module is about
// 29 MB, embedded in the binary. Translating it to machine code is expensive
// enough to matter, so it happens once, lazily, only when a query is actually
// partitioned, and the result is cached on disk between runs. Measured on an
// arm64 laptop:
//
//	                        init      per call
//	compiled, cold          10.6s       534us
//	compiled, cached         207ms      288us
//	interpreted              739ms      7.7ms
//
// Under the race detector, which this project's tests run with, a cold compile
// costs 2m32s and a cached one 3.1s. That difference is why the disk cache is
// not optional: without it every test run that touches partitioning stalls for
// over two minutes.
//
// The interpreter needs no compilation but is roughly fifteen times slower per
// call, which would be the better trade only for a process that partitions
// once and exits on a machine with a cold cache. The cache makes that case
// rare, so the compiler is the default.

// polyglotModule is the WASI build of polyglot-sql.
//
// Rebuild it from the polyglot checkout with:
//
//	make build-wasi
//	cp target/wasm32-wasip1/release/polyglot_sql_wasi.wasm \
//	   <driver>/go/wasm/polyglot_sql.wasm
//
// The module in the tree was built from chrisfw/polyglot, branch wasi-module,
// at e07fa38b7572. That branch is upstream main plus the WASI crate and
// nothing else: the Oracle flashback support this driver depends on is now
// upstream's own, so the only outstanding fork is the packaging. It is
// upstream pull request tobilg/polyglot#519, after which the module can come
// from an upstream build.
//
// Do not replace it with a module built before that commit. Two defects were
// fixed there and both are quiet:
//
//   - Earlier builds linked the WASI target with the default 1 MiB stack, and
//     a query whose expressions nest even moderately deeply, such as a sum of
//     forty columns, overflowed it. In WASM that reaches the host as "out of
//     bounds memory access" with no message, and the driver could only treat
//     it as the parser being unavailable and fall back to an unsplit read.
//   - Earlier builds accepted trailing content after a request, so a
//     truncated or concatenated payload was indistinguishable from a complete
//     one.
//
// Checking the module is the module you think it is: the string
// "trailing content" appears in a build that carries the second fix. A stale
// artifact otherwise looks identical and simply performs worse.
//
//go:embed wasm/polyglot_sql.wasm
var polyglotFS embed.FS

const polyglotModulePath = "wasm/polyglot_sql.wasm"

// sqlAnalyzer parses and generates Oracle SQL through the embedded module.
//
// Safe for concurrent use: wazero's runtime and compiled modules are, and each
// request runs in a fresh instance with no shared state.
type sqlAnalyzer struct {
	runtime  wazero.Runtime
	compiled wazero.CompiledModule
}

var (
	analyzerOnce sync.Once
	analyzer     *sqlAnalyzer
	analyzerErr  error
)

// sharedAnalyzer compiles the module on first use and reuses it thereafter.
//
// One instance per process rather than per connection, because compiling is
// the expensive part and the result is immutable and concurrency safe.
func sharedAnalyzer(ctx context.Context) (*sqlAnalyzer, error) {
	analyzerOnce.Do(func() {
		wasmBytes, err := polyglotFS.ReadFile(polyglotModulePath)
		if err != nil {
			analyzerErr = fmt.Errorf("the embedded SQL parser is missing: %w", err)
			return
		}
		runtime := wazero.NewRuntimeWithConfig(ctx, analyzerRuntimeConfig())
		if _, err := wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
			runtime.Close(ctx)
			analyzerErr = fmt.Errorf("could not start the WebAssembly runtime: %w", err)
			return
		}
		compiled, err := runtime.CompileModule(ctx, wasmBytes)
		if err != nil {
			runtime.Close(ctx)
			// wazero before 1.11 cannot run this module, failing with
			// "invalid table access". Naming the version saves the next
			// person the bisect.
			analyzerErr = fmt.Errorf(
				"could not compile the SQL parser (wazero 1.11 or newer is required): %w", err)
			return
		}
		analyzer = &sqlAnalyzer{runtime: runtime, compiled: compiled}
	})
	return analyzer, analyzerErr
}

// analyzerRuntimeConfig returns the runtime configuration, with a disk backed
// compilation cache when one can be opened.
//
// The cache is keyed by wazero's own version and the module's content, so a new
// driver release or a new wazero invalidates it without any help from here.
// Anything that goes wrong opening it is not worth failing over: the driver
// still works, it just pays the full compile.
func analyzerRuntimeConfig() wazero.RuntimeConfig {
	config := wazero.NewRuntimeConfig()

	base, err := os.UserCacheDir()
	if err != nil {
		return config
	}
	dir := filepath.Join(base, "adbc-driver-oracle", "wazero")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return config
	}
	cache, err := wazero.NewCompilationCacheWithDir(dir)
	if err != nil {
		return config
	}
	// The cache outlives this call deliberately: it is shared by every module
	// the process compiles and is released when the process exits.
	return config.WithCompilationCache(cache)
}

// polyglotRequest is the module's JSON protocol: one request in, one response
// out, per run.
type polyglotRequest struct {
	Action  string          `json:"action"`
	SQL     string          `json:"sql,omitempty"`
	AST     json.RawMessage `json:"ast,omitempty"`
	Dialect string          `json:"dialect,omitempty"`
}

type polyglotResponse struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *string         `json:"error"`
}

func (a *sqlAnalyzer) call(ctx context.Context, req polyglotRequest) (json.RawMessage, error) {
	req.Dialect = "oracle"
	in, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	var stdout, stderr bytes.Buffer
	config := wazero.NewModuleConfig().
		WithStdin(bytes.NewReader(in)).
		WithStdout(&stdout).
		WithStderr(&stderr).
		// An empty name lets instances run concurrently; a named module can
		// only be instantiated once at a time.
		WithName("")

	if _, err := a.runtime.InstantiateModule(ctx, a.compiled, config); err != nil {
		// A WASI command exits when main returns, which wazero reports as an
		// ExitError. Exit code 0 is the normal path, not a failure.
		exitErr, ok := err.(*sys.ExitError)
		if !ok || exitErr.ExitCode() != 0 {
			return nil, fmt.Errorf("the SQL parser failed: %w", err)
		}
	}

	var resp polyglotResponse
	if err := json.Unmarshal(stdout.Bytes(), &resp); err != nil {
		return nil, fmt.Errorf("the SQL parser returned something unreadable (%q): %w",
			truncateForError(stdout.String()), err)
	}
	if !resp.OK {
		message := "the SQL parser reported no reason"
		if resp.Error != nil {
			message = *resp.Error
		}
		return nil, fmt.Errorf("%s", message)
	}
	return resp.Result, nil
}

// truncateForError keeps an unexpected payload out of an unreadable error.
func truncateForError(s string) string {
	const limit = 200
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}

// astNode is one node of the parsed query.
//
// The AST crosses a WebAssembly boundary as JSON, so it is handled as decoded
// JSON rather than as Go types mirroring polyglot's enum. That keeps this
// driver from having to track every node kind in a parser it does not own: it
// reads the few fields it cares about and passes the rest through untouched.
type astNode = map[string]interface{}

// parseStatements parses SQL into its statements.
func (a *sqlAnalyzer) parseStatements(ctx context.Context, sql string) ([]astNode, error) {
	raw, err := a.call(ctx, polyglotRequest{Action: "parse", SQL: sql})
	if err != nil {
		return nil, err
	}
	var statements []astNode
	if err := json.Unmarshal(raw, &statements); err != nil {
		return nil, fmt.Errorf("could not read the parsed query: %w", err)
	}
	return statements, nil
}

// generate renders statements back to Oracle SQL.
func (a *sqlAnalyzer) generate(ctx context.Context, statements []astNode) (string, error) {
	encoded, err := json.Marshal(statements)
	if err != nil {
		return "", err
	}
	raw, err := a.call(ctx, polyglotRequest{Action: "generate", AST: encoded})
	if err != nil {
		return "", err
	}
	var rendered []string
	if err := json.Unmarshal(raw, &rendered); err != nil {
		return "", fmt.Errorf("could not read the generated SQL: %w", err)
	}
	if len(rendered) != 1 {
		return "", fmt.Errorf("expected one statement back, got %d", len(rendered))
	}
	return rendered[0], nil
}

// --- AST navigation ---
//
// Small helpers rather than a type per node, for the reason given on astNode.

// child returns a node's single child by key, or nil.
func child(node astNode, key string) astNode {
	if node == nil {
		return nil
	}
	value, ok := node[key].(map[string]interface{})
	if !ok {
		return nil
	}
	return value
}

// list returns a node's list-valued field, or nil.
func list(node astNode, key string) []interface{} {
	if node == nil {
		return nil
	}
	value, _ := node[key].([]interface{})
	return value
}

// text returns a node's string-valued field.
func text(node astNode, key string) string {
	if node == nil {
		return ""
	}
	value, _ := node[key].(string)
	return value
}

// isSet reports whether a field carries anything meaningful. A parsed query
// spells "absent" as null for most clauses, as an empty list for repeated
// ones, and as false for flags, so one predicate covers all three.
func isSet(node astNode, key string) bool {
	if node == nil {
		return false
	}
	switch value := node[key].(type) {
	case nil:
		return false
	case bool:
		return value
	case []interface{}:
		return len(value) > 0
	case map[string]interface{}:
		return len(value) > 0
	case string:
		return value != ""
	default:
		return true
	}
}

// nodeKind returns the single key that names a node's type. polyglot encodes
// its AST as externally tagged enums, so `{"count": {...}}` is a COUNT and
// `{"column": {...}}` is a column reference.
func nodeKind(value interface{}) string {
	node, ok := value.(map[string]interface{})
	if !ok || len(node) != 1 {
		return ""
	}
	for key := range node {
		return key
	}
	return ""
}

// walkAST calls visit for every node in the tree, depth first. Returning false
// stops the walk.
func walkAST(value interface{}, visit func(kind string, node astNode) bool) bool {
	switch typed := value.(type) {
	case map[string]interface{}:
		if kind := nodeKind(typed); kind != "" {
			if !visit(kind, typed) {
				return false
			}
		}
		for _, field := range typed {
			if !walkAST(field, visit) {
				return false
			}
		}
	case []interface{}:
		for _, element := range typed {
			if !walkAST(element, visit) {
				return false
			}
		}
	}
	return true
}

// containsKind reports whether any node in the tree has one of the given kinds.
func containsKind(value interface{}, kinds map[string]bool) (string, bool) {
	found := ""
	walkAST(value, func(kind string, _ astNode) bool {
		if kinds[kind] {
			found = kind
			return false
		}
		return true
	})
	return found, found != ""
}
