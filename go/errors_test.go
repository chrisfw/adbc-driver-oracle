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
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/sijms/go-ora/v2/network"
)

// --- Unit: code extraction ---

func TestOraCodesFromText(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want []int
	}{
		{"single", "ORA-00942: table or view does not exist", []int{942}},
		{"no code", "dial tcp 127.0.0.1:1521: connect: connection refused", nil},
		{"empty", "", nil},
		// Oracle stacks the triggering error first and the PL/SQL backtrace
		// after it, so order is what tells them apart.
		{"stacked", "ORA-00942: table or view does not exist\nORA-06512: at line 1", []int{942, 6512}},
		{"five digit", "ORA-12899: value too large for column", []int{12899}},
		{"prefix only", "ORA-: malformed", nil},
		{"zero is not a code", "ORA-00000: normal, successful completion", nil},
		{"embedded in prose", "failed: ORA-00001: unique constraint (A.B) violated", []int{1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := oraCodesFromText(tc.msg); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("oraCodesFromText(%q) = %v, want %v", tc.msg, got, tc.want)
			}
		})
	}
}

// go-ora's own error type is authoritative, and a flattened message is the
// fallback. Both have to work, because database/sql and this driver both
// reduce driver errors to text on the way up.
func TestOraErrorCodeSources(t *testing.T) {
	typed := &network.OracleError{ErrCode: 942}
	if got := oraErrorCode(typed); got != 942 {
		t.Errorf("typed error: got %d, want 942", got)
	}
	if got := oraErrorCode(fmt.Errorf("wrapped: %w", typed)); got != 942 {
		t.Errorf("wrapped typed error: got %d, want 942", got)
	}
	flattened := errors.New("[Oracle] query failed: ORA-00942: table or view does not exist")
	if got := oraErrorCode(flattened); got != 942 {
		t.Errorf("flattened error: got %d, want 942", got)
	}
	if got := oraErrorCode(nil); got != 0 {
		t.Errorf("nil: got %d, want 0", got)
	}
	if got := oraErrorCode(errors.New("no oracle code here")); got != 0 {
		t.Errorf("non-Oracle error: got %d, want 0", got)
	}
}

// isOraCode asks "did this particular thing go wrong", so it has to look past
// the first code: a stacked error can carry the interesting one behind a
// backtrace entry. prepareIngestTable depends on this for ORA-00942 and
// ORA-00955.
func TestIsOraCodeSearchesTheWholeStack(t *testing.T) {
	stacked := errors.New("ORA-06512: at line 1\nORA-00942: table or view does not exist")
	if !isOraCode(stacked, 942) {
		t.Error("942 behind a backtrace entry was not found")
	}
	if !isOraCode(stacked, 6512) {
		t.Error("6512 was not found")
	}
	if isOraCode(stacked, 955) {
		t.Error("955 reported present when it is not")
	}
	if isOraCode(nil, 942) {
		t.Error("nil error reported a code")
	}
}

// --- Unit: the mapping itself ---

func TestInspectErrorMapping(t *testing.T) {
	cases := []struct {
		name         string
		err          error
		wantStatus   adbc.Status
		wantSQLState string
		wantVendor   int32
	}{
		// The codes the plan names explicitly.
		{"ORA-00001 unique", oraErr(1), adbc.StatusIntegrity, "23000", 1},
		{"ORA-00942 no table", oraErr(942), adbc.StatusNotFound, "42S02", 942},
		{"ORA-00904 bad column", oraErr(904), adbc.StatusNotFound, "42S22", 904},
		{"ORA-01017 bad credential", oraErr(1017), adbc.StatusUnauthenticated, "28000", 1017},
		{"ORA-01013 cancelled", oraErr(1013), adbc.StatusCancelled, "HY008", 1013},
		{"ORA-12899 too wide", oraErr(12899), adbc.StatusInvalidData, "22001", 12899},
		{"ORA-01438 out of range", oraErr(1438), adbc.StatusInvalidData, "22003", 1438},
		{"ORA-00060 deadlock", oraErr(60), adbc.StatusInternal, "40001", 60},

		// Constraint subclasses, so a facade can tell them apart.
		{"ORA-01400 null", oraErr(1400), adbc.StatusIntegrity, "23502", 1400},
		{"ORA-02291 fk parent", oraErr(2291), adbc.StatusIntegrity, "23503", 2291},
		{"ORA-00955 exists", oraErr(955), adbc.StatusAlreadyExists, "42S01", 955},
		{"ORA-08177 serialization", oraErr(8177), adbc.StatusInternal, "40001", 8177},

		// The TNS range, matched by rule rather than enumerated.
		{"ORA-12154 unresolved", oraErr(12154), adbc.StatusIO, "08001", 12154},
		{"ORA-12541 no listener", oraErr(12541), adbc.StatusIO, "08001", 12541},
		{"ORA-12514 unknown service", oraErr(12514), adbc.StatusIO, "08001", 12514},

		// A dead connection is a different class from failing to reach one.
		{"ORA-03113 eof", oraErr(3113), adbc.StatusIO, "08S01", 3113},
		{"ORA-03135 lost contact", oraErr(3135), adbc.StatusIO, "08S01", 3135},

		// An unmapped Oracle code still reports its vendor code and keeps the
		// status the call site chose, rather than guessing a class.
		{"unmapped code", oraErr(7445), adbc.StatusIO, "HY000", 7445},

		// No Oracle code at all: Oracle never saw these, so no vendor code.
		{"deadline", context.DeadlineExceeded, adbc.StatusTimeout, "HY008", 0},
		{"cancelled ctx", context.Canceled, adbc.StatusCancelled, "HY008", 0},
		{"eof", io.ErrUnexpectedEOF, adbc.StatusIO, "08S01", 0},
		{"net error", &net.OpError{Op: "read", Err: errors.New("reset")}, adbc.StatusIO, "08S01", 0},
		{"unknown", errors.New("something else"), adbc.StatusIO, "HY000", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// StatusIO is what every Wrap call site in this driver defaults to.
			got := oracleErrorInspector{}.InspectError(tc.err, adbc.StatusIO)
			if got.Code != tc.wantStatus {
				t.Errorf("status = %v, want %v", got.Code, tc.wantStatus)
			}
			if sqlState := string(got.SqlState[:]); sqlState != tc.wantSQLState {
				t.Errorf("sqlstate = %q, want %q", sqlState, tc.wantSQLState)
			}
			if got.VendorCode != tc.wantVendor {
				t.Errorf("vendor code = %d, want %d", got.VendorCode, tc.wantVendor)
			}
		})
	}
}

// An unmapped failure must inherit whatever status the call site asked for,
// not be flattened to one. That is what lets a non-IO call site keep its
// meaning while still gaining a SQLSTATE.
func TestInspectErrorKeepsDefaultStatusWhenUnmapped(t *testing.T) {
	for _, def := range []adbc.Status{adbc.StatusIO, adbc.StatusInternal, adbc.StatusInvalidArgument} {
		got := oracleErrorInspector{}.InspectError(errors.New("unrecognised"), def)
		if got.Code != def {
			t.Errorf("default %v: got %v", def, got.Code)
		}
	}
}

// Every SQLSTATE in the table has to be exactly five characters, or adbc.Error
// silently truncates or pads it and consumers read garbage.
func TestSQLStatesAreWellFormed(t *testing.T) {
	seen := map[int]bool{}
	for code, st := range oraCodeStatus {
		if len(st.sqlState) != 5 {
			t.Errorf("ORA-%05d: sqlstate %q is %d characters, want 5", code, st.sqlState, len(st.sqlState))
		}
		if seen[code] {
			t.Errorf("ORA-%05d listed twice", code)
		}
		seen[code] = true
	}
	// toSQLState must refuse a malformed value rather than pass it through.
	malformed := toSQLState("abc")
	if got := string(malformed[:]); got != sqlStateGeneralError {
		t.Errorf("toSQLState(%q) = %q, want %q", "abc", got, sqlStateGeneralError)
	}
}

// --- Integration: real Oracle errors ---

// The plan's required three, plus the hint path, against a live server. Each
// asserts the status, the SQLSTATE and the Oracle code a facade would relay.
func TestIntegration_ErrorMapping(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	const table = "ADBC_T25_ERR"
	dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn,
		"CREATE TABLE "+table+" (ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(10))")
	defer dropTable(ctx, t, cnxn, table)
	execUpdate(ctx, t, cnxn, "INSERT INTO "+table+" VALUES (1, 'a')")

	cases := []struct {
		name         string
		sql          string
		wantStatus   adbc.Status
		wantSQLState string
		wantVendor   int32
	}{
		{
			"ORA-00001 unique constraint",
			"INSERT INTO " + table + " VALUES (1, 'dup')",
			adbc.StatusIntegrity, "23000", 1,
		},
		{
			"ORA-00942 table does not exist",
			"SELECT * FROM ADBC_T25_NO_SUCH_TABLE",
			adbc.StatusNotFound, "42S02", 942,
		},
		{
			"ORA-00904 invalid identifier",
			"SELECT NO_SUCH_COLUMN FROM " + table,
			adbc.StatusNotFound, "42S22", 904,
		},
		{
			"ORA-12899 value too large",
			"INSERT INTO " + table + " VALUES (2, 'far too long for the column')",
			adbc.StatusInvalidData, "22001", 12899,
		},
		{
			"ORA-01400 cannot insert null",
			"INSERT INTO " + table + " (ID, NAME) VALUES (NULL, 'x')",
			adbc.StatusIntegrity, "23502", 1400,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tryExec(ctx, cnxn, tc.sql)
			if err == nil {
				err = tryQuery(ctx, cnxn, tc.sql)
			}
			if err == nil {
				t.Fatalf("%q succeeded, want an error", tc.sql)
			}
			assertADBCError(t, err, tc.wantStatus, tc.wantSQLState, tc.wantVendor)
		})
	}
}

// ORA-01017 needs its own connection, since the failure is the connect itself.
func TestIntegration_BadCredentialIsUnauthenticated(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{
		OptionDSN: replaceDSNPassword(dsn, "definitely-not-the-password"),
	})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err == nil {
		cnxn.Close()
		t.Fatal("connected with a wrong password")
	}
	assertADBCError(t, err, adbc.StatusUnauthenticated, "28000", 1017)
}

// --- helpers ---

func oraErr(code int) error {
	return &network.OracleError{ErrCode: code}
}

func assertADBCError(t *testing.T, err error, wantStatus adbc.Status, wantSQLState string, wantVendor int32) {
	t.Helper()
	var adbcErr adbc.Error
	if !errors.As(err, &adbcErr) {
		t.Fatalf("got %T (%v), want an adbc.Error", err, err)
	}
	if adbcErr.Code != wantStatus {
		t.Errorf("status = %v, want %v (err: %v)", adbcErr.Code, wantStatus, err)
	}
	if got := string(adbcErr.SqlState[:]); got != wantSQLState {
		t.Errorf("sqlstate = %q, want %q (err: %v)", got, wantSQLState, err)
	}
	if adbcErr.VendorCode != wantVendor {
		t.Errorf("vendor code = %d, want %d (err: %v)", adbcErr.VendorCode, wantVendor, err)
	}
}

// replaceDSNPassword swaps the password in an oracle:// URL, leaving the rest
// intact, so the bad-credential test reaches the same host and service.
func replaceDSNPassword(dsn, password string) string {
	at := strings.LastIndex(dsn, "@")
	scheme := strings.Index(dsn, "://")
	if at < 0 || scheme < 0 {
		return dsn
	}
	userinfo := dsn[scheme+3 : at]
	user := userinfo
	if colon := strings.Index(userinfo, ":"); colon >= 0 {
		user = userinfo[:colon]
	}
	return dsn[:scheme+3] + user + ":" + password + dsn[at:]
}

// A failed connect must not put the password in the error text. The password
// reaches go-ora inside the DSN, so any path that formats the DSN, or that
// echoes go-ora's parsed config, leaks a credential into whatever logs the
// error. No path does today; this is the guard that keeps it that way.
//
// A redaction helper is deliberately absent: with nothing formatting the DSN
// there would be no caller for it, and an unused helper is worse than a test
// that fails the moment one appears.
func TestIntegration_ConnectErrorDoesNotLeakPassword(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	const secret = "Sup3rSecret-SentinelValue"

	cases := []struct {
		name string
		dsn  string
	}{
		// Rejected by Oracle: the error comes back from the server.
		{"wrong password", replaceDSNPassword(dsn, secret)},
		// Never reaches Oracle: the error is raised locally, which is the
		// path most likely to echo the connect string back.
		{"unreachable host", "oracle://someuser:" + secret + "@127.0.0.1:1/NOSUCHSERVICE"},
		{"malformed dsn", "oracle://someuser:" + secret + "@:::/"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			drv := NewDriver(memory.DefaultAllocator)
			db, err := drv.NewDatabase(map[string]string{OptionDSN: tc.dsn})
			if err != nil {
				assertNoSecret(t, err, secret)
				return
			}
			defer db.Close()

			cnxn, err := db.Open(ctx)
			if err == nil {
				cnxn.Close()
				t.Skip("connect unexpectedly succeeded, nothing to inspect")
			}
			// The whole chain, not just the outermost message: wrapError joins
			// the inspected error with the original, so a leak could hide in
			// either half.
			assertNoSecret(t, err, secret)

			var adbcErr adbc.Error
			if errors.As(err, &adbcErr) && strings.Contains(adbcErr.Msg, secret) {
				t.Errorf("adbc.Error.Msg leaks the password: %s", adbcErr.Msg)
			}
		})
	}
}

// Passwords must not survive into an option read back off the database either,
// which is the other way a credential escapes into a log.
func TestPasswordIsNotReadableAsAnOption(t *testing.T) {
	const secret = "Sup3rSecret-SentinelValue"
	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{
		OptionServerHostname: "localhost",
		OptionServiceName:    "FREEPDB1",
		OptionUser:           "someuser",
		OptionPassword:       secret,
	})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	getter, ok := db.(adbc.GetSetOptions)
	if !ok {
		t.Skip("database does not implement adbc.GetSetOptions")
	}
	for _, key := range []string{OptionPassword, adbc.OptionKeyPassword} {
		val, err := getter.GetOption(key)
		if err != nil {
			continue // refusing to report it at all is the better answer
		}
		if strings.Contains(val, secret) {
			t.Errorf("GetOption(%q) returned the password", key)
		}
	}
}

// assertNoSecret fails if the secret appears anywhere in the error chain's text.
func assertNoSecret(t *testing.T, err error, secret string) {
	t.Helper()
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error text leaks the password: %v", err)
	}
}
