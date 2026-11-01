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
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// --- Unit: identifier quoting and validation ---

func TestQuoteOracleIdentifier(t *testing.T) {
	cases := []struct{ in, want string }{
		{"HR", `"HR"`},
		{"", `""`},
		{"with space", `"with space"`},
		// An embedded quote is doubled, which is what keeps the result a
		// single identifier rather than a way out of it.
		{`A"B`, `"A""B"`},
		{`"`, `""""`},
		{`A" OR "1`, `"A"" OR ""1"`},
	}
	for _, tc := range cases {
		if got := quoteOracleIdentifier(tc.in); got != tc.want {
			t.Errorf("quoteOracleIdentifier(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestNormalizeSchemaIdentifier(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		// Bare names fold to upper case, matching Oracle's own rule for an
		// unquoted identifier and this driver's long-standing behaviour.
		{"bare lower", "hr", "HR", false},
		{"bare upper", "HR", "HR", false},
		{"bare mixed", "Hr", "HR", false},
		{"surrounding space", "  hr  ", "HR", false},
		// A name the caller already quoted is taken exactly.
		{"quoted exact case", `"hr"`, "hr", false},
		{"quoted mixed case", `"MixedCase"`, "MixedCase", false},

		{"empty", "", "", true},
		{"blank", "   ", "", true},
		{"empty quoted", `""`, "", true},
		// The injection this item exists to close. Verified against Oracle:
		// unquoted, this set NLS_DATE_FORMAT as well as the schema.
		{"second session parameter", `ADBC NLS_DATE_FORMAT = 'YYYY'`, `ADBC NLS_DATE_FORMAT = 'YYYY'`, false},
		// A quote cannot reach the statement.
		{"embedded quote", `HR" EVENTS '10046 trace name context forever'`, "", true},
		{"quote only", `a"b`, "", true},
		{"nul byte", "HR\x00", "", true},
		{"over length", strings.Repeat("A", maxOracleIdentifierBytes+1), "", true},
		{"at length", strings.Repeat("A", maxOracleIdentifierBytes), strings.Repeat("A", maxOracleIdentifierBytes), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeSchemaIdentifier(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("normalizeSchemaIdentifier(%q) = %q, want an error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeSchemaIdentifier(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("normalizeSchemaIdentifier(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// Whatever survives validation must still come out as exactly one quoted
// identifier, so no input can extend the ALTER SESSION statement. This is the
// invariant that actually closes the injection; the validation above only
// improves the error message.
func TestSchemaIdentifierAlwaysRendersOneIdentifier(t *testing.T) {
	inputs := []string{
		"HR",
		"hr",
		`ADBC NLS_DATE_FORMAT = 'YYYY'`,
		`ADBC EVENTS '10046 trace name context forever'`,
		`A"B`,
		`" OR ""=""`,
		"with space",
	}
	for _, in := range inputs {
		// Quote directly, skipping validation, to pin the escaping itself.
		got := quoteOracleIdentifier(in)
		if !strings.HasPrefix(got, `"`) || !strings.HasSuffix(got, `"`) {
			t.Errorf("quoteOracleIdentifier(%q) = %q, not delimited", in, got)
			continue
		}
		// Inside the delimiters every quote must be doubled, which is what
		// makes the identifier impossible to escape from.
		inner := got[1 : len(got)-1]
		if strings.Count(inner, `"`)%2 != 0 {
			t.Errorf("quoteOracleIdentifier(%q) = %q has an unpaired quote inside", in, got)
		}
		if strings.ReplaceAll(inner, `""`, "") != strings.ReplaceAll(in, `"`, "") {
			t.Errorf("quoteOracleIdentifier(%q) = %q mangled the name", in, got)
		}
	}
}

// --- Integration: session state follows the session ---

// Setting the schema must actually redirect unqualified name resolution on
// that connection, which is the point of owning a session.
func TestIntegration_SetSchemaRedirectsUnqualifiedSelect(t *testing.T) {
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

	home := querySingleString(ctx, t, cnxn, currentSchemaSQL)

	// A table that exists only in a second schema, so an unqualified SELECT
	// resolving to it proves CURRENT_SCHEMA took effect. Needs CREATE ANY
	// TABLE; skip rather than fail where the test user lacks it.
	const other = "ADBC_OTHER"
	const table = "ADBC_T13_ONLY_THERE"
	qualified := other + "." + table

	execIgnoreError(ctx, t, cnxn, "DROP TABLE "+qualified+" PURGE")
	if err := tryExec(ctx, cnxn, "CREATE TABLE "+qualified+" (ID NUMBER(10))"); err != nil {
		t.Skipf("cannot provision %s (needs a second schema and CREATE ANY TABLE): %v", qualified, err)
	}
	defer execIgnoreError(ctx, t, cnxn, "DROP TABLE "+qualified+" PURGE")
	execUpdate(ctx, t, cnxn, "INSERT INTO "+qualified+" VALUES (42)")

	// Unqualified, the table is not visible from the home schema.
	if err := tryQuery(ctx, cnxn, "SELECT ID FROM "+table); err == nil {
		t.Fatalf("%s resolved unqualified from %s before the schema switch", table, home)
	}

	setter := cnxn.(adbc.GetSetOptions)
	if err := setter.SetOption(adbc.OptionKeyCurrentDbSchema, other); err != nil {
		t.Fatalf("SetOption(CurrentDbSchema=%s): %v", other, err)
	}
	if got := querySingleString(ctx, t, cnxn, currentSchemaSQL); !strings.EqualFold(got, other) {
		t.Fatalf("CURRENT_SCHEMA = %q, want %q", got, other)
	}

	// Now it resolves without qualification.
	if got := querySingleString(ctx, t, cnxn, "SELECT ID FROM "+table); got != "42" {
		t.Errorf("unqualified SELECT returned %q, want 42", got)
	}

	// Switching back restores the original resolution.
	if err := setter.SetOption(adbc.OptionKeyCurrentDbSchema, home); err != nil {
		t.Fatalf("SetOption(CurrentDbSchema=%s): %v", home, err)
	}
	if err := tryQuery(ctx, cnxn, "SELECT ID FROM "+table); err == nil {
		t.Errorf("%s still resolved unqualified after switching back to %s", table, home)
	}
}

// A schema name carrying extra ALTER SESSION syntax must never reach Oracle as
// syntax. Unquoted, `ADBC NLS_DATE_FORMAT = 'YYYY'` set the date format as
// well as the schema; quoted, it is one identifier that does not exist.
func TestIntegration_SetSchemaRejectsInjection(t *testing.T) {
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

	const dateFormatSQL = "SELECT SYS_CONTEXT('USERENV','NLS_DATE_FORMAT') FROM DUAL"
	homeSchema := querySingleString(ctx, t, cnxn, currentSchemaSQL)
	homeFormat := querySingleString(ctx, t, cnxn, dateFormatSQL)

	setter := cnxn.(adbc.GetSetOptions)
	payloads := []string{
		homeSchema + " NLS_DATE_FORMAT = 'YYYY'",
		homeSchema + ` EVENTS '10046 trace name context forever'`,
		homeSchema + `" NLS_DATE_FORMAT = 'YYYY'`,
		homeSchema + `"; DROP TABLE X`,
	}
	for _, payload := range payloads {
		t.Run(payload, func(t *testing.T) {
			// Either refused outright or executed as one dead identifier;
			// what must never happen is a second parameter taking effect.
			if err := setter.SetOption(adbc.OptionKeyCurrentDbSchema, payload); err == nil {
				t.Errorf("SetOption(%q) succeeded; it should not name a real schema", payload)
			}
			if got := querySingleString(ctx, t, cnxn, dateFormatSQL); got != homeFormat {
				t.Errorf("NLS_DATE_FORMAT changed to %q (was %q): the payload executed as syntax",
					got, homeFormat)
			}
			if got := querySingleString(ctx, t, cnxn, currentSchemaSQL); !strings.EqualFold(got, homeSchema) {
				t.Errorf("CURRENT_SCHEMA changed to %q (was %q)", got, homeSchema)
			}
		})
	}

	// A legitimate name still works after all that.
	if err := setter.SetOption(adbc.OptionKeyCurrentDbSchema, strings.ToLower(homeSchema)); err != nil {
		t.Errorf("SetOption(%q): %v", strings.ToLower(homeSchema), err)
	}
}

// A prepared statement must run inside the caller's transaction, and must
// still work after a Commit. database/sql closes a transaction's prepared
// statements along with the transaction, so this is where that gets caught.
func TestIntegration_PreparedStatementFollowsTransaction(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	writer, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open writer: %v", err)
	}
	defer writer.Close()
	reader, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open reader: %v", err)
	}
	defer reader.Close()

	const table = "ADBC_T13_PREPARED"
	dropTable(ctx, t, writer, table)
	execUpdate(ctx, t, writer, "CREATE TABLE "+table+" (ID NUMBER(10))")
	defer dropTable(ctx, t, writer, table)

	setAutocommit(t, writer, false)

	stmt, err := writer.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery("INSERT INTO " + table + " VALUES (1)"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	if err := stmt.Prepare(ctx); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	// Inside the transaction: the insert must be rollback-able, so the
	// prepared statement has to be bound to the transaction.
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		t.Fatalf("ExecuteUpdate in transaction: %v", err)
	}
	if err := writer.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if got := countRows(ctx, t, writer, table); got != "0" {
		t.Errorf("prepared insert escaped the transaction: %q rows survived rollback", got)
	}

	// After the transaction ended, the same prepared statement must still
	// execute: database/sql closed it with the transaction, so the driver has
	// to prepare it again rather than fail.
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		t.Fatalf("ExecuteUpdate after rollback (statement not re-prepared?): %v", err)
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := countRows(ctx, t, reader, table); got != "1" {
		t.Errorf("reader sees %q rows after commit, want 1", got)
	}

	// And once more across the commit boundary.
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		t.Fatalf("ExecuteUpdate after commit: %v", err)
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	if got := countRows(ctx, t, reader, table); got != "2" {
		t.Errorf("reader sees %q rows after the second commit, want 2", got)
	}
}

// --- helpers ---

// tryExec runs a statement and returns its error instead of failing the test,
// for setup whose success is not guaranteed by privileges.
func tryExec(ctx context.Context, cnxn adbc.Connection, query string) error {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(query); err != nil {
		return err
	}
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

// tryQuery reports whether a query runs, draining it so a lazy failure
// surfaces here rather than later.
func tryQuery(ctx context.Context, cnxn adbc.Connection, query string) error {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(query); err != nil {
		return err
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}
	defer reader.Release()
	for reader.Next() {
	}
	return reader.Err()
}
