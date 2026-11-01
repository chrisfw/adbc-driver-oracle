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
	"io"
	"net"
	"slices"
	"strconv"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/sijms/go-ora/v2/network"
)

// SQLSTATE values this driver reports. Named rather than inline so the mapping
// table reads as intent, and so the same class is spelled one way everywhere.
//
// Classes follow SQL:2003, which is what the ODBC and JDBC facades switch on:
// 08 connection, 22 data, 23 constraint, 25 invalid transaction state,
// 28 authorization, 40 transaction rollback, 42 syntax or access, HY general.
const (
	sqlStateUnableToConnect     = "08001"
	sqlStateConnectionFailure   = "08S01"
	sqlStateStringTruncation    = "22001"
	sqlStateNumericOutOfRange   = "22003"
	sqlStateInvalidDatetime     = "22007"
	sqlStateInvalidCastValue    = "22018"
	sqlStateIntegrityConstraint = "23000"
	sqlStateNotNullViolation    = "23502"
	sqlStateForeignKeyViolation = "23503"
	sqlStateInvalidAuth         = "28000"
	sqlStateSerializationFail   = "40001"
	sqlStateSyntaxOrAccess      = "42000"
	sqlStateTableExists         = "42S01"
	sqlStateTableNotFound       = "42S02"
	sqlStateColumnNotFound      = "42S22"
	sqlStateGeneralError        = "HY000"
	sqlStateOperationCancelled  = "HY008"
)

// oraStatus is how one Oracle error code is reported to an ADBC caller.
type oraStatus struct {
	status   adbc.Status
	sqlState string
}

// oraCodeStatus maps Oracle error codes to an ADBC status and a SQLSTATE.
//
// Only codes whose meaning is unambiguous belong here. Anything absent keeps
// the status the call site already chose and reports HY000, which is honest
// about not knowing rather than guessing a class the caller might branch on.
var oraCodeStatus = map[int]oraStatus{
	// Constraint violations. A facade switching on SQLSTATE class 23 needs
	// these distinguished, so the specific subclasses are spelled out.
	1:    {adbc.StatusIntegrity, sqlStateIntegrityConstraint}, // unique constraint violated
	1400: {adbc.StatusIntegrity, sqlStateNotNullViolation},    // cannot insert NULL
	2290: {adbc.StatusIntegrity, sqlStateIntegrityConstraint}, // check constraint violated
	2291: {adbc.StatusIntegrity, sqlStateForeignKeyViolation}, // parent key not found
	2292: {adbc.StatusIntegrity, sqlStateForeignKeyViolation}, // child record found

	// Missing or wrong object names. NotFound rather than InvalidArgument:
	// the SQL was well formed, the object was not there.
	942: {adbc.StatusNotFound, sqlStateTableNotFound},  // table or view does not exist
	904: {adbc.StatusNotFound, sqlStateColumnNotFound}, // invalid identifier
	955: {adbc.StatusAlreadyExists, sqlStateTableExists},

	// Authentication and authorization.
	1017: {adbc.StatusUnauthenticated, sqlStateInvalidAuth}, // invalid credential
	1031: {adbc.StatusUnauthorized, sqlStateSyntaxOrAccess}, // insufficient privileges
	1045: {adbc.StatusUnauthorized, sqlStateSyntaxOrAccess}, // CREATE SESSION not granted

	// Cancellation.
	1013: {adbc.StatusCancelled, sqlStateOperationCancelled}, // user requested cancel

	// Data that will not fit the destination.
	12899: {adbc.StatusInvalidData, sqlStateStringTruncation},  // value too large for column
	1438:  {adbc.StatusInvalidData, sqlStateNumericOutOfRange}, // value larger than precision
	1722:  {adbc.StatusInvalidData, sqlStateInvalidCastValue},  // invalid number
	1858:  {adbc.StatusInvalidData, sqlStateInvalidDatetime},   // non-numeric where numeric expected
	1861:  {adbc.StatusInvalidData, sqlStateInvalidDatetime},   // literal does not match format

	// Transaction conflicts. ADBC has no retryable status, so the SQLSTATE is
	// the signal a caller should act on: 40001 is what tells a facade, and
	// through it the application, that retrying the transaction is the fix.
	60:   {adbc.StatusInternal, sqlStateSerializationFail}, // deadlock detected
	8177: {adbc.StatusInternal, sqlStateSerializationFail}, // cannot serialize access

	// The connection itself is gone. Separate class from a failure to reach
	// the listener, which statusForOraCode handles as a range.
	28:   {adbc.StatusIO, sqlStateConnectionFailure}, // session has been killed
	3113: {adbc.StatusIO, sqlStateConnectionFailure}, // end-of-file on communication channel
	3114: {adbc.StatusIO, sqlStateConnectionFailure}, // not connected to Oracle
	3135: {adbc.StatusIO, sqlStateConnectionFailure}, // connection lost contact
}

// statusForOraCode resolves an Oracle error code, falling back to the TNS
// range for the listener and name-resolution failures that are too numerous to
// enumerate (ORA-12154, 12170, 12514, 12541 and relatives).
func statusForOraCode(code int) (oraStatus, bool) {
	if s, ok := oraCodeStatus[code]; ok {
		return s, true
	}
	if code >= 12000 && code <= 12999 {
		return oraStatus{adbc.StatusIO, sqlStateUnableToConnect}, true
	}
	return oraStatus{}, false
}

// oracleErrorInspector turns a go-ora error into an adbc.Error carrying the
// Oracle code in VendorCode and a SQLSTATE in SqlState.
//
// Installed on the driver's ErrorHelper in NewDriver, so every ErrorHelper
// Wrap call gets the mapping without each call site knowing about it.
type oracleErrorInspector struct{}

var _ driverbase.ErrorInspector = oracleErrorInspector{}

func (oracleErrorInspector) InspectError(err error, defaultStatus adbc.Status) adbc.Error {
	out := adbc.Error{Code: defaultStatus, Msg: err.Error()}

	// An Oracle code is the most specific thing available, so it wins.
	if code := oraErrorCode(err); code != 0 {
		out.VendorCode = int32(code)
		if s, ok := statusForOraCode(code); ok {
			out.Code = s.status
			out.SqlState = toSQLState(s.sqlState)
		} else {
			out.SqlState = toSQLState(sqlStateGeneralError)
		}
		return out
	}

	// No Oracle code: the failure happened below the protocol, or the caller
	// walked away. Oracle never saw it, so there is no vendor code to report.
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		out.Code = adbc.StatusTimeout
		out.SqlState = toSQLState(sqlStateOperationCancelled)
	case errors.Is(err, context.Canceled):
		out.Code = adbc.StatusCancelled
		out.SqlState = toSQLState(sqlStateOperationCancelled)
	case isNetworkError(err):
		out.Code = adbc.StatusIO
		out.SqlState = toSQLState(sqlStateConnectionFailure)
	default:
		out.SqlState = toSQLState(sqlStateGeneralError)
	}
	return out
}

// toSQLState renders a five-character SQLSTATE into the fixed array adbc.Error
// carries. A value of the wrong length would silently truncate or pad, so it
// is treated as a programming error and reported as the general class.
func toSQLState(s string) [5]byte {
	var out [5]byte
	if len(s) != 5 {
		copy(out[:], sqlStateGeneralError)
		return out
	}
	copy(out[:], s)
	return out
}

// isNetworkError reports whether an error is a transport failure rather than
// something Oracle said. A half-open socket surfaces as io.EOF or a net error
// with no ORA code attached.
func isNetworkError(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// oraErrorCodes returns every ORA code carried by err, outermost first.
//
// go-ora's own error type is authoritative when present. Otherwise the codes
// are read out of the message, because database/sql and this driver's own
// formatting both reduce a driver error to text on the way up.
func oraErrorCodes(err error) []int {
	if err == nil {
		return nil
	}
	var oraErr *network.OracleError
	if errors.As(err, &oraErr) && oraErr.ErrCode != 0 {
		return []int{oraErr.ErrCode}
	}
	return oraCodesFromText(err.Error())
}

// oraErrorCode returns the primary ORA code, or 0 when there is none.
//
// Oracle stacks errors with the triggering one first and the PL/SQL backtrace
// (ORA-06512 and friends) after it, so the first code is the one worth
// reporting.
func oraErrorCode(err error) int {
	codes := oraErrorCodes(err)
	if len(codes) == 0 {
		return 0
	}
	return codes[0]
}

// isOraCode reports whether err carries the given Oracle error code
// (e.g. 942 for "ORA-00942: table or view does not exist").
//
// Anywhere in the stack, not just first: callers use this to ask "did this
// particular thing go wrong", and a stacked error can carry the code they care
// about behind a backtrace entry.
func isOraCode(err error, code int) bool {
	return slices.Contains(oraErrorCodes(err), code)
}

// oraCodesFromText extracts ORA-nnnnn codes from an error message in order.
func oraCodesFromText(msg string) []int {
	const prefix = "ORA-"
	var codes []int
	for i := 0; i < len(msg); {
		j := strings.Index(msg[i:], prefix)
		if j < 0 {
			break
		}
		i += j + len(prefix)
		end := i
		for end < len(msg) && msg[end] >= '0' && msg[end] <= '9' {
			end++
		}
		if end > i {
			if code, err := strconv.Atoi(msg[i:end]); err == nil && code != 0 {
				codes = append(codes, code)
			}
			i = end
		}
	}
	return codes
}
