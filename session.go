// Copyright (C) 2018 Yasuhiro Matsumoto <mattn.jp@gmail.com>.
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
// +build session

package sqlite3

/*
#cgo CFLAGS: -DSQLITE_ENABLE_SESSION
#cgo CFLAGS: -DSQLITE_ENABLE_PREUPDATE_HOOK

#ifndef USE_LIBSQLITE3
#include <sqlite3-binding.h>
#else
#include <sqlite3.h>
#endif
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// SQLiteSession implement interface of Session.
type SQLiteSession struct {
	s *C.sqlite3_session
}

// Session creates a new session attached to the given database of this
// connection.
//
// Session objects must be closed before the SQLiteConn they are attached to is
// closed.
func (c *SQLiteConn) Session(database string) (*SQLiteSession, error) {
	databaseptr := C.CString(database)
	defer C.free(unsafe.Pointer(databaseptr))

	session := &SQLiteSession{}
	rv := C.sqlite3session_create(c.db, databaseptr, &session.s)

	if rv != C.SQLITE_OK {
		return nil, c.lastError()
	}

	runtime.SetFinalizer(session, (*SQLiteSession).Finish)
	return session, nil
}

// Attach a table to this session.
//
// All subsequent changes made to the table while the session object is enabled
// will be recorded.
//
// If an empty string is passed, then changes are recorded for all tables in
// the database.
func (s *SQLiteSession) Attach(table string) error {
	var tableptr *C.char
	if table != "" {
		tableptr = C.CString(table)
		defer C.free(unsafe.Pointer(tableptr))
	}

	rv := C.sqlite3session_attach(s.s, tableptr)

	if rv != C.SQLITE_OK {
		return Error{Code: ErrNo(rv)}
	}

	return nil
}

// ChangeSet obtains a changeset containing changes to the tables attached to
// this session.
func (s *SQLiteSession) ChangeSet() (*SQLiteChangeSet, error) {
	changeset := &SQLiteChangeSet{}

	rv := C.sqlite3session_changeset(s.s, &changeset.n, &changeset.data)

	if rv != C.SQLITE_OK {
		return nil, Error{Code: ErrNo(rv)}
	}

	runtime.SetFinalizer(changeset, (*SQLiteSession).Finish)
	return changeset, nil
}

// Finish deletes the session.
func (s *SQLiteSession) Finish() error {
	return s.Close()
}

// Close deletes the session.
func (s *SQLiteSession) Close() error {
	// sqlite3session_delete() never fails.
	C.sqlite3session_delete(s.s)

	s.s = nil
	runtime.SetFinalizer(s, nil)

	return nil
}

// SQLiteChangeSet implement interface of ChangeSet.
type SQLiteChangeSet struct {
	n    C.int          // Size of the data buffer
	data unsafe.Pointer // Data buffer
}

// Bytes returns a copy of the data contained in this change set.
func (c *SQLiteChangeSet) Bytes() []byte {
	return C.GoBytes(c.data, c.n)
}

// Apply a changeset to a database.
//
// The underlying schema must match the one the change set was taken on.
func (c *SQLiteChangeSet) Apply(conn *SQLiteConn) error {
	rv := C.sqlite3changeset_apply(conn.db, c.n, c.data, nil, nil, nil)

	if rv != C.SQLITE_OK {
		return Error{Code: ErrNo(rv)}
	}

	return nil
}

// Close releases the memory allocated for this change set.
func (c *SQLiteChangeSet) Close() error {
	C.free(c.data)

	c.n = 0
	c.data = nil

	runtime.SetFinalizer(c, nil)

	return nil
}

// Finish deletes this change set
func (c *SQLiteChangeSet) Finish() error {
	return c.Close()
}
