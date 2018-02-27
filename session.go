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
