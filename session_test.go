// Copyright (C) 2018 Yasuhiro Matsumoto <mattn.jp@gmail.com>.
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
// +build session

package sqlite3

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestSessionUninitialized(t *testing.T) {
	session := SQLiteSession{}
	if session.s != nil {
		t.Fatal("Expected reference to sqlite3_session to be nil")
	}
}

func TestSessionOpenAndClose(t *testing.T) {
	_, _, cleanup := testSessionOpen(t, "openAndClose")
	defer cleanup()
}

// Create a SQLiteSession object attached to an in-memory test database.
//
// Return the test database, the newly created session and a cleanup function
// to safely close both the session and the database.
func testSessionOpen(t *testing.T, test string) (*sql.DB, *SQLiteSession, func()) {
	driverConns := []*SQLiteConn{}
	driverName := fmt.Sprintf("sqlite3_testSession_%s", test)
	sql.Register(driverName, &SQLiteDriver{
		ConnectHook: func(conn *SQLiteConn) error {
			driverConns = append(driverConns, conn)
			return nil
		},
	})

	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatal("Failed to open the session test database:", err)
	}

	if err := db.Ping(); err != nil {
		t.Fatal("Failed to connect to the session test database:", err)
	}

	conn := driverConns[0]
	session, err := conn.Session("main")
	if err != nil {
		t.Fatal("Failed to create new session:", err)
	}

	cleanup := func() {
		if err := session.Close(); err != nil {
			t.Fatal("Failed to close test session:", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal("Failed to close the session test database:", err)
		}
	}

	return db, session, cleanup
}
