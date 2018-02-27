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

func TestSessionAttachSingleTable(t *testing.T) {
	db, session, cleanup := testSessionOpen(t, "attachSingleTable")
	defer cleanup()

	if err := session.Attach("test1"); err != nil {
		t.Fatal("Failed to attach table 'test1' to test session:", err)
	}

	testSessionCreateTables(t, db)

	// Insert into test2 table. Since it's not attached to the session, it
	// should not result in any change set data.
	testSessionInsert(t, db, "test2", 1, "hello")

	changeset, err := session.ChangeSet()
	if err != nil {
		t.Fatal("Failed to obtain change set after inserting into table 'test2':", err)
	}
	bytes := changeset.Bytes()
	if n := len(bytes); n > 0 {
		t.Fatal("Expected change set to have no data after inserting into table 'test2'")
	}

	// Insert into test1 table. Since this table is attached to the
	// session, it should result in a non-empty change set.
	testSessionInsert(t, db, "test1", 1, "hello")

	changeset, err = session.ChangeSet()
	if err != nil {
		t.Fatal("Failed to obtain change set after inserting into table 'test1':", err)
	}

	bytes = changeset.Bytes()
	if n := len(bytes); n == 0 {
		t.Fatal("Expected change set to have some data after inserting into table 'test1'")
	}
}

func TestSessionAttachAllTables(t *testing.T) {
	db, session, cleanup := testSessionOpen(t, "attachAllTables")
	defer cleanup()

	if err := session.Attach(""); err != nil {
		t.Fatal("Failed to attach all tables to test session:", err)
	}

	testSessionCreateTables(t, db)

	// Insert into test1 table. Since all tables are attached to the
	// session, it should result in a non-empty change set.
	testSessionInsert(t, db, "test1", 1, "hello")

	changeset, err := session.ChangeSet()
	if err != nil {
		t.Fatal("Failed to obtain change set after inserting into table 'test1':", err)
	}

	bytes := changeset.Bytes()
	n1 := len(bytes)
	if n1 == 0 {
		t.Fatal("Expected change set to have some data after inserting into table 'test1'")
	}
	if err := changeset.Close(); err != nil {
		t.Fatal("Failed to release change set data after inserting into table 'test1':", err)
	}

	// Insert into test2 table. Since all tables are attached to the
	// session, it should result in a bigger change set.
	testSessionInsert(t, db, "test2", 1, "hello")

	changeset, err = session.ChangeSet()
	if err != nil {
		t.Fatal("Failed to obtain change set after inserting into table 'test2':", err)
	}
	bytes = changeset.Bytes()
	n2 := len(bytes)
	if n2 == 0 {
		t.Fatal("Expected change set to have some data after inserting into table 'test2'")
	}

	if n1 >= n2 {
		t.Fatalf("Expected size of second change set (%d) to be greater than the first (%d)", n2, n1)
	}

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

// Create test tables ('test1' and 'test2') to be used in session tests.
func testSessionCreateTables(t *testing.T, db *sql.DB) {
	for _, table := range []string{"test1", "test2"} {
		stmt := fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, value TEXT)", table)
		_, err := db.Exec(stmt)
		if err != nil {
			t.Fatalf("Failed to create '%s' table in the session test database: %v", table, err)
		}
	}
}

// Insert a new row in the given test table.
func testSessionInsert(t *testing.T, db *sql.DB, table string, id int, value string) {
	stmt := fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", table)
	if _, err := db.Exec(stmt, id, value); err != nil {
		t.Fatalf("Failed to insert row (%d, %s) into table '%s': %v", id, value, table, err)
	}
}
