package sqlite3

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"unsafe"
)

// Register and unregister a WAL replication implementation.
func TestWalReplicationRegistration(t *testing.T) {
	replication := NoopWalReplication()

	if err := WalReplicationRegister("noop", replication); err != nil {
		t.Fatal("WAL replication registration failed", err)
	}

	if err := WalReplicationUnregister(replication); err != nil {
		t.Fatal("WAL replication unregistration failed", err)
	}
}

// Exercise failure modes when enabling WAL replication.
func TestWalReplication_EnableErrors(t *testing.T) {
	cases := []struct {
		name string                                     // Name of the test
		f    func(t *testing.T, conn *SQLiteConn) error // Scenario leading to an error
	}{
		{
			"connection not in WAL mode: follower",
			func(t *testing.T, conn *SQLiteConn) error {
				return conn.WalReplicationLeader("noop")
			},
		},
		{
			"connection not in WAL mode: leader",
			func(t *testing.T, conn *SQLiteConn) error {
				return conn.WalReplicationFollower()
			},
		},
		{
			"cannot set twice: leader",
			func(t *testing.T, conn *SQLiteConn) error {
				pragmaWAL(t, conn)
				err := conn.WalReplicationLeader("noop")
				if err != nil {
					t.Fatal("failed to set leader replication:", err)
				}
				return conn.WalReplicationLeader("noop")
			},
		},
		{
			"cannot set twice: follower",
			func(t *testing.T, conn *SQLiteConn) error {
				pragmaWAL(t, conn)
				err := conn.WalReplicationFollower()
				if err != nil {
					t.Fatal("failed to set follower replication:", err)
				}
				return conn.WalReplicationFollower()
			},
		},
		{
			"cannot switch from leader to follower",
			func(t *testing.T, conn *SQLiteConn) error {
				pragmaWAL(t, conn)
				err := conn.WalReplicationLeader("noop")
				if err != nil {
					t.Fatal("failed to set leader replication:", err)
				}
				return conn.WalReplicationFollower()
			},
		},
		{
			"cannot switch from follower to leader",
			func(t *testing.T, conn *SQLiteConn) error {
				pragmaWAL(t, conn)
				err := conn.WalReplicationFollower()
				if err != nil {
					t.Fatal("failed to set follower replication:", err)
				}
				return conn.WalReplicationLeader("noop")
			},
		},
		{
			"cannot run queries as follower",
			func(t *testing.T, conn *SQLiteConn) error {
				pragmaWAL(t, conn)
				err := conn.WalReplicationFollower()
				if err != nil {
					t.Fatal("failed to set follower replication:", err)
				}
				_, err = conn.Query("SELECT 1", nil)
				return err
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tempFilename := TempFilename(t)
			defer os.Remove(tempFilename)

			replication := NoopWalReplication()
			if err := WalReplicationRegister("noop", replication); err != nil {
				t.Fatal("WAL replication registration failed", err)
			}
			defer WalReplicationUnregister(replication)

			driver := &SQLiteDriver{}
			conn, err := driver.Open(tempFilename)
			if err != nil {
				t.Fatalf("can't open connection to %s: %v", tempFilename, err)
			}
			conni := conn.(*SQLiteConn)
			defer conni.Close()

			err = c.f(t, conni)
			if err == nil {
				t.Fatal("no error was returned")
			}
			erri, ok := err.(Error)
			if !ok {
				t.Fatalf("returned error %#v is not of type Error", erri)
			}
			if erri.Code != ErrError {
				t.Errorf("expected error code %d, got %d", ErrError, erri.Code)
			}

		})
	}
}

func TestWalReplication(t *testing.T) {
	conns := make([]*SQLiteConn, 2) // Index 0 is the leader and index 1 is the follower

	// Open the connections.
	driver := &SQLiteDriver{}
	for i := range conns {
		tempFilename := TempFilename(t)
		//defer os.Remove(tempFilename)
		conn, err := driver.Open(tempFilename)
		if err != nil {
			t.Fatalf("can't open connection to %s: %v", tempFilename, err)
		}
		defer conn.Close()

		conni := conn.(*SQLiteConn)
		pragmaWAL(t, conni)
		conns[i] = conni
	}
	leader := conns[0]
	follower := conns[1]

	replication := &directWalReplication{
		follower: follower,
	}
	if err := WalReplicationRegister("direct", replication); err != nil {
		t.Fatal("WAL replication registration failed", err)
	}
	defer WalReplicationUnregister(replication)

	// Set leader replication on conn 0.
	if err := leader.WalReplicationLeader("direct"); err != nil {
		t.Fatal("failed to switch to leader replication:", err)
	}

	// Set follower replication on conn 1.
	if err := follower.WalReplicationFollower(); err != nil {
		t.Fatal("failed to switch to follower replication:", err)
	}

	// Create a table on the leader.
	if _, err := leader.Exec("CREATE TABLE a (n INT)", nil); err != nil {
		t.Fatal("failed to execute query on leader:", err)
	}

	// Rollback a transaction on the leader.
	if _, err := leader.Exec("BEGIN; CREATE TABLE b (n INT); ROLLBACK", nil); err != nil {
		t.Fatal("failed to rollback query on leader:", err)
	}

	// Check that the follower has replicated the commit but not the rollback.
	if err := follower.WalReplicationNone(); err != nil {
		t.Fatal("failed to turn off follower replication:", err)
	}
	if _, err := follower.Query("SELECT 1", nil); err != nil {
		t.Fatal("failed to execute query on follower:", err)
	}
	if _, err := follower.Query("SELECT n FROM a", nil); err != nil {
		t.Fatal("failed to execute query on follower:", err)
	}
	if _, err := follower.Query("SELECT n FROM b", nil); err == nil {
		t.Fatal("expected error when querying rolled back table:", err)
	}
}

// An xBegin error never triggers an xUndo callback and SQLite takes care of
// releasing the WAL write lock, if acquired.
func TestWalReplication_BeginError(t *testing.T) {
	// Open the leader connection.
	cases := []struct {
		errno ErrNoExtended
		lock  bool
	}{
		{ErrConstraintCheck, false},
		{ErrConstraintCheck, true},
		{ErrCorruptVTab, true},
		{ErrCorruptVTab, false},
		{ErrIoErrNotLeader, true},
		{ErrIoErrNotLeader, false},
		{ErrIoErrLeadershipLost, true},
		{ErrIoErrLeadershipLost, false},
		{ErrIoErrRead, true},
		{ErrIoErrRead, false},
		{ErrIoErrWrite, true},
		{ErrIoErrWrite, false},
	}

	for _, c := range cases {
		name := fmt.Sprintf("%s-%v", c.errno, c.lock)
		t.Run(name, func(t *testing.T) {
			// Create a leader connection with the appropriate
			// replication methods.
			driver := &SQLiteDriver{}
			tempFilename := TempFilename(t)
			defer os.Remove(tempFilename)

			conni, err := driver.Open(tempFilename)
			if err != nil {
				t.Fatalf("can't open connection to %s: %v", tempFilename, err)
			}
			defer conni.Close()

			conn := conni.(*SQLiteConn)
			pragmaWAL(t, conn)

			// Set leader replication on conn 0.
			replication := &failingWalReplication{
				conn:  conn,
				hook:  "begin",
				lock:  c.lock,
				errno: c.errno,
			}
			if err := WalReplicationRegister("failing", replication); err != nil {
				t.Fatal("WAL replication registration failed", err)
			}
			defer WalReplicationUnregister(replication)

			if err := conn.WalReplicationLeader("failing"); err != nil {
				t.Fatal("failed to switch to leader replication:", err)
			}

			// Execute a query that should error and be rolled back.
			tx, err := conn.Begin()
			if err != nil {
				t.Fatal("failed to begin transaction", err)
			}
			_, err = conn.Exec("CREATE TABLE test (n INT)", nil)
			erri, ok := err.(Error)
			if !ok {
				t.Fatalf("returned error %#v is not of type Error", erri)
			}
			if erri.ExtendedCode != c.errno {
				t.Errorf("expected error code %d, got %d", c.errno, erri.ExtendedCode)
			}
			err = tx.Rollback()

			// ErrIo errors will also fail to rollback, while other
			// errors are fine.
			if erri.Code == ErrIoErr {
				if err == nil {
					t.Fatal("expected rollback error")
				}
				if err.Error() != "cannot rollback - no transaction is active" {
					t.Fatal("expected different rollback error")
				}
			} else {
				if err != nil {
					t.Fatal("failed to rollback", err)
				}
			}

			// Execute a second query with no error.
			replication.hook = ""
			tx, err = conn.Begin()
			if err != nil {
				t.Fatal("failed to begin transaction", err)
			}
			_, err = conn.Exec("CREATE TABLE test (n INT)", nil)
			if err != nil {
				t.Fatal("failed to execute query", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal("failed to commit transaction", err)
			}
		})
	}
}

// An xFrames error triggers the xUndo callback.
func TestWalReplication_FramesError(t *testing.T) {
	// Create a leader connection with the appropriate
	// replication methods.
	driver := &SQLiteDriver{}
	tempFilename := TempFilename(t)
	defer os.Remove(tempFilename)

	conni, err := driver.Open(tempFilename)
	if err != nil {
		t.Fatalf("can't open connection to %s: %v", tempFilename, err)
	}
	defer conni.Close()

	conn := conni.(*SQLiteConn)
	pragmaWAL(t, conn)

	// Set leader replication on conn 0.
	replication := &failingWalReplication{
		conn:  conn,
		hook:  "frames",
		errno: ErrIoErrNotLeader,
	}
	if err := WalReplicationRegister("failing", replication); err != nil {
		t.Fatal("WAL replication registration failed", err)
	}
	defer WalReplicationUnregister(replication)

	if err := conn.WalReplicationLeader("failing"); err != nil {
		t.Fatal("failed to switch to leader replication:", err)
	}
	_, err = conn.Exec("CREATE TABLE test (n INT)", nil)
	erri, ok := err.(Error)
	if !ok {
		t.Fatalf("returned error %#v is not of type Error", erri)
	}
	if erri.ExtendedCode != ErrIoErrNotLeader {
		t.Errorf("expected error code %d, got %d", ErrIoErrNotLeader, erri.ExtendedCode)
	}
	if n := len(replication.fired); n != 4 {
		t.Fatalf("expected 4 hooks to be fired, instead of %d", n)
	}
	hooks := []string{"begin", "frames", "undo", "end"}
	for i := range replication.fired {
		if hook := replication.fired[i]; hook != hooks[i] {
			t.Errorf("expected hook %s to be fired, instead of %s", hooks[i], hook)
		}
	}
}

// If an xUndo hook fails, the ROLLBACK query still succeeds.
func TestReplicationMethods_UndoError(t *testing.T) {
	// Create a leader connection with the appropriate
	// replication methods.
	driver := &SQLiteDriver{}
	tempFilename := TempFilename(t)
	defer os.Remove(tempFilename)

	conni, err := driver.Open(tempFilename)
	if err != nil {
		t.Fatalf("can't open connection to %s: %v", tempFilename, err)
	}
	defer conni.Close()

	conn := conni.(*SQLiteConn)
	pragmaWAL(t, conn)

	// Set leader replication on conn 0.
	replication := &failingWalReplication{
		conn:  conn,
		hook:  "undo",
		errno: ErrIoErrNotLeader,
	}
	if err := WalReplicationRegister("failing", replication); err != nil {
		t.Fatal("WAL replication registration failed", err)
	}
	defer WalReplicationUnregister(replication)

	if err := conn.WalReplicationLeader("failing"); err != nil {
		t.Fatal("failed to switch to leader replication:", err)
	}
	_, err = conn.Exec("BEGIN; CREATE TABLE test (n INT); ROLLBACK", nil)
	if err != nil {
		t.Fatal("rollback failed", err)
	}
	if n := len(replication.fired); n != 3 {
		t.Fatalf("expected 3 hooks to be fired, instead of %d", n)
	}
	hooks := []string{"begin", "undo", "end"}
	for i := range replication.fired {
		if hook := replication.fired[i]; hook != hooks[i] {
			t.Errorf("expected hook %s to be fired, instead of %s", hooks[i], hook)
		}
	}
}

// WalReplication implementation that replicates WAL commands directly to the
// given follower.
type directWalReplication struct {
	follower *SQLiteConn
	writing  bool
}

func (r *directWalReplication) Begin(conn *SQLiteConn) ErrNo {
	return 0
}

func (r *directWalReplication) Abort(conn *SQLiteConn) ErrNo {
	return 0
}

func (r *directWalReplication) Frames(conn *SQLiteConn, list WalReplicationFrameList) ErrNo {
	begin := false
	if !r.writing {
		begin = true
		r.writing = true
	}

	pageSize := list.PageSize()
	length := list.Len()

	info := WalReplicationFrameInfo{}
	info.IsBegin(begin)
	info.PageSize(pageSize)
	info.Len(length)
	info.Truncate(list.Truncate())
	info.IsCommit(list.IsCommit())

	numbers := make([]PageNumber, length)
	pages := make([]byte, length*pageSize)
	for i := range numbers {
		data, pgno, _ := list.Frame(i)
		numbers[i] = pgno
		header := reflect.SliceHeader{Data: uintptr(data), Len: pageSize, Cap: pageSize}
		var slice []byte
		slice = reflect.NewAt(reflect.TypeOf(slice), unsafe.Pointer(&header)).Elem().Interface().([]byte)
		copy(pages[i*pageSize:(i+1)*pageSize], slice)
	}
	info.Pages(numbers, unsafe.Pointer(&pages[0]))

	if err := r.follower.WalReplicationFrames(info); err != nil {
		panic(fmt.Sprintf("frames failed: %v", err))
	}

	if list.IsCommit() {
		r.writing = false
	}
	return 0
}

func (r *directWalReplication) Undo(conn *SQLiteConn) ErrNo {
	if r.writing {
		if err := r.follower.WalReplicationUndo(); err != nil {
			panic(fmt.Sprintf("undo failed: %v", err))
		}
	}
	return 0
}

func (r *directWalReplication) End(conn *SQLiteConn) ErrNo {
	return 0
}

// WalReplication implementation that fails in a programmable way.
type failingWalReplication struct {
	conn  *SQLiteConn   // Leader connection
	lock  bool          // Whether to acquire the WAL write lock before failing
	hook  string        // Name of the hook that should fail
	errno ErrNoExtended // Error to be returned by the hook
	fired []string      // Hooks that were fired
}

func (r *failingWalReplication) Begin(conn *SQLiteConn) ErrNo {
	r.fired = append(r.fired, "begin")

	if r.hook == "begin" {
		return ErrNo(r.errno)
	}

	return 0
}

func (r *failingWalReplication) Abort(conn *SQLiteConn) ErrNo {
	return 0
}

func (r *failingWalReplication) Frames(conn *SQLiteConn, list WalReplicationFrameList) ErrNo {
	r.fired = append(r.fired, "frames")

	if r.hook == "begin" {
		panic("frames hook should not be reached")
	}
	if r.hook == "frames" {
		return ErrNo(r.errno)
	}

	return 0
}

func (r *failingWalReplication) Undo(conn *SQLiteConn) ErrNo {
	r.fired = append(r.fired, "undo")

	if r.hook == "begin" {
		panic("undo hook should not be reached")
	}
	if r.hook == "undo" {
		return ErrNo(r.errno)
	}

	return 0
}

func (r *failingWalReplication) End(conn *SQLiteConn) ErrNo {
	r.fired = append(r.fired, "end")

	if r.hook == "end" {
		return ErrNo(r.errno)
	}

	return 0
}
