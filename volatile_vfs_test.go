package sqlite3

import (
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// Exercise the volatile VFS registration.
func TestVolatileFileSystem_Registration(t *testing.T) {
	// Loop a number of times, using different names, as smoke test against
	// memory allocation issues.
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("volatile-%d", i%5)
		fs := RegisterVolatileFileSystem(name)
		defer UnregisterVolatileFileSystem(fs)

		if fs.Name() != name {
			t.Errorf("expected VFS name to be %s, got %s", name, fs.Name())
		}
	}
}

// If the file does not exist and the SQLITE_OPEN_CREATE flag is not passed, an
// error is returned.
func TestVolatileFileSystem_Open_NoEnt(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	file, err := fs.Open("test.db", 0)
	if file != nil {
		t.Errorf("expected file to be nil")
	}
	if err == nil {
		t.Fatalf("expected Open to return an error without the create flag")
	}

	const expectedError = "unable to open database file"
	if err.Error() != expectedError {
		t.Fatalf("Open returned error: %q; expected: %q", err.Error(), expectedError)
	}
	if errno := fs.LastError(); errno != syscall.ENOENT {
		t.Fatalf("expected last error to be ENOENT, got %d instead", errno)
	}
}

// Open a file and close it.
func TestVolatileFileSystem_OpenAndClose(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	file, err := fs.Open("test.db", os.O_CREATE)
	if err != nil {
		t.Fatal("failed to open file", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal("failed to close file", err)
	}
}

// Accessing an existing file returns true.
func TestVolatileFileSystem_Access(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	file, err := fs.Open("test.db", os.O_CREATE)
	if err != nil {
		t.Fatal("failed to open file", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal("failed to close file", err)
	}

	exists, err := fs.Access("test.db")
	if err != nil {
		t.Fatal("failed to access file", err)
	}
	if !exists {
		t.Fatal("Access reported that existent file does not exist")
	}
}

// Trying to access a non existing file returns false.
func TestVolatileFileSystem_Access_NoEnt(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	exists, err := fs.Access("test.db")
	if err != nil {
		t.Fatal("failed to access file", err)
	}
	if exists {
		t.Fatal("Access reported that non-existent file exists")
	}

	if errno := fs.LastError(); errno != syscall.ENOENT {
		t.Fatalf("expected last error to be ENOENT, got %d instead", errno)
	}
}

// Delete a file.
func TestVolatileFileSystem_Delete(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	file, err := fs.Open("test.db", os.O_CREATE)
	if err != nil {
		t.Fatal("failed to open file", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal("failed to close file", err)
	}
	if err := fs.Delete("test.db"); err != nil {
		t.Fatal("failed to delete file", err)
	}

	// Trying to open the file again without the O_CREATE flag results in
	// an error.
	_, err = fs.Open("test.db", 0)
	if err == nil {
		t.Fatalf("expected Open after Delete to fail: got %q", err)
	}
}

// Attempt to delete a file with open file descriptors.
func TestVolatileFileSystem_Delete_Busy(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	file, err := fs.Open("test.db", os.O_CREATE)
	if err != nil {
		t.Fatal("failed to open file", err)
	}
	defer file.Close()

	err = fs.Delete("test.db")
	if err == nil {
		t.Fatalf("expected Delete to return an error when the file is opened")
	}
	const expectedError = "disk I/O error"
	if err.Error() != expectedError {
		t.Fatalf("Delete returned error: %q; expected: %q", err.Error(), expectedError)
	}
	if errno := fs.LastError(); errno != syscall.EBUSY {
		t.Fatalf("expected last error to be EBUSY, got %d instead", errno)
	}
}

// Attempt to read a file that was not written yet, results in an error.
func TestVolatileFileSystem_Read_NeverWritten(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	file, err := fs.Open("test.db", os.O_CREATE)
	if err != nil {
		t.Fatal("failed to open file", err)
	}
	defer file.Close()

	buffer := []byte{'x', 'x'}
	err = file.Read(buffer, 0)
	if err == nil {
		t.Fatal("expected read to never written file to return an error")
	}
	const expectedError = "disk I/O error"
	if err.Error() != expectedError {
		t.Fatalf("read returned error: %q; expected: %q", err.Error(), expectedError)
	}
}

// Write the header of the database file.
func TestVolatileFileSystem_Write_DatabaseHeader(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	file, err := fs.Open("test.db", os.O_CREATE)
	if err != nil {
		t.Fatal("failed to open file", err)
	}
	defer file.Close()

	buffer := make([]byte, 100)

	// Set page size to 512.
	buffer[16] = 2
	buffer[17] = 0

	if err := file.Write(buffer, 0); err != nil {
		t.Fatal("failed to write database header", err)
	}
}

// Write the header of the database file, then the full first page and a second
// page.
func TestVolatileFileSystem_WriteAndRead_DatabasePages(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	file, err := fs.Open("test.db", os.O_CREATE)
	if err != nil {
		t.Fatal("failed to open file", err)
	}
	defer file.Close()

	// Write the header.
	buffer := make([]byte, 100)
	buffer[16] = 2
	buffer[17] = 0
	if err := file.Write(buffer, 0); err != nil {
		t.Fatal("failed to write database header", err)
	}

	// Write the first page, containing the header and some other content.
	buffer = make([]byte, 512)
	buffer[16] = 2
	buffer[17] = 0
	buffer[101] = 1
	buffer[256] = 2
	buffer[511] = 3
	if err := file.Write(buffer, 0); err != nil {
		t.Fatal("failed to write database page 1", err)
	}

	// Write a second page.
	buffer = make([]byte, 512)
	buffer[0] = 4
	buffer[256] = 5
	buffer[511] = 6
	if err := file.Write(buffer, 512); err != nil {
		t.Fatal("failed to database page 2", err)
	}

	// Read the page header.
	buffer = make([]byte, 100)
	if err := file.Read(buffer, 0); err != nil {
		t.Fatal("failed to read database header", err)
	}
	if buffer[16] != 2 {
		t.Fatalf("expected most significant page size byte to be 2, got %d", buffer[16])
	}
	if buffer[17] != 0 {
		t.Fatalf("expected least significant page size byte to be 0, got %d", buffer[17])
	}

	// Read the first page.
	buffer = make([]byte, 512)
	if err := file.Read(buffer, 0); err != nil {
		t.Fatal("failed to read database page 1", err)
	}
	if buffer[16] != 2 {
		t.Fatalf("expected most significant page size byte to be 2, got %d", buffer[16])
	}
	if buffer[17] != 0 {
		t.Fatalf("expected least significant page size byte to be 0, got %d", buffer[17])
	}
	if buffer[101] != 1 {
		t.Fatalf("expected byte 101 of page 1 to be 1, got %d", buffer[101])
	}
	if buffer[256] != 2 {
		t.Fatalf("expected byte 256 of page 1 to be 2, got %d", buffer[256])
	}
	if buffer[511] != 3 {
		t.Fatalf("expected byte 511 of page 1 to be 3, got %d", buffer[511])
	}

	// Read the second page.
	buffer = make([]byte, 512)
	if err := file.Read(buffer, 512); err != nil {
		t.Fatal("failed to read database page 1", err)
	}
	if buffer[0] != 4 {
		t.Fatalf("expected byte 0 of page 2 to be 4, got %d", buffer[101])
	}
	if buffer[256] != 5 {
		t.Fatalf("expected byte 256 of page 2 to be 5, got %d", buffer[256])
	}
	if buffer[511] != 6 {
		t.Fatalf("expected byte 511 of page 1 to be 6, got %d", buffer[511])
	}
}

// Write the header of a WAL file, then two frames.
func TestVolatileFileSystem_WriteAndRead_WalFrames(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	// First write the main database header, which sets the page size.
	file, err := fs.Open("test.db", os.O_CREATE)
	if err != nil {
		t.Fatal("failed to open file", err)
	}
	defer file.Close()
	buffer := make([]byte, 100)
	buffer[16] = 2
	buffer[17] = 0
	if err := file.Write(buffer, 0); err != nil {
		t.Fatal("failed to write database header", err)
	}

	file, err = fs.Open("test.db-wal", os.O_CREATE)
	if err != nil {
		t.Fatal("failed to open file", err)
	}
	defer file.Close()

	// Write the header.
	buffer = make([]byte, 32)
	buffer[10] = 2
	buffer[11] = 0
	if err := file.Write(buffer, 0); err != nil {
		t.Fatal("failed to write WAL header", err)
	}

	// Write the header of the first frame.
	buffer = make([]byte, 24)
	if err := file.Write(buffer, 32); err != nil {
		t.Fatal("failed to write WAL frame header 1", err)
	}

	// Write the page of the first frame.
	buffer = make([]byte, 512)
	if err := file.Write(buffer, 32+24); err != nil {
		t.Fatal("failed to write WAL frame page 1", err)
	}

	// Write the header of the second frame.
	buffer = make([]byte, 24)
	if err := file.Write(buffer, 32+24+512); err != nil {
		t.Fatal("failed to write WAL frame header 2", err)
	}

	// Write the page of the second frame.
	buffer = make([]byte, 512)
	if err := file.Write(buffer, 32+24+512+24); err != nil {
		t.Fatal("failed to write WAL frame page 2", err)
	}

	// Read the WAL header.
	buffer = make([]byte, 32)
	if err := file.Read(buffer, 0); err != nil {
		t.Fatal("failed to read WAL header", err)
	}

	// Read the header of the first frame.
	buffer = make([]byte, 24)
	if err := file.Read(buffer, 32); err != nil {
		t.Fatal("failed to read WAL frame header 1", err)
	}

	// Read the page of the first frame.
	buffer = make([]byte, 512)
	if err := file.Read(buffer, 32+24); err != nil {
		t.Fatal("failed to read WAL frame page 1", err)
	}

	// Read the header of the second frame.
	buffer = make([]byte, 24)
	if err := file.Read(buffer, 32+24+512); err != nil {
		t.Fatal("failed to read WAL frame header 2", err)
	}

	// Read the page of the second frame.
	buffer = make([]byte, 512)
	if err := file.Read(buffer, 32+24+512+24); err != nil {
		t.Fatal("failed to read WAL frame page 2", err)
	}
}

// Truncate the main database file.
func TestVolatileFileSystem_Truncate_Database(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	file, err := fs.Open("test.db", os.O_CREATE)
	if err != nil {
		t.Fatal("failed to open file", err)
	}
	defer file.Close()

	// Initial size is 0.
	size, err := file.Size()
	if err != nil {
		t.Fatal("failed to get database size", err)
	}
	if size != 0 {
		t.Fatal("initial database size is not zero", size)
	}

	// Truncating an empty file is a no-op.
	if err := file.Truncate(0); err != nil {
		t.Fatal("failed to truncate empty database", err)
	}

	// The size is still 0.
	size, err = file.Size()
	if err != nil {
		t.Fatal("failed to get database size", err)
	}
	if size != 0 {
		t.Fatal("database size after truncate is not zero", size)
	}

	// Write the first page, containing the header.
	buffer := make([]byte, 512)
	buffer[16] = 2
	buffer[17] = 0
	if err := file.Write(buffer, 0); err != nil {
		t.Fatal("failed to write database page 1", err)
	}

	// Write a second page.
	buffer = make([]byte, 512)
	if err := file.Write(buffer, 512); err != nil {
		t.Fatal("failed to database page 2", err)
	}

	// The size is 1024.
	size, err = file.Size()
	if err != nil {
		t.Fatal("failed to get database size", err)
	}
	if size != 1024 {
		t.Fatal("database size after writing page 2 is not 1024", size)
	}

	// Truncate the second page.
	if err := file.Truncate(512); err != nil {
		t.Fatal("failed to truncate database page 2", err)
	}

	// The size is 512.
	size, err = file.Size()
	if err != nil {
		t.Fatal("failed to get database size", err)
	}
	if size != 512 {
		t.Fatal("database size after truncating page 2 is not 512", size)
	}

	// Truncate also the first.
	if err := file.Truncate(0); err != nil {
		t.Fatal("failed to truncate database page 1", err)
	}

	// The size is 0.
	size, err = file.Size()
	if err != nil {
		t.Fatal("failed to get database size", err)
	}
	if size != 0 {
		t.Fatal("database size after truncating page 1 is not 0", size)
	}
}

// Truncate the WAL file.
func TestVolatileFileSystem_Truncate_WAL(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	// First write the main database header, which sets the page size.
	file, err := fs.Open("test.db", os.O_CREATE)
	if err != nil {
		t.Fatal("failed to open file", err)
	}
	defer file.Close()
	buffer := make([]byte, 100)
	buffer[16] = 2
	buffer[17] = 0
	if err := file.Write(buffer, 0); err != nil {
		t.Fatal("failed to write database header", err)
	}

	file, err = fs.Open("test.db-wal", os.O_CREATE)
	if err != nil {
		t.Fatal("failed to open file", err)
	}
	defer file.Close()

	// Initial size is 0.
	size, err := file.Size()
	if err != nil {
		t.Fatal("failed to get WAL size", err)
	}
	if size != 0 {
		t.Fatal("initial WAL size is not zero", size)
	}

	// Truncating an empty file is a no-op.
	if err := file.Truncate(0); err != nil {
		t.Fatal("failed to truncate empty WAL", err)
	}

	// The size is still 0.
	size, err = file.Size()
	if err != nil {
		t.Fatal("failed to get WAL size", err)
	}
	if size != 0 {
		t.Fatal("WAL size after truncate is not zero", size)
	}

	// Write the WAL header.
	buffer = make([]byte, 32)
	buffer[10] = 2
	buffer[11] = 0
	if err := file.Write(buffer, 0); err != nil {
		t.Fatal("failed to write WAL header", err)
	}

	// Write the header of the first frame.
	buffer = make([]byte, 24)
	if err := file.Write(buffer, 32); err != nil {
		t.Fatal("failed to write WAL frame header 1", err)
	}

	// Write the page of the first frame.
	buffer = make([]byte, 512)
	if err := file.Write(buffer, 32+24); err != nil {
		t.Fatal("failed to write WAL frame page 1", err)
	}

	// Write the header of the second frame.
	buffer = make([]byte, 24)
	if err := file.Write(buffer, 32+24+512); err != nil {
		t.Fatal("failed to write WAL frame header 2", err)
	}

	// Write the page of the second frame.
	buffer = make([]byte, 512)
	if err := file.Write(buffer, 32+24+512+24); err != nil {
		t.Fatal("failed to write WAL frame page 2", err)
	}

	// The size is 1104.
	size, err = file.Size()
	if err != nil {
		t.Fatal("failed to get WAL size", err)
	}
	if size != 1104 {
		t.Fatal("WAL size after writing frame 2 is not 1104", size)
	}

	// Truncate the wal File.
	if err := file.Truncate(0); err != nil {
		t.Fatal("failed to truncate WAL", err)
	}

	// The size is 0.
	size, err = file.Size()
	if err != nil {
		t.Fatal("failed to get WAL size", err)
	}
	if size != 0 {
		t.Fatal("WAL size after truncating frame 1 is not 0", size)
	}

}

// Exercise the volatile VFS implementation.
func TestVolatileFileSystem_Integration(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)

	// Open a connection using the volatile VFS as backend.
	drv := &SQLiteDriver{}
	conni, err := drv.Open("file:test.db?vfs=volatile")
	if err != nil {
		t.Fatal("failed to open connection with volatile VFS", err)
	}
	conn := conni.(*SQLiteConn)

	// Set the page size and disable syncs.
	if _, err := conn.Exec("PRAGMA page_size=512;", nil); err != nil {
		t.Fatal("Failed to Exec PRAGMA page_size:", err)
	}
	if _, err := conn.Exec("PRAGMA synchronous=OFF;", nil); err != nil {
		t.Fatal("Failed to Exec PRAGMA page_size:", err)
	}

	// Set WAL journaling.
	if _, err := conn.Exec("PRAGMA journal_mode=WAL;", nil); err != nil {
		t.Fatal("Failed to Exec PRAGMA journal_mode:", err)
	}

	// Create a test table and insert a few rows into it.
	if _, err := conn.Exec("CREATE TABLE test (n INT)", nil); err != nil {
		t.Fatal("failed to create table on volatile VFS", err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal("failed to begin transaction on volatile VFS", err)
	}

	for i := 0; i < 100; i++ {
		_, err = conn.Exec("INSERT INTO test(n) VALUES(?)", []driver.Value{int64(i)})
		if err != nil {
			t.Fatal("failed to insert value on volatile VFS", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal("failed to commit transaction on volatile VFS", err)
	}

	// Assert that the rows are actually there.
	assertTestTableRows(t, conn, 100)

	// Take a full checkpoint of the volatile database.
	size, ckpt, err := conn.WalCheckpoint("main", WalCheckpointTruncate)
	if err != nil {
		t.Fatal("failed to perform WAL checkpoint on volatile VFS", err)
	}
	if size != 0 {
		t.Fatalf("expected size to be %d, got %d", 0, size)
	}
	if ckpt != 0 {
		t.Fatalf("expected ckpt to be %d, got %d", 0, ckpt)
	}

	// Close the connection to the volatile database.
	if err := conn.Close(); err != nil {
		t.Fatal("failed to close connection on volatile VFS", err)
	}

	// Dump the content of the volatile file system and check that the
	// database data are still intact when queried with a regular
	// connection.
	dir, err := ioutil.TempDir("", "go-sqlite3-volatile-vfs-")
	if err != nil {
		t.Fatal("failed to create temporary directory for VFS dump", err)
	}
	defer os.RemoveAll(dir)
	if err := fs.Dump(dir); err != nil {
		t.Fatal("failed to dump volatile VFS", err)
	}

	conni, err = drv.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal("failed to open connection to dumped volatile database", err)
	}
	conn = conni.(*SQLiteConn)
	assertTestTableRows(t, conn, 100)
	if err := conn.Close(); err != nil {
		t.Fatal("failed to close connection to dumped volatile database", err)
	}
}

// Exercise ReadFile and WriteFile APIs.
func TestVolatileFileSystem_CreateFile(t *testing.T) {
	fs := RegisterVolatileFileSystem("volatile")
	defer UnregisterVolatileFileSystem(fs)
	// Open a connection using the volatile VFS as backend.

	drv := &SQLiteDriver{}
	conni, err := drv.Open("file:test.db?vfs=volatile")
	if err != nil {
		t.Fatal("failed to open connection with volatile VFS", err)
	}
	conn := conni.(*SQLiteConn)

	// Set the page size and disable syncs.
	if _, err := conn.Exec("PRAGMA page_size=512;", nil); err != nil {
		t.Fatal("Failed to Exec PRAGMA page_size:", err)
	}
	if _, err := conn.Exec("PRAGMA synchronous=OFF;", nil); err != nil {
		t.Fatal("Failed to Exec PRAGMA page_size:", err)
	}

	// Set WAL journaling.
	if _, err := conn.Exec("PRAGMA journal_mode=WAL;", nil); err != nil {
		t.Fatal("Failed to Exec PRAGMA journal_mode:", err)
	}

	// Create a test table.
	if _, err := conn.Exec("CREATE TABLE test (n INT)", nil); err != nil {
		t.Fatal("failed to create table on volatile VFS", err)
	}

	// Read the database and the WAL.
	database, err := fs.ReadFile("test.db")
	if err != nil {
		t.Fatal("failed to read database file", err)
	}
	wal, err := fs.ReadFile("test.db-wal")
	if err != nil {
		t.Fatal("failed to read WAL file", err)
	}

	// Close the connection to the volatile database.
	if err := conn.Close(); err != nil {
		t.Fatal("failed to close connection on volatile VFS", err)
	}

	// Write the database and WAL under a different name.
	if err := fs.CreateFile("test2.db", database); err != nil {
		t.Fatal("failed to write database file", err)
	}
	if err := fs.CreateFile("test2.db-wal", wal); err != nil {
		t.Fatal("failed to write WAL file", err)
	}

	// Open a connection against the new database.
	conni, err = drv.Open("file:test2.db?vfs=volatile")
	if err != nil {
		t.Fatal("failed to open connection with volatile VFS", err)
	}
	conn = conni.(*SQLiteConn)

	assertTestTableRows(t, conn, 0)

	// Close the connection against the new database.
	if err := conn.Close(); err != nil {
		t.Fatal("failed to close connection on volatile VFS", err)
	}
}

func assertTestTableRows(t *testing.T, conn *SQLiteConn, n int) {
	rows, err := conn.Query("SELECT n FROM test", nil)
	if err != nil {
		t.Fatal("failed to query test table", err)
	}
	for i := 0; i < n; i++ {
		values := make([]driver.Value, 1)
		if err := rows.Next(values); err != nil {
			t.Fatal("failed to fetch test table row", err)
		}
		n, ok := values[0].(int64)
		if !ok {
			t.Fatal("expected int64 row value")
		}
		if int(n) != i {
			t.Fatalf("expected row value to be %d, got %d", i, n)
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal("failed to close test table result set", err)
	}
}
