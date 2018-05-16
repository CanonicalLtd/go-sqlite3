package sqlite3

/*
#ifndef USE_LIBSQLITE3
#include <sqlite3-binding.h>
#else
#include <sqlite3.h>
#endif
#include <stdlib.h>

// WAL replication trampolines.
int walReplicationBegin(int iReplication, int iConn);
int walReplicationAbort(int iReplication, int iConn);
int walReplicationFrames(int iReplication, int iConn,
      int, int, sqlite3_wal_replication_frame*, unsigned, int, unsigned);
int walReplicationUndo(int iReplication, int iConn);
int walReplicationEnd(int iReplication, int iConn);

// Wal replication methods.
static int sqlite3WalReplicationBegin(sqlite3_wal_replication *p, void *pArg){
  int iReplication = *(int*)(p->pAppData);
  int iConn = *(int*)(pArg);
  return walReplicationBegin(iReplication, iConn);
}

static int sqlite3WalReplicationAbort(sqlite3_wal_replication *p, void *pArg){
  int iReplication = *(int*)(p->pAppData);
  int iConn = *(int*)(pArg);
  return walReplicationAbort(iReplication, iConn);
}

static int sqlite3WalReplicationFrames(sqlite3_wal_replication *p, void *pArg,
      int szPage, int nList, sqlite3_wal_replication_frame *pList,
       unsigned nTruncate, int isCommit, unsigned sync_flags
){
  int iReplication = *(int*)(p->pAppData);
  int iConn = *(int*)(pArg);
  return walReplicationFrames(
      iReplication, iConn, szPage, nList, pList, nTruncate, isCommit, sync_flags);
}

static int sqlite3WalReplicationUndo(sqlite3_wal_replication *p, void *pArg){
  int iReplication = *(int*)(p->pAppData);
  int iConn = *(int*)(pArg);
  return walReplicationUndo(iReplication, iConn);
}

static int sqlite3WalReplicationEnd(sqlite3_wal_replication *p, void *pArg){
  int iReplication = *(int*)(p->pAppData);
  int iConn = *(int*)(pArg);
  return walReplicationEnd(iReplication, iConn);
}

static int sqlite3WalReplicationRegister(char *zName, int iReplication){
  sqlite3_wal_replication *p;
  void *pAppData;
  int rc;

  p = (sqlite3_wal_replication*)sqlite3_malloc(sizeof(sqlite3_wal_replication));
  if( !p ){
    return SQLITE_NOMEM;
  }

  pAppData = (void*)malloc(sizeof(int));
  if( !pAppData ){
    return SQLITE_NOMEM;
  }
  *(int*)(pAppData) = iReplication;

  p->iVersion = 1;
  p->zName    = (const char*)(zName);
  p->pAppData = pAppData;
  p->xBegin   = sqlite3WalReplicationBegin;
  p->xAbort   = sqlite3WalReplicationAbort;
  p->xFrames  = sqlite3WalReplicationFrames;
  p->xUndo    = sqlite3WalReplicationUndo;
  p->xEnd     = sqlite3WalReplicationEnd;

  rc = sqlite3_wal_replication_register(p, 0);

  return rc;
}

static int sqlite3WalReplicationUnregister(char *zName) {
  int rc;
  sqlite3_wal_replication *p = sqlite3_wal_replication_find(zName);
  if( !p ){
    return SQLITE_ERROR;
  }

  rc = sqlite3_wal_replication_unregister(p);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  free(p->pAppData);
  free((char*)(p->zName));
  sqlite3_free(p);

  return SQLITE_OK;
}

*/
import "C"
import (
	"sync"
	"unsafe"
)

// WalReplicationFrames information about a single batch of WAL frames that are
// being dispatched for replication. They map to the parameters of the
// sqlite3_wal_replication.xFrames API and sqlite3_wal_replication_frames C
// APIs.
type WalReplicationFrames struct {
	PageSize  int
	Len       int
	List      unsafe.Pointer
	Truncate  uint32
	IsCommit  int
	SyncFlags uint8
}

// WalReplication offers a Go-friendly interface around the low level
// sqlite3_wal_replication C type. The methods are supposed to implement
// application-specific logic in response to replication callbacks triggered by
// sqlite.
type WalReplication interface {
	// Begin a new write transaction. The implementation should check
	// that the connection is eligible for starting a replicated write
	// transaction (e.g. this node is the leader), and perform internal
	// state changes as appropriate.
	Begin(*SQLiteConn) ErrNo

	// Abort a write transaction. The implementation should clear any
	// state previously set by the Begin hook.
	Abort(*SQLiteConn) ErrNo

	// Write new frames to the write-ahead log. The implementation should
	// broadcast this write to other nodes and wait for a quorum.
	Frames(*SQLiteConn, WalReplicationFrames) ErrNo

	// Undo a write transaction. The implementation should broadcast
	// this event to other nodes and wait for a quorum. The return code
	// is currently ignored by SQLite.
	Undo(*SQLiteConn) ErrNo

	// End a write transaction. The implementation should update its
	// internal state and be ready for a new transaction.
	End(*SQLiteConn) ErrNo
}

// WalReplicationRegister registers a WalReplication implementation under the
// given name.
func WalReplicationRegister(name string, replication WalReplication) error {
	walReplicationLock.Lock()
	defer walReplicationLock.Unlock()

	if _, ok := walReplicationNames[name]; ok {
		return newError(C.SQLITE_ERROR)
	}

	iReplication := walReplicationHandles
	walReplicationHandles++

	zName := C.CString(name)
	rv := C.sqlite3WalReplicationRegister(zName, iReplication)
	if rv != C.SQLITE_OK {
		return newError(rv)
	}

	walReplicationNames[name] = replication
	walReplications[iReplication] = replication
	walReplicationConns[iReplication] = make(map[C.int]*SQLiteConn)

	return nil
}

// WalReplicationUnregister unregisters the given WalReplication
// implementation.
func WalReplicationUnregister(replication WalReplication) error {
	walReplicationLock.Lock()
	defer walReplicationLock.Unlock()

	// Figure out the name this replication is registered with.
	var name string
	for name = range walReplicationNames {
		if walReplicationNames[name] == replication {
			break
		}
	}
	if name == "" {
		return newError(C.SQLITE_ERROR)
	}

	// Unregister from SQLite.
	zName := C.CString(name)
	defer C.free(unsafe.Pointer(zName))

	rv := C.sqlite3WalReplicationUnregister(zName)
	if rv != C.SQLITE_OK {
		return newError(rv)
	}

	// Cleanup registry and allocated connection args.
	var iReplication C.int
	for iReplication = range walReplications {
		if walReplications[iReplication] == replication {
			delete(walReplications, iReplication)
			break
		}
	}
	for iConn := range walReplicationConns[iReplication] {
		C.free(walReplicationConnsArg[iConn])
		delete(walReplicationConnsArg, iConn)
	}
	delete(walReplicationConns, iReplication)
	delete(walReplicationNames, name)

	return nil
}

// WalReplicationLeader switches this sqlite connection to leader WAL
// replication mode. The WalReplication instance registered with the given name
// are hooks for driving the execution of the WAL replication in "follower"
// connections.
func (c *SQLiteConn) WalReplicationLeader(name string) error {
	walReplicationLock.Lock()
	defer walReplicationLock.Unlock()

	// Check that we have a matching WalReplication registered.
	replication, ok := walReplicationNames[name]
	if !ok {
		return newError(C.SQLITE_ERROR)
	}

	// Figure out the WalReplication ID.
	var iReplication C.int
	for iReplication = range walReplications {
		if walReplications[iReplication] == replication {
			break
		}
	}

	// Check that this connection is not already registered as leader for
	// this WAL replication.
	for _, conn := range walReplicationConns[iReplication] {
		if conn == c {
			return newError(C.SQLITE_ERROR)
		}
	}

	// Assign an handle to the connection and associate it with the given
	// replication.
	iConn := walReplicationConnsIndex
	walReplicationConns[iReplication][iConn] = c
	walReplicationConnsIndex++

	zName := C.CString(name)
	defer C.free(unsafe.Pointer(zName))

	pArg := C.malloc(C.size_t(iConn))
	*(*C.int)(pArg) = iConn
	walReplicationConnsArg[iConn] = pArg

	rv := C.sqlite3_wal_replication_leader(c.db, walReplicationSchema, zName, pArg)
	if rv != C.SQLITE_OK {
		return newError(rv)
	}

	return nil
}

// WalReplicationFollower switches the given sqlite connection to follower WAL
// replication mode. In this mode no regular operation is possible, and the
// connection should be driven with the WalReplicationFrames, and
// WalReplicationUndo APIs.
func (c *SQLiteConn) WalReplicationFollower() error {
	rv := C.sqlite3_wal_replication_follower(c.db, walReplicationSchema)
	if rv != C.SQLITE_OK {
		return newError(rv)
	}

	return nil
}

// WalReplicationNone switches off WAL replication on the given sqlite connection.
func (c *SQLiteConn) WalReplicationNone() error {
	walReplicationLock.Lock()
	defer walReplicationLock.Unlock()

	rv := C.sqlite3_wal_replication_none(c.db, walReplicationSchema)
	if rv != C.SQLITE_OK {
		return newError(rv)
	}

	// Figure if this was a leader connection.
	for iReplication := range walReplications {
		for iConn, conn := range walReplicationConns[iReplication] {
			if conn != c {
				continue
			}
			delete(walReplicationConns[iReplication], iConn)
			C.free(walReplicationConnsArg[iConn])
			delete(walReplicationConnsArg, iConn)
			break
		}
	}
	return nil
}

// WalReplicationFrames writes the given batch of frames to the write-ahead log
// linked to the given connection. This should be called with a "follower"
// connection, meant to replicate the "leader" one.
func (c *SQLiteConn) WalReplicationFrames(begin bool, frames WalReplicationFrames) error {
	// Convert to C types
	isBegin := C.int(0)
	if begin {
		isBegin = C.int(1)
	}
	szPage := C.int(frames.PageSize)
	nList := C.int(frames.Len)
	nTruncate := C.uint(frames.Truncate)
	isCommit := C.int(frames.IsCommit)
	syncFlags := C.int(frames.SyncFlags)

	pList := (*C.sqlite3_wal_replication_frame)(frames.List)
	rc := C.sqlite3_wal_replication_frames(
		c.db, walReplicationSchema, isBegin, szPage, nList, pList, nTruncate, isCommit, syncFlags)
	if rc != C.SQLITE_OK {
		return newError(rc)
	}

	return nil
}

// WalReplicationUndo rollbacks a write transaction in the given sqlite
// connection. This should be called with a "follower" connection, meant to
// replicate the "leader" one.
func (c *SQLiteConn) WalReplicationUndo() error {
	rc := C.sqlite3_wal_replication_undo(c.db, walReplicationSchema)
	if rc != C.SQLITE_OK {
		return newError(rc)
	}
	return nil
}

// NoopWalReplication returns a new instance of a WalReplication implementation
// whose hooks do nothing.
func NoopWalReplication() WalReplication {
	return &noopReplicationMethods{}
}

type noopReplicationMethods struct{}

func (m *noopReplicationMethods) Begin(conn *SQLiteConn) ErrNo {
	return 0
}

func (m *noopReplicationMethods) Abort(conn *SQLiteConn) ErrNo {
	return 0
}

func (m *noopReplicationMethods) Frames(conn *SQLiteConn, params WalReplicationFrames) ErrNo {
	return 0
}

func (m *noopReplicationMethods) Undo(conn *SQLiteConn) ErrNo {
	return 0
}

func (m *noopReplicationMethods) End(conn *SQLiteConn) ErrNo {
	return 0
}

//export walReplicationBegin
func walReplicationBegin(iReplication C.int, iConn C.int) C.int {
	replication, conn := walReplicationConnLookup(iReplication, iConn)

	return C.int(replication.Begin(conn))
}

//export walReplicationAbort
func walReplicationAbort(iReplication C.int, iConn C.int) C.int {
	replication, conn := walReplicationConnLookup(iReplication, iConn)

	return C.int(replication.Abort(conn))
}

//export walReplicationFrames
func walReplicationFrames(
	iReplication C.int,
	iConn C.int,
	szPage C.int,
	nList C.int,
	pList *C.sqlite3_wal_replication_frame,
	nTruncate C.uint,
	isCommit C.int,
	syncFlags C.uint,
) C.int {
	replication, conn := walReplicationConnLookup(iReplication, iConn)

	frames := WalReplicationFrames{
		PageSize:  int(szPage),
		Len:       int(nList),
		List:      unsafe.Pointer(pList),
		Truncate:  uint32(nTruncate),
		IsCommit:  int(isCommit),
		SyncFlags: uint8(syncFlags),
	}

	return C.int(replication.Frames(conn, frames))
}

//export walReplicationUndo
func walReplicationUndo(iReplication C.int, iConn C.int) C.int {
	replication, conn := walReplicationConnLookup(iReplication, iConn)

	return C.int(replication.Undo(conn))
}

//export walReplicationEnd
func walReplicationEnd(iReplication C.int, iConn C.int) C.int {
	replication, conn := walReplicationConnLookup(iReplication, iConn)

	return C.int(replication.End(conn))
}

// Find the a registered replication implementation and connection by ID.
func walReplicationConnLookup(iReplication C.int, iConn C.int) (WalReplication, *SQLiteConn) {
	walReplicationLock.RLock()
	defer walReplicationLock.RUnlock()

	replication := walReplications[iReplication]
	conn := walReplicationConns[iReplication][iConn]

	return replication, conn
}

// Global registry of WalReplication instances.
var walReplicationLock sync.RWMutex
var walReplicationNames = make(map[string]WalReplication)
var walReplicationHandles C.int
var walReplications = make(map[C.int]WalReplication)
var walReplicationConns = make(map[C.int]map[C.int]*SQLiteConn)
var walReplicationConnsIndex C.int
var walReplicationConnsArg = make(map[C.int]unsafe.Pointer)

// Hard-coded main schema name.
//
// TODO: support replicating also attached databases.
var walReplicationSchema = C.CString("main")
