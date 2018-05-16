package sqlite3

/*
#include <string.h>
#ifndef USE_LIBSQLITE3
#include <sqlite3-binding.h>
#else
#include <sqlite3.h>
#endif
#include <sys/time.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <assert.h>

// Maximum pathname length supported by this VFS.
#define MAXPATHNAME 512

// Maximum number of files this VFS can create.
#define MAXFILES 64

// Default, minumum and maximum page size.
#define PAGE_MIN_SIZE 512
#define PAGE_MAX_SIZE 65536

// Possible file content types.
#define CONTENT_MAIN_DB      0
#define CONTENT_WAL          1
#define CONTENT_OTHER        2

// Size of the database header.
#define MAIN_DB_HDRSIZE 100

// Size of write ahead log header
#define WAL_HDRSIZE 32

// Size of header before each frame in wal
#define WAL_FRAME_HDRSIZE 24

// SQLite VFS Go implementation.
int volatileRandomness(int nBuf, char *zBuf);
int volatileSleep(int microseconds);

// Hold content for a single page or frame in a volatile file.
typedef struct sqlite3VolatilePage {
  void *pBuf;                    // Content of the page.
  void *pHdr;                    // Page header (only for WAL pages).
} sqlite3VolatilePage;

// Create a new volatile page for a database or WAL file.
//
// If it's a page for a WAL file, the WAL header will
// also be allocated.
static int sqlite3VolatilePageCreate(int nPageSize, int bWal, sqlite3VolatilePage **ppPage){
  sqlite3VolatilePage *pPage;

  assert( nPageSize );
  assert( bWal==0 || bWal ==1 );
  assert( ppPage );

  *ppPage = 0; // In case of errors.

  pPage = sqlite3_malloc(sizeof(sqlite3VolatilePage));
  if( !pPage ){
    return SQLITE_NOMEM;
  }

  pPage->pBuf = sqlite3_malloc(nPageSize);
  if( !pPage->pBuf ){
      sqlite3_free(pPage);
      return SQLITE_NOMEM;
  }
  memset(pPage->pBuf, 0, nPageSize);

  if( bWal ){
    pPage->pHdr = sqlite3_malloc(WAL_FRAME_HDRSIZE);
    if( !pPage->pHdr ){
      sqlite3_free(pPage->pBuf);
      sqlite3_free(pPage);
      return SQLITE_NOMEM;
    }
    memset(pPage->pHdr, 0, WAL_FRAME_HDRSIZE);
  }else{
    pPage->pHdr = 0;
  }

  *ppPage = pPage;

  return SQLITE_OK;
}

// Destroy a volatile page.
static void sqlite3VolatilePageDestroy(sqlite3VolatilePage *pPage){
  assert( pPage );
  assert( pPage->pBuf );

  sqlite3_free(pPage->pBuf);

  if( pPage->pHdr ){
    sqlite3_free(pPage->pHdr);
  }

  sqlite3_free(pPage);
}

// Hold content for a single file in the volatile file system.
typedef struct sqlite3VolatileContent {
  char *zName;                   // Name of the file.
  void *pHdr;                    // File header (only for WAL files).
  sqlite3VolatilePage **apPage;  // Pointers to all pages in the file.
  int nPage;                     // Size of apPage
  int nPageSize;                 // Size of pBuf for each page.
  int nRefCount;                 // Number of open FDs referencing this file.
  int eType;                     // Content type (either main db or WAL).
  void **apShmRegion;            // Pointers to shared memory regions.
  int nShmRegion;                // Number of shared memory regions.
  int nShmRefCount;              // Number of opened files using the shared memory.
} sqlite3VolatileContent;

// Create the content structure for a new volatile file.
static int sqlite3VolatileContentCreate(
  const char* zName,
  int eType,
  sqlite3VolatileContent **ppContent
){
  sqlite3VolatileContent *pContent; // Newly allocated content object;

  assert( zName );
  assert( eType==CONTENT_MAIN_DB
       || eType==CONTENT_WAL
       || eType==CONTENT_OTHER
   );
  assert( ppContent );

  *ppContent = 0; // In case of errors

  pContent = (sqlite3VolatileContent*)(sqlite3_malloc(sizeof(sqlite3VolatileContent)));
  if( !pContent ){
    return SQLITE_NOMEM;
  }

  // Copy the name, since when called from Go, the pointer will be freed.
  pContent->zName = (char*)(sqlite3_malloc(strlen(zName) + 1));
  if( !pContent->zName ){
    sqlite3_free(pContent);
    return SQLITE_NOMEM;
  }
  pContent->zName = strncpy(pContent->zName, zName, strlen(zName) + 1);

  // For WAL files, also allocate the WAL file header.
  if( eType==CONTENT_WAL ){
    pContent->pHdr = sqlite3_malloc(WAL_HDRSIZE);
    if( !pContent->pHdr ){
      sqlite3_free(pContent->zName);
      sqlite3_free(pContent);
      return SQLITE_NOMEM;
    }
    memset(pContent->pHdr, 0, WAL_HDRSIZE);
  }else{
    pContent->pHdr = 0;
  }

  pContent->apPage = 0;
  pContent->nPage = 0;
  pContent->nPageSize = 0;
  pContent->nRefCount = 0;
  pContent->eType = eType;
  pContent->apShmRegion = 0;
  pContent->nShmRegion = 0;
  pContent->nShmRefCount = 0;

  *ppContent = pContent;

  return SQLITE_OK;
}

// Destroy the content of a volatile file.
static void sqlite3VolatileContentDestroy(sqlite3VolatileContent *pContent, int bForce){
  int i;
  sqlite3VolatilePage *pPage;
  void *pShmRegion;

  assert( pContent );
  assert( pContent->zName );
  assert( bForce==0 || bForce== 1);
  assert( bForce || pContent->nRefCount==0 );

  // Free the name.
  sqlite3_free(pContent->zName);

  // Free the header if it's a WAL file.
  if( pContent->eType==CONTENT_WAL ){
    assert( pContent->pHdr );
    sqlite3_free(pContent->pHdr);
  }else{
    assert( !pContent->pHdr );
  }

  // Free all pages.
  for( i=0; i<pContent->nPage; i++ ){
    pPage = *(pContent->apPage + i);
    assert( pPage );
    sqlite3VolatilePageDestroy(pPage);
  }

  // Free the page array.
  if( pContent->apPage ){
    sqlite3_free(pContent->apPage);
  }

  // Free all shared memory regions.
  for( i=0; i<pContent->nShmRegion; i++ ){
    pShmRegion = *(pContent->apShmRegion + i);
    assert( pShmRegion );
    sqlite3_free(pShmRegion);
  }

  // Free the shared memory region array.
  if( pContent->apShmRegion ){
    sqlite3_free(pContent->apShmRegion);
  }

  sqlite3_free(pContent);
}

// Return true if this file has no content.
static int sqlite3VolatileContentIsEmpty(sqlite3VolatileContent *pContent){
  assert( pContent );

  if( pContent->nPage==0 ){
    assert( !pContent->apPage );
    return 1;
  }

  // If it was written, a page list and a page size must have been set.
  assert( pContent->apPage
       && pContent->nPage
       && pContent->nPageSize
  );

  return 0;
}

// Get a page from this file, possibly creating a new one.
static int sqlite3VolatileContentPageGet(
  sqlite3VolatileContent *pContent,
  int nPage,
  sqlite3VolatilePage **ppPage
){
  sqlite3VolatilePage *pPage;
  int rc;
  int bWal;

  assert( pContent );
  assert( nPage );
  assert( ppPage );

  *ppPage = 0; // In case of errors.

  bWal = pContent->eType==CONTENT_WAL;

  // At most one new page should be appended.
  assert( nPage <= (pContent->nPage + 1) );

  if( nPage == (pContent->nPage + 1)){
    // Create a new page, grow the page array, and append the
    // new page to it.
    sqlite3VolatilePage **apPage; // New page array.

    // We assume that the page size has been set, either by intercepting
    // the first main database file write, or by handling a 'PRAGMA page_size=N'
    // command in sqlite3VolatileFileControl().
    assert( pContent->nPageSize );

    rc = sqlite3VolatilePageCreate(pContent->nPageSize, bWal, &pPage);
    if( rc!=SQLITE_OK ){
      return rc;
    }

    apPage = (sqlite3VolatilePage**)sqlite3_realloc(
      pContent->apPage, sizeof(sqlite3VolatilePage*) * nPage);
    if( !apPage ){
      sqlite3VolatilePageDestroy(pPage);
      return SQLITE_NOMEM;
    }

    *(apPage + nPage - 1) = pPage; // Append the new page to the new page array.

    pContent->apPage = apPage;     // Update the page array.
    pContent->nPage = nPage;       // Update the page count.
  }else{
    // Return the existing page.
    assert( pContent->apPage );
    pPage = *(pContent->apPage + nPage - 1);
  }

  *ppPage = pPage;

  return SQLITE_OK;
}

// Lookup a page from this file, returning NULL if it doesn't exist.
static sqlite3VolatilePage* sqlite3VolatileContentPageLookup(
  sqlite3VolatileContent *pContent,
  int nPage
){
  sqlite3VolatilePage* pPage;

  assert( pContent );

  if( nPage>pContent->nPage ){
    // This page hasn't been written yet.
    return 0;
  }

  pPage = *(pContent->apPage + nPage - 1);

  assert( pPage );

  if( pContent->eType==CONTENT_WAL ){
    assert( pPage->pHdr );
  }

  return pPage;
}

// Truncate the file to be exactly the given number of pages.
static void sqlite3VolatileContentTruncate(
  sqlite3VolatileContent *pContent,
  int nPage
){
  sqlite3VolatilePage **ppCursor;
  int i;

  // Truncate should always shrink a file.
  assert( pContent->nPage );
  assert( nPage<=pContent->nPage );
  assert( pContent->apPage );

  // Destroy pages beyond nPage.
  ppCursor = pContent->apPage + nPage;
  for( i=0; i<(pContent->nPage-nPage); i++ ){
    sqlite3VolatilePageDestroy(*ppCursor);
    ppCursor++;
  }

  // Reset the file header (for WAL files).
  if( pContent->eType==CONTENT_WAL ){
    assert( pContent->pHdr );
    memset(pContent->pHdr, 0, WAL_HDRSIZE);
  }else{
    assert( !pContent->pHdr );
  }

  // Shrink the page array, possibly to 0.
  pContent->apPage = (sqlite3VolatilePage**)sqlite3_realloc(
    pContent->apPage, sizeof(sqlite3VolatilePage*) * nPage);

  // Update the page count.
  pContent->nPage = nPage;
}

// Root of the volatile file system. Contains pointers to the content
// of all files that were created.
typedef struct sqlite3VolatileRoot {
  int iVfs;                           // File system identifier.
  int nContent;                       // Number of files in the file system
  sqlite3VolatileContent **apContent; // Pointers to files in the file system
  sqlite3_mutex *mutex;               // Serialize to access this object
  int iErrno;                         // Last error occurred.
} sqlite3VolatileRoot;

// Initialize a new sqlite3VolatileRoot object.
static int sqlite3VolatileRootCreate(int iVfs, sqlite3VolatileRoot **ppRoot){
  sqlite3VolatileRoot* pRoot;
  int szContentArray = sizeof(sqlite3VolatileContent*) * MAXFILES;

  assert( ppRoot );

  *ppRoot = 0; // In case of errors.

  pRoot = (sqlite3VolatileRoot*)sqlite3_malloc(sizeof(sqlite3VolatileRoot));
  if( !pRoot ){
    return SQLITE_NOMEM;
  }

  pRoot->iVfs = iVfs;
  pRoot->nContent = MAXFILES;

  pRoot->apContent = (sqlite3VolatileContent**)(sqlite3_malloc(szContentArray));
  if( !pRoot->apContent ){
    sqlite3_free(pRoot);
    return SQLITE_NOMEM;
  }

  memset(pRoot->apContent, 0, szContentArray);
  pRoot->mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  if( !pRoot->mutex ){
    sqlite3_free(pRoot->apContent);
    sqlite3_free(pRoot);
    return SQLITE_NOMEM;
  }

  *ppRoot = pRoot;

  return SQLITE_OK;
}

// Destroy sqlite3VolatileRoot object.
//
// All file content will be de-allocated, so dangling open FDs against
// those files will be broken.
static void sqlite3VolatileRootDestroy(sqlite3VolatileRoot *pRoot){
  sqlite3VolatileContent **ppCursor; // Iterator for pRoot->apContent
  int i;

  assert( pRoot );
  assert( pRoot->apContent );
  assert( pRoot->mutex );

  ppCursor = pRoot->apContent;

  // The content array has been allocated and has at least one slot.
  assert( ppCursor );
  assert( pRoot->nContent );

  for( i=0; i<pRoot->nContent; i++ ){
    sqlite3VolatileContent *pContent = *ppCursor;
    if( pContent ){
      sqlite3VolatileContentDestroy(pContent, 1);
    }
    ppCursor++;
  }

  sqlite3_free(pRoot->apContent);
  sqlite3_mutex_free(pRoot->mutex);
}

// Find a content object by name.
//
// Fill ppContent and return its index if found, otherwise return the index
// of a free slot (or -1, if there are no free slots).
static int sqlite3VolatileRootContentLookup(
  sqlite3VolatileRoot *pRoot,
  const char *zName,
  sqlite3VolatileContent **ppContent // OUT: content object or NULL
){
  sqlite3VolatileContent **ppCursor; // Iterator for pRoot->apContent
  int i;
  int iFreeContent = -1; // Index of the content or of a free slot in the apContent array.

  assert( pRoot );
  assert( zName );

  ppCursor = pRoot->apContent;

  // The content array has been allocated and has at least one slot.
  assert( ppCursor );
  assert( pRoot->nContent );

  for( i=0; i<pRoot->nContent; i++ ){
    sqlite3VolatileContent *pContent = *ppCursor;
    if( pContent && strcmp(pContent->zName, zName)==0 ){
      // Found matching file.
      *ppContent = pContent;
      return i;
    }
    if( !pContent && iFreeContent==-1 ){
      // Keep track of the index of this empty slot.
      iFreeContent = i;
    }
    ppCursor++;
  }

  // No matching content object.
  *ppContent = 0;

  return iFreeContent;
}

// Return the size of the database file whose WAL file has the given name.
//
// The size must have been previously set when this routine is called.
static int sqlite3VolatileRootGetDatabasePageSize(
  sqlite3VolatileRoot *pRoot,
  const char *zWalName,
  int *pnPageSize
){
  sqlite3VolatileContent *pContent;
  int nMainName;
  char *zMainName;

  assert( pRoot );
  assert( zWalName );
  assert( pnPageSize );

  *pnPageSize = 0; // In case of errors.

  nMainName = strlen(zWalName) - strlen("-wal") + 1;
  zMainName = sqlite3_malloc(nMainName);

  if( !zMainName ){
    return SQLITE_NOMEM;
   }

  strncpy(zMainName, zWalName, nMainName-1);
  zMainName[nMainName-1] = '\0';

  sqlite3VolatileRootContentLookup(pRoot, zMainName, &pContent);

  sqlite3_free(zMainName);

  assert( pContent );
  assert( pContent->nPageSize );

  *pnPageSize = pContent->nPageSize;

  return SQLITE_OK;
}

// Extract the page size from the content of the first
// database page.
static unsigned sqlite3VolatileDatabaseParsePageSize(void *zBuf){
  unsigned nPageSize;

  assert( zBuf );

  nPageSize = (((char*)zBuf)[16] << 8) + ((char*)zBuf)[17];

  // Validate the page size, see https://www.sqlite.org/fileformat2.html.
  if( nPageSize==1 ){
    nPageSize = PAGE_MAX_SIZE;
  }else{
    assert( nPageSize>=PAGE_MIN_SIZE
         && nPageSize<=(PAGE_MAX_SIZE / 2)
         && ((nPageSize-1)&nPageSize)==0
    );
  }

  return nPageSize;
}

// Extract the page size from the content of the WAL header.
static unsigned sqlite3VolatileWalParsePageSize(const void *zBuf){
  unsigned nPageSize;

  assert( zBuf );

  // See wal.c for a description of the WAL header format.
  nPageSize = (
      ( ((char*)zBuf)[8]  << 24 ) +
      ( ((char*)zBuf)[9]  << 16 ) +
      ( ((char*)zBuf)[10] << 8  ) +
        ((char*)zBuf)[11]
  );

  // Validate the page size, see https://www.sqlite.org/fileformat2.html.
  if( nPageSize==1 ){
    nPageSize = PAGE_MAX_SIZE;
  }else{
    assert( nPageSize>=PAGE_MIN_SIZE
         && nPageSize<=(PAGE_MAX_SIZE / 2)
         && ((nPageSize-1)&nPageSize)==0
    );
  }

  return nPageSize;
}

typedef struct sqlite3VolatileFile {
  sqlite3_file base;                // Base class. Must be first.
  sqlite3VolatileRoot *pRoot;       // Pointer to our volatile VFS instance data.
  sqlite3VolatileContent *pContent; // Handle to the file content.
} sqlite3VolatileFile;

static int sqlite3VolatileClose(sqlite3_file *pFile){
  sqlite3VolatileFile *p = (sqlite3VolatileFile*)pFile;
  sqlite3VolatileRoot *pRoot = (sqlite3VolatileRoot*)(p->pRoot);

  sqlite3_mutex_enter(pRoot->mutex);

  assert( p->pContent->nRefCount );
  p->pContent->nRefCount--;

  sqlite3_mutex_leave(pRoot->mutex);

  return SQLITE_OK;
}

static int sqlite3VolatileRead(
  sqlite3_file *pFile,
  void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
  sqlite3VolatileFile *p = (sqlite3VolatileFile*)pFile;
  int nPage;
  sqlite3VolatilePage *pPage;

  assert( zBuf );
  assert( iAmt );
  assert( p );
  assert( p->pContent );
  assert( p->pContent->zName );
  assert( p->pContent->nRefCount );

  // From SQLite docs:
  //
  //   If xRead() returns SQLITE_IOERR_SHORT_READ it must also fill
  //   in the unread portions of the buffer with zeros.  A VFS that
  //   fails to zero-fill short reads might seem to work.  However,
  //   failure to zero-fill short reads will eventually lead to
  //   database corruption.

  // Check if the file is empty.
  if( sqlite3VolatileContentIsEmpty(p->pContent) ){
    memset(zBuf, 0, iAmt);
    return SQLITE_IOERR_SHORT_READ;
  }

  // From this point on we can assume that the file was written at least
  // once.

  // Since writes to all files other than the main database or the WAL are
  // no-ops and the associated content object remains empty, we expect
  // the content type to be either CONTENT_MAIN_DB or CONTENT_WAL.
  assert( p->pContent->eType==CONTENT_MAIN_DB
       || p->pContent->eType==CONTENT_WAL
  );

  switch( p->pContent->eType ){
  case CONTENT_MAIN_DB:
    // If the main database file is not empty, we expect the page size
    // to have been set by an initial write.
    assert( p->pContent->nPageSize );

    // Main database
    if( iOfst<p->pContent->nPageSize ){
      // This is page 1 read. We expect the read to be at most nPageSize
      // bytes.
      assert( iAmt<=p->pContent->nPageSize );

      nPage = 1;
    }else{
      // For pages greater than 1, we expect a full page read, with
      // an offset that starts exectly at the page boundary.
      assert( iAmt==p->pContent->nPageSize );
      assert( (iOfst % p->pContent->nPageSize)==0 );

      nPage = (iOfst / p->pContent->nPageSize) + 1;
    }

    assert( nPage );

    pPage = sqlite3VolatileContentPageLookup(p->pContent, nPage);

    if( !nPage ){
      // This is an attempt to read a page that was never written.
      memset(zBuf, 0, iAmt);
      return SQLITE_IOERR_SHORT_READ;
    }

    if( nPage==1 ){
      // Read the desired part of page 1.
      memcpy(zBuf, pPage->pBuf+iOfst, iAmt);
    }else{
      // Read the full page.
      memcpy(zBuf, pPage->pBuf, iAmt);
    }
    return SQLITE_OK;

 case CONTENT_WAL:
    // WAL file
    if( !p->pContent->nPageSize ){
      // If the page size hasn't been set yet, set it by copy the one from
      // the associated main database file.
      int rc = sqlite3VolatileRootGetDatabasePageSize(
        p->pRoot, p->pContent->zName, &p->pContent->nPageSize);
      if( rc!=SQLITE_OK ){
        return rc;
      }
    }

    if( iOfst==0 ){
      // Read the header.
      assert( iAmt==WAL_HDRSIZE );
      assert( p->pContent->pHdr );
      memcpy(zBuf, p->pContent->pHdr, WAL_HDRSIZE);
      return SQLITE_OK;
    }

    // For any other frame, we expect either a header read, a page read
    // or a full frame read.
    if( iAmt==WAL_FRAME_HDRSIZE ){
      assert( ((iOfst-WAL_HDRSIZE) % (p->pContent->nPageSize+WAL_FRAME_HDRSIZE))==0 );
      nPage = ((iOfst-WAL_HDRSIZE) / (p->pContent->nPageSize+WAL_FRAME_HDRSIZE)) + 1;
    }else if( iAmt==p->pContent->nPageSize ){
      assert( ((iOfst-WAL_HDRSIZE-WAL_FRAME_HDRSIZE) % (p->pContent->nPageSize+WAL_FRAME_HDRSIZE))==0 );
      nPage = ((iOfst-WAL_HDRSIZE-WAL_FRAME_HDRSIZE) / (p->pContent->nPageSize+WAL_FRAME_HDRSIZE)) + 1;
    }else{
      assert( iAmt==(WAL_FRAME_HDRSIZE+p->pContent->nPageSize) );
      nPage = ((iOfst-WAL_HDRSIZE) / (p->pContent->nPageSize+WAL_FRAME_HDRSIZE)) + 1;
    }

    pPage = sqlite3VolatileContentPageLookup(p->pContent, nPage);
    if( !nPage ){
      // This is an attempt to read a page that was never written.
      memset(zBuf, 0, iAmt);
      return SQLITE_IOERR_SHORT_READ;
    }

    if( iAmt==WAL_FRAME_HDRSIZE ){
      memcpy(zBuf, pPage->pHdr, iAmt);
    }else if( iAmt==p->pContent->nPageSize ){
      memcpy(zBuf, pPage->pBuf, iAmt);
    }else{
      memcpy(zBuf, pPage->pHdr, WAL_FRAME_HDRSIZE);
      memcpy(zBuf+WAL_FRAME_HDRSIZE, pPage->pBuf, p->pContent->nPageSize);
    }

    return SQLITE_OK;
 }

  return SQLITE_IOERR_READ;
}

static int sqlite3VolatileWrite(
  sqlite3_file *pFile,
  const void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
  sqlite3VolatileFile *p = (sqlite3VolatileFile*)pFile;
  unsigned nPage;
  sqlite3VolatilePage *pPage;
  int rc;

  assert( zBuf );
  assert( iAmt );
  assert( p );
  assert( p->pContent );
  assert( p->pContent->zName );
  assert( p->pContent->nRefCount );

  switch( p->pContent->eType ){
  case CONTENT_MAIN_DB:
    // Main database.
    if( iOfst==0 ){
      int nPageSize;

      // This is the first database page. We expect the data to contain at
      // least the header.
      assert( iAmt>=100 );

      // Extract the page size from the header.
      nPageSize = sqlite3VolatileDatabaseParsePageSize((void*)zBuf);

      if( p->pContent->nPageSize ){
        // Check that the given page size actually matches what we have
        // recorded. Since we make 'PRAGMA page_size=N' fail if the page
        // is already set (see sqlite3VolatileFileControl), there should
        // be no way for the user to change it.
        assert( nPageSize==p->pContent->nPageSize );
      }else{
        // This must be the very first write to the database. Keep track
        // of the page size.
        p->pContent->nPageSize = nPageSize;
      }

      nPage = 1;
    }else{
      // The header must have been written and the page size set.
      assert( p->pContent->nPageSize );

      // For pages beyond the first we expect iOfst to be a multiple of
      // the page size.
      assert( (iOfst % p->pContent->nPageSize)==0 );

      // We expect that SQLite writes a page at time.
      assert( iAmt==p->pContent->nPageSize );

      nPage = (iOfst / p->pContent->nPageSize) + 1;
    }

    rc = sqlite3VolatileContentPageGet(p->pContent, nPage, &pPage);
    if( rc!=SQLITE_OK){
      return rc;
    }

    assert( pPage );
    assert( pPage->pBuf );

    memcpy(pPage->pBuf, zBuf, iAmt);

    return SQLITE_OK;

  case CONTENT_WAL:
    // WAL file.
    if( !p->pContent->nPageSize ){
      // If the page size hasn't been set yet, set it by copy the one from
      // the associated main database file.
      int rc = sqlite3VolatileRootGetDatabasePageSize(
        p->pRoot, p->pContent->zName, &p->pContent->nPageSize);
      if( rc!=SQLITE_OK ){
        return rc;
      }
    }

    if( iOfst==0 ){
      // This is the WAL header.
      int nPageSize;

      //  We expect the data to contain exactly 32 bytes.
      assert( iAmt==WAL_HDRSIZE );

      // The page size indicated in the header must match the one of the database file.
      assert( sqlite3VolatileWalParsePageSize(zBuf)==p->pContent->nPageSize );

      memcpy(p->pContent->pHdr, zBuf, iAmt);
      return SQLITE_OK;
    }

    assert( p->pContent->nPageSize );

    // This is a WAL frame write. We expect either a frame header or page
    // write.
    if( iAmt==WAL_FRAME_HDRSIZE ){
      // Frame header write.
      assert( ((iOfst-WAL_HDRSIZE) % (p->pContent->nPageSize+WAL_FRAME_HDRSIZE))==0 );
      nPage = ((iOfst-WAL_HDRSIZE) / (p->pContent->nPageSize+WAL_FRAME_HDRSIZE)) + 1;

      rc = sqlite3VolatileContentPageGet(p->pContent, nPage, &pPage);
      if( rc!=SQLITE_OK ){
        return rc;
      }
      memcpy(pPage->pHdr, zBuf, iAmt);
    }else{
      // Frame page write.
      assert( iAmt==p->pContent->nPageSize );
      assert( ((iOfst-WAL_HDRSIZE-WAL_FRAME_HDRSIZE) % (p->pContent->nPageSize+WAL_FRAME_HDRSIZE))==0 );
      nPage = ((iOfst-WAL_HDRSIZE-WAL_FRAME_HDRSIZE) / (p->pContent->nPageSize+WAL_FRAME_HDRSIZE)) + 1;

      // The header for the this frame must already have been written,
      // so the page is there.
      pPage = sqlite3VolatileContentPageLookup(p->pContent, nPage);

      assert( pPage );

      memcpy(pPage->pBuf, zBuf, iAmt);
    }

    return SQLITE_OK;

  case CONTENT_OTHER:
    // Silently swallow writes to any other file.
    return SQLITE_OK;
  }

  return SQLITE_IOERR_WRITE;
}

static int sqlite3VolatileTruncate(sqlite3_file *pFile, sqlite_int64 size){
  sqlite3VolatileFile *p = (sqlite3VolatileFile*)pFile;
  int nPage;

  assert( p );
  assert( p->pContent );

  // We expect calls to xTruncate only for database and WAL files.
  assert( p->pContent->eType==CONTENT_MAIN_DB
       || p->pContent->eType==CONTENT_WAL
  );

  // Check if this file empty.
  if( sqlite3VolatileContentIsEmpty(p->pContent) ){
    // We don't expect SQLite to grow empty files.
    assert( size==0 );
    return SQLITE_OK;
  }

  switch( p->pContent->eType ){
  case CONTENT_MAIN_DB:
    // Main database.
    assert( p->pContent->nPageSize );
    assert( (size % p->pContent->nPageSize)==0 );
    nPage = size / p->pContent->nPageSize;
    break;

  case CONTENT_WAL:
    // WAL file.
    //
    // We expect SQLite to only truncate to zero, after a full checkpoint.
    //
    // TODO: figure out other case where SQLite might truncate to a
    //       different size.
    assert( size==0 );
    nPage = 0;
    break;
  }

  sqlite3VolatileContentTruncate(p->pContent, nPage);

  return SQLITE_OK;
}

static int sqlite3VolatileSync(sqlite3_file *pFile, int flags){
  sqlite3VolatileFile *p = (sqlite3VolatileFile*)pFile;
  return SQLITE_OK;
}

static int sqlite3VolatileFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  sqlite3VolatileFile *p = (sqlite3VolatileFile*)pFile;

  // Check if this file empty.
  if( sqlite3VolatileContentIsEmpty(p->pContent) ){
    *pSize = 0;
    return SQLITE_OK;
  }

  // Since we don't allow writing any other file, this must be
  // either a database file or WAL file.
  assert( p->pContent->eType==CONTENT_MAIN_DB
       || p->pContent->eType==CONTENT_WAL
  );

  // Since this file is not empty, the page size must have been set.
  assert( p->pContent->nPageSize );

  switch( p->pContent->eType ){
  case CONTENT_MAIN_DB:
    *pSize = p->pContent->nPage * p->pContent->nPageSize;
    break;

  case CONTENT_WAL:
    // TODO? here we assume that FileSize() is never invoked between
    //       a header write and a page write.
    *pSize = WAL_HDRSIZE + (p->pContent->nPage * (WAL_FRAME_HDRSIZE + p->pContent->nPageSize));
    break;
  }

  return SQLITE_OK;
}

// Locking a file is a no-op, since no other process has visibility on it.
static int sqlite3VolatileLock(sqlite3_file *pFile, int eLock){
  return SQLITE_OK;
}

// Unlocking a file is a no-op, since no other process has visibility on it.
static int sqlite3VolatileUnlock(sqlite3_file *pFile, int eLock){
  return SQLITE_OK;
}

// We always report that a lock is held. This routine should be used only in
// journal mode, so it doesn't matter.
static int sqlite3VolatileCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  *pResOut = 1;
  return SQLITE_OK;
}

static int sqlite3VolatileFileControl(sqlite3_file *pFile, int op, void *pArg){
  sqlite3VolatileFile *p = (sqlite3VolatileFile*)pFile;

  if( op==SQLITE_FCNTL_PRAGMA ){
    // Handle pragma a pragma file control. See the xFileControl docstring
    // in sqlite.h.in for more details.
    char **aPragmaFnctl;
    const char *zPragmaLeft;
    const char *zPragmaRight;

    aPragmaFnctl = (char**)pArg;
    assert( aPragmaFnctl );

    zPragmaLeft = aPragmaFnctl[1];
    zPragmaRight = aPragmaFnctl[2];
    assert( zPragmaLeft );

    if( strcmp(zPragmaLeft, "page_size")==0 && zPragmaRight ){
      // When the user executes 'PRAGMA page_size=N' we save the size internally.
      //
      // The page size must be between 512 and 65536, and be a power of two. The
      // check below was copied from sqlite3BtreeSetPageSize in btree.c.
      //
      // Invalid sizes are simply ignored, SQLite will do the same.
      //
      // It's not possible to change the size after it's set.
      int nPageSize = atoi(zPragmaRight);

      if( nPageSize>=PAGE_MIN_SIZE && nPageSize<=PAGE_MAX_SIZE &&
        ((nPageSize-1)&nPageSize)==0 ){
        if( p->pContent->nPageSize && nPageSize != p->pContent->nPageSize ){
          aPragmaFnctl[0] = "changing page size is not supported";
          return SQLITE_ERROR;
        }
        p->pContent->nPageSize = nPageSize;
      }
    }else if( strcmp(zPragmaLeft, "journal_mode")==0 && zPragmaRight ){
      // When the user executes 'PRAGMA journal_mode=x' we ensure that the
      // desired mode is 'wal'.
      if( strcasecmp(zPragmaRight, "wal")!=0 ){
          aPragmaFnctl[0] = "only WAL mode is supported";
          return SQLITE_ERROR;
      }
    }
    return SQLITE_NOTFOUND;
  }
  return SQLITE_OK;
}

static int sqlite3VolatileSectorSize(sqlite3_file *pFile){
  return 0;
}

static int sqlite3VolatileDeviceCharacteristics(sqlite3_file *pFile){
  return 0;
}

// Simulate shared memory by allocating on the C heap.
static int sqlite3VolatileShmMap(
  sqlite3_file *pFile,            // Handle open on database file
  int iRegion,                    // Region to retrieve
  int szRegion,                   // Size of regions
  int bExtend,                    // True to extend file if necessary
  void volatile **pp              // OUT: Mapped memory
){
  sqlite3VolatileFile *p = (sqlite3VolatileFile*)pFile;
  void *pRegion;
  int rc = SQLITE_OK;

  if( p->pContent->apShmRegion && iRegion<p->pContent->nShmRegion ){
    // The region was already allocated.
    pRegion = *(p->pContent->apShmRegion + iRegion);
    assert( pRegion );
  }else{
    if( bExtend ){
      // We should grow the map one region at a time.
      assert( iRegion==p->pContent->nShmRegion );

      pRegion = sqlite3_malloc(szRegion);
      if( pRegion ){
        memset(pRegion, 0, szRegion);
        p->pContent->apShmRegion = (void**)sqlite3_realloc(
          p->pContent->apShmRegion, sizeof(void*) * (iRegion+1));
        if( !p->pContent->apShmRegion ){
          sqlite3_free(pRegion);
          pRegion = 0;
          rc = SQLITE_NOMEM;
        }else{
          *(p->pContent->apShmRegion + iRegion) = pRegion;
          p->pContent->nShmRegion++;
        }
      }else{
        rc = SQLITE_NOMEM;
      }
    }else{
      // The region was not allocated and we don't have to extend the map.
      pRegion = 0;
    }
  }

  if( pRegion ){
    p->pContent->nShmRefCount++;
  }

  *pp = pRegion;

  return rc;
}

static int sqlite3VolatileShmLock(sqlite3_file *pFile, int ofst, int n, int flags){
  // This is a no-op since shared-memory locking is relevant only for
  // inter-process concurrency. See also the unix-excl branch from upstream
  // (git commit cda6b3249167a54a0cf892f949d52760ee557129).
  return SQLITE_OK;
}

static void sqlite3VolatileShmBarrier(sqlite3_file *pFile){
  // This is a no-op since we expect SQLite to be compiled with mutex
  // support (i.e. SQLITE_MUTEX_OMIT or SQLITE_MUTEX_NOOP are *not*
  // defined, see sqliteInt.h).
}

static int sqlite3VolatileShmUnmap(sqlite3_file *pFile, int deleteFlag){
  sqlite3VolatileFile *p = (sqlite3VolatileFile*)pFile;
  void **pCursor;
  int i;

  // Reference count must be greater than 0;
  assert( p->pContent->nShmRefCount );

  p->pContent->nShmRefCount--;

  // If we got zero references, free the entire map.
  if( p->pContent->nShmRefCount==0) {
    pCursor = p->pContent->apShmRegion;
    for( i=0; i<p->pContent->nShmRegion; i++){
      sqlite3_free(*pCursor);
      pCursor++;
    }
    sqlite3_free(p->pContent->apShmRegion);
    p->pContent->apShmRegion = 0;
    p->pContent->nShmRegion = 0;
  }

  return 0;
}

static int sqlite3VolatileOpen(
  sqlite3_vfs *pVfs,              // VFS
  const char *zName,              // File to open, or 0 for a temp file
  sqlite3_file *pFile,            // Pointer to DemoFile struct to populate
  int flags,                      // Input SQLITE_OPEN_XXX flags
  int *pOutFlags                  // Output SQLITE_OPEN_XXX flags (or NULL)
){
  assert( pVfs );
  assert( pFile );
  assert( zName );

  sqlite3VolatileRoot *pRoot = (sqlite3VolatileRoot*)(pVfs->pAppData);
  sqlite3VolatileFile *p = (sqlite3VolatileFile*)pFile;

  assert( pRoot );

  sqlite3VolatileContent *pContent;

  int i;                 // For loops.
  int iFreeContent = -1; // Index of a free slot in the pRoot->apContent array.
  int bExists = 0;       // Whether the file exists already.
  int eType;             // File content type (e.g. database or WAL).
  int rc;                // Return code.

  // Flags
  int bExclusive = flags & SQLITE_OPEN_EXCLUSIVE;
  int bCreate    = flags & SQLITE_OPEN_CREATE;

  // This signals SQLite to not call Close() in case we return an error.
  p->base.pMethods = 0;

  sqlite3_mutex_enter(pRoot->mutex);

  // Search if the file exists already, and (if it doesn't) if there are
  // free slots.
  iFreeContent = sqlite3VolatileRootContentLookup(pRoot, zName, &pContent);
  bExists = pContent != 0;

  // If file exists, and the exclusive flag is on, then return an error.
  //
  // From sqlite3.h.in:
  //
  //   The SQLITE_OPEN_EXCLUSIVE flag is always used in conjunction with
  //   the SQLITE_OPEN_CREATE flag, which are both directly analogous to
  //   the O_EXCL and O_CREAT flags of the POSIX open() API.  The
  //   SQLITE_OPEN_EXCLUSIVE flag, when paired with the
  //   SQLITE_OPEN_CREATE, is used to indicate that file should always be
  //   created, and that it is an error if it already exists.  It is not
  //   used to indicate the file should be opened for exclusive access.
  //
  if( bExists && bExclusive && bCreate ){
    pRoot->iErrno = EEXIST;
    sqlite3_mutex_leave(pRoot->mutex);
    return SQLITE_CANTOPEN;
  }

  if( !bExists ){
    // Check the create flag.
    if( !bCreate ){
      pRoot->iErrno = ENOENT;
      sqlite3_mutex_leave(pRoot->mutex);
      return SQLITE_CANTOPEN;
    }

    // This is a new file, so try to create a new entry.
    if( iFreeContent==-1){
      // No more file content slots available.
      pRoot->iErrno = ENFILE;
      sqlite3_mutex_leave(pRoot->mutex);
      return SQLITE_CANTOPEN;
    }

    if( flags & SQLITE_OPEN_MAIN_DB  ){
      eType = CONTENT_MAIN_DB;
    }else if( flags & SQLITE_OPEN_WAL  ){
      eType = CONTENT_WAL;
    }else{
      eType = CONTENT_OTHER;
    }

    rc = sqlite3VolatileContentCreate(zName, eType, &pContent);
    if( rc!=SQLITE_OK ){
      pRoot->iErrno = ENOMEM;
      sqlite3_mutex_leave(pRoot->mutex);
      return SQLITE_NOMEM;
    }

    // Save the new file content in a free entry of the root file
    // content array.
    *(pRoot->apContent + iFreeContent) = pContent;
  }

  // Populate the new file handle.
  static const sqlite3_io_methods io = {
    2,                                       // iVersion
    sqlite3VolatileClose,                    // xClose
    sqlite3VolatileRead,                     // xRead
    sqlite3VolatileWrite,                    // xWrite
    sqlite3VolatileTruncate,                 // xTruncate
    sqlite3VolatileSync,                     // xSync
    sqlite3VolatileFileSize,                 // xFileSize
    sqlite3VolatileLock,                     // xLock
    sqlite3VolatileUnlock,                   // xUnlock
    sqlite3VolatileCheckReservedLock,        // xCheckReservedLock
    sqlite3VolatileFileControl,              // xFileControl
    sqlite3VolatileSectorSize,               // xSectorSize
    sqlite3VolatileDeviceCharacteristics,    // xDeviceCharacteristics
    sqlite3VolatileShmMap,                   // xShmMap
    sqlite3VolatileShmLock,                  // xShmLock
    sqlite3VolatileShmBarrier,               // xShmBarrier
    sqlite3VolatileShmUnmap                  // xShmUnmap
  };

  p->base.pMethods = &io;
  p->pRoot = pRoot;
  p->pContent = pContent;

  pContent->nRefCount++;

  sqlite3_mutex_leave(pRoot->mutex);

  return SQLITE_OK;
}

static int sqlite3VolatileDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  sqlite3VolatileRoot *pRoot = (sqlite3VolatileRoot*)(pVfs->pAppData);
  sqlite3VolatileContent *pContent;
  sqlite3VolatilePage *pPage;
  int iContent;
  int i;

  sqlite3_mutex_enter(pRoot->mutex);

  // Check if the file exists.
  iContent = sqlite3VolatileRootContentLookup(pRoot, zPath, &pContent);
  if( !pContent ){
    pRoot->iErrno = ENOENT;
    sqlite3_mutex_leave(pRoot->mutex);
    return SQLITE_IOERR_DELETE_NOENT;
  }

  // Check that there are no consumers of this file.
  if( pContent->nRefCount > 0 ){
    pRoot->iErrno = EBUSY;
    sqlite3_mutex_leave(pRoot->mutex);
    return SQLITE_IOERR_DELETE;
  }

  // Free all memory allocated for this file.
  sqlite3VolatileContentDestroy(pContent, 0);

  // Reset the file content slot.
  *(pRoot->apContent + iContent) = 0;

  sqlite3_mutex_leave(pRoot->mutex);

  return SQLITE_OK;
}

static int sqlite3VolatileAccess(
  sqlite3_vfs *pVfs,
  const char *zPath,
  int flags,
  int *pResOut
){
  sqlite3VolatileRoot *pRoot = (sqlite3VolatileRoot*)(pVfs->pAppData);
  sqlite3VolatileContent *pContent;

  sqlite3_mutex_enter(pRoot->mutex);

  // If the file exists, access is always granted.
  sqlite3VolatileRootContentLookup(pRoot, zPath, &pContent);
  if( !pContent ){
    pRoot->iErrno = ENOENT;
    *pResOut = 0;
  }else{
    *pResOut = 1;
  }

  sqlite3_mutex_leave(pRoot->mutex);

  return SQLITE_OK;
}

static int sqlite3VolatileFullPathname(
  sqlite3_vfs *pVfs,              // VFS
  const char *zPath,              // Input path (possibly a relative path)
  int nPathOut,                   // Size of output buffer in bytes
  char *zPathOut                  // Pointer to output buffer
){
  // Just return the path unchanged.
  sqlite3_snprintf(nPathOut, zPathOut, "%s", zPath);
  return SQLITE_OK;
}

static void* sqlite3VolatileDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return 0;
}

static void sqlite3VolatileDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  sqlite3_snprintf(nByte, zErrMsg, "Loadable extensions are not supported");
  zErrMsg[nByte-1] = '\0';
}

static void (*sqlite3VolatileDlSym(sqlite3_vfs *pVfs, void *pH, const char *z))(void){
  return 0;
}

static void sqlite3VolatileDlClose(sqlite3_vfs *pVfs, void *pHandle){
  return;
}

static int sqlite3VolatileRandomness(sqlite3_vfs *pVfs, int nByte, char *zByte){
  return volatileRandomness(nByte, zByte);
}

static int sqlite3VolatileSleep(sqlite3_vfs *NotUsed, int microseconds){
  // Sleep in Go, to avoid the scheduler unconditionally preempting the
  // SQLite API call being invoked.
  return volatileSleep(microseconds);
}

static int sqlite3VolatileCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *piNow){
  static const sqlite3_int64 unixEpoch = 24405875*(sqlite3_int64)8640000;
  struct timeval sNow;
  (void)gettimeofday(&sNow, 0);
  *piNow = unixEpoch + 1000*(sqlite3_int64)sNow.tv_sec + sNow.tv_usec/1000;
  return SQLITE_OK;
}

static int sqlite3VolatileCurrentTime(sqlite3_vfs *pVfs, double *piNow){
  // TODO: check if it's always safe to cast a double* to a sqlite3_int64*.
  return sqlite3VolatileCurrentTimeInt64(pVfs, (sqlite3_int64*)piNow);
}

static int sqlite3VolatileGetLastError(sqlite3_vfs *pVfs, int NotUsed2, char *NotUsed3){
  sqlite3VolatileRoot *pRoot = (sqlite3VolatileRoot*)(pVfs->pAppData);
  int rc;

  sqlite3_mutex_enter(pRoot->mutex);
  rc = pRoot->iErrno;
  sqlite3_mutex_leave(pRoot->mutex);

  return rc;
}

static int sqlite3VolatileRegister(char *zName, int iVfs, sqlite3_vfs **ppVfs) {
  sqlite3_vfs* pVfs;
  sqlite3VolatileRoot *pRoot;
  int rc;

  assert(zName);
  assert(ppVfs);

  *ppVfs = 0; // In case of errors

  pVfs = (sqlite3_vfs*)sqlite3_malloc(sizeof(sqlite3_vfs));
  if( !pVfs ){
    return SQLITE_NOMEM;
  }

  rc = sqlite3VolatileRootCreate(iVfs, &pRoot);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pVfs);
    return rc;
  }

  pVfs->iVersion =          2;
  pVfs->szOsFile =          sizeof(sqlite3VolatileFile);
  pVfs->mxPathname =        MAXPATHNAME;
  pVfs->pNext =             0;
  pVfs->zName =             (const char*)zName;
  pVfs->pAppData =          (void*)pRoot;
  pVfs->xOpen =             sqlite3VolatileOpen;
  pVfs->xDelete =           sqlite3VolatileDelete;
  pVfs->xAccess =           sqlite3VolatileAccess;
  pVfs->xFullPathname =     sqlite3VolatileFullPathname;
  pVfs->xDlOpen =           sqlite3VolatileDlOpen;
  pVfs->xDlError =          sqlite3VolatileDlError;
  pVfs->xDlSym =            sqlite3VolatileDlSym;
  pVfs->xDlClose =          sqlite3VolatileDlClose;
  pVfs->xRandomness =       sqlite3VolatileRandomness;
  pVfs->xSleep =            sqlite3VolatileSleep;
  pVfs->xCurrentTime =      sqlite3VolatileCurrentTime;
  pVfs->xGetLastError =     sqlite3VolatileGetLastError;
  pVfs->xCurrentTimeInt64 = sqlite3VolatileCurrentTimeInt64;

  sqlite3_vfs_register(pVfs, 0);

  *ppVfs = pVfs;

  return SQLITE_OK;
}

static void sqlite3VolatileUnregister(sqlite3_vfs* pVfs) {
  assert( pVfs );

  sqlite3_vfs_unregister(pVfs);

  sqlite3VolatileRootDestroy((sqlite3VolatileRoot*)(pVfs->pAppData));
  sqlite3_free(pVfs);
}
*/
import "C"
import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/pkg/errors"
)

// RegisterVolatileFileSystem registers a new volatile VFS under the given
// name.
func RegisterVolatileFileSystem(name string) *VolatileFileSystem {
	volatileVFSLock.Lock()
	defer volatileVFSLock.Unlock()

	iFs := volatileVFSHandles
	volatileVFSHandles++

	var pVfs *C.sqlite3_vfs
	zName := C.CString(name)
	rc := C.sqlite3VolatileRegister(zName, iFs, &pVfs)
	if rc != C.SQLITE_OK {
		panic("out of memory")
	}

	return &VolatileFileSystem{
		zName: zName,
		pVfs:  pVfs,
	}
}

// UnregisterVolatileFileSystem unregisters the given volatile VFS.
func UnregisterVolatileFileSystem(fs *VolatileFileSystem) {
	volatileVFSLock.Lock()
	defer volatileVFSLock.Unlock()

	C.sqlite3VolatileUnregister(fs.pVfs)
}

// Global registry of volatileFileSystem instances.
var volatileVFSLock sync.RWMutex

//var volatileVFSs = make(map[C.int]*volatileVFS)
var volatileVFSHandles C.int

// VolatileFileSystem implements Go-bindings for the volatile sqlite3_vfs
// implementation.
type VolatileFileSystem struct {
	zName *C.char        // C string used for registration.
	pVfs  *C.sqlite3_vfs // VFS implementation.
}

// Name returns the VFS name this volatile file system was registered with.
func (fs *VolatileFileSystem) Name() string {
	return C.GoString(fs.zName)
}

// Open a new volatile file.
func (fs *VolatileFileSystem) Open(name string, flags int) (*VolatileFile, error) {
	zName := C.CString(name)
	defer C.free(unsafe.Pointer(zName))

	pFileSize := unsafe.Sizeof(C.sqlite3VolatileFile{})
	pFile := (*C.sqlite3_file)(C.sqlite3_malloc(C.int(pFileSize)))
	if pFile == nil {
		return nil, newError(C.SQLITE_NOMEM)
	}

	iFlags := C.int(0)

	if (flags & os.O_CREATE) != 0 {
		iFlags = iFlags | C.SQLITE_OPEN_CREATE
	}
	if (flags & os.O_EXCL) != 0 {
		iFlags = iFlags | C.SQLITE_OPEN_EXCLUSIVE
	}
	if (flags & os.O_RDWR) != 0 {
		iFlags = iFlags | C.SQLITE_OPEN_READWRITE
	}
	if strings.HasSuffix(name, "-wal") {
		iFlags = iFlags | C.SQLITE_OPEN_WAL
	} else {
		iFlags = iFlags | C.SQLITE_OPEN_MAIN_DB
	}

	rc := C.sqlite3VolatileOpen(fs.pVfs, zName, pFile, iFlags, &iFlags)
	if rc != C.SQLITE_OK {
		return nil, newError(rc)
	}

	file := &VolatileFile{pFile: pFile}

	return file, nil
}

// Access returns true if the file exists.
func (fs *VolatileFileSystem) Access(name string) (bool, error) {
	zName := C.CString(name)
	defer C.free(unsafe.Pointer(zName))

	var bExists C.int
	rc := C.sqlite3VolatileAccess(fs.pVfs, zName, 0, &bExists)
	if rc != C.SQLITE_OK {
		return false, newError(rc)
	}

	exists := false
	if bExists == 1 {
		exists = true
	}

	return exists, nil
}

// Delete a volatile file.
func (fs *VolatileFileSystem) Delete(name string) error {
	zName := C.CString(name)
	defer C.free(unsafe.Pointer(zName))

	rc := C.sqlite3VolatileDelete(fs.pVfs, zName, 0)
	if rc != C.SQLITE_OK {
		return newError(rc)
	}

	return nil
}

// LastError returns the last error happened.
func (fs *VolatileFileSystem) LastError() syscall.Errno {
	errno := C.sqlite3VolatileGetLastError(fs.pVfs, 0, nil)
	return syscall.Errno(errno)
}

// ReadFile returns a copy of the content of the volatile file with the given
// name.
//
// If the file does not exists, an error is returned.
func (fs *VolatileFileSystem) ReadFile(name string) ([]byte, error) {
	file, err := fs.Open(name, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	size, err := file.Size()
	if err != nil {
		return nil, err
	}

	data := make([]byte, size)

	if size == 0 {
		return data, nil
	}

	p := (*C.sqlite3VolatileFile)(unsafe.Pointer(file.pFile))
	pageSize := int(p.pContent.nPageSize)

	if pageSize == 0 {
		panic("unexpected zero page size")
	}

	switch p.pContent.eType {
	case C.CONTENT_MAIN_DB:
		// Read all pages.
		n := (int(size) / pageSize)
		for i := 0; i < n; i++ {
			offset := i * pageSize
			buffer := data[offset : offset+pageSize]
			if err := file.Read(buffer, int64(offset)); err != nil {
				return nil, err
			}
		}
	case C.CONTENT_WAL:
		// Read the WAL header.
		buffer := data[0:int(C.WAL_HDRSIZE)]
		if err := file.Read(buffer, 0); err != nil {
			return nil, err
		}

		// Read all frames.
		frameSize := int(C.WAL_FRAME_HDRSIZE) + pageSize
		n := (int(size-int64(C.WAL_HDRSIZE)) / frameSize)
		for i := 0; i < n; i++ {
			// Read the frame header.
			offset := int(C.WAL_HDRSIZE) + (i * frameSize)
			buffer := data[offset : offset+int(C.WAL_FRAME_HDRSIZE)]
			if err := file.Read(buffer, int64(offset)); err != nil {
				return nil, err
			}

			// Read the frame page.
			offset += int(C.WAL_FRAME_HDRSIZE)
			buffer = data[offset : offset+pageSize]
			if err := file.Read(buffer, int64(offset)); err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("invalid file type")
	}

	return data, nil
}

// CreateFile writes the given data to the given filename.
//
// If the file exists already, an error is returned.
func (fs *VolatileFileSystem) CreateFile(name string, data []byte) error {
	file, err := fs.Open(name, os.O_CREATE|os.O_EXCL)
	if err != nil {
		return err
	}
	defer file.Close()

	size := len(data)

	if size == 0 {
		return nil
	}

	p := (*C.sqlite3VolatileFile)(unsafe.Pointer(file.pFile))

	switch p.pContent.eType {
	case C.CONTENT_MAIN_DB:
		// TODO: assert that the given data is actually a db image.
		header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
		zBuf := unsafe.Pointer(header.Data)
		pageSize := int(C.sqlite3VolatileDatabaseParsePageSize(zBuf))

		n := (int(size) / pageSize)
		for i := 0; i < n; i++ {
			offset := i * pageSize
			buffer := data[offset : offset+pageSize]
			if err := file.Write(buffer, int64(offset)); err != nil {
				return err
			}
		}
	case C.CONTENT_WAL:
		// We require that a database file with matching name exists,
		// and we read its header to figure the page size.
		databaseName := name[0:(len(name) - len("-wal"))]
		databaseFile, err := fs.Open(databaseName, 0)
		if err != nil {
			return errors.Wrap(err, "failed to open database file")
		}
		defer databaseFile.Close()

		buffer := make([]byte, 100)
		if err := databaseFile.Read(buffer, 0); err != nil {
			return errors.Wrap(err, "failed to read database header")
		}
		header := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
		zBuf := unsafe.Pointer(header.Data)
		pageSize := int(C.sqlite3VolatileDatabaseParsePageSize(zBuf))

		// Write the WAL header.
		buffer = data[0:int(C.WAL_HDRSIZE)]
		if err := file.Write(buffer, 0); err != nil {
			return err
		}

		// Write all frames.
		frameSize := int(C.WAL_FRAME_HDRSIZE) + pageSize
		n := (int(size-int(C.WAL_HDRSIZE)) / frameSize)
		for i := 0; i < n; i++ {
			// Write the frame header.
			offset := int(C.WAL_HDRSIZE) + (i * frameSize)
			buffer := data[offset : offset+int(C.WAL_FRAME_HDRSIZE)]
			if err := file.Write(buffer, int64(offset)); err != nil {
				return err
			}

			// Write the frame page.
			offset += int(C.WAL_FRAME_HDRSIZE)
			buffer = data[offset : offset+pageSize]
			if err := file.Write(buffer, int64(offset)); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid file type")
	}

	return nil
}

// VolatileFile implements Go-bindings for the volatile sqlite3_file
// implementation.
type VolatileFile struct {
	pFile *C.sqlite3_file
}

// Close a volatile file.
func (f *VolatileFile) Close() error {
	rc := C.sqlite3VolatileClose(f.pFile)
	if rc != C.SQLITE_OK {
		return newError(rc)
	}

	C.sqlite3_free(unsafe.Pointer(f.pFile))

	return nil
}

// Read data from the file.
func (f *VolatileFile) Read(buffer []byte, offset int64) error {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))

	zBuf := unsafe.Pointer(header.Data)
	iAmt := C.int(len(buffer))
	iOfst := C.sqlite3_int64(offset)
	rc := C.sqlite3VolatileRead(f.pFile, zBuf, iAmt, iOfst)
	if rc != C.SQLITE_OK {
		return newError(rc)
	}

	return nil
}

// Write data to the file.
func (f *VolatileFile) Write(buffer []byte, offset int64) error {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))

	zBuf := unsafe.Pointer(header.Data)
	iAmt := C.int(len(buffer))
	iOfst := C.sqlite3_int64(offset)
	rc := C.sqlite3VolatileWrite(f.pFile, zBuf, iAmt, iOfst)
	if rc != C.SQLITE_OK {
		return newError(rc)
	}

	return nil
}

// Truncate the file.
func (f *VolatileFile) Truncate(size int64) error {
	rc := C.sqlite3VolatileTruncate(f.pFile, C.sqlite3_int64(size))
	if rc != C.SQLITE_OK {
		return newError(rc)
	}

	return nil
}

// Size returns the size of the file.
func (f *VolatileFile) Size() (int64, error) {
	var iSize C.sqlite3_int64

	rc := C.sqlite3VolatileFileSize(f.pFile, &iSize)
	if rc != C.SQLITE_OK {
		return -1, newError(rc)
	}

	return int64(iSize), nil
}

// Dump the content of all volatile files to the given directory.
func (fs *VolatileFileSystem) Dump(dir string) error {
	pRoot := (*C.sqlite3VolatileRoot)(unsafe.Pointer(fs.pVfs.pAppData))

	for i := 0; i < int(pRoot.nContent); i++ {
		var pContent *C.sqlite3VolatileContent
		pContent = *(**C.sqlite3VolatileContent)(
			unsafe.Pointer(
				uintptr(unsafe.Pointer(pRoot.apContent)) +
					unsafe.Sizeof(pContent)*uintptr(i)))
		if pContent == nil {
			continue
		}
		name := C.GoString(pContent.zName)
		data, err := fs.ReadFile(name)
		if err != nil {
			return errors.Wrapf(err, "failed to read file %s", name)
		}
		if err := volatileDumpFile(data, dir, name); err != nil {
			return errors.Wrapf(err, "failed to dump file %s", name)
		}
	}

	return nil
}

// Dump the content of a volatile file to the actual file system.
func volatileDumpFile(data []byte, dir string, name string) error {
	if strings.HasPrefix(name, "/") {
		return fmt.Errorf("can't dump absolute file path %s", name)
	}

	path := filepath.Join(dir, name)

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return errors.Wrap(err, "failed to create parent directory")
	}
	if err := ioutil.WriteFile(path, data, 0644); err != nil {
		return errors.Wrap(err, "failed to write file")
	}

	return nil
}

//export volatileRandomness
func volatileRandomness(nBuf C.int, zBuf *C.char) C.int {
	buf := make([]byte, nBuf)
	rand.Read(buf) // According to the documentation this never fails.

	start := unsafe.Pointer(zBuf)
	size := unsafe.Sizeof(*zBuf)
	for i := 0; i < int(nBuf); i++ {
		pChar := (*C.char)(unsafe.Pointer(uintptr(start) + size*uintptr(i)))
		*pChar = C.char(buf[i])
	}

	return C.SQLITE_OK
}

//export volatileSleep
func volatileSleep(microseconds C.int) C.int {
	time.Sleep(time.Duration(microseconds) * time.Microsecond)
	return microseconds
}
