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

// SQLiteSession implement interface of Session.
type SQLiteSession struct {
	s *C.sqlite3_session
}
