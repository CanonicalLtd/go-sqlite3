// Copyright (C) 2018 Yasuhiro Matsumoto <mattn.jp@gmail.com>.
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
// +build session

package sqlite3

import "testing"

func TestSessionUninitialized(t *testing.T) {
	session := SQLiteSession{}
	if session.s != nil {
		t.Fatal("Expected reference to sqlite3_session to be nil")
	}
}
