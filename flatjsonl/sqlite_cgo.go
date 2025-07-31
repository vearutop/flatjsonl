//go:build cgo_sqlite

package flatjsonl

import _ "github.com/mattn/go-sqlite3" // Database driver.

const sqliteDriver = "sqlite3"
