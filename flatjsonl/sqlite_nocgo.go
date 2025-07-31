//go:build !cgo_sqlite

package flatjsonl

import _ "modernc.org/sqlite" // Database driver.

const sqliteDriver = "sqlite"
