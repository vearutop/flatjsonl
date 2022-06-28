package flatjsonl

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite" // Database driver.
)

// SQLiteWriter inserts rows into SQLite database.
type SQLiteWriter struct {
	db           *sql.DB
	tableCreated bool
	tableName    string
	row          []string
	tx           *sql.Tx
	rowsTx       int
}

// NewSQLiteWriter creates an instance of SQLiteWriter.
func NewSQLiteWriter(fn string, tableName string) (*SQLiteWriter, error) {
	var err error

	db, err := sql.Open("sqlite", fn)
	if err != nil {
		return nil, err
	}

	c := &SQLiteWriter{
		db:        db,
		tableName: tableName,
	}

	return c, nil
}

// ReceiveRow receives rows.
func (c *SQLiteWriter) ReceiveRow(keys []string, values []interface{}) error {
	if !c.tableCreated {
		if err := c.createTable(keys); err != nil {
			return err
		}
	}

	c.row = c.row[:0]

	res := `INSERT INTO "` + c.tableName + `" VALUES (`

	for _, v := range values {
		if v != nil {
			res += `"` + Format(v) + `",`
		} else {
			res += `NULL,`
		}
	}

	res = res[:len(res)-1] + ")"

	if c.tx == nil {
		tx, err := c.db.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin SQLite transaction: %w", err)
		}

		c.tx = tx
		c.rowsTx = 0
	}

	_, err := c.tx.Exec(res)
	if err != nil {
		return fmt.Errorf("failed to insert SQLite row: %w", err)
	}

	c.rowsTx++

	if c.rowsTx >= 1000 {
		err = c.tx.Commit()
		if err != nil {
			return fmt.Errorf("failed to commit SQLite transaction: %w", err)
		}

		c.tx = nil
		c.rowsTx = 0
	}

	return nil
}

func (c *SQLiteWriter) createTable(keys []string) error {
	c.tableCreated = true

	createTable := `CREATE TABLE "` + c.tableName + `" (`

	for _, k := range keys {
		createTable += `"` + k + `",`
	}

	createTable = createTable[:len(createTable)-1] + ")"

	_, err := c.db.Exec(createTable)
	if err != nil {
		return fmt.Errorf("failed to create SQLite table: %w", err)
	}

	return nil
}

// Close commits outstanding transaction and closes database instance.
func (c *SQLiteWriter) Close() error {
	if c.tx != nil {
		err := c.tx.Commit()
		if err != nil {
			println(err.Error())
		}
	}

	return c.db.Close()
}
