package flatjsonl

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

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
	seq          int
	replacer     *strings.Replacer
	p            *Processor
}

// NewSQLiteWriter creates an instance of SQLiteWriter.
func NewSQLiteWriter(fn string, tableName string, p *Processor) (*SQLiteWriter, error) {
	var err error

	db, err := sql.Open("sqlite", fn)
	if err != nil {
		return nil, err
	}

	c := &SQLiteWriter{
		db:        db,
		tableName: tableName,
		replacer:  strings.NewReplacer(`"`, `""`),
		p:         p,
	}

	return c, nil
}

// ReceiveRow receives rows.
func (c *SQLiteWriter) ReceiveRow(keys []string, values []Value) error {
	if !c.tableCreated {
		if err := c.createTable(keys); err != nil {
			return err
		}
	}

	c.row = c.row[:0]

	c.seq++
	tableName := c.tableName
	res := `INSERT INTO "` + tableName + `" VALUES (` + strconv.Itoa(c.seq) + `,`
	part := 1

	for i, v := range values {
		if i > 0 && i%sqliteMaxKeys == 0 {
			res = res[:len(res)-1] + ")"

			if err := c.execTx(res); err != nil {
				return err
			}

			part++
			tableName = c.tableName + "_part" + strconv.Itoa(part)
			res = `INSERT INTO "` + tableName + `" VALUES (` + strconv.Itoa(c.seq) + `,`
		}

		if v.Type != TypeNull && v.Type != TypeAbsent {
			res += `"` + c.replacer.Replace(v.Format()) + `",`
		} else {
			res += `NULL,`
		}
	}

	res = res[:len(res)-1] + ")"

	if err := c.execTx(res); err != nil {
		return err
	}

	c.rowsTx++

	if c.rowsTx >= 1000 {
		return c.commitTx()
	}

	return nil
}

func (c *SQLiteWriter) commitTx() error {
	if err := c.tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit SQLite transaction: %w", err)
	}

	c.tx = nil
	c.rowsTx = 0

	return nil
}

func (c *SQLiteWriter) execTx(res string) error {
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

	return nil
}

const sqliteMaxKeys = 1999 // 2000-1 for _seq_id.

func (c *SQLiteWriter) createTable(keys []string) error {
	c.tableCreated = true

	tableName := c.tableName
	createTable := `CREATE TABLE "` + tableName + `" (
_seq_id integer primary key,
`
	part := 1

	for i, k := range keys {
		if i > 0 && i%sqliteMaxKeys == 0 {
			createTable = createTable[:len(createTable)-2] + "\n)"

			_, err := c.db.Exec(createTable)
			if err != nil {
				return fmt.Errorf("failed to create SQLite table with %d keys: %w", len(keys), err)
			}

			part++
			tableName = c.tableName + "_part" + strconv.Itoa(part)
			createTable = `CREATE TABLE "` + tableName + `" (
_seq_id integer primary key,
`
		}

		tp := ""

		t := c.p.types[i]
		switch t { //nolint: exhaustive
		case TypeInt, TypeBool:
			tp = " INTEGER"
		case TypeFloat:
			tp = " REAL"
		}

		createTable += `"` + k + `"` + tp + `,` + "\n"
	}

	createTable = createTable[:len(createTable)-2] + "\n)"

	_, err := c.db.Exec(createTable)
	if err != nil {
		return fmt.Errorf("failed to create SQLite table with %d keys: %w", len(keys), err)
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
