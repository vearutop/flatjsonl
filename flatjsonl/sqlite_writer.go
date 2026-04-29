package flatjsonl

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/bool64/sqluct"
	_ "modernc.org/sqlite" // Database driver.
)

// SQLiteWriter inserts rows into SQLite database.
type SQLiteWriter struct {
	db        *sql.DB
	tableName string
	tx        *sql.Tx
	rowsTx    int
	sizeTx    int
	replacer  *strings.Replacer
	p         *Processor
	maxCols   int

	transposed map[string]transposeSchema
}

// NewSQLiteWriter creates an instance of SQLiteWriter.
func NewSQLiteWriter(fn string, tableName string, p *Processor) (*SQLiteWriter, error) {
	var err error

	db := p.f.SQLiteInstance

	if db == nil {
		db, err = sql.Open("sqlite", fn)
		if err != nil {
			return nil, err
		}
	}

	_, err = db.Exec("pragma journal_mode=off;")
	if err != nil {
		return nil, err
	}

	if p.f.SQLMaxCols == 0 {
		p.f.SQLMaxCols = 2000
	}

	c := &SQLiteWriter{
		db:        db,
		tableName: tableName,
		replacer:  strings.NewReplacer(`"`, `""`),
		p:         p,
		maxCols:   p.f.SQLMaxCols - 1, // -1 for _seq_id.
	}

	return c, nil
}

// SetupKeys creates tables.
func (c *SQLiteWriter) SetupKeys(keys []flKey, transposed map[string]transposeSchema) error {
	c.transposed = transposed

	if err := c.createTable(c.tableName, keys, false); err != nil {
		return err
	}

	for dst, ts := range transposed {
		if err := c.createTable(c.table(dst), ts.filteredKeys, true); err != nil {
			return err
		}
	}

	switch {
	case len(keys) < 100:
		c.sizeTx = 10000
	case len(keys) < 1000:
		c.sizeTx = 1000
	default:
		c.sizeTx = 100
	}

	return nil
}

func (c *SQLiteWriter) table(dst string) string {
	if dst != "" {
		return c.tableName + "_" + dst
	}

	return c.tableName
}

// ReceiveRow receives rows.
func (c *SQLiteWriter) ReceiveRow(seq int64, values []Value, transposed map[string][][]Value) error {
	if err := c.insert(seq, c.tableName, values); err != nil {
		return fmt.Errorf("writing SQLite row: %w", err)
	}

	if c.rowsTx >= c.sizeTx {
		if err := c.commitTx(); err != nil {
			return fmt.Errorf("committing tx: %w", err)
		}
	}

	for dst, rows := range transposed {
		for _, r := range rows {
			if err := c.insert(seq, c.table(dst), r); err != nil {
				return fmt.Errorf("transposed rows for %s: %w", dst, err)
			}

			if c.rowsTx >= c.sizeTx {
				if err := c.commitTx(); err != nil {
					return fmt.Errorf("committing tx: %w", err)
				}
			}
		}
	}

	return nil
}

func (c *SQLiteWriter) insert(seq int64, tn string, values []Value) error {
	c.rowsTx++

	tableName := tn
	res := `INSERT INTO "` + tableName + `" VALUES (` + strconv.Itoa(int(seq)) + `,`
	part := 1

	for i, v := range values {
		if i > 0 && i%c.maxCols == 0 {
			c.rowsTx++

			res = res[:len(res)-1] + ")"

			if err := c.execTx(res); err != nil {
				return err
			}

			part++
			tableName = tn + partSuffix + strconv.Itoa(part)
			res = `INSERT INTO "` + tableName + `" VALUES (` + strconv.Itoa(int(seq)) + `,`
		}

		if v.Type != TypeNull && v.Type != TypeAbsent {
			res += `"` + c.replacer.Replace(v.Format()) + `",`
		} else {
			res += `NULL,`
		}
	}

	res = res[:len(res)-1] + ")"

	return c.execTx(res)
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

func (c *SQLiteWriter) createTable(tn string, keys []flKey, isTransposed bool) error {
	tableName := tn
	createTable := `CREATE TABLE "` + tableName + `" (
`

	if !isTransposed {
		createTable += `
_seq_id integer primary key,
`
	} else {
		createTable += `
_seq_id integer,
`
	}

	part := 1

	for i, k := range keys {
		if i > 0 && i%c.maxCols == 0 {
			createTable = createTable[:len(createTable)-2] + "\n)"

			_, err := c.db.Exec(createTable)
			if err != nil {
				return fmt.Errorf("failed to create SQLite table with %d keys: %w", len(keys), err)
			}

			part++
			tableName = tn + partSuffix + strconv.Itoa(part)
			createTable = `CREATE TABLE "` + sqluct.QuoteBackticks(tableName) + `" (
`

			if !isTransposed {
				createTable += `
_seq_id integer primary key,
`
			} else {
				createTable += `
_seq_id integer,
`
			}
		}

		tp := ""

		switch k.t { //nolint: exhaustive
		case TypeInt, TypeBool:
			tp = " INTEGER"
		case TypeFloat:
			tp = " REAL"
		}

		createTable += sqluct.QuoteRequiredBackticks(k.replaced) + tp + `,` + "\n"
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
