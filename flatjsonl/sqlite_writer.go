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

	posByDst map[string][]int
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

func (c *SQLiteWriter) SetupKeys(keys []flKey) error {
	c.posByDst = map[string][]int{}
	keysByDst := map[string][]flKey{}

	for i, k := range keys {
		c.posByDst[k.transposeDst] = append(c.posByDst[k.transposeDst], i)
		keysByDst[k.transposeDst] = append(keysByDst[k.transposeDst], k)
	}

	for dst, kk := range keysByDst {
		if err := c.createTable(c.table(dst), kk); err != nil {
			return err
		}
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
func (c *SQLiteWriter) ReceiveRow(seq int64, values []Value) error {
	//if !c.tableCreated {
	//	if err := c.createTable(keys); err != nil {
	//		return err
	//	}
	//}

	c.row = c.row[:0]

	c.seq++

	for dst := range c.posByDst {
		if err := c.insertDst(dst, values); err != nil {
			return err
		}
	}

	if c.rowsTx >= 1000 {
		return c.commitTx()
	}

	return nil
}

func (c *SQLiteWriter) insertDst(dst string, values []Value) error {
	tableName := c.table(dst)

	c.rowsTx++
	res := `INSERT INTO "` + tableName + `" VALUES (` + strconv.Itoa(c.seq) + `,`
	part := 1

	for i, pos := range c.posByDst[dst] {
		if i > 0 && i%sqliteMaxKeys == 0 {
			c.rowsTx++

			res = res[:len(res)-1] + ")"

			if err := c.execTx(res); err != nil {
				return err
			}

			part++
			tableName = c.tableName + "_part" + strconv.Itoa(part)
			res = `INSERT INTO "` + tableName + `" VALUES (` + strconv.Itoa(c.seq) + `,`
		}

		v := values[pos]

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

const sqliteMaxKeys = 1999 // 2000-1 for _seq_id.

func (c *SQLiteWriter) createTable(tn string, keys []flKey) error {
	c.tableCreated = true

	tableName := tn
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

		switch k.t { //nolint: exhaustive
		case TypeInt, TypeBool:
			tp = " INTEGER"
		case TypeFloat:
			tp = " REAL"
		}

		createTable += `"` + k.replaced + `"` + tp + `,` + "\n"
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
