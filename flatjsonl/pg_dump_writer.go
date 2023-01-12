package flatjsonl

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/bool64/sqluct"
	"github.com/klauspost/compress/zstd"
	gzip "github.com/klauspost/pgzip"
)

// PGDumpWriter creates PostgreSQL dump file.   .
type PGDumpWriter struct {
	fn string
	f  io.WriteCloser

	tableName string
	replacer  *strings.Replacer
	p         *Processor
	maxCols   int

	linesTx int

	sortedTransposed []string

	b          *baseWriter
	csvCopiers map[string]csvCopier
}

type csvCopier struct {
	stmt string
	cb   *bytes.Buffer
	cw   *csv.Writer
}

// NewPGDumpWriter creates an instance of PGDumpWriter.
func NewPGDumpWriter(fn string, tableName string, p *Processor) (*PGDumpWriter, error) {
	var err error

	c := &PGDumpWriter{
		fn:         fn,
		tableName:  tableName,
		replacer:   strings.NewReplacer(`"`, `""`),
		p:          p,
		maxCols:    p.f.SQLMaxCols - 1, // -1 for _seq_id.
		csvCopiers: map[string]csvCopier{},
	}

	if fn == "<nop>" {
		c.f = nopWriter{}
	} else {
		c.f, err = os.Create(fn)
		if err != nil {
			return nil, fmt.Errorf("failed to create file: %w", err)
		}

		switch {
		case strings.HasSuffix(fn, ".gz"):
			c.f = gzip.NewWriter(c.f)
		case strings.HasSuffix(fn, ".zst"):
			c.f, err = zstd.NewWriter(c.f, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithLowerEncoderMem(true))
			if err != nil {
				return nil, err
			}
		}
	}

	return c, nil
}

// SetupKeys creates tables.
func (c *PGDumpWriter) SetupKeys(keys []flKey) error {
	c.b = &baseWriter{}
	c.b.p = c.p
	c.b.setupKeys(keys)

	if err := c.createTable(c.tableName, c.b.filteredKeys, false); err != nil {
		return err
	}

	for dst := range c.b.transposed {
		c.sortedTransposed = append(c.sortedTransposed, dst)
	}

	sort.Strings(c.sortedTransposed)

	for _, dst := range c.sortedTransposed {
		tw := c.b.transposed[dst]
		tw.extName = c.table(dst)
		tw.p = c.p

		if err := c.createTable(c.table(dst), tw.filteredKeys, true); err != nil {
			return err
		}
	}

	return nil
}

func (c *PGDumpWriter) table(dst string) string {
	if dst != "" {
		return c.tableName + "_" + dst
	}

	return c.tableName
}

// ReceiveRow receives rows.
func (c *PGDumpWriter) ReceiveRow(seq int64, values []Value) error {
	c.linesTx++

	c.b.receiveRow(seq, values)

	if err := c.insert(seq, c.tableName, c.b.row); err != nil {
		return fmt.Errorf("writing SQLite row: %w", err)
	}

	for _, dst := range c.sortedTransposed {
		tw := c.b.transposed[dst]
		transposedRows := tw.receiveRow(seq, values)

		for _, r := range transposedRows {
			if err := c.insert(seq, tw.extName, r); err != nil {
				return fmt.Errorf("transposed rows for %s: %w", dst, err)
			}
		}
	}

	if c.linesTx > 1000 {
		c.linesTx = 0

		if err := c.flush(); err != nil {
			return err
		}
	}

	return nil
}

func (c *PGDumpWriter) flush() error {
	for _, cc := range c.csvCopiers {
		cc.cw.Flush()

		if _, err := c.f.Write([]byte(cc.stmt)); err != nil {
			return err
		}

		if _, err := c.f.Write(cc.cb.Bytes()); err != nil {
			return err
		}

		cc.cb.Reset()

		if _, err := c.f.Write([]byte("\\.\n\n")); err != nil {
			return err
		}
	}

	return nil
}

func (c *PGDumpWriter) insert(seq int64, tn string, values []string) error {
	tableName := tn
	part := 1
	tw := c.csvCopiers[tableName]

	for {
		if len(values) < c.maxCols {
			row := make([]string, 0, len(values)+1)
			row = append(row, strconv.Itoa(int(seq)))
			row = append(row, values...)

			if err := tw.cw.Write(row); err != nil {
				return err
			}

			break
		} else {
			row := make([]string, 0, c.maxCols+1)
			row = append(row, strconv.Itoa(int(seq)))
			row = append(row, values[:c.maxCols]...)
			values = values[c.maxCols:]

			if err := tw.cw.Write(row); err != nil {
				return err
			}

			part++
			tableName = tn + "_part" + strconv.Itoa(part)
			tw = c.csvCopiers[tableName]
		}
	}

	return nil
}

func (c *PGDumpWriter) createTable(tn string, keys []flKey, isTransposed bool) error {
	tableName := tn
	createTable := `CREATE TABLE ` + sqluct.QuoteANSI(tableName) + ` (`
	// COPY public.products (product_no, name, price) FROM stdin;
	copyStmt := `COPY ` + sqluct.QuoteANSI(tableName) + ` ("_seq_id",`

	if !isTransposed {
		createTable += `
	"_seq_id" INT8 primary key,
`
	} else {
		createTable += `
	"_seq_id" INT8,
`
	}

	part := 1

	for i, k := range keys {
		if i > 0 && i%c.maxCols == 0 {
			createTable = createTable[:len(createTable)-2] + "\n);\n\n"
			copyStmt = copyStmt[:len(copyStmt)-1] + ") FROM stdin WITH (FORMAT csv);\n"

			cb := bytes.NewBuffer(nil)
			c.csvCopiers[tableName] = csvCopier{
				stmt: copyStmt,
				cb:   cb,
				cw:   csv.NewWriter(cb),
			}

			_, err := c.f.Write([]byte(createTable))
			if err != nil {
				return fmt.Errorf("failed to create table with %d keys: %w", len(keys), err)
			}

			part++
			tableName = tn + "_part" + strconv.Itoa(part)
			createTable = `CREATE TABLE ` + sqluct.QuoteANSI(tableName) + ` (`
			copyStmt = `COPY ` + sqluct.QuoteANSI(tableName) + ` ("_seq_id",`

			if !isTransposed {
				createTable += `
"_seq_id" INT8 primary key,
`
			} else {
				createTable += `
"_seq_id" INT8,
`
			}
		}

		tp := ""

		switch k.t { //nolint: exhaustive
		case TypeInt:
			tp = " INT8"
		case TypeBool:
			tp = " BOOL"
		case TypeFloat:
			tp = " FLOAT8"
		case TypeString:
			tp = " VARCHAR"
		}

		createTable += "\t" + sqluct.QuoteANSI(k.replaced) + tp + `,` + "\n"
		copyStmt += sqluct.QuoteANSI(k.replaced) + `,`
	}

	createTable = createTable[:len(createTable)-2] + "\n);\n\n"
	copyStmt = copyStmt[:len(copyStmt)-1] + ") FROM stdin WITH (FORMAT csv);\n"

	_, err := c.f.Write([]byte(createTable))
	if err != nil {
		return fmt.Errorf("failed to create table with %d keys: %w", len(keys), err)
	}

	cb := bytes.NewBuffer(nil)
	c.csvCopiers[tableName] = csvCopier{
		stmt: copyStmt,
		cb:   cb,
		cw:   csv.NewWriter(cb),
	}

	return nil
}

// Close flushes CSV and closes output file.
func (c *PGDumpWriter) Close() error {
	if err := c.flush(); err != nil {
		return err
	}

	return c.f.Close()
}
