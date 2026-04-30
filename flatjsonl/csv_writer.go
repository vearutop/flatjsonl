package flatjsonl

import (
	"encoding/csv"
	"fmt"
)

// CSVWriter writes rows to CSV file.
type CSVWriter struct {
	fn        string
	nullValue string
	w         *csv.Writer

	keys              []flKey
	transposedSchemas map[string]transposeSchema
	transposed        map[string]*CSVWriter

	*fileWriter
}

// NewCSVWriter creates an instance of CSVWriter.
func NewCSVWriter(fn string, nullValue string) (*CSVWriter, error) {
	var err error

	c := &CSVWriter{
		fn:        fn,
		nullValue: nullValue,
	}

	c.fileWriter, err = newFileWriter(fn)
	if err != nil {
		return nil, err
	}

	c.w = csv.NewWriter(c.uncompressed)

	return c, nil
}

// SetupKeys writes CSV headers.
func (c *CSVWriter) SetupKeys(keys []flKey, transposed map[string]transposeSchema) (err error) {
	c.keys = keys
	c.transposedSchemas = transposed

	if err := c.writeHead(keys); err != nil {
		return err
	}

	c.transposed = map[string]*CSVWriter{}

	for dst, schema := range transposed {
		fn := transposedFileName(c.fn, dst)

		if c.fn == NopFile {
			fn = c.fn
		}

		ctw, err := NewCSVWriter(fn, c.nullValue)
		if err != nil {
			return fmt.Errorf("failed to init transposed CSV writer for %s: %w", dst, err)
		}

		c.transposed[dst] = ctw

		if err := ctw.writeHead(schema.filteredKeys); err != nil {
			return fmt.Errorf("failed to write transposed head for %s: %w", dst, err)
		}
	}

	return nil
}

func (c *CSVWriter) writeHead(schema []flKey) error {
	keys := make([]string, 0, len(schema))
	for _, k := range schema {
		keys = append(keys, k.replaced)
	}

	err := c.w.Write(keys)
	if err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	return nil
}

// ReceiveRow receives rows.
func (c *CSVWriter) ReceiveRow(_ int64, values []Value, transposed map[string][][]Value) error {
	row := c.renderValues(values)
	if len(row) > 0 {
		if err := c.w.Write(row); err != nil {
			return fmt.Errorf("writing CSV row: %w", err)
		}
	}

	for dst, rows := range transposed {
		tw := c.transposed[dst]
		for _, row := range rows {
			if err := tw.w.Write(tw.renderValues(row)); err != nil {
				return fmt.Errorf("transposed rows for %s: %w", dst, err)
			}
		}
	}

	return nil
}

func (c *CSVWriter) renderValues(values []Value) []string {
	row := make([]string, len(values))
	for i, v := range values {
		if v.Type != TypeNull && v.Type != TypeAbsent {
			row[i] = v.Format()
		} else {
			row[i] = c.nullValue
		}
	}

	return row
}

// Close flushes rows and closes file.
func (c *CSVWriter) Close() error {
	c.w.Flush()

	for _, tw := range c.transposed {
		tw.w.Flush()

		if err := tw.f.Close(); err != nil {
			println("failed to close transpose CSV file: " + err.Error())
		}
	}

	if c.fileWriter == nil {
		return nil
	}

	return c.f.Close()
}
