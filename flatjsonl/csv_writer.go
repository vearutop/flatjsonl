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

	transposed map[string]*CSVWriter

	*fileWriter
	b *baseWriter
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
	c.b = &baseWriter{}

	return c, nil
}

// SetupKeys writes CSV headers.
func (c *CSVWriter) SetupKeys(keys []flKey) (err error) {
	c.b.setupKeys(keys)

	if err := c.writeHead(); err != nil {
		return err
	}

	c.transposed = map[string]*CSVWriter{}

	for dst, tw := range c.b.transposed {
		fn := c.b.transposedFileName(c.fn, dst)
		tw.extName = fn

		if c.fn == NopFile {
			fn = c.fn
		}

		ctw, err := NewCSVWriter(fn, c.nullValue)
		if err != nil {
			return fmt.Errorf("failed to init transposed CSV writer for %s: %w", dst, err)
		}

		ctw.b = tw
		c.transposed[dst] = ctw

		if err := ctw.writeHead(); err != nil {
			return fmt.Errorf("failed to write transposed head for %s: %w", dst, err)
		}
	}

	return nil
}

func (c *CSVWriter) writeHead() error {
	var keys []string

	if c.b.isTransposed {
		keys = make([]string, len(c.b.trimmedKeys))
		for _, i := range c.b.trimmedKeys {
			keys[i.idx] = i.k.replaced
		}
	} else {
		keys = make([]string, 0, len(c.b.keyIndexes))

		for _, i := range c.b.keyIndexes {
			keys = append(keys, c.b.keys[i].replaced) //nolint:makezero // False positive: append to slice `keys` with non-zero initialized length.
		}
	}

	err := c.w.Write(keys)
	if err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	return nil
}

// ReceiveRow receives rows.
func (c *CSVWriter) ReceiveRow(seq int64, values []Value) error {
	if c.b.isTransposed {
		transposedRows := c.receiveRow(seq, values)

		for _, row := range transposedRows {
			if err := c.w.Write(row); err != nil {
				return fmt.Errorf("writing transposed CSV row: %w", err)
			}
		}

		return nil
	}

	c.receiveRow(seq, values)

	if len(c.b.row) > 0 {
		if err := c.w.Write(c.b.row); err != nil {
			return fmt.Errorf("writing CSV row: %w", err)
		}
	}

	for dst, tw := range c.transposed {
		if err := tw.ReceiveRow(seq, values); err != nil {
			return fmt.Errorf("transposed rows for %s: %w", dst, err)
		}
	}

	return nil
}

func (c *CSVWriter) receiveRow(seq int64, values []Value) (transposedRows [][]string) {
	if len(c.b.keys) != len(values) {
		panic(fmt.Sprintf("BUG: keys and values mismatch:\nKeys:\n%v\nValues:\n%v\n", c.b.keys, values))
	}

	c.b.row = c.b.row[:0]

	transposedRowsIdx := map[string][]string{}

	for _, i := range c.b.keyIndexes {
		v := values[i]

		f := c.nullValue
		if v.Type != TypeNull && v.Type != TypeAbsent {
			f = v.Format()
		}

		if c.b.isTransposed {
			if v.Type == TypeAbsent {
				continue
			}

			k := c.b.keys[i]

			transposeKey := k.transposeKey.String()
			row := transposedRowsIdx[transposeKey]

			if row == nil {
				row = make([]string, len(c.b.trimmedKeys))
				row[0] = Format(float64(seq)) // Add sequence.
				row[1] = transposeKey         // Add array idx/object property.
				transposedRowsIdx[transposeKey] = row
				transposedRows = append(transposedRows, row)
			}

			row[c.b.transposedMapping[i]] = f

			continue
		}

		c.b.row = append(c.b.row, f)
	}

	return transposedRows
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
