package flatjsonl

import (
	"encoding/csv"
	"fmt"
	"os"
)

// CSVWriter writes rows to CSV file.
type CSVWriter struct {
	f           *os.File
	w           *csv.Writer
	headWritten bool
	row         []string
}

// NewCSVWriter creates an instance of CSVWriter.
func NewCSVWriter(fn string) (*CSVWriter, error) {
	var err error

	c := &CSVWriter{}

	c.f, err = os.Create(fn)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV file: %w", err)
	}

	c.w = csv.NewWriter(c.f)

	return c, nil
}

// ReceiveRow receives rows.
func (c *CSVWriter) ReceiveRow(keys []string, values []Value) error {
	if !c.headWritten {
		c.headWritten = true

		err := c.w.Write(keys)
		if err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
	}

	c.row = c.row[:0]

	for _, v := range values {
		if v.Type != TypeNull && v.Type != TypeAbsent {
			c.row = append(c.row, v.Format())
		} else {
			c.row = append(c.row, "")
		}
	}

	err := c.w.Write(c.row)
	if err != nil {
		return fmt.Errorf("failed to write CSV row: %w", err)
	}

	return nil
}

// Close flushes rows anc closes file.
func (c *CSVWriter) Close() error {
	c.w.Flush()

	return c.f.Close()
}
