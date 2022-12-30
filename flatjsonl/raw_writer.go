package flatjsonl

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	gzip "github.com/klauspost/pgzip"
)

// RawWriter writes rows to RAW file.
type RawWriter struct {
	f     io.WriteCloser
	w     *bufio.Writer
	row   []string
	delim []byte
}

// NewRawWriter creates an instance of RawWriter.
func NewRawWriter(fn string, delimiter string) (*RawWriter, error) {
	var err error

	c := &RawWriter{}

	c.delim = []byte(delimiter)

	c.f, err = os.Create(fn)
	if err != nil {
		return nil, fmt.Errorf("failed to create RAW file: %w", err)
	}

	if strings.HasSuffix(fn, ".gz") {
		c.f = gzip.NewWriter(c.f)
	}

	c.w = bufio.NewWriter(c.f)

	return c, nil
}

// ReceiveRow receives rows.
func (c *RawWriter) ReceiveRow(keys []string, values []Value) error {
	if len(keys) != len(values) {
		panic(fmt.Sprintf("BUG: keys (%d) and values (%d) mismatch:\nKeys:\n%v\nValues:\n%v\n",
			len(keys), len(values), keys, values))
	}

	c.row = c.row[:0]

	for _, v := range values {
		if v.Type != TypeNull && v.Type != TypeAbsent {
			c.row = append(c.row, v.Format())
		} else {
			c.row = append(c.row, "")
		}
	}

	for i, v := range c.row {
		if i > 0 && len(c.delim) > 0 {
			if _, err := c.w.Write(c.delim); err != nil {
				return fmt.Errorf("delimiter write failed: %w", err)
			}
		}

		if _, err := c.w.Write([]byte(v)); err != nil {
			return fmt.Errorf("column write failed: %w", err)
		}
	}

	if _, err := c.w.Write([]byte("\n")); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}

	return nil
}

// Close flushes rows and closes file.
func (c *RawWriter) Close() error {
	if err := c.w.Flush(); err != nil {
		println("failed to flush:", err.Error())
	}

	return c.f.Close()
}
