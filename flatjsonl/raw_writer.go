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
	fn    string
	f     io.WriteCloser
	w     *bufio.Writer
	delim []byte

	transposed map[string]*RawWriter

	b *baseWriter
}

// NewRawWriter creates an instance of RawWriter.
func NewRawWriter(fn string, delimiter string) (*RawWriter, error) {
	var err error

	c := &RawWriter{fn: fn}

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

// SetupKeys initializes writer.
func (c *RawWriter) SetupKeys(keys []flKey) (err error) {
	c.b = &baseWriter{}
	c.b.setupKeys(keys)

	c.transposed = map[string]*RawWriter{}

	for dst, tw := range c.b.transposed {
		ctw, err := NewRawWriter(c.b.transposedFileName(c.fn, dst), string(c.delim))
		if err != nil {
			return fmt.Errorf("failed to init transposed CSV writer for %s: %w", dst, err)
		}

		ctw.b = tw
		c.transposed[dst] = ctw
	}

	return nil
}

// ReceiveRow receives rows.
func (c *RawWriter) ReceiveRow(seq int64, values []Value) error {
	if c.b.isTransposed {
		transposedRows := c.b.receiveRow(seq, values)

		for _, row := range transposedRows {
			if err := c.writeRow(row); err != nil {
				return fmt.Errorf("writing transposed RAW row: %w", err)
			}
		}

		return nil
	}

	c.b.receiveRow(seq, values)

	if len(c.b.row) > 0 {
		if err := c.writeRow(c.b.row); err != nil {
			return fmt.Errorf("writing RAW row: %w", err)
		}
	}

	for dst, tw := range c.transposed {
		if err := tw.ReceiveRow(seq, values); err != nil {
			return fmt.Errorf("transposed rows for %s: %w", dst, err)
		}
	}

	return nil
}

func (c *RawWriter) writeRow(row []string) error {
	for i, v := range row {
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
		return fmt.Errorf("failed to flush RAW file: %w", err)
	}

	for _, tw := range c.transposed {
		if err := tw.w.Flush(); err != nil {
			return fmt.Errorf("failed to flush transposed RAW file: %w", err)
		}

		if err := tw.f.Close(); err != nil {
			println("failed to close transposed RAW file: " + err.Error())
		}
	}

	return c.f.Close()
}
