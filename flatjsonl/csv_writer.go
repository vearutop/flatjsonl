package flatjsonl

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/klauspost/compress/zstd"
	gzip "github.com/klauspost/pgzip"
)

// CSVWriter writes rows to CSV file.
type CSVWriter struct {
	fn string
	f  io.WriteCloser
	w  *csv.Writer

	uncompressed *CountingWriter
	compressed   *CountingWriter

	transposed map[string]*CSVWriter

	b *baseWriter
}

func (c *CSVWriter) Metrics() []ProgressMetric {
	var res []ProgressMetric

	if c.compressed != nil {
		res = append(res, ProgressMetric{
			Name:  path.Base(c.fn) + " (comp)",
			Type:  ProgressBytes,
			Value: &c.compressed.writtenBytes,
		})
	}

	if c.uncompressed != nil {
		res = append(res, ProgressMetric{
			Name:  path.Base(c.fn),
			Type:  ProgressBytes,
			Value: &c.uncompressed.writtenBytes,
		})
	}

	return res
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (nopWriter) Close() error {
	return nil
}

// NewCSVWriter creates an instance of CSVWriter.
func NewCSVWriter(fn string) (*CSVWriter, error) {
	var err error

	c := &CSVWriter{
		fn: fn,
	}

	if fn == NopFile {
		c.f = nopWriter{}
	} else {
		c.f, err = os.Create(fn)
		if err != nil {
			return nil, fmt.Errorf("failed to create CSV file: %w", err)
		}

		switch {
		case strings.HasSuffix(fn, ".gz"):
			c.compressed = &CountingWriter{Writer: c.f}
			c.f = gzip.NewWriter(c.compressed)
		case strings.HasSuffix(fn, ".zst"):
			c.compressed = &CountingWriter{Writer: c.f}
			c.f, err = zstd.NewWriter(c.compressed, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithLowerEncoderMem(true))
			if err != nil {
				return nil, err
			}
		}
	}

	c.uncompressed = &CountingWriter{Writer: c.f}
	c.w = csv.NewWriter(c.uncompressed)

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

		ctw, err := NewCSVWriter(fn)
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
		transposedRows := c.b.receiveRow(seq, values)

		for _, row := range transposedRows {
			if err := c.w.Write(row); err != nil {
				return fmt.Errorf("writing transposed CSV row: %w", err)
			}
		}

		return nil
	}

	c.b.receiveRow(seq, values)

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

// Close flushes rows and closes file.
func (c *CSVWriter) Close() error {
	c.w.Flush()

	for _, tw := range c.transposed {
		tw.w.Flush()

		if err := tw.f.Close(); err != nil {
			println("failed to close transpose CSV file: " + err.Error())
		}
	}

	return c.f.Close()
}
