package flatjsonl

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	gzip "github.com/klauspost/pgzip"
)

// RawWriter writes rows to RAW file.
type RawWriter struct {
	fn    string
	f     io.WriteCloser
	w     *bufio.Writer
	delim []byte

	row        []string
	keyIndexes []int
	keys       []flKey

	isTransposed bool
	transposed   map[string]*RawWriter
	trimmedKeys  map[string]int

	// transposedMapping maps original key index to reduced set of trimmed keys.
	transposedMapping map[int]int
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
	c.keys = keys

	for i, key := range keys {
		if key.transposeDst == "" {
			c.keyIndexes = append(c.keyIndexes, i)

			continue
		}

		tw, err := c.transposedWriter(key.transposeDst, keys)
		if err != nil {
			return err
		}

		tw.keyIndexes = append(tw.keyIndexes, i)

		mappedIdx, ok := tw.trimmedKeys[key.transposeTrimmed]
		if !ok {
			mappedIdx = len(tw.trimmedKeys)
			tw.trimmedKeys[key.transposeTrimmed] = mappedIdx
		}

		tw.transposedMapping[i] = mappedIdx
	}

	return nil
}

func (c *RawWriter) transposedWriter(dst string, keys []flKey) (*RawWriter, error) {
	tw := c.transposed[dst]
	if tw != nil {
		return tw, nil
	}

	if c.transposed == nil {
		c.transposed = map[string]*RawWriter{}
	}

	dir, fn := path.Split(c.fn)
	ext := path.Ext(fn)
	fn = fn[0 : len(fn)-len(ext)]
	fn = path.Join(dir, fn+"_"+dst+ext)

	tw, err := NewRawWriter(fn, string(c.delim))
	if err != nil {
		return nil, fmt.Errorf("failed to init transposed RAW writer for %s: %w", dst, err)
	}

	tw.isTransposed = true
	tw.keys = keys
	tw.trimmedKeys = map[string]int{
		".sequence": 0,
		".index":    1,
	}
	tw.transposedMapping = map[int]int{}

	c.transposed[dst] = tw

	return tw, nil
}

func (c *RawWriter) makeRow(seq int64, values []Value) [][]string {
	c.row = c.row[:0]

	var (
		transposedRowsIdx = map[string][]string{}
		transposedRows    [][]string
		allAbsent         = true
	)

	for _, i := range c.keyIndexes {
		v := values[i]

		if v.Type != TypeAbsent {
			allAbsent = false
		}

		var f string
		if v.Type != TypeNull && v.Type != TypeAbsent {
			f = v.Format()
		}

		if c.isTransposed {
			if v.Type == TypeAbsent {
				continue
			}

			k := c.keys[i]

			transposeKey := k.transposeKey.String()
			row := transposedRowsIdx[transposeKey]

			if row == nil {
				row = make([]string, len(c.trimmedKeys))
				row[0] = strconv.Itoa(int(seq)) // Add sequence.
				row[1] = transposeKey           // Add array idx/object property.
				transposedRowsIdx[transposeKey] = row
				transposedRows = append(transposedRows, row)
			}

			row[c.transposedMapping[i]] = f
		} else {
			c.row = append(c.row, f)
		}
	}

	if allAbsent {
		return nil
	}

	return transposedRows
}

// ReceiveRow receives rows.
func (c *RawWriter) ReceiveRow(seq int64, values []Value) error {
	if len(c.keys) != len(values) {
		panic(fmt.Sprintf("BUG: keys and values mismatch:\nKeys:\n%v\nValues:\n%v\n", c.keys, values))
	}

	transposedRows := c.makeRow(seq, values)

	if c.isTransposed {
		for _, row := range transposedRows {
			if err := c.writeRow(row); err != nil {
				return fmt.Errorf("failed to write RAW row: %w", err)
			}
		}
	} else {
		if len(c.row) == 0 {
			return nil
		}

		if err := c.writeRow(c.row); err != nil {
			return fmt.Errorf("failed to write RAW row: %w", err)
		}

		for dst, tw := range c.transposed {
			if err := tw.ReceiveRow(seq, values); err != nil {
				return fmt.Errorf("transposed %s: %w", dst, err)
			}
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
