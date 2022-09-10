package flatjsonl

import (
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"strconv"
)

// CSVWriter writes rows to CSV file.
type CSVWriter struct {
	fn string
	f  *os.File
	w  *csv.Writer

	row        []string
	keyIndexes []int
	keys       []flKey

	isTransposed bool
	transposed   map[string]*CSVWriter
	trimmedKeys  map[string]int

	// transposedMapping maps original key index to reduced set of trimmed keys.
	transposedMapping map[int]int
}

// NewCSVWriter creates an instance of CSVWriter.
func NewCSVWriter(fn string) (*CSVWriter, error) {
	var err error

	c := &CSVWriter{
		fn: fn,
	}

	c.f, err = os.Create(fn)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV file: %w", err)
	}

	c.w = csv.NewWriter(c.f)

	return c, nil
}

func (c *CSVWriter) SetupKeys(keys []flKey) (err error) {
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

	if err := c.writeHead(); err != nil {
		return err
	}

	for dst, tw := range c.transposed {
		if err := tw.writeHead(); err != nil {
			return fmt.Errorf("failed to write transposed head for %s: %w", dst, err)
		}
	}

	return nil
}

func (c *CSVWriter) transposedWriter(dst string, keys []flKey) (*CSVWriter, error) {
	tw := c.transposed[dst]
	if tw != nil {
		return tw, nil
	}

	if c.transposed == nil {
		c.transposed = map[string]*CSVWriter{}
	}

	dir, fn := path.Split(c.fn)
	ext := path.Ext(fn)
	fn = fn[0 : len(fn)-len(ext)]
	fn = path.Join(dir, fn+"_"+dst+ext)

	tw, err := NewCSVWriter(fn)
	if err != nil {
		return nil, fmt.Errorf("failed to init transposed CSV writer for %s: %w", dst, err)
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

func (c *CSVWriter) writeHead() error {
	var keys []string

	if c.isTransposed {
		keys = make([]string, len(c.trimmedKeys))
		for k, i := range c.trimmedKeys {
			keys[i] = k
		}
	} else {
		keys = make([]string, 0, len(c.keyIndexes))

		for _, i := range c.keyIndexes {
			keys = append(keys, c.keys[i].replaced)
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
	if len(c.keys) != len(values) {
		panic(fmt.Sprintf("BUG: keys and values mismatch:\nKeys:\n%v\nValues:\n%v\n", c.keys, values))
	}

	c.row = c.row[:0]

	transposedRows := map[string][]string{}
	allAbsent := true
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
			row := transposedRows[transposeKey]
			if row == nil {
				row = make([]string, len(c.trimmedKeys))
				row[0] = strconv.Itoa(int(seq)) // Add sequence.
				row[1] = transposeKey           // Add array idx/object property.
				transposedRows[transposeKey] = row
			}

			row[c.transposedMapping[i]] = f
		} else {
			c.row = append(c.row, f)
		}
	}

	if allAbsent {
		return nil
	}

	if c.isTransposed {
		// TODO: deterministic iteration.
		for _, row := range transposedRows {
			err := c.w.Write(row)
			if err != nil {
				return fmt.Errorf("failed to write CSV row: %w", err)
			}
		}
	} else {
		err := c.w.Write(c.row)
		if err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}

		for dst, tw := range c.transposed {
			if err := tw.ReceiveRow(seq, values); err != nil {
				return fmt.Errorf("transposed %s: %w", dst, err)
			}
		}
	}

	return nil
}

// Close flushes rows anc closes file.
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
