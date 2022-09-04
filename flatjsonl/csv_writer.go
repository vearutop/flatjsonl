package flatjsonl

import (
	"encoding/csv"
	"fmt"
	"os"
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

		tw := c.transposed[key.transposeDst]
		if tw == nil {
			if c.transposed == nil {
				c.transposed = map[string]*CSVWriter{}
			}

			tw, err = NewCSVWriter(key.transposeDst + "_" + c.fn)
			if err != nil {
				return fmt.Errorf("failed to init transposed CSV writer for %s: %w", key.transposeDst, err)
			}

			tw.isTransposed = true
			tw.keys = keys
			tw.trimmedKeys = map[string]int{
				".sequence": 0,
				".index":    1,
			}
			tw.transposedMapping = map[int]int{}

			c.transposed[key.transposeDst] = tw
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
func (c *CSVWriter) ReceiveRow(values []Value) error {
	if len(c.keys) != len(values) {
		panic(fmt.Sprintf("BUG: keys and values mismatch:\nKeys:\n%v\nValues:\n%v\n", keys, values))
	}

	c.row = c.row[:0]

	rows := map[int][]string{}
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
			k := c.keys[i]

			row := rows[k.transposeIdx]
			if row == nil {
				row = make([]string, len(c.trimmedKeys))
				row[0] = strconv.Itoa(int(v.Seq))
				row[1] = strconv.Itoa(k.transposeIdx)
				rows[k.transposeIdx] = row
			}

			if f != "" {
				_ = f
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
		for i, row := range rows {
			row = append(row, strconv.Itoa(i))
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
	}

	for dst, tw := range c.transposed {
		if err := tw.ReceiveRow(values); err != nil {
			return fmt.Errorf("transposed %s: %w", dst, err)
		}
	}

	return nil
}

// Close flushes rows anc closes file.
func (c *CSVWriter) Close() error {
	c.w.Flush()

	return c.f.Close()
}
