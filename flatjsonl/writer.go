package flatjsonl

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
)

// WriteReceiver can receive a row for processing.
type WriteReceiver interface {
	SetupKeys(keys []flKey) error
	ReceiveRow(seq int64, values []Value) error
	Close() error
}

// Writer dispatches rows to multiple receivers.
type Writer struct {
	receivers []WriteReceiver
}

// Value encapsulates value of an allowed Type.
type Value struct {
	Dst       string
	Type      Type
	String    string
	Number    float64
	RawNumber string
	Bool      bool
}

// Format formats Value as string.
func (v Value) Format() string {
	switch v.Type { //nolint: exhaustive
	case TypeString:
		return v.String
	case TypeFloat:
		if len(v.RawNumber) > 0 {
			return v.RawNumber
		}

		return strconv.FormatFloat(v.Number, 'g', 5, 64)
	case TypeBool:
		return strconv.FormatBool(v.Bool)
	case TypeNull:
		return "NULL"
	case TypeAbsent:
		return "ABSENT"
	default:
		panic(fmt.Sprintf("BUG: don't know how to format: %+v", v))
	}
}

// SetupKeys configures writers.
func (w *Writer) SetupKeys(keys []flKey) error {
	var errs []string

	for _, r := range w.receivers {
		if err := r.SetupKeys(keys); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ", "))
	}

	return nil
}

// ReceiveRow passes row to all receivers.
func (w *Writer) ReceiveRow(seq int64, values []Value) error {
	var errs []string

	for _, r := range w.receivers {
		if err := r.ReceiveRow(seq, values); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ", "))
	}

	return nil
}

// Add adds another row receiver.
func (w *Writer) Add(r WriteReceiver) {
	w.receivers = append(w.receivers, r)
}

// HasReceivers is true if there are receivers.
func (w *Writer) HasReceivers() bool {
	return len(w.receivers) > 0
}

// Close tries to close all receivers and returns combined error in case of failures.
func (w *Writer) Close() error {
	var errs []string

	for _, r := range w.receivers {
		if err := r.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ", "))
	}

	return nil
}

type baseWriter struct {
	row        []string
	keyIndexes []int
	keys       []flKey

	isTransposed bool
	transposed   map[string]*baseWriter
	trimmedKeys  map[string]int

	// transposedMapping maps original key index to reduced set of trimmed keys.
	transposedMapping map[int]int
}

func (b *baseWriter) setupKeys(keys []flKey) {
	b.keys = keys

	for i, key := range keys {
		if key.transposeDst == "" {
			b.keyIndexes = append(b.keyIndexes, i)

			continue
		}

		tw := b.transposedWriter(key.transposeDst, keys)

		tw.keyIndexes = append(tw.keyIndexes, i)

		mappedIdx, ok := tw.trimmedKeys[key.transposeTrimmed]
		if !ok {
			mappedIdx = len(tw.trimmedKeys)
			tw.trimmedKeys[key.transposeTrimmed] = mappedIdx
		}

		tw.transposedMapping[i] = mappedIdx
	}
}

func (b *baseWriter) transposedWriter(dst string, keys []flKey) *baseWriter {
	tw := b.transposed[dst]
	if tw != nil {
		return tw
	}

	if b.transposed == nil {
		b.transposed = map[string]*baseWriter{}
	}

	tw = &baseWriter{}

	tw.isTransposed = true
	tw.keys = keys
	tw.trimmedKeys = map[string]int{
		".sequence": 0,
		".index":    1,
	}
	tw.transposedMapping = map[int]int{}

	b.transposed[dst] = tw

	return tw
}

func (b *baseWriter) transposedFileName(base string, dst string) string {
	dir, fn := path.Split(base)
	ext := path.Ext(fn)
	fn = fn[0 : len(fn)-len(ext)]
	fn = path.Join(dir, fn+"_"+dst+ext)

	return fn
}

func (b *baseWriter) receiveRow(seq int64, values []Value) (transposedRows [][]string) {
	if len(b.keys) != len(values) {
		panic(fmt.Sprintf("BUG: keys and values mismatch:\nKeys:\n%v\nValues:\n%v\n", b.keys, values))
	}

	b.row = b.row[:0]

	transposedRowsIdx := map[string][]string{}

	for _, i := range b.keyIndexes {
		v := values[i]

		var f string
		if v.Type != TypeNull && v.Type != TypeAbsent {
			f = v.Format()
		}

		if b.isTransposed {
			if v.Type == TypeAbsent {
				continue
			}

			k := b.keys[i]

			transposeKey := k.transposeKey.String()
			row := transposedRowsIdx[transposeKey]

			if row == nil {
				row = make([]string, len(b.trimmedKeys))
				row[0] = strconv.Itoa(int(seq)) // Add sequence.
				row[1] = transposeKey           // Add array idx/object property.
				transposedRowsIdx[transposeKey] = row
				transposedRows = append(transposedRows, row)
			}

			row[b.transposedMapping[i]] = f
		} else {
			b.row = append(b.row, f)
		}
	}

	return transposedRows
}
