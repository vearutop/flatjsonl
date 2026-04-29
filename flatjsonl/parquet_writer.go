package flatjsonl

import (
	"fmt"
	"strconv"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	pgzip "github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// ParquetWriter writes rows to Parquet file.
type ParquetWriter struct {
	fn          string
	compression string
	p           *Processor

	w *parquet.GenericWriter[any]

	rowsInGroup int
	flushSize   int

	orderedColumns []parquetColumn
	rowsBatch      []parquet.Row
	batchSize      int

	keys       []flKey
	transposed map[string]*ParquetWriter

	*fileWriter
}

type parquetColumn struct {
	valueIndex  int
	columnIndex int
	columnType  Type
	columnName  string
}

// NewParquetWriter creates an instance of ParquetWriter.
func NewParquetWriter(fn string, compression string, p *Processor) (*ParquetWriter, error) {
	var err error

	c := &ParquetWriter{
		fn:          fn,
		compression: compression,
		p:           p,
	}

	c.fileWriter, err = newFileWriter(fn)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// SetupKeys initializes writer.
func (c *ParquetWriter) SetupKeys(keys []flKey, transposed map[string]transposeSchema) (err error) {
	c.keys = keys

	if err := c.setupParquetWriter(keys); err != nil {
		return err
	}

	c.transposed = map[string]*ParquetWriter{}

	for dst, ts := range transposed {
		fn := transposedFileName(c.fn, dst)

		if c.fn == NopFile {
			fn = c.fn
		}

		ctw, err := NewParquetWriter(fn, c.compression, c.p)
		if err != nil {
			return fmt.Errorf("failed to init transposed Parquet writer for %s: %w", dst, err)
		}

		ctw.keys = ts.filteredKeys
		c.transposed[dst] = ctw

		if err := ctw.setupParquetWriter(ts.filteredKeys); err != nil {
			return fmt.Errorf("failed to setup transposed Parquet writer for %s: %w", dst, err)
		}
	}

	return nil
}

func (c *ParquetWriter) setupParquetWriter(keys []flKey) error {
	group := parquet.Group{}

	codec, err := parquetCompressionCodec(c.compression)
	if err != nil {
		return err
	}

	for _, k := range keys {
		group[k.replaced] = parquet.Optional(parquetNode(k.t))
	}

	schema := parquet.NewSchema("flatjsonl", group)
	c.w = parquet.NewGenericWriter[any](c.uncompressed, &parquet.WriterConfig{
		Schema:      schema,
		Compression: codec,
	})

	c.flushSize = parquetFlushSize(len(keys))
	c.batchSize = parquetBatchSize(len(keys))

	for _, colPath := range schema.Columns() {
		leaf, ok := schema.Lookup(colPath...)
		if !ok {
			return fmt.Errorf("failed to look up parquet column %v", colPath)
		}

		name := colPath[len(colPath)-1]
		found := false

		for i, k := range keys {
			if k.replaced == name {
				c.orderedColumns = append(c.orderedColumns, parquetColumn{
					valueIndex:  i,
					columnIndex: leaf.ColumnIndex,
					columnType:  k.t,
					columnName:  k.replaced,
				})
				found = true

				break
			}
		}

		if !found {
			return fmt.Errorf("failed to map parquet column %q to filtered keys", name)
		}
	}

	return nil
}

func parquetNode(t Type) parquet.Node {
	switch t { //nolint:exhaustive
	case TypeBool:
		return parquet.Leaf(parquet.BooleanType)
	case TypeInt:
		return parquet.Int(64)
	case TypeFloat:
		return parquet.Leaf(parquet.DoubleType)
	default:
		return parquet.String()
	}
}

func parquetFlushSize(numCols int) int {
	switch {
	case numCols >= 1500:
		return 500
	case numCols >= 1000:
		return 1000
	case numCols >= 500:
		return 2500
	default:
		return 10000
	}
}

func parquetBatchSize(numCols int) int {
	switch {
	case numCols >= 1500:
		return 64
	case numCols >= 1000:
		return 128
	case numCols >= 500:
		return 256
	default:
		return 512
	}
}

func parquetCompressionCodec(name string) (compress.Codec, error) {
	switch name {
	case "", "snappy":
		return &snappy.Codec{}, nil
	case "zstd":
		return &zstd.Codec{}, nil
	case "gzip":
		return &pgzip.Codec{}, nil
	case "none", "uncompressed":
		return &uncompressed.Codec{}, nil
	default:
		return nil, fmt.Errorf("unsupported parquet compression %q", name)
	}
}

// ReceiveRow receives rows.
func (c *ParquetWriter) ReceiveRow(_ int64, values []Value, transposed map[string][][]Value) error {
	if err := c.writeRow(values); err != nil {
		return fmt.Errorf("writing Parquet row: %w", err)
	}

	for dst, rows := range transposed {
		tw := c.transposed[dst]
		for _, row := range rows {
			if err := tw.writeRow(row); err != nil {
				return fmt.Errorf("transposed rows for %s: %w", dst, err)
			}
		}
	}

	return nil
}

func (c *ParquetWriter) writeRow(values []Value) error {
	row := make(parquet.Row, 0, len(c.orderedColumns))

	for _, col := range c.orderedColumns {
		v := values[col.valueIndex]

		pv, err := parquetValue(v, col.columnType, col.columnIndex)
		if err != nil {
			return fmt.Errorf("column %s: %w", col.columnName, err)
		}

		row = append(row, pv)
	}

	c.rowsBatch = append(c.rowsBatch, row)

	if len(c.rowsBatch) >= c.batchSize {
		if _, err := c.w.WriteRows(c.rowsBatch); err != nil {
			return err
		}

		clear(c.rowsBatch)
		c.rowsBatch = c.rowsBatch[:0]
	}

	c.rowsInGroup++

	if c.rowsInGroup >= c.flushSize {
		if err := c.flushBatch(); err != nil {
			return err
		}

		if err := c.w.Flush(); err != nil {
			return fmt.Errorf("failed to flush parquet row group: %w", err)
		}

		c.rowsInGroup = 0
	}

	return nil
}

func (c *ParquetWriter) flushBatch() error {
	if len(c.rowsBatch) == 0 {
		return nil
	}

	if _, err := c.w.WriteRows(c.rowsBatch); err != nil {
		return fmt.Errorf("failed to write parquet rows: %w", err)
	}

	clear(c.rowsBatch)
	c.rowsBatch = c.rowsBatch[:0]

	return nil
}

func parquetValue(v Value, inferredType Type, columnIndex int) (parquet.Value, error) {
	if v.Type == TypeNull || v.Type == TypeAbsent {
		return parquet.ValueOf(nil).Level(0, 0, columnIndex), nil
	}

	switch inferredType { //nolint:exhaustive
	case TypeBool:
		switch v.Type { //nolint:exhaustive
		case TypeBool:
			return parquet.BooleanValue(v.Bool).Level(0, 1, columnIndex), nil
		case TypeFloat:
			return parquet.BooleanValue(v.Number != 0).Level(0, 1, columnIndex), nil
		case TypeString:
			b, err := strconv.ParseBool(v.String)
			if err != nil {
				return parquet.Value{}, fmt.Errorf("parse bool value %q: %w", v.String, err)
			}

			return parquet.BooleanValue(b).Level(0, 1, columnIndex), nil
		default:
			return parquet.Value{}, fmt.Errorf("unexpected value type %s for bool column", v.Type)
		}
	case TypeInt:
		switch v.Type { //nolint:exhaustive
		case TypeFloat:
			return parquet.Int64Value(int64(v.Number)).Level(0, 1, columnIndex), nil
		case TypeBool:
			if v.Bool {
				return parquet.Int64Value(1).Level(0, 1, columnIndex), nil
			}

			return parquet.Int64Value(0).Level(0, 1, columnIndex), nil
		case TypeString:
			i, err := strconv.ParseInt(v.String, 10, 64)
			if err != nil {
				return parquet.Value{}, fmt.Errorf("parse int value %q: %w", v.String, err)
			}

			return parquet.Int64Value(i).Level(0, 1, columnIndex), nil
		default:
			return parquet.Value{}, fmt.Errorf("unexpected value type %s for int column", v.Type)
		}
	case TypeFloat:
		switch v.Type { //nolint:exhaustive
		case TypeFloat:
			return parquet.DoubleValue(v.Number).Level(0, 1, columnIndex), nil
		case TypeBool:
			if v.Bool {
				return parquet.DoubleValue(1).Level(0, 1, columnIndex), nil
			}

			return parquet.DoubleValue(0).Level(0, 1, columnIndex), nil
		case TypeString:
			f, err := strconv.ParseFloat(v.String, 64)
			if err != nil {
				return parquet.Value{}, fmt.Errorf("parse float value %q: %w", v.String, err)
			}

			return parquet.DoubleValue(f).Level(0, 1, columnIndex), nil
		default:
			return parquet.Value{}, fmt.Errorf("unexpected value type %s for float column", v.Type)
		}
	default:
		return parquet.ByteArrayValue([]byte(v.Format())).Level(0, 1, columnIndex), nil
	}
}

// Close flushes rows and closes file.
func (c *ParquetWriter) Close() error {
	for _, tw := range c.transposed {
		if err := tw.Close(); err != nil {
			return err
		}
	}

	if c.w != nil {
		if err := c.flushBatch(); err != nil {
			return err
		}

		if err := c.w.Close(); err != nil {
			return fmt.Errorf("failed to close parquet writer: %w", err)
		}
	}

	if c.fileWriter == nil {
		return nil
	}

	return c.f.Close()
}
