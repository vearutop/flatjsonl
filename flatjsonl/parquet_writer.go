package flatjsonl

import (
	"fmt"
	"reflect"
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

	w *parquet.Writer

	rowType reflect.Type
	setters []parquetFieldSetter

	transposed map[string]*ParquetWriter

	*fileWriter
	b *baseWriter
}

type parquetFieldSetter func(field reflect.Value, value Value) error

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

	c.b = &baseWriter{p: p}

	return c, nil
}

// SetupKeys initializes writer.
func (c *ParquetWriter) SetupKeys(keys []flKey) (err error) {
	c.b.setupKeys(keys)

	if err := c.setupParquetWriter(); err != nil {
		return err
	}

	c.transposed = map[string]*ParquetWriter{}

	for dst, tw := range c.b.transposed {
		fn := c.b.transposedFileName(c.fn, dst)
		tw.extName = fn

		if c.fn == NopFile {
			fn = c.fn
		}

		ctw, err := NewParquetWriter(fn, c.compression, c.p)
		if err != nil {
			return fmt.Errorf("failed to init transposed Parquet writer for %s: %w", dst, err)
		}

		ctw.b = tw
		ctw.b.p = c.p
		c.transposed[dst] = ctw

		if err := ctw.setupParquetWriter(); err != nil {
			return fmt.Errorf("failed to setup transposed Parquet writer for %s: %w", dst, err)
		}
	}

	return nil
}

func (c *ParquetWriter) setupParquetWriter() error {
	fields := make([]reflect.StructField, len(c.b.filteredKeys))
	c.setters = make([]parquetFieldSetter, len(c.b.filteredKeys))

	for i, k := range c.b.filteredKeys {
		ft, setter := parquetFieldForType(k.t)

		fields[i] = reflect.StructField{
			Name: "Field" + strconv.Itoa(i),
			Type: ft,
			Tag:  reflect.StructTag(`parquet:` + strconv.Quote(k.replaced+",optional")),
		}
		c.setters[i] = setter
	}

	c.rowType = reflect.StructOf(fields)

	schema := parquet.SchemaOf(reflect.New(c.rowType).Interface())
	codec, err := parquetCompressionCodec(c.compression)

	if err != nil {
		return err
	}

	c.w = parquet.NewWriter(c.uncompressed, &parquet.WriterConfig{
		Schema:      schema,
		Compression: codec,
	})

	return nil
}

func parquetFieldForType(t Type) (reflect.Type, parquetFieldSetter) {
	switch t { //nolint:exhaustive
	case TypeBool:
		return reflect.TypeOf((*bool)(nil)), func(field reflect.Value, value Value) error {
			v := value.Bool
			field.Set(reflect.ValueOf(&v))

			return nil
		}
	case TypeInt:
		return reflect.TypeOf((*int64)(nil)), func(field reflect.Value, value Value) error {
			var v int64

			switch value.Type { //nolint:exhaustive
			case TypeFloat:
				v = int64(value.Number)
			case TypeBool:
				if value.Bool {
					v = 1
				}
			case TypeString:
				var err error

				v, err = strconv.ParseInt(value.String, 10, 64)
				if err != nil {
					return fmt.Errorf("parse int value %q: %w", value.String, err)
				}
			default:
				return fmt.Errorf("unexpected value type %s for int column", value.Type)
			}

			field.Set(reflect.ValueOf(&v))

			return nil
		}
	case TypeFloat:
		return reflect.TypeOf((*float64)(nil)), func(field reflect.Value, value Value) error {
			var v float64

			switch value.Type { //nolint:exhaustive
			case TypeFloat:
				v = value.Number
			case TypeBool:
				if value.Bool {
					v = 1
				}
			case TypeString:
				var err error

				v, err = strconv.ParseFloat(value.String, 64)
				if err != nil {
					return fmt.Errorf("parse float value %q: %w", value.String, err)
				}
			default:
				return fmt.Errorf("unexpected value type %s for float column", value.Type)
			}

			field.Set(reflect.ValueOf(&v))

			return nil
		}
	default:
		return reflect.TypeOf((*string)(nil)), func(field reflect.Value, value Value) error {
			v := value.Format()
			field.Set(reflect.ValueOf(&v))

			return nil
		}
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
func (c *ParquetWriter) ReceiveRow(seq int64, values []Value) error {
	if c.b.isTransposed {
		transposedRows := c.b.receiveTransposedRowValues(seq, values)

		for _, row := range transposedRows {
			if err := c.writeRow(row); err != nil {
				return fmt.Errorf("writing transposed Parquet row: %w", err)
			}
		}

		return nil
	}

	if err := c.writeRow(c.b.receiveRowValues(values)); err != nil {
		return fmt.Errorf("writing Parquet row: %w", err)
	}

	for dst, tw := range c.transposed {
		if err := tw.ReceiveRow(seq, values); err != nil {
			return fmt.Errorf("transposed rows for %s: %w", dst, err)
		}
	}

	return nil
}

func (c *ParquetWriter) writeRow(values []Value) error {
	row := reflect.New(c.rowType).Elem()

	for i, v := range values {
		if v.Type == TypeAbsent || v.Type == TypeNull {
			continue
		}

		if err := c.setters[i](row.Field(i), v); err != nil {
			return fmt.Errorf("column %s: %w", c.b.filteredKeys[i].replaced, err)
		}
	}

	return c.w.Write(row.Addr().Interface())
}

// Close flushes rows and closes file.
func (c *ParquetWriter) Close() error {
	for _, tw := range c.transposed {
		if err := tw.Close(); err != nil {
			return err
		}
	}

	if c.w != nil {
		if err := c.w.Close(); err != nil {
			return fmt.Errorf("failed to close parquet writer: %w", err)
		}
	}

	if c.fileWriter == nil {
		return nil
	}

	return c.f.Close()
}
