package flatjsonl

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
)

// DuckDBCLIWriter writes data to DuckDB DB with CLI CSV import.
type DuckDBCLIWriter struct {
	mainCSV *CSVWriter
	cmd     *exec.Cmd
	w       io.WriteCloser
}

// NewDuckDBCLIWriter creates DuckDB CLI writer.
func NewDuckDBCLIWriter(fn string, tableName string, nullValue string) (*DuckDBCLIWriter, error) {
	dw := &DuckDBCLIWriter{}

	c := &CSVWriter{
		fn:        NopFile,
		nullValue: nullValue,
	}

	r, w := io.Pipe()
	dw.w = w

	c.w = csv.NewWriter(w)
	c.b = &baseWriter{}

	dw.mainCSV = c

	cliPath, err := exec.LookPath("duckdb")
	if err != nil {
		return nil, errors.New("duckdb CLI is not available in PATH")
	}

	query := duckDBReadCSVQuery(tableName, c.nullValue)

	dw.cmd = exec.Command(cliPath, fn, "-c", query)
	dw.cmd.Stdin = r
	dw.cmd.Stdout = os.Stdout
	dw.cmd.Stderr = os.Stderr

	if err := dw.cmd.Start(); err != nil {
		return nil, err
	}

	return dw, nil
}

// SetupKeys inits writer with list of known keys.
func (w *DuckDBCLIWriter) SetupKeys(keys []flKey) error {
	return w.mainCSV.SetupKeys(keys)
}

// ReceiveRow collects data values.
func (w *DuckDBCLIWriter) ReceiveRow(seq int64, values []Value) error {
	return w.mainCSV.ReceiveRow(seq, values)
}

// Close flushes all the remainders and closes resources.
func (w *DuckDBCLIWriter) Close() error {
	if err := w.mainCSV.Close(); err != nil {
		return err
	}

	if err := w.w.Close(); err != nil {
		return err
	}

	if err := w.cmd.Wait(); err != nil {
		return err
	}

	return nil
}

func quoteDuckDBIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteDuckDBString(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

func duckDBReadCSVQuery(tableName string, nullValue string) string {
	query := "CREATE TABLE " + quoteDuckDBIdent(tableName) + //nolint: unqueryvet
		" AS SELECT * FROM read_csv('/dev/stdin', header=true, auto_detect=true"

	if nullValue != "" {
		query += ", nullstr=" + quoteDuckDBString(nullValue)
	}

	query += ")"

	return query
}
