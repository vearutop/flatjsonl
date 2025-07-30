package flatjsonl

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"os/exec"
)

// SQLite3CLIWriter writes data to SQLite3 DB with CLI CSV import.
type SQLite3CLIWriter struct {
	mainCSV *CSVWriter
	cmd     *exec.Cmd
	w       io.WriteCloser
}

// NewSQLite3CLIWriter creates SQLite3 CLI writer.
func NewSQLite3CLIWriter(fn string, tableName string) (*SQLite3CLIWriter, error) {
	sw := &SQLite3CLIWriter{}

	c := &CSVWriter{
		fn: NopFile,
	}

	// Pipe provides io.Reader to attach as stdin of sqlite3 process
	// and io.Writer to pass to mainCSV CSVWriter.
	r, w := io.Pipe()

	sw.w = w

	// Main CSVWriter renders mainCSV table into the pipe,
	// and creates separate files for transposed data.
	// Transposed files are to be imported separately after mainCSV completion.
	c.w = csv.NewWriter(w)
	c.b = &baseWriter{}

	sw.mainCSV = c

	// Check if sqlite3 is available in PATH
	cliPath, err := exec.LookPath("sqlite3")
	if err != nil {
		return nil, errors.New("sqlite3 CLI is not available in PATH")
	}

	// cat my_data.csv | sqlite3 -csv my_db.sqlite ".import '|cat -' my_table"
	sw.cmd = exec.Command(cliPath, "-csv", fn, ".import '|cat -' "+tableName)
	sw.cmd.Stdin = r

	sw.cmd.Stdout = os.Stdout
	sw.cmd.Stderr = os.Stderr

	if err := sw.cmd.Start(); err != nil {
		return nil, err
	}

	return sw, nil
}

// SetupKeys inits writer with list of known keys.
func (w *SQLite3CLIWriter) SetupKeys(keys []flKey) error {
	return w.mainCSV.SetupKeys(keys)
}

// ReceiveRow collects data values.
func (w *SQLite3CLIWriter) ReceiveRow(seq int64, values []Value) error {
	return w.mainCSV.ReceiveRow(seq, values)
}

// Close flushes all the remainders and closes resources.
func (w *SQLite3CLIWriter) Close() error {
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
