package flatjsonl

import (
	"errors"
	"fmt"
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
