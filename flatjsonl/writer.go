package flatjsonl

import (
	"errors"
	"strings"
)

// WriteReceiver can receive a row for processing.
type WriteReceiver interface {
	ReceiveRow(keys []string, values []interface{}) error
	Close() error
}

// Writer dispatches rows to multiple receivers.
type Writer struct {
	receivers []WriteReceiver
}

// ReceiveRow passes row to all receivers.
func (w *Writer) ReceiveRow(keys []string, values []interface{}) error {
	var errs []string

	for _, r := range w.receivers {
		if err := r.ReceiveRow(keys, values); err != nil {
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
