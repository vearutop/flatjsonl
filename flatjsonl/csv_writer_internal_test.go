package flatjsonl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCSVWriter_receiveRow(t *testing.T) {
	cw := &CSVWriter{nullValue: "\\N"}

	row := cw.renderValues([]Value{
		{Type: TypeString, String: ""},
		{Type: TypeNull},
		{Type: TypeAbsent},
		{Type: TypeString, String: "value"},
		{Type: TypeBool, Bool: true},
		{Type: TypeFloat, RawNumber: "12.34"},
	})

	assert.Equal(t, []string{"", "\\N", "\\N", "value", "true", "12.34"}, row)
}
