package flatjsonl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCSVWriter_receiveRow(t *testing.T) {
	cw := &CSVWriter{nullValue: "\\N"}
	cw.b = &baseWriter{
		keys:       []flKey{{replaced: "a"}, {replaced: "b"}, {replaced: "c"}, {replaced: "d"}, {replaced: "e"}, {replaced: "f"}},
		keyIndexes: []int{0, 1, 2, 3, 4, 5},
	}

	cw.receiveRow(1, []Value{
		{Type: TypeString, String: ""},
		{Type: TypeNull},
		{Type: TypeAbsent},
		{Type: TypeString, String: "value"},
		{Type: TypeBool, Bool: true},
		{Type: TypeFloat, RawNumber: "12.34"},
	})

	assert.Equal(t, []string{"", "\\N", "\\N", "value", "true", "12.34"}, cw.b.row)
}
