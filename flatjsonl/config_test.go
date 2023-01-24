package flatjsonl_test

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"regexp"
	"testing"
)

func TestRegex(t *testing.T) {
	s := ".context.request.Form.1674172237565.[0]"

	var rs string
	err := json.Unmarshal([]byte(`"^\\.context\\.request\\.Form\\.([^\\d][^.]+)\\.([^.]+)$"`), &rs)
	require.NoError(t, err)

	r, err := regexp.Compile(rs)
	require.NoError(t, err)

	assert.False(t, r.MatchString(s))
}
