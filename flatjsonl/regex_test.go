package flatjsonl_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vearutop/flatjsonl/flatjsonl"
)

func TestPrepareRegex(t *testing.T) {
	reg := flatjsonl.PrepareRegex(".context.request.Form.*.*")
	assert.Equal(t, `^\.context\.request\.Form\.([^.]+)\.([^.]+)$`, reg)

	j, err := json.Marshal(reg)
	require.NoError(t, err)
	assert.Equal(t, `"^\\.context\\.request\\.Form\\.([^.]+)\\.([^.]+)$"`, string(j))
}
