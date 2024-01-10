package flatjsonl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/swaggest/assertjson"
)

func TestExtract_Decoder_URL(t *testing.T) {
	u := "https://user:pass@example.com:1234/foo/bar/?baz=1&baz=2&quux=abc#piu"

	uv, err := decodeURL(u)
	require.NoError(t, err)
	assertjson.EqMarshal(t, `{
	  "scheme":"https","user":"user","pass":"pass","host":"example.com",
	  "port":"1234","path":["foo","bar"],"query":{"baz":["1","2"],"quux":["abc"]},
	  "fragment":"piu"
	}`, uv)
}
