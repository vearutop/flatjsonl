package flatjsonl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/swaggest/assertjson"
)

func TestExtract_Decoder_URL(t *testing.T) {
	for _, tc := range []struct {
		u string
		j string
	}{
		{
			u: "https://user:pass@example.com:1234/foo/bar/?baz=1&baz=2&quux=abc#piu",
			j: `{
				  "scheme":"https","user":"user","pass":"pass","host":"example.com",
				  "port":"1234","path":["foo","bar"],"query":{"baz":["1","2"],"quux":["abc"]},
				  "fragment":"piu"
				}`,
		},
		{
			u: "/bar?url=https%3A%2F%2Fexample.com%2Ffoo",
			j: `{"path":["bar"],"query":{"url":["https://example.com/foo"]}}`,
		},
	} {
		t.Run(tc.u, func(t *testing.T) {
			uv, err := decodeURL(tc.u)
			require.NoError(t, err)
			assertjson.EqMarshal(t, tc.j, uv)
		})
	}
}
