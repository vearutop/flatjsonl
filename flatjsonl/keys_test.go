package flatjsonl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_toSnakeCase(t *testing.T) {
	assert.Equal(t, "", toSnakeCase(""))
	assert.Equal(t, "foo", toSnakeCase("Foo"))
	assert.Equal(t, "foo_bar", toSnakeCase("FooBar"))
	assert.Equal(t, "foo_bar", toSnakeCase("Foo_Bar"))
	assert.Equal(t, "foo_bar", toSnakeCase("_Foo_Bar"))
	assert.Equal(t, "foo_bar", toSnakeCase("__Foo-Bar"))
}
