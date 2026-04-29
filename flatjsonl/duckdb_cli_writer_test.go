package flatjsonl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDuckDBReadCSVQuery(t *testing.T) {
	assert.Equal(t,
		`CREATE TABLE "flatjsonl" AS SELECT * FROM read_csv('/dev/stdin', header=true, auto_detect=true)`,
		duckDBReadCSVQuery("flatjsonl", ""),
	)

	assert.Equal(t,
		`CREATE TABLE "flatjsonl" AS SELECT * FROM read_csv('/dev/stdin', header=true, auto_detect=true, nullstr='\N')`,
		duckDBReadCSVQuery("flatjsonl", `\N`),
	)

	assert.Equal(t,
		`CREATE TABLE "quoted""name" AS SELECT * FROM read_csv('/dev/stdin', header=true, auto_detect=true, nullstr='it''s null')`,
		duckDBReadCSVQuery(`quoted"name`, "it's null"),
	)
}
