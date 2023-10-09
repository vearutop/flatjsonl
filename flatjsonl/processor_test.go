package flatjsonl_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vearutop/flatjsonl/flatjsonl"
	"gopkg.in/yaml.v3"
)

func TestNewProcessor(t *testing.T) {
	f := flatjsonl.Flags{}
	f.ExtractStrings = true
	f.AddSequence = true
	f.Input = "_testdata/test.log"
	f.Output = "_testdata/test.csv,_testdata/test.sqlite"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`
	f.MaxLines = 3
	f.SQLTable = "temp_" + strconv.Itoa(int(time.Now().Unix()))
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.Concurrency = 1
	f.PrepareOutput()

	if err := os.Remove("_testdata/test.sqlite"); err != nil {
		require.Contains(t, err.Error(), "no such file or directory")
	}

	cj, err := os.ReadFile("_testdata/config.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)

	assert.NoError(t, proc.Process())

	b, err := os.ReadFile("_testdata/test.csv")
	require.NoError(t, err)

	assert.Equal(t, `sequence,host,timestamp,name,wins_0_0,wins_0_1,wins_1_0,wins_1_1,f00_bar VARCHAR(255),f00_qux_baz VARCHAR(255),nested_literal,foo,bar
1,host-13,2022-06-24 14:13:36,Gilbert,straight,7♣,one pair,10♥,1,abc,,,
2,host-14,2022-06-24 14:13:37,"""'Alexa'""",two pair,4♠,two pair,9♠,,,,,
3,host-13,2022-06-24 14:13:38,May,,,,,,,"{""foo"":1, ""bar"": 2}",1,2
`, string(b))
}

func TestNewProcessor_concurrency(t *testing.T) {
	f := flatjsonl.Flags{}
	f.ExtractStrings = true
	f.AddSequence = true
	f.Input = "_testdata/test.log"
	f.Output = "_testdata/test.csv,_testdata/test.sqlite"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`
	f.MaxLines = 3
	f.SQLTable = "temp_" + strconv.Itoa(int(time.Now().Unix()))
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.PrepareOutput()

	if err := os.Remove("_testdata/test.sqlite"); err != nil {
		require.Contains(t, err.Error(), "no such file or directory")
	}

	cj, err := os.ReadFile("_testdata/config.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)

	assert.NoError(t, proc.Process())

	b, err := os.ReadFile("_testdata/test.csv")
	require.NoError(t, err)

	assert.Len(t, string(b), len(`sequence,host,timestamp,name,wins_0_0,wins_0_1,wins_1_0,wins_1_1,f00_bar VARCHAR(255),f00_qux_baz VARCHAR(255),nested_literal,foo,bar
1,host-13,2022-06-24 14:13:36,Gilbert,straight,7♣,one pair,10♥,1,abc,,,
2,host-14,2022-06-24 14:13:37,"""'Alexa'""",two pair,4♠,two pair,9♠,,,,,
3,host-13,2022-06-24 14:13:38,May,,,,,,,"{""foo"":1, ""bar"": 2}",1,2
`))
}

func TestNewProcessor_prefixNoJSON(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "_testdata/prefix_no_json.log"
	f.Output = "_testdata/prefix_no_json.csv"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+) (\w+): ([\w\d]+), ([\w\d]+) ([\w\d]+)`
	f.PrepareOutput()

	proc := flatjsonl.NewProcessor(f, flatjsonl.Config{}, f.Inputs()...)

	assert.NoError(t, proc.Process())

	b, err := os.ReadFile("_testdata/prefix_no_json.csv")
	require.NoError(t, err)

	assert.Equal(t, `._sequence,._prefix.[0],._prefix.[1],._prefix.[2],._prefix.[3],._prefix.[4],._prefix.[5]
1,host-13,2022/06/24 14:13:36.393275,fooa,bar1,bazd,qux4
2,host-14,2022/06/24 14:13:37.393275,foob,bar2,bazc,qux3
3,host-13,2022/06/24 14:13:38.393275,fooc,bar3,bazb,qux2
4,host-14,2022/06/24 14:13:39.393275,food,bar4,baza,qux1
`, string(b))
}

func TestNewProcessor_coalesceMultipleCols(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "_testdata/coalesce.log"
	f.Output = "_testdata/coalesce.csv"
	f.PrepareOutput()

	proc := flatjsonl.NewProcessor(f, flatjsonl.Config{
		ReplaceKeys: map[string]string{
			".a": "shared",
			".b": "shared",
			".c": "shared",
		},
	}, f.Inputs()...)

	assert.NoError(t, proc.Process())

	b, err := os.ReadFile("_testdata/coalesce.csv")
	require.NoError(t, err)

	assert.Equal(t, `._sequence,shared,.foo
1,1,true
2,b,false
3,false,true
4,10,true
`, string(b))
}

func TestNewProcessor_concatMultipleCols(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "_testdata/coalesce.log"
	f.Output = "_testdata/coalesce.csv"
	f.PrepareOutput()

	delim := "::"

	proc := flatjsonl.NewProcessor(f, flatjsonl.Config{
		ConcatDelimiter: &delim,
		ReplaceKeys: map[string]string{
			".a": "shared",
			".b": "shared",
			".c": "shared",
		},
	}, f.Inputs()...)

	assert.NoError(t, proc.Process())

	b, err := os.ReadFile("_testdata/coalesce.csv")
	require.NoError(t, err)

	assert.Equal(t, `._sequence,shared,.foo
1,1,true
2,b::123,false
3,false,true
4,10,true
`, string(b))
}

func TestNewProcessor_constVal(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "_testdata/coalesce.log"
	f.Output = "_testdata/constVal.csv"
	f.PrepareOutput()

	proc := flatjsonl.NewProcessor(f, flatjsonl.Config{
		IncludeKeys: []string{
			".a",
			".b",
			".c",
			"const:bar",
			".foo",
		},
		ReplaceKeys: map[string]string{
			".a":        "shared",
			".b":        "shared",
			".c":        "shared",
			"const:bar": "bar_name",
		},
	}, f.Inputs()...)

	assert.NoError(t, proc.Process())

	b, err := os.ReadFile("_testdata/constVal.csv")
	require.NoError(t, err)

	assert.Equal(t, `shared,bar_name,.foo
1,bar,true
b,bar,false
false,bar,true
10,bar,true
`, string(b))
}

func TestNewProcessor_rawWriter(t *testing.T) {
	f := flatjsonl.Flags{}
	f.ExtractStrings = true
	f.AddSequence = true
	f.Input = "_testdata/test.log"
	f.Output = "_testdata/test.raw.gz"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`
	f.MaxLines = 3
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.Concurrency = 1
	f.RawDelim = ":::"
	f.PrepareOutput()

	cj, err := os.ReadFile("_testdata/config.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)

	assert.NoError(t, proc.Process())

	b, err := os.ReadFile("_testdata/test.raw.gz")
	require.NoError(t, err)

	r, err := gzip.NewReader(bytes.NewReader(b))
	require.NoError(t, err)

	b, err = io.ReadAll(r)
	require.NoError(t, err)

	assert.Equal(t, `1:::host-13:::2022-06-24 14:13:36:::Gilbert:::straight:::7♣:::one pair:::10♥:::1:::abc:::::::::
2:::host-14:::2022-06-24 14:13:37:::"'Alexa'":::two pair:::4♠:::two pair:::9♠:::::::::::::::
3:::host-13:::2022-06-24 14:13:38:::May:::::::::::::::::::::{"foo":1, "bar": 2}:::1:::2
`, string(b))
}

func TestNewProcessor_sqlite(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "_testdata/test.log"
	f.Output = "_testdata/test.sqlite"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`
	f.MaxLines = 3
	f.SQLTable = "temp_" + strconv.Itoa(int(time.Now().Unix()))
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.PrepareOutput()

	if err := os.Remove("_testdata/test.sqlite"); err != nil {
		require.Contains(t, err.Error(), "no such file or directory")
	}

	cj, err := os.ReadFile("_testdata/config.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)

	assert.NoError(t, proc.Process())
}

func TestNewProcessor_transpose(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "_testdata/transpose.jsonl"
	f.Output = "_testdata/transpose.csv,_testdata/transpose.raw"
	f.SQLTable = "whatever"
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.Concurrency = 1
	f.RawDelim = ","
	f.ReplaceKeys = true
	f.PrepareOutput()

	cj, err := os.ReadFile("_testdata/transpose_cfg.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)

	assert.NoError(t, proc.Process())

	assertFileEquals(t, "_testdata/transpose.csv",
		`sequence,name
1,a
2,b
3,c
`)
	assertFileEquals(t, "_testdata/transpose.raw",
		`1,a
2,b
3,c
`)

	assertFileEquals(t, "_testdata/transpose_deep_arr.csv",
		`sequence,index,abaz_a,abaz_b,afoo_a,afoo_b,abar_a,abar_b
1,0,5,6,15,12,,
3,0,,,,,1,2
`)
	assertFileEquals(t, "_testdata/transpose_deep_arr.raw",
		`1,0,5,6,15,12,,
3,0,,,,,1,2
`)

	assertFileEquals(t, "_testdata/transpose_flat_map.csv",
		`sequence,index,value
1,ccc,123
1,ddd,456
2,rrr,aaa
2,fff,334
`)
	assertFileEquals(t, "_testdata/transpose_flat_map.raw",
		`1,ccc,123
1,ddd,456
2,rrr,aaa
2,fff,334
`)

	assertFileEquals(t, "_testdata/transpose_tags.csv",
		`sequence,index,value
1,0,t1
1,1,t2
1,2,t3
2,0,t1
2,1,t5
2,2,t6
3,0,t1
3,1,t4
3,2,t5
`)
	assertFileEquals(t, "_testdata/transpose_tags.raw",
		`1,0,t1
1,1,t2
1,2,t3
2,0,t1
2,1,t5
2,2,t6
3,0,t1
3,1,t4
3,2,t5
`)

	assertFileEquals(t, "_testdata/transpose_tokens.csv",
		`sequence,index,a,b
1,foo,1,2
2,bar,3,4
3,foo,15,12
3,baz,5,6
`)
	assertFileEquals(t, "_testdata/transpose_tokens.raw",
		`1,foo,1,2
2,bar,3,4
3,foo,15,12
3,baz,5,6
`)
}

func TestNewProcessor_transpose_sqlite(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "_testdata/transpose.jsonl"
	f.Output = "_testdata/transpose.sqlite"
	f.SQLTable = "whatever"
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.Concurrency = 1
	f.PrepareOutput()

	if err := os.Remove("_testdata/transpose.sqlite"); err != nil {
		require.Contains(t, err.Error(), "no such file or directory")
	}

	cj, err := os.ReadFile("_testdata/transpose_cfg.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)

	assert.NoError(t, proc.Process())
}

func TestNewProcessor_transpose_pg_dump(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "_testdata/transpose.jsonl"
	f.PGDump = "_testdata/transpose.pg.sql"
	f.SQLMaxCols = 500
	f.SQLTable = "whatever"
	f.Concurrency = 1
	f.ReplaceKeys = true
	f.PrepareOutput()

	cj, err := os.ReadFile("_testdata/transpose_cfg.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	cfg.ReplaceKeys = map[string]string{
		".abaz.a": "abaz_a",
	}

	proc := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)

	assert.NoError(t, proc.Process())

	ex, err := os.ReadFile("_testdata/transpose.pg.sql.expected")
	require.NoError(t, err)
	assertFileEquals(t, "_testdata/transpose.pg.sql", string(ex))
}

func assertFileEquals(t *testing.T, fn string, contents string) {
	t.Helper()

	b, err := os.ReadFile(fn)
	require.NoError(t, err)

	assert.Equal(t, contents, string(b), fn)
}

func TestNewProcessor_showKeysInfo(t *testing.T) {
	f := flatjsonl.Flags{}
	f.ShowKeysInfo = true
	f.Config = "_testdata/large_cfg.yaml"
	f.Input = "_testdata/large.json"

	c, err := os.ReadFile(f.Config)
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, yaml.Unmarshal(c, &cfg))

	proc := flatjsonl.NewProcessor(f, cfg, flatjsonl.Input{FileName: f.Input})
	out := bytes.NewBuffer(nil)
	proc.Stdout = out
	assert.NoError(t, proc.Process())

	assertFileEquals(t, "_testdata/large_out.txt", out.String())
}
