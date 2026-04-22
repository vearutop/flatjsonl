package flatjsonl_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vearutop/flatjsonl/flatjsonl"
	"gopkg.in/yaml.v3"
)

func TestNewProcessor(t *testing.T) {
	f := flatjsonl.Flags{}
	f.ExtractStrings = true
	f.AddSequence = true
	f.Input = "testdata/test.log"
	f.Output = "testdata/test.csv,testdata/test.sqlite"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`
	f.MaxLines = 3
	f.SQLTable = "temp_" + strconv.Itoa(int(time.Now().Unix()))
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.ShowJSONSchema = true
	f.Concurrency = 1
	f.PrepareOutput()

	if err := os.Remove("testdata/test.sqlite"); err != nil {
		require.Contains(t, err.Error(), "no such file or directory")
	}

	cj, err := os.ReadFile("testdata/config.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	b, err := os.ReadFile("testdata/test.csv")
	require.NoError(t, err)

	assert.Equal(t, `sequence,host,timestamp,name,wins_0_0,wins_0_1,wins_1_0,wins_1_1,f00_bar VARCHAR(255),f00_qux_baz VARCHAR(255),nested_literal,foo,bar
1,host-13,2022-06-24 14:13:36,Gilbert,straight,7♣,one pair,10♥,1,abc,,,
2,host-14,2022-06-24 14:13:37,"""'Alexa'""",two pair,4♠,two pair,9♠,,,,,
3,host-13,2022-06-24 14:13:38,May,,,,,,,"{""foo"":1, ""bar"": 2}",1,2
`, string(b))
}

func TestNewProcessor_exclude(t *testing.T) {
	f := flatjsonl.Flags{}
	f.ExtractStrings = true
	f.AddSequence = true
	f.Input = "testdata/test.log"
	f.Output = "testdata/test-exclude.csv"
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.Concurrency = 1
	f.PrepareOutput()

	cj, err := os.ReadFile("testdata/config-exclude.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	b, err := os.ReadFile("testdata/test-exclude.csv")
	require.NoError(t, err)

	assert.Equal(t, `sequence,name,wins_0_0,wins_1_0,f00_bar VARCHAR(255),f00_qux_baz VARCHAR(255),nested_literal,foo,bar
1,Gilbert,straight,one pair,1,abc,,,
2,"""'Alexa'""",two pair,two pair,,,,,
3,May,,,,,"{""foo"":1, ""bar"": 2}",1,2
4,Deloise,three of a kind,,,,,,
`, string(b))
}

func TestNewProcessor_excludeRegex(t *testing.T) {
	f := flatjsonl.Flags{}
	f.ExtractStrings = true
	f.AddSequence = true
	f.Input = "testdata/test.log"
	f.Output = "testdata/test-exclude.csv"
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.Concurrency = 1
	f.PrepareOutput()

	cj, err := os.ReadFile("testdata/config-exclude-regex.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	b, err := os.ReadFile("testdata/test-exclude.csv")
	require.NoError(t, err)

	assert.Equal(t, `sequence,name,wins_0_0,wins_1_0,f00_bar VARCHAR(255),f00_qux_baz VARCHAR(255),nested_literal,foo,bar
1,Gilbert,straight,one pair,1,abc,,,
2,"""'Alexa'""",two pair,two pair,,,,,
3,May,,,,,"{""foo"":1, ""bar"": 2}",1,2
4,Deloise,three of a kind,,,,,,
`, string(b))
}

func TestNewProcessor_concurrency(t *testing.T) {
	f := flatjsonl.Flags{}
	f.ExtractStrings = true
	f.AddSequence = true
	f.Input = "testdata/test.log"
	f.Output = "testdata/test.csv,testdata/test.sqlite"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`
	f.MaxLines = 3
	f.SQLTable = "temp_" + strconv.Itoa(int(time.Now().Unix()))
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.PrepareOutput()

	if err := os.Remove("testdata/test.sqlite"); err != nil {
		require.Contains(t, err.Error(), "no such file or directory")
	}

	cj, err := os.ReadFile("testdata/config.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	b, err := os.ReadFile("testdata/test.csv")
	require.NoError(t, err)

	assert.Len(t, string(b), len(`sequence,host,timestamp,name,wins_0_0,wins_0_1,wins_1_0,wins_1_1,f00_bar VARCHAR(255),f00_qux_baz VARCHAR(255),nested_literal,foo,bar
1,host-13,2022-06-24 14:13:36,Gilbert,straight,7♣,one pair,10♥,1,abc,,,
2,host-14,2022-06-24 14:13:37,"""'Alexa'""",two pair,4♠,two pair,9♠,,,,,
3,host-13,2022-06-24 14:13:38,May,,,,,,,"{""foo"":1, ""bar"": 2}",1,2
`))
}

func TestNewProcessor_parquet(t *testing.T) {
	f := flatjsonl.Flags{}
	f.ExtractStrings = true
	f.AddSequence = true
	f.Input = "testdata/test.log"
	f.Parquet = "testdata/test.parquet"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`
	f.MaxLines = 3
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.Concurrency = 1

	cj, err := os.ReadFile("testdata/config.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))
	require.NoError(t, os.RemoveAll(f.Parquet))
	t.Cleanup(func() { require.NoError(t, os.Remove(f.Parquet)) })

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	pf := openParquetFile(t, f.Parquet)
	require.NotEmpty(t, pf.RowGroups())
	require.NotEmpty(t, pf.Metadata().RowGroups)
	require.NotEmpty(t, pf.Metadata().RowGroups[0].Columns)
	assert.Equal(t, format.Snappy, pf.Metadata().RowGroups[0].Columns[0].MetaData.Codec)

	rows := readParquetRows(t, f.Parquet)
	require.Len(t, rows, 3)

	assert.Equal(t, map[string]string{
		"sequence":                 "1",
		"host":                     "host-13",
		"timestamp":                "2022-06-24 14:13:36",
		"name":                     "Gilbert",
		"wins_0_0":                 "straight",
		"wins_0_1":                 "7♣",
		"wins_1_0":                 "one pair",
		"wins_1_1":                 "10♥",
		"f00_bar VARCHAR(255)":     "1",
		"f00_qux_baz VARCHAR(255)": "abc",
	}, rows[0])

	assert.Equal(t, "May", rows[2]["name"])
	assert.Equal(t, "1", rows[2]["foo"])
	assert.Equal(t, "2", rows[2]["bar"])
	assert.Equal(t, `{"foo":1, "bar": 2}`, rows[2]["nested_literal"])
}

func TestNewProcessor_transpose_parquet(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/transpose.jsonl"
	f.Parquet = "testdata/transpose.parquet"
	f.ParquetCompression = "zstd"
	f.ReplaceKeys = true
	f.Concurrency = 1

	cj, err := os.ReadFile("testdata/transpose_cfg.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	for _, fn := range []string{
		f.Parquet,
		"testdata/transpose_tags.parquet",
		"testdata/transpose_tokens.parquet",
		"testdata/transpose_deep_arr.parquet",
		"testdata/transpose_flat_map.parquet",
	} {
		require.NoError(t, os.RemoveAll(fn))
		t.Cleanup(func(name string) func() { return func() { require.NoError(t, os.Remove(name)) } }(fn))
	}

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	pf := openParquetFile(t, f.Parquet)
	assert.Equal(t, format.Zstd, pf.Metadata().RowGroups[0].Columns[0].MetaData.Codec)
	assert.Len(t, readParquetRows(t, f.Parquet), 3)

	tagRows := readParquetRows(t, "testdata/transpose_tags.parquet")
	require.Len(t, tagRows, 9)
	assert.Equal(t, map[string]string{
		"sequence": "1",
		"index":    "0",
		"value":    "t1",
	}, tagRows[0])

	flatMapRows := readParquetRows(t, "testdata/transpose_flat_map.parquet")
	require.NotEmpty(t, flatMapRows)
	assert.Equal(t, "ccc", flatMapRows[0]["index"])
	assert.Equal(t, "123", flatMapRows[0]["value"])
}

func TestNewProcessor_prefixNoJSON(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/prefix_no_json.log"
	f.Output = "testdata/prefix_no_json.csv"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+) (\w+): ([\w\d]+), ([\w\d]+) ([\w\d]+)`
	f.PrepareOutput()

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{}, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	b, err := os.ReadFile("testdata/prefix_no_json.csv")
	require.NoError(t, err)

	assert.Equal(t, `._sequence,._prefix.[0],._prefix.[1],._prefix.[2],._prefix.[3],._prefix.[4],._prefix.[5]
1,host-13,2022/06/24 14:13:36.393275,fooa,bar1,bazd,qux4
2,host-14,2022/06/24 14:13:37.393275,foob,bar2,bazc,qux3
3,host-13,2022/06/24 14:13:38.393275,fooc,bar3,bazb,qux2
4,host-14,2022/06/24 14:13:39.393275,food,bar4,baza,qux1
`, string(b))
}

func openParquetFile(t *testing.T, fn string) *parquet.File {
	t.Helper()

	f, err := os.Open(fn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	fi, err := f.Stat()
	require.NoError(t, err)

	pf, err := parquet.OpenFile(f, fi.Size())
	require.NoError(t, err)

	return pf
}

func readParquetRows(t *testing.T, fn string) []map[string]string {
	t.Helper()

	f, err := os.Open(fn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	r := parquet.NewReader(f)
	defer func() {
		require.NoError(t, r.Close())
	}()

	columns := r.Schema().Columns()
	rows := make([]map[string]string, 0)
	buf := make([]parquet.Row, 1)

	for {
		n, err := r.ReadRows(buf)
		require.LessOrEqual(t, n, 1)

		if n > 0 {
			row := make(map[string]string)

			buf[0].Range(func(columnIndex int, values []parquet.Value) bool {
				if len(values) == 0 {
					return true
				}

				for i := len(values) - 1; i >= 0; i-- {
					if values[i].IsNull() {
						continue
					}

					row[strings.Join(columns[columnIndex], ".")] = parquetValueString(values[i])

					break
				}

				return true
			})

			rows = append(rows, row)
			buf[0] = nil
		}

		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
	}

	return rows
}

func parquetValueString(v parquet.Value) string {
	if v.IsNull() {
		return ""
	}

	switch v.Kind() { //nolint:exhaustive
	case parquet.Boolean:
		return strconv.FormatBool(v.Boolean())
	case parquet.Int32:
		return strconv.FormatInt(int64(v.Int32()), 10)
	case parquet.Int64:
		return strconv.FormatInt(v.Int64(), 10)
	case parquet.Float:
		return strconv.FormatFloat(float64(v.Float()), 'g', -1, 32)
	case parquet.Double:
		return strconv.FormatFloat(v.Double(), 'g', -1, 64)
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return string(v.ByteArray())
	default:
		return v.String()
	}
}

func TestNewProcessor_coalesceMultipleCols(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/coalesce.log"
	f.Output = "testdata/coalesce.csv"
	f.PrepareOutput()

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{
		ReplaceKeys: map[string]string{
			".a": "shared",
			".b": "shared",
			".c": "shared",
		},
	}, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	b, err := os.ReadFile("testdata/coalesce.csv")
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
	f.Input = "testdata/coalesce.log"
	f.Output = "testdata/coalesce.csv"
	f.PrepareOutput()

	delim := "::"

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{
		ConcatDelimiter: &delim,
		ReplaceKeys: map[string]string{
			".a": "shared",
			".b": "shared",
			".c": "shared",
		},
	}, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	b, err := os.ReadFile("testdata/coalesce.csv")
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
	f.Input = "testdata/coalesce.log"
	f.Output = "testdata/constVal.csv"
	f.PrepareOutput()

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{
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
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	b, err := os.ReadFile("testdata/constVal.csv")
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
	f.Input = "testdata/test.log"
	f.Output = "testdata/test.raw.gz"
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

	cj, err := os.ReadFile("testdata/config.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	b, err := os.ReadFile("testdata/test.raw.gz")
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
	f.Input = "testdata/test.log"
	f.Output = "testdata/test.sqlite"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`
	f.MaxLines = 3
	f.SQLTable = "temp_" + strconv.Itoa(int(time.Now().Unix()))
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.PrepareOutput()

	if err := os.Remove("testdata/test.sqlite"); err != nil {
		require.Contains(t, err.Error(), "no such file or directory")
	}

	cj, err := os.ReadFile("testdata/config.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())
}

func TestNewProcessor_transpose(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/transpose.jsonl"
	f.Output = "testdata/transpose.csv,testdata/transpose.raw"
	f.SQLTable = "whatever"
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.Concurrency = 1
	f.RawDelim = ","
	f.ReplaceKeys = true
	f.PrepareOutput()

	cj, err := os.ReadFile("testdata/transpose_cfg.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	assertFileEquals(t, "testdata/transpose.csv",
		`sequence,name
1,a
2,b
3,c
`)
	assertFileEquals(t, "testdata/transpose.raw",
		`1,a
2,b
3,c
`)

	assertFileEquals(t, "testdata/transpose_deep_arr.csv",
		`sequence,index,abaz_a,abaz_b,afoo_a,afoo_b,abar_a,abar_b
1,0,5,6,15,12,,
3,0,,,,,1,2
`)
	assertFileEquals(t, "testdata/transpose_deep_arr.raw",
		`1,0,5,6,15,12,,
3,0,,,,,1,2
`)

	assertFileEquals(t, "testdata/transpose_flat_map.csv",
		`sequence,index,value
1,ccc,123
1,ddd,456
2,rrr,aaa
2,fff,334
`)
	assertFileEquals(t, "testdata/transpose_flat_map.raw",
		`1,ccc,123
1,ddd,456
2,rrr,aaa
2,fff,334
`)

	assertFileEquals(t, "testdata/transpose_tags.csv",
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
	assertFileEquals(t, "testdata/transpose_tags.raw",
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

	assertFileEquals(t, "testdata/transpose_tokens.csv",
		`sequence,index,a,b
1,foo,1,2
2,bar,3,4
3,foo,15,12
3,baz,5,6
`)
	assertFileEquals(t, "testdata/transpose_tokens.raw",
		`1,foo,1,2
2,bar,3,4
3,foo,15,12
3,baz,5,6
`)
}

func TestNewProcessor_transpose_sqlite(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/transpose.jsonl"
	f.Output = "testdata/transpose.sqlite"
	f.SQLTable = "whatever"
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.Concurrency = 1
	f.PrepareOutput()

	if err := os.Remove("testdata/transpose.sqlite"); err != nil {
		require.Contains(t, err.Error(), "no such file or directory")
	}

	cj, err := os.ReadFile("testdata/transpose_cfg.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())
}

func TestNewProcessor_transpose_pg_dump(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/transpose.jsonl"
	f.PGDump = "testdata/transpose.pg.sql"
	f.SQLMaxCols = 500
	f.SQLTable = "whatever"
	f.Concurrency = 1
	f.ReplaceKeys = true
	f.PrepareOutput()

	cj, err := os.ReadFile("testdata/transpose_cfg.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	cfg.ReplaceKeys = map[string]string{
		".abaz.a": "abaz_a",
	}

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	ex, err := os.ReadFile("testdata/transpose.pg.sql.expected")
	require.NoError(t, err)
	assertFileEquals(t, "testdata/transpose.pg.sql", string(ex))
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
	f.Config = "testdata/large_cfg.yaml"
	f.Input = "testdata/large.json"

	c, err := os.ReadFile(f.Config)
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, yaml.Unmarshal(c, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, flatjsonl.Input{FileName: f.Input})
	require.NoError(t, err)

	out := bytes.NewBuffer(nil)
	proc.Stdout = out
	require.NoError(t, proc.Process())

	assertFileEquals(t, "testdata/large_out.txt", out.String())
}

func TestNewProcessor_extract(t *testing.T) {
	type Foo struct {
		Link   string `json:"link"`
		Nested string `json:"nested"`
	}

	type Bar struct {
		Foo Foo `json:"foo"`
	}

	f := flatjsonl.Flags{}
	f.Config = "testdata/extract_strings.json5"
	f.Input = "testdata/extract_strings.jsonl"
	f.CSV = "testdata/extract_strings_cfg.csv"
	f.ShowKeysInfo = true
	require.NoError(t, f.LoadNetrieDB("testdata/asns.bin"))
	require.NoError(t, f.LoadGeoIPDB("testdata/GeoIP2-City-Test.mmdb"))
	require.NoError(t, f.LoadGeoIPDB("testdata/GeoLite2-ASN-Test.mmdb"))

	proc, err := flatjsonl.New(f)
	require.NoError(t, err)

	out := bytes.NewBuffer(nil)
	proc.Stdout = out
	require.NoError(t, proc.Process())

	assertFileEquals(t, f.CSV, `.ip,.ip.NETIP.asns,.ip.GEOIP.city.names.en,.ip.GEOIP.autonomous_system_organization,.foo.link.URL.scheme,.foo.link.URL.user,.foo.link.URL.pass,.foo.link.URL.host,.foo.link.URL.port,request_query_baz_0,request_query_baz_1,request_query_i_0,request_query_quux_0,.foo.link.URL.path.[0],.foo.link.URL.path.[1],.foo.link.URL.fragment,nested_quux
81.2.69.145,,London,,https,user,pass,example.com,1234,1,2,0,abc,foo,bar,piu,123
71.96.0.3,"AS701 MCI Communications Services, Inc. d/b/a Verizon Business",,"MCI Communications Services, Inc. d/b/a Verizon Business",https,user,pass,example.com,1234,1,2,1,abc,foo,bar,piu,124
,,,,https,user,pass,example.com,1234,1,2,2,abc,foo,bar,piu,125
2001:480:10::1,,San Diego,,https,user,pass,example.com,1234,1,2,3,abc,foo,bar,piu,126
1.0.0.4,AS15169 Google Inc.,,Google Inc.,https,user,pass,example.com,1234,1,2,4,abc,foo,bar,piu,127
`)

	fmt.Println(out.String())
}

func TestNewProcessor_extractStrings(t *testing.T) {
	f := flatjsonl.Flags{}
	f.Input = "testdata/extract_strings.jsonl"
	f.CSV = "testdata/extract_strings.csv"
	f.ExtractStrings = true
	f.ShowKeysInfo = true

	proc, err := flatjsonl.New(f)
	require.NoError(t, err)

	out := bytes.NewBuffer(nil)
	proc.Stdout = out
	require.NoError(t, proc.Process())

	assertFileEquals(t, f.CSV, `.foo.link,.foo.link.URL.scheme,.foo.link.URL.user,.foo.link.URL.pass,.foo.link.URL.host,.foo.link.URL.port,.foo.link.URL.query.baz.[0],.foo.link.URL.query.baz.[1],.foo.link.URL.query.i.[0],.foo.link.URL.query.quux.[0],.foo.link.URL.path.[0],.foo.link.URL.path.[1],.foo.link.URL.fragment,.foo.nested,.foo.nested.JSON.quux,.ip
https://user:pass@example.com:1234/foo/bar/?baz=1&baz=2&quux=abc&i=0#piu,https,user,pass,example.com,1234,1,2,0,abc,foo,bar,piu,"{""quux"":123}",123,81.2.69.145
https://user:pass@example.com:1234/foo/bar/?baz=1&baz=2&quux=abc&i=1#piu,https,user,pass,example.com,1234,1,2,1,abc,foo,bar,piu,"{""quux"":124}",124,71.96.0.3
https://user:pass@example.com:1234/foo/bar/?baz=1&baz=2&quux=abc&i=2#piu,https,user,pass,example.com,1234,1,2,2,abc,foo,bar,piu,"{""quux"":125}",125,
https://user:pass@example.com:1234/foo/bar/?baz=1&baz=2&quux=abc&i=3#piu,https,user,pass,example.com,1234,1,2,3,abc,foo,bar,piu,"{""quux"":126}",126,2001:480:10::1
https://user:pass@example.com:1234/foo/bar/?baz=1&baz=2&quux=abc&i=4#piu,https,user,pass,example.com,1234,1,2,4,abc,foo,bar,piu,"{""quux"":127}",127,1.0.0.4
`)

	assert.Equal(t, `keys info:
1: .foo.link, TYPE string
2: .foo.link.URL.scheme, TYPE string
3: .foo.link.URL.user, TYPE string
4: .foo.link.URL.pass, TYPE string
5: .foo.link.URL.host, TYPE string
6: .foo.link.URL.port, TYPE string
7: .foo.link.URL.query.baz.[0], TYPE string
8: .foo.link.URL.query.baz.[1], TYPE string
9: .foo.link.URL.query.i.[0], TYPE string
10: .foo.link.URL.query.quux.[0], TYPE string
11: .foo.link.URL.path.[0], TYPE string
12: .foo.link.URL.path.[1], TYPE string
13: .foo.link.URL.fragment, TYPE string
14: .foo.nested, TYPE string
15: .foo.nested.JSON.quux, TYPE int
16: .ip, TYPE string
`, out.String(), out.String())
}

func TestProcessor_Process(t *testing.T) {
	f := flatjsonl.Flags{}
	f.ShowKeysInfo = true
	f.Config = "testdata/keys-with-spaces-cfg.json"
	f.Input = "testdata/keys-with-spaces.jsonl"
	f.CSV = "testdata/keys-with-spaces.csv"
	f.Concurrency = 1

	c, err := os.ReadFile(f.Config)
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, yaml.Unmarshal(c, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, flatjsonl.Input{FileName: f.Input})
	require.NoError(t, err)

	out := bytes.NewBuffer(nil)
	proc.Stdout = out
	require.NoError(t, proc.Process())

	assertFileEquals(t, f.CSV, `foo_bar_baz,.quux,foo_ba_r_baz
123,true,
456,,
789,,
,,789
012,,
`)
}

func TestNewProcessor_transpose_keep_json(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/transpose.jsonl"
	f.Output = "testdata/transpose_json.csv"
	f.SQLTable = "whatever"
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.Concurrency = 1
	f.RawDelim = ","
	f.ReplaceKeys = true
	f.PrepareOutput()

	cj, err := os.ReadFile("testdata/transpose_keep_json_cfg.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	assertFileEquals(t, f.Output, `sequence,name,tags,deep_arr,tokens,flat_map
1,a,"[""t1"",""t2"",""t3""]","[{""abaz"":{""a"":5,""b"":6},""afoo"":{""a"":15,""b"":12}}]","{""foo"":{""a"":1,""b"":2}}","{""ccc"":123,""ddd"":456}"
2,b,"[""t1"",""t5"",""t6""]",,"{""bar"":{""a"":3,""b"":4}}","{""rrr"":""aaa"",""fff"":334}"
3,c,"[""t1"",""t4"",""t5""]","[{""abar"":{""a"":1,""b"":2}}]","{""baz"":{""a"":5,""b"":6},""foo"":{""a"":15,""b"":12}}",
`)
}

func TestNewProcessor_transpose_keep_json_regex(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/transpose2.jsonl"
	f.Output = "testdata/transpose2_json.csv"
	f.SQLTable = "whatever"
	f.ShowKeysFlat = true
	f.ShowKeysHier = true
	f.ShowKeysInfo = true
	f.Concurrency = 1
	f.RawDelim = ","
	f.ReplaceKeys = true
	f.PrepareOutput()

	cj, err := os.ReadFile("testdata/transpose_keep_json_regex_cfg.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	require.NoError(t, proc.Process())

	assertFileEquals(t, f.Output, `sequence,name,tags,deep_arr,tokens,flat_map
1,a,"[""t1"",""t2"",""t3""]","[{""abaz"":{""a"":5,""b"":6},""afoo"":{""a"":15,""b"":12}}]","{""foo"":{""a"":1,""b"":2}}","{""ccc"":123,""ddd"":456}"
2,b,"[""t1"",""t5"",""t6""]",,"{""bar"":{""a"":3,""b"":4}}","{""rrr"":""aaa"",""fff"":334}"
3,c,"[""t1"",""t4"",""t5""]","[{""abar"":{""a"":1,""b"":2}}]","{""baz"":{""a"":5,""b"":6},""foo"":{""a"":15,""b"":12}}",
`)
}

func TestNewProcessor_highCardinality(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/high_cardinality.jsonl.zst"
	f.SQLite = "testdata/high_cardinality.sqlite"
	f.SQLTable = "stats"
	f.CSV = "testdata/high_cardinality.csv"
	f.ShowKeysFlat = true
	f.ShowKeysInfo = true
	f.ShowJSONSchema = true
	f.Concurrency = 1
	f.ChildrenLimitObject = 30
	f.PrepareOutput()

	var cfg flatjsonl.Config

	if err := os.Remove("testdata/high_cardinality.sqlite"); err != nil {
		require.Contains(t, err.Error(), "no such file or directory")
	}

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	out := bytes.NewBuffer(nil)
	proc.Stdout = out

	require.NoError(t, proc.Process())

	println(out.String())

	assert.Equal(t, `keys:
"._sequence",
".id",
".Deeper.params",
".Deeper.struct_map",
".numbers",
".structs",
keys info:
1: ._sequence, TYPE int
2: .id, TYPE int
3: .Deeper.params, TYPE json
4: .Deeper.struct_map, TYPE json
5: .numbers, TYPE json
6: .structs, TYPE json
{
 "title":"Root",
 "properties":{
  "Deeper":{"properties":{"params":{"type":["object"]},"struct_map":{"type":["object"]}}},
  "_sequence":{"type":["integer"]},"id":{"type":["integer"]},"numbers":{"type":["array","null"]},
  "structs":{"type":["array","null"]}
 }
}
`, out.String(), out.String())
}

func TestNewProcessor_scalar_vs_array(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/float_vs_array.jsonl"
	f.CSV = "testdata/float_vs_array.csv"
	f.ShowKeysFlat = true
	f.ShowKeysInfo = true
	f.ShowJSONSchema = true
	f.Concurrency = 1
	f.ChildrenLimitObject = 30
	f.PrepareOutput()

	var cfg flatjsonl.Config

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	out := bytes.NewBuffer(nil)
	proc.Stdout = out

	require.NoError(t, proc.Process())

	println(out.String())

	schemaStart := strings.Index(out.String(), "{\n")
	require.NotEqual(t, -1, schemaStart)

	assert.Equal(t, `keys:
"._sequence",
".a",
".b",
".a.[0]",
".a.[1]",
".a.[2]",
".a.a1",
".a.a2",
".a.a3",
keys info:
1: ._sequence, TYPE int
2: .a, TYPE float
3: .b, TYPE string
4: .a.[0], TYPE int
5: .a.[1], TYPE int
6: .a.[2], TYPE int
7: .a.a1, TYPE int
8: .a.a2, TYPE int
9: .a.a3, TYPE int
`, out.String()[:schemaStart], out.String())

	var schema struct {
		Properties map[string]struct {
			Type []string `json:"type"`
		} `json:"properties"`
	}

	require.NoError(t, json.Unmarshal([]byte(out.String()[schemaStart:]), &schema))
	assert.Equal(t, []string{"integer"}, schema.Properties["_sequence"].Type)
	assert.ElementsMatch(t, []string{"integer", "number", "array", "object"}, schema.Properties["a"].Type)
	assert.ElementsMatch(t, []string{"integer", "string", "boolean"}, schema.Properties["b"].Type)
}
