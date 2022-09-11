package flatjsonl_test

import (
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vearutop/flatjsonl/flatjsonl"
)

func TestNewProcessor(t *testing.T) {
	f := flatjsonl.Flags{}
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

	proc := flatjsonl.NewProcessor(f, cfg, f.Inputs())

	assert.NoError(t, proc.Process())

	b, err := os.ReadFile("_testdata/test.csv")
	require.NoError(t, err)

	assert.Equal(t, `sequence,host,timestamp,name,wins_0_0,wins_0_1,wins_1_0,wins_1_1,f00_bar VARCHAR(255),f00_qux_baz VARCHAR(255)
1,host-13,2022-06-24 14:13:36,Gilbert,straight,7♣,one pair,10♥,1,abc
2,host-14,2022-06-24 14:13:37,"""'Alexa'""",two pair,4♠,two pair,9♠,,
3,host-13,2022-06-24 14:13:38,May,,,,,,
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

	proc := flatjsonl.NewProcessor(f, cfg, f.Inputs())

	assert.NoError(t, proc.Process())
}

func TestNewProcessor_transpose(t *testing.T) {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "_testdata/transpose.jsonl"
	f.Output = "_testdata/transpose.sqlite,_testdata/transpose.csv"
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

	proc := flatjsonl.NewProcessor(f, cfg, f.Inputs())

	assert.NoError(t, proc.Process())

	assertFileEquals(t, "_testdata/transpose_deep_arr.csv",
		`.sequence,.index,.abaz.a,.abaz.b,.afoo.a,.afoo.b,.abar.a,.abar.b
1,0,5,6,15,12,,
3,0,,,,,1,2
`)
	//.sequence,.index,.abaz.a,.abaz.b,.afoo.a,.afoo.b,.abar.a,.abar.b
	//1,0,5,6,15,12,,
	//3,0,,,,,1,2
	assertFileEquals(t, "_testdata/transpose_flat_map.csv",
		`.sequence,.index,value
1,ccc,123
1,ddd,456
2,rrr,aaa
2,fff,334
`)
	assertFileEquals(t, "_testdata/transpose_tags.csv",
		`.sequence,.index,value
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
	assertFileEquals(t, "_testdata/transpose_tokens.csv",
		`.sequence,.index,.a,.b
1,foo,1,2
2,bar,3,4
3,foo,15,12
3,baz,5,6
`)
}

func assertFileEquals(t *testing.T, fn string, contents string) {
	t.Helper()

	b, err := os.ReadFile(fn)
	require.NoError(t, err)

	assert.Equal(t, contents, string(b))
}
