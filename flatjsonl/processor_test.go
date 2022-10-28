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
	f.Concurrency = 1
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

	assert.Equal(t, `sequence,host,timestamp,name,wins_0_0,wins_0_1,wins_1_0,wins_1_1,f00_bar VARCHAR(255),f00_qux_baz VARCHAR(255),nested_literal,foo,bar
1,host-13,2022-06-24 14:13:36,Gilbert,straight,7♣,one pair,10♥,1,abc,,,
2,host-14,2022-06-24 14:13:37,"""'Alexa'""",two pair,4♠,two pair,9♠,,,,,
3,host-13,2022-06-24 14:13:38,May,,,,,,,"{""foo"":1, ""bar"": 2}",1,2
`, string(b))
}

func TestNewProcessor_concurrency(t *testing.T) {
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

	proc := flatjsonl.NewProcessor(f, flatjsonl.Config{}, f.Inputs())

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
