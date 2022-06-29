package flatjsonl_test

import (
	"io/ioutil"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	f.PrepareOutput()

	cfg := flatjsonl.Config{
		ReplaceKeys: map[string]string{
			"._prefix.[0]": "host",
			"._prefix.[1]": "timestamp",
		},
	}

	proc := flatjsonl.NewProcessor(f, cfg, f.Inputs())

	assert.NoError(t, proc.Process())

	b, err := ioutil.ReadFile("_testdata/test.csv")
	assert.NoError(t, err)

	assert.Equal(t, `_sequence,host,timestamp,name,wins_0_0,wins_0_1,wins_1_0,wins_1_1
1,host-13,2022/06/24 14:13:36.393275,Gilbert,straight,7♣,one pair,10♥
2,host-14,2022/06/24 14:13:37.393275,Alexa,two pair,4♠,two pair,9♠
3,host-13,2022/06/24 14:13:38.393275,May,,,,
`, string(b))
}
