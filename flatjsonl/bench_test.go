package flatjsonl_test

import (
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/flatjsonl/flatjsonl"
)

func BenchmarkNewProcessor(b *testing.B) {
	for name, f := range map[string]flatjsonl.Flags{
		"test": {
			AddSequence:     true,
			Input:           "testdata/test.log",
			CSV:             "<nop>",
			MatchLinePrefix: `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`,
			ReplaceKeys:     true,
			SkipZeroCols:    true,
			Config:          "testdata/config.json",
		},
		"test_get_key": {
			Input:  "testdata/large.json",
			GetKey: ".topics.draft_key",
			Raw:    "<nop>",
		},
		"transpose": {
			AddSequence: true,
			Input:       "testdata/transpose.jsonl",
			CSV:         "<nop>",
			Config:      "testdata/transpose_cfg.json",
		},
		"coalesce": {
			AddSequence: true,
			Input:       "testdata/coalesce.log",
			CSV:         "<nop>",
			Config:      "testdata/coalesce_cfg.json",
		},
	} {
		f.PrepareOutput()

		lr, err := flatjsonl.LoopReaderFromFile(f.Input, b.N)
		require.NoError(b, err)

		var cfg flatjsonl.Config

		if f.Config != "" {
			cj, err := os.ReadFile(f.Config)
			require.NoError(b, err)

			require.NoError(b, json.Unmarshal(cj, &cfg))
		}

		proc := flatjsonl.NewProcessor(f, cfg, flatjsonl.Input{Reader: lr})
		proc.Log = func(args ...any) {}

		b.Run(name+"_scanKeys", func(b *testing.B) {
			b.ReportAllocs()
			lr.BytesLimit = b.N
			require.NoError(b, proc.PrepareKeys())
		})

		b.Run(name+"_writeOutput", func(b *testing.B) {
			b.ReportAllocs()
			lr.BytesLimit = b.N
			require.NoError(b, proc.WriteOutput())
		})
	}
}

func Test_loopReader(t *testing.T) {
	lr, err := flatjsonl.LoopReaderFromFile("testdata/test.log", 10000)
	require.NoError(t, err)

	b, err := io.ReadAll(lr)
	require.NoError(t, err)

	println(string(b))
}

func Test_loopReader_scan(t *testing.T) {
	lr, err := flatjsonl.LoopReaderFromFile("testdata/test.log", 10000)
	require.NoError(t, err)

	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/test.log"
	f.CSV = "<nop>"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.PrepareOutput()

	cj, err := os.ReadFile("testdata/config.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc := flatjsonl.NewProcessor(f, cfg, flatjsonl.Input{Reader: lr})

	require.NoError(t, proc.Process())
}
