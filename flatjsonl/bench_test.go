package flatjsonl_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vearutop/flatjsonl/flatjsonl"
)

func BenchmarkNewProcessor(b *testing.B) {
	for name, f := range map[string]flatjsonl.Flags{
		"test": {
			AddSequence:     true,
			Input:           "_testdata/test.log",
			CSV:             "<nop>",
			MatchLinePrefix: `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`,
			ReplaceKeys:     true,
			SkipZeroCols:    true,
			Config:          "_testdata/config.json",
		},
		"coalesce": {
			AddSequence: true,
			Input:       "_testdata/coalesce.log",
			CSV:         "<nop>",
			Config:      "_testdata/coalesce_cfg.json",
		},
	} {
		f.PrepareOutput()

		lr, err := loopReaderFromFile(f.Input, b.N)
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
			lr.bytesLimit = b.N
			assert.NoError(b, proc.PrepareKeys())
		})

		b.Run(name+"_writeOutput", func(b *testing.B) {
			b.ReportAllocs()
			lr.bytesLimit = b.N
			assert.NoError(b, proc.WriteOutput())
		})
	}
}

type loopReader struct {
	bytesLimit int
	bytesRead  int
	src        *bytes.Reader
}

func loopReaderFromFile(fn string, bytesLimit int) (*loopReader, error) {
	f, err := os.ReadFile(fn)
	if err != nil {
		return nil, err
	}

	return &loopReader{
		bytesLimit: bytesLimit,
		src:        bytes.NewReader(f),
	}, nil
}

func (l *loopReader) IsGzip() bool {
	return false
}

func (l *loopReader) Size() int64 {
	return int64(l.bytesLimit)
}

func (l *loopReader) Reset() {
	l.bytesRead = 0
}

func (l *loopReader) Read(p []byte) (n int, err error) {
	if l.bytesRead >= l.bytesLimit {
		return 0, io.EOF
	}

	n, err = l.src.Read(p)

	if err != nil && errors.Is(err, io.EOF) {
		if _, err := l.src.Seek(0, io.SeekStart); err != nil {
			return 0, fmt.Errorf("seek to start: %w", err)
		}

		return l.Read(p)
	}

	l.bytesRead += n

	return n, err
}

func Test_loopReader(t *testing.T) {
	lr, err := loopReaderFromFile("_testdata/test.log", 10000)
	require.NoError(t, err)

	b, err := io.ReadAll(lr)
	require.NoError(t, err)

	println(string(b))
}

func Test_loopReader_scan(t *testing.T) {
	lr, err := loopReaderFromFile("_testdata/test.log", 10000)
	require.NoError(t, err)

	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "_testdata/test.log"
	f.CSV = "<nop>"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.PrepareOutput()

	cj, err := os.ReadFile("_testdata/config.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config

	require.NoError(t, json.Unmarshal(cj, &cfg))

	proc := flatjsonl.NewProcessor(f, cfg, flatjsonl.Input{Reader: lr})

	assert.NoError(t, proc.Process())
}
