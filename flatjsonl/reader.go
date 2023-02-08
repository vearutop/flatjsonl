package flatjsonl

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
	gzip "github.com/klauspost/pgzip"
	"github.com/valyala/fastjson"
)

// Input can be either a file name or a reader.
type Input struct {
	FileName string
	Reader   interface {
		io.Reader
		Size() int64
		Reset()
		Compression() string
	}
}

// Reader scans lines and decodes JSON in them.
type Reader struct {
	AddSequence bool
	MaxLines    int64
	OffsetLines int64
	OnError     func(err error)
	Progress    *Progress
	Buf         []byte
	Concurrency int
	Processor   *Processor

	Sequence int64

	MatchPrefix    *regexp.Regexp
	ExtractStrings bool

	singleKeyFlat []byte
	singleKeyPath []string
}

type readSession struct {
	pr      *Progress
	scanner *bufio.Scanner
	fj      *os.File
	r       io.Reader

	setupWalker  func(w *FastWalker)
	lineStarted  func(seq int64) error
	lineFinished func(seq int64) error
}

func (rs *readSession) Close() {
	rs.pr.Stop()

	if rs.fj != nil {
		if err := rs.fj.Close(); err != nil {
			println("failed to close file:", err.Error())
		}
	}

	if c, ok := rs.r.(io.Closer); ok {
		if err := c.Close(); err != nil {
			println("failed to close reader:", err.Error())
		}
	}
}

func (rd *Reader) session(in Input, task string) (sess *readSession, err error) {
	sess = &readSession{}

	sess.pr = rd.Progress
	if sess.pr == nil {
		sess.pr = &Progress{}
	}

	var (
		r   io.Reader
		s   int64
		cmp string
	)

	if in.FileName != "" { //nolint:nestif
		fj, err := os.Open(in.FileName)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %w", in, err)
		}

		defer func() {
			if err != nil && fj != nil {
				if clErr := fj.Close(); clErr != nil {
					err = fmt.Errorf("%w, failed close file (%s)", err, clErr.Error())
				}
			}
		}()

		st, err := fj.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to read file stats %s: %w", in, err)
		}

		r = fj
		s = st.Size()

		switch {
		case strings.HasSuffix(in.FileName, ".gz"):
			cmp = "gzip"
		case strings.HasSuffix(in.FileName, ".zst"):
			cmp = "zst"
		}
	} else {
		r = in.Reader
		in.Reader.Reset()
		s = in.Reader.Size()
		cmp = in.Reader.Compression()
	}

	cr := &CountingReader{Reader: r}

	sess.pr.Start(s, func() int64 {
		return cr.Bytes()
	}, task)

	sess.r = cr

	switch cmp {
	case "gzip":
		if sess.r, err = gzip.NewReader(sess.r); err != nil {
			return nil, fmt.Errorf("failed to init gzip reader: %w", err)
		}
	case "zst":
		if sess.r, err = zstd.NewReader(sess.r); err != nil {
			return nil, fmt.Errorf("failed to init gzip reader: %w", err)
		}
	}

	sess.scanner = bufio.NewScanner(sess.r)

	if len(rd.Buf) != 0 {
		sess.scanner.Buffer(rd.Buf, len(rd.Buf))
	}

	return sess, nil
}

type syncWorker struct {
	i        int
	p        *fastjson.Parser
	path     []string
	flatPath []byte
	walker   *FastWalker
	line     []byte
}

// Read reads single file with JSON lines.
func (rd *Reader) Read(sess *readSession) error {
	concurrency := rd.Concurrency
	if concurrency == 0 {
		concurrency = 2 * runtime.NumCPU()
	}

	semaphore := make(chan *syncWorker, concurrency)
	for i := 0; i < cap(semaphore); i++ {
		w := &FastWalker{}
		sess.setupWalker(w)
		w.ExtractStrings = rd.ExtractStrings

		semaphore <- &syncWorker{
			i:        i,
			p:        &fastjson.Parser{},
			path:     make([]string, 0, 20),
			flatPath: make([]byte, 0, 5000),
			line:     make([]byte, 0, 100),
			walker:   w,
		}
	}

	stop := int64(0)

	var (
		mu        sync.Mutex
		doLineErr error
	)

	if len(rd.Processor.includeKeys) == 1 {
		for _, i := range rd.Processor.includeKeys {
			kk := rd.Processor.keys[i]
			rd.singleKeyPath = kk.path
			rd.singleKeyFlat = []byte("." + strings.Join(kk.path, "."))
		}
	}

	for sess.scanner.Scan() {
		if err := sess.scanner.Err(); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		line := sess.scanner.Bytes()
		n := sess.pr.CountLine()

		if rd.OffsetLines > 0 && n <= rd.OffsetLines {
			continue
		}

		seq := atomic.AddInt64(&rd.Sequence, 1)

		worker := <-semaphore
		worker.line = append(worker.line[:0], line...)

		go func() {
			defer func() {
				semaphore <- worker
			}()

			if err := rd.doLine(worker, seq, n, sess); err != nil {
				atomic.AddInt64(&stop, 1)

				mu.Lock()
				doLineErr = err
				mu.Unlock()
			}
		}()

		if atomic.LoadInt64(&stop) != 0 {
			break
		}

		if rd.MaxLines > 0 && rd.MaxLines+rd.OffsetLines <= n {
			break
		}
	}

	// Wait for goroutines to finish.
	for i := 0; i < cap(semaphore); i++ {
		<-semaphore
	}

	if doLineErr != nil {
		return doLineErr
	}

	return sess.scanner.Err()
}

func (rd *Reader) doLine(w *syncWorker, seq, n int64, sess *readSession) error {
	defer func() {
		if r := recover(); r != nil {
			println(string(w.line))
			println(r)
		}
	}()

	if sess.lineStarted != nil {
		if err := sess.lineStarted(seq); err != nil {
			return fmt.Errorf("failure in line started callback, line %d: %w", n, err)
		}
	}

	if rd.AddSequence {
		seqf := float64(seq)
		w.walker.FnNumber(seq, []byte("._sequence"), []string{"_sequence"}, seqf, []byte(Format(seqf)))
	}

	line := w.line
	if len(line) < 2 || line[0] != '{' {
		if line = rd.prefixedLine(seq, line, w.walker.FnString); line == nil {
			return nil
		}
	}

	p := w.p
	flatPath := w.flatPath[:0]

	var path []string

	if w.walker.WantPath {
		path = w.path[:0]
	}

	pv, err := p.ParseBytes(line)
	if err != nil {
		if rd.OnError != nil {
			rd.OnError(fmt.Errorf("skipping malformed JSON line %s: %w", string(line), err))
		}
	} else {
		if rd.singleKeyPath != nil {
			w.walker.GetKey(seq, rd.singleKeyFlat, rd.singleKeyPath, pv)
		} else {
			w.walker.WalkFastJSON(seq, flatPath, path, pv)
		}
	}

	if sess.lineFinished != nil {
		if err := sess.lineFinished(seq); err != nil {
			return fmt.Errorf("failure in line finished callback, line %d: %w", n, err)
		}
	}

	return nil
}

func (rd *Reader) prefixedLine(seq int64, line []byte, walkFn func(seq int64, flatPath []byte, path []string, value []byte)) []byte {
	pos := bytes.Index(line, []byte("{"))

	if pos == -1 {
		// If prefix matching is enabled, it may be ok to not have any JSON in line.
		// All data would be parsed only from prefix (which may also describe whole line).
		if rd.MatchPrefix == nil {
			if rd.OnError != nil {
				rd.OnError(fmt.Errorf("could not find JSON in line %s", string(line)))
			}

			return nil
		}
	}

	if rd.MatchPrefix != nil {
		pref := line

		if pos > 0 {
			pref = line[:pos]
		}

		sm := rd.MatchPrefix.FindAllSubmatch(pref, -1)

		for _, m := range sm {
			for j := 1; j < len(m); j++ {
				walkFn(seq, []byte("._prefix.["+strconv.Itoa(j-1)+"]"), []string{"_prefix", "[" + strconv.Itoa(j-1) + "]"}, m[j])
			}
		}
	}

	if pos == -1 {
		return []byte(`{}`)
	}

	line = line[pos:]

	return line
}

// LoopReader repeats bytes buffer until the limit is hit.
type LoopReader struct {
	BytesLimit int
	bytesRead  int
	src        *bytes.Reader
}

// LoopReaderFromFile creates LoopReader from a file.
func LoopReaderFromFile(fn string, bytesLimit int) (*LoopReader, error) {
	f, err := os.ReadFile(fn)
	if err != nil {
		return nil, err
	}

	return &LoopReader{
		BytesLimit: bytesLimit,
		src:        bytes.NewReader(f),
	}, nil
}

// Compression implements Input.
func (l *LoopReader) Compression() string {
	return ""
}

// Size implements Input.
func (l *LoopReader) Size() int64 {
	return int64(l.BytesLimit)
}

// Reset resets the counter.
func (l *LoopReader) Reset() {
	l.bytesRead = 0
}

// Read implements io.Reader.
func (l *LoopReader) Read(p []byte) (n int, err error) {
	if l.bytesRead >= l.BytesLimit {
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
