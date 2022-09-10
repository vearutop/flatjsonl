package flatjsonl

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/valyala/fastjson"
	"golang.org/x/sync/errgroup"
)

// Reader scans lines and decodes JSON in them.
type Reader struct {
	AddSequence bool
	MaxLines    int64
	OffsetLines int64
	OnError     func(err error)
	Progress    *Progress
	Buf         []byte
	Concurrency int

	Sequence int64

	MatchPrefix *regexp.Regexp
}

type readSession struct {
	pr      *Progress
	scanner *bufio.Scanner
	fj      *os.File
	r       io.Reader

	setupWalker  func(w *FastWalker)
	lineStarted  func(seq, n int64) error
	lineFinished func(seq, n int64) error
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

func (rd *Reader) session(fn string, task string) (sess *readSession, err error) {
	sess = &readSession{}

	sess.pr = rd.Progress
	if sess.pr == nil {
		sess.pr = &Progress{}
	}

	fj, err := os.Open(fn)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fn, err)
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
		return nil, fmt.Errorf("failed to read file stats %s: %w", fn, err)
	}

	cr := &CountingReader{Reader: fj}

	sess.pr.Start(st.Size(), cr, task)

	sess.r = cr

	if strings.HasSuffix(fn, ".gz") {
		if sess.r, err = gzip.NewReader(sess.r); err != nil {
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

		semaphore <- &syncWorker{
			i:        i,
			p:        &fastjson.Parser{},
			path:     make([]string, 0, 20),
			flatPath: make([]byte, 0, 500),
			line:     make([]byte, 0, 100),
			walker:   w,
		}
	}

	stop := int64(0)
	g := new(errgroup.Group)

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

		func() {
			worker := <-semaphore
			worker.line = append(worker.line[:0], line...)

			g.Go(func() error {
				defer func() {
					semaphore <- worker
				}()

				if err := rd.doLine(worker, seq, n, sess); err != nil {
					atomic.AddInt64(&stop, 1)
					return err
				}

				return nil
			})
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

	return g.Wait()
}

func (rd *Reader) doLine(w *syncWorker, seq, n int64, sess *readSession) error {
	if sess.lineStarted != nil {
		if err := sess.lineStarted(seq, n); err != nil {
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
	path := w.path[:0]
	flatPath := w.flatPath[:0]
	flatPath = append(flatPath, '.')

	pv, err := p.ParseBytes(line)
	if err != nil {
		if rd.OnError != nil {
			rd.OnError(fmt.Errorf("skipping malformed JSON line %s: %w", string(line), err))
		}
	} else {
		w.walker.WalkFastJSON(seq, flatPath, path, pv)
	}

	if sess.lineFinished != nil {
		if err := sess.lineFinished(seq, n); err != nil {
			return fmt.Errorf("failure in line finished callback, line %d: %w", n, err)
		}
	}

	return nil
}

func (rd *Reader) prefixedLine(seq int64, line []byte, walkFn func(seq int64, flatPath []byte, path []string, value []byte)) []byte {
	pos := bytes.Index(line, []byte("{"))

	if pos == -1 {
		if rd.OnError != nil {
			rd.OnError(fmt.Errorf("could not find JSON in line %s", string(line)))
		}

		return nil
	}

	if pos > 0 && rd.MatchPrefix != nil {
		pref := line[:pos]
		sm := rd.MatchPrefix.FindSubmatch(pref)

		if len(sm) > 1 {
			for i, m := range sm[1:] {
				si := strconv.Itoa(i)
				walkFn(seq, []byte("._prefix.["+si+"]"), []string{"_prefix", "[" + si + "]"}, m)
			}
		}
	}

	line = line[pos:]

	return line
}
