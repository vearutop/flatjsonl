package flatjsonl

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"sync/atomic"

	"github.com/valyala/fastjson"
)

// Reader scans lines and decodes JSON in them.
type Reader struct {
	AddSequence bool
	MaxLines    int64
	OnError     func(err error)
	Progress    *Progress
	Buf         []byte

	Sequence int64

	MatchPrefix *regexp.Regexp
}

type readSession struct {
	pr      *Progress
	scanner *bufio.Scanner
	fj      *os.File

	setupWalker  func(w *FastWalker)
	lineStarted  func(seq, n int64) error
	lineFinished func(seq, n int64) error
	async        bool
	onError      func(err error)
}

func (rs *readSession) Close() {
	rs.pr.Stop()

	if rs.fj != nil {
		if err := rs.fj.Close(); err != nil {
			println("failed to close file:", err.Error())
		}
	}
}

func (rd *Reader) session(fn string, task string) (*readSession, error) {
	sess := &readSession{}

	sess.pr = rd.Progress
	if sess.pr == nil {
		sess.pr = &Progress{}
	}

	fj, err := os.Open(fn)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fn, err)
	}

	st, err := fj.Stat()
	if err != nil {
		if clErr := fj.Close(); clErr != nil {
			err = fmt.Errorf("%w, failed close file (%s)", err, clErr.Error())
		}

		return nil, fmt.Errorf("failed to read file stats %s: %w", fn, err)
	}

	cr := &CountingReader{Reader: fj}

	sess.pr.Start(st.Size(), cr, task)

	sess.scanner = bufio.NewScanner(cr)

	if len(rd.Buf) != 0 {
		sess.scanner.Buffer(rd.Buf, len(rd.Buf))
	}

	return sess, nil
}

type syncWorker struct {
	i      int
	p      *fastjson.Parser
	path   []string
	walker *FastWalker
	line   []byte
}

// Read reads single file with JSON lines.
func (rd *Reader) Read(sess *readSession) error {
	semaphore := make(chan *syncWorker, 2*runtime.NumCPU())
	for i := 0; i < cap(semaphore); i++ {
		w := &FastWalker{}
		sess.setupWalker(w)

		semaphore <- &syncWorker{
			i:      i,
			p:      &fastjson.Parser{},
			path:   make([]string, 0, 20),
			line:   make([]byte, 0, 100),
			walker: w,
		}
	}

	stop := int64(0)

	for sess.scanner.Scan() {
		if err := sess.scanner.Err(); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		line := sess.scanner.Bytes()
		n := sess.pr.CountLine()

		seq := atomic.AddInt64(&rd.Sequence, 1)

		func() {
			worker := <-semaphore
			worker.line = append(worker.line[:0], line...)

			go func() {
				defer func() {
					semaphore <- worker
				}()

				if err := rd.doLine(worker, seq, n, sess); err != nil {
					if rd.OnError != nil {
						rd.OnError(err)
					}

					atomic.AddInt64(&stop, 1)
				}
			}()
		}()

		if atomic.LoadInt64(&stop) != 0 {
			break
		}

		if rd.MaxLines > 0 && rd.MaxLines <= n {
			break
		}
	}

	// Wait for goroutines to finish.
	for i := 0; i < cap(semaphore); i++ {
		<-semaphore
	}

	return nil
}

func (rd *Reader) doLine(w *syncWorker, seq, n int64, sess *readSession) error {
	if sess.lineStarted != nil {
		if err := sess.lineStarted(seq, n); err != nil {
			return fmt.Errorf("failure in line started callback, line %d: %w", n, err)
		}
	}

	if rd.AddSequence {
		seqf := float64(seq)
		w.walker.FnNumber(seq, []string{"_sequence"}, seqf, []byte(Format(seqf)))
	}

	line := w.line
	if len(line) < 2 || line[0] != '{' {
		if line = rd.prefixedLine(seq, line, w.walker.FnString); line == nil {
			return nil
		}
	}

	p := w.p
	path := w.path[:0]

	pv, err := p.ParseBytes(line)
	if err != nil {
		if rd.OnError != nil {
			rd.OnError(fmt.Errorf("skipping malformed JSON line %s: %w", string(line), err))
		}

		return nil
	}

	w.walker.WalkFastJSON(seq, path, pv)

	if sess.lineFinished != nil {
		if err := sess.lineFinished(seq, n); err != nil {
			return fmt.Errorf("failure in line finished callback, line %d: %w", n, err)
		}
	}

	return nil
}

func (rd *Reader) prefixedLine(seq int64, line []byte, walkFn func(seq int64, path []string, value []byte)) []byte {
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
				walkFn(seq, []string{"_prefix", "[" + strconv.Itoa(i) + "]"}, m)
			}
		}
	}

	line = line[pos:]

	return line
}
