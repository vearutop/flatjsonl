package flatjsonl

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"sync/atomic"

	"github.com/valyala/fastjson"
)

// Reader scans lines and decodes JSON in them.
type Reader struct {
	AddSequence bool
	MaxLines    int64
	OnDecodeErr func(err error)
	Progress    *Progress
	Buf         []byte

	Sequence int64

	MatchPrefix *regexp.Regexp
}

type readSession struct {
	pr      *Progress
	scanner *bufio.Scanner
	fj      *os.File
}

func (rs *readSession) Close() {
	rs.pr.Stop()

	if rs.fj != nil {
		if err := rs.fj.Close(); err != nil {
			println("failed to close file:", err.Error())
		}
	}
}

func (rd *Reader) session(fn string) (*readSession, error) {
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

	sess.pr.Start(st.Size(), cr)

	sess.scanner = bufio.NewScanner(cr)

	if len(rd.Buf) != 0 {
		sess.scanner.Buffer(rd.Buf, len(rd.Buf))
	}

	return sess, nil
}

// Read reads single file with JSON lines.
func (rd *Reader) Read(fn string, walkFn func(path []string, value interface{}), lineDone func(n int64) error) error { // nolint:cyclop
	sess, err := rd.session(fn)
	if err != nil {
		return err
	}

	defer sess.Close()

	var p fastjson.Parser

	for sess.scanner.Scan() {
		if err := sess.scanner.Err(); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		line := sess.scanner.Bytes()
		n := sess.pr.CountLine()

		if rd.AddSequence {
			walkFn([]string{"_sequence"}, float64(atomic.AddInt64(&rd.Sequence, 1)))
		}

		if len(line) < 2 || line[0] != '{' {
			if line = rd.prefixedLine(line, walkFn); line == nil {
				continue
			}
		}

		pv, err := p.ParseBytes(line)
		if err != nil {
			if rd.OnDecodeErr != nil {
				rd.OnDecodeErr(fmt.Errorf("skipping malformed JSON line %s: %w", string(line), err))
			}

			continue
		}

		WalkFastJSON(nil, pv, walkFn)

		if lineDone != nil {
			if err := lineDone(n); err != nil {
				return fmt.Errorf("failure in line done callback, line %d: %w", n, err)
			}
		}

		if rd.MaxLines > 0 && rd.MaxLines <= n {
			break
		}
	}

	return nil
}

func (rd *Reader) prefixedLine(line []byte, walkFn func(path []string, value interface{})) []byte {
	pos := bytes.Index(line, []byte("{"))

	if pos == -1 {
		if rd.OnDecodeErr != nil {
			rd.OnDecodeErr(fmt.Errorf("could not find JSON in line %s", string(line)))
		}

		return nil
	}

	if pos > 0 && rd.MatchPrefix != nil {
		pref := line[:pos]
		sm := rd.MatchPrefix.FindSubmatch(pref)

		if len(sm) > 1 {
			for i, m := range sm[1:] {
				walkFn([]string{"_prefix", "[" + strconv.Itoa(i) + "]"}, string(m))
			}
		}
	}

	line = line[pos:]

	return line
}
