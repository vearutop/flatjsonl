package flatjsonl

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// Progress reports reading performance.
type Progress struct {
	Interval time.Duration
	Print    func(donePercent float64, lines int64, spdMBPS float64, remaining time.Duration)
	done     chan bool
	lines    int64
}

// Start spawns background progress reporter.
func (p *Progress) Start(total int64, cr *CountingReader) {
	p.done = make(chan bool)
	p.lines = 0

	interval := p.Interval
	if interval == 0 {
		interval = time.Second
	}

	prnt := p.Print
	if prnt == nil {
		prnt = func(donePercent float64, lines int64, spdMBPS float64, remaining time.Duration) {
			println(fmt.Sprintf("%.1f%% bytes read, %d lines processed, %.1f MB/s, remaining %s", donePercent, lines, spdMBPS, remaining.String()))
		}
	}

	start := time.Now()
	tot := float64(total)
	done := p.done

	go func() {
		for {
			select {
			case <-time.After(interval):
				b := float64(cr.Bytes())
				done := 100 * b / tot
				elapsed := time.Since(start)
				spd := (b / elapsed.Seconds()) / (1024 * 1024)

				remaining := time.Duration(float64(100*elapsed)/done) - elapsed
				remaining = remaining.Truncate(time.Second)

				if remaining > 100*time.Millisecond {
					prnt(done, atomic.LoadInt64(&p.lines), spd, remaining)
				}

			case <-done:
				return
			}
		}
	}()
}

// CountLine increments line counter.
func (p *Progress) CountLine() int64 {
	return atomic.AddInt64(&p.lines, 1)
}

// Lines returns number of counted lines.
func (p *Progress) Lines() int64 {
	return atomic.LoadInt64(&p.lines)
}

// Stop stops progress reporting.
func (p *Progress) Stop() {
	close(p.done)
}

// CountingReader wraps io.Reader to count bytes.
type CountingReader struct {
	Reader io.Reader

	readBytes int64
}

// Read reads and counts bytes.
func (cr *CountingReader) Read(p []byte) (n int, err error) {
	n, err = cr.Reader.Read(p)

	atomic.AddInt64(&cr.readBytes, int64(n))

	return n, err
}

// Bytes returns number of read bytes.
func (cr *CountingReader) Bytes() int64 {
	return atomic.LoadInt64(&cr.readBytes)
}
