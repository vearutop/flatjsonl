package flatjsonl

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

type ProgressStatus struct {
	Task           string
	DonePercent    float64
	LinesCompleted int64
	SpeedMBPS      float64
	SpeedLPS       float64
	Elapsed        time.Duration
	Remaining      time.Duration
}

// Progress reports reading performance.
type Progress struct {
	Interval time.Duration
	Print    func(status ProgressStatus)
	done     chan bool
	lines    int64
}

// Start spawns background progress reporter.
func (p *Progress) Start(total int64, cr *CountingReader, task string) {
	p.done = make(chan bool)
	p.lines = 0

	interval := p.Interval
	if interval == 0 {
		interval = time.Second
	}

	prnt := p.Print
	if prnt == nil {
		prnt = func(s ProgressStatus) {
			if s.Task != "" {
				s.Task = s.Task + ": "
			}

			println(fmt.Sprintf(s.Task+"%.1f%% bytes read, %d lines processed, %.1f l/s, %.1f MB/s, remaining %s",
				s.DonePercent, s.LinesCompleted, s.SpeedLPS, s.SpeedMBPS, s.Remaining.String()))
		}
	}

	start := time.Now()
	tot := float64(total)
	done := p.done

	go func() {
		for {
			select {
			case <-time.After(interval):
				s := ProgressStatus{}
				s.Task = task
				s.LinesCompleted = atomic.LoadInt64(&p.lines)

				b := float64(cr.Bytes())
				s.DonePercent = 100 * b / tot
				s.Elapsed = time.Since(start)
				s.SpeedMBPS = (b / s.Elapsed.Seconds()) / (1024 * 1024)
				s.SpeedLPS = float64(s.LinesCompleted) / s.Elapsed.Seconds()

				s.Remaining = time.Duration(float64(100*s.Elapsed)/s.DonePercent) - s.Elapsed
				s.Remaining = s.Remaining.Truncate(time.Second)

				if s.Remaining > 100*time.Millisecond {
					prnt(s)
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
