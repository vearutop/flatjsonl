package flatjsonl

import (
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"time"
)

// ProgressStatus describes current progress.
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
	task     string
	current  func() int64
	prnt     func(s ProgressStatus)
	start    time.Time
	tot      float64
}

// DefaultStatus renders ProgressStatus as a string.
func DefaultStatus(s ProgressStatus) string {
	if s.Task != "" {
		s.Task += ": "
	}

	ms := runtime.MemStats{}
	runtime.ReadMemStats(&ms)

	heapMB := ms.HeapInuse / (1024 * 1024)

	return fmt.Sprintf(s.Task+"%.1f%% bytes read, %d lines processed, %.1f l/s, %.1f MB/s, elapsed %s, remaining %s, heap %d MB",
		s.DonePercent, s.LinesCompleted, s.SpeedLPS, s.SpeedMBPS,
		s.Elapsed.Round(10*time.Millisecond).String(), s.Remaining.String(), heapMB)
}

// Start spawns background progress reporter.
func (p *Progress) Start(total int64, current func() int64, task string) {
	p.done = make(chan bool)
	atomic.StoreInt64(&p.lines, 0)
	p.task = task
	p.current = current

	interval := p.Interval
	if interval == 0 {
		interval = time.Second
	}

	p.prnt = p.Print
	if p.prnt == nil {
		p.prnt = func(s ProgressStatus) {
			println(DefaultStatus(s))
		}
	}

	p.start = time.Now()
	p.tot = float64(total)
	done := p.done
	t := time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-t.C:
				p.printStatus(false)

			case <-done:
				t.Stop()

				return
			}
		}
	}()
}

func (p *Progress) printStatus(last bool) {
	s := ProgressStatus{}
	s.Task = p.task
	s.LinesCompleted = atomic.LoadInt64(&p.lines)

	b := float64(p.current())
	s.DonePercent = 100 * b / p.tot
	s.Elapsed = time.Since(p.start)
	s.SpeedMBPS = (b / s.Elapsed.Seconds()) / (1024 * 1024)
	s.SpeedLPS = float64(s.LinesCompleted) / s.Elapsed.Seconds()

	s.Remaining = time.Duration(float64(100*s.Elapsed)/s.DonePercent) - s.Elapsed
	s.Remaining = s.Remaining.Truncate(time.Second)

	if s.Remaining > 100*time.Millisecond || last {
		p.prnt(s)
	}
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
	p.printStatus(true)

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
