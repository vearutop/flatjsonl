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
	Metrics        []ProgressMetric
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
	metrics  []ProgressMetric
}

// ProgressType describes metric value.
type ProgressType string

// ProgressType values.
const (
	ProgressBytes    = ProgressType("bytes")
	ProgressDuration = ProgressType("duration")
	ProgressGauge    = ProgressType("gauge")
)

// ProgressMetric is an operation metric.
type ProgressMetric struct {
	Name  string
	Type  ProgressType
	Value *int64
}

// DefaultStatus renders ProgressStatus as a string.
func DefaultStatus(s ProgressStatus) string {
	if s.Task != "" {
		s.Task += ": "
	}

	ms := runtime.MemStats{}
	runtime.ReadMemStats(&ms)

	heapMB := ms.HeapInuse / (1024 * 1024)

	res := fmt.Sprintf(s.Task+"%.1f%% bytes read, %d lines processed, %.1f l/s, %.1f MB/s, elapsed %s, remaining %s, heap %d MB",
		s.DonePercent, s.LinesCompleted, s.SpeedLPS, s.SpeedMBPS,
		s.Elapsed.Round(10*time.Millisecond).String(), s.Remaining.String(), heapMB)

	return res
}

// MetricsStatus renders ProgressStatus metrics as a string.
func MetricsStatus(s ProgressStatus) string {
	metrics := ""

	for _, m := range s.Metrics {
		switch m.Type {
		case ProgressBytes:
			spdMBPS := float64(atomic.LoadInt64(m.Value)) / (s.Elapsed.Seconds() * 1024 * 1024)
			metrics += fmt.Sprintf("%s: %.1f MB/s, ", m.Name, spdMBPS)
		case ProgressDuration:
			metrics += m.Name + ": " + time.Duration(atomic.LoadInt64(m.Value)).String() + ", "
		case ProgressGauge:
			metrics += fmt.Sprintf("%s: %d, ", m.Name, atomic.LoadInt64(m.Value))
		}
	}

	if metrics != "" {
		metrics = metrics[:len(metrics)-2]
	}

	return metrics
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

// AddMetrics adds more metrics to progress status message.
func (p *Progress) AddMetrics(metrics ...ProgressMetric) {
	p.metrics = append(p.metrics, metrics...)
}

func (p *Progress) printStatus(last bool) {
	s := ProgressStatus{}
	s.Task = p.task
	s.LinesCompleted = atomic.LoadInt64(&p.lines)
	s.Metrics = p.metrics

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
	p.metrics = nil

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

// CountingWriter wraps io.Writer to count bytes.
type CountingWriter struct {
	Writer io.Writer

	writtenBytes int64
}

// Write writes and counts bytes.
func (cr *CountingWriter) Write(p []byte) (n int, err error) {
	n, err = cr.Writer.Write(p)

	atomic.AddInt64(&cr.writtenBytes, int64(n))

	return n, err
}

// Bytes returns number of written bytes.
func (cr *CountingWriter) Bytes() int64 {
	return atomic.LoadInt64(&cr.writtenBytes)
}

// MetricsExposer provides metric counters.
type MetricsExposer interface {
	Metrics() []ProgressMetric
}
