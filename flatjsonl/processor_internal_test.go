package flatjsonl

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bool64/progress"
	"github.com/stretchr/testify/require"
)

type blockingReceiver struct {
	calls   chan int64
	blocked sync.Map
	unblock chan struct{}
}

type testInputReader struct {
	data []byte
	*bytes.Reader
}

func newTestInputReader(data string) *testInputReader {
	b := []byte(data)

	return &testInputReader{
		data:   b,
		Reader: bytes.NewReader(b),
	}
}

func (t *testInputReader) Size() int64 {
	return int64(len(t.data))
}

func (t *testInputReader) Reset() {
	_, err := t.Seek(0, 0)
	if err != nil {
		panic(err)
	}
}

func (t *testInputReader) Compression() string {
	return ""
}

func (b *blockingReceiver) SetupKeys(_ []flKey) error {
	return nil
}

func (b *blockingReceiver) ReceiveRow(seq int64, _ []Value) error {
	b.calls <- seq

	if seq == 1 {
		b.blocked.Store(seq, true)
		<-b.unblock
	}

	return nil
}

func (b *blockingReceiver) Close() error {
	return nil
}

func TestWriteIterator_lineFinishedBackpressuresWhenThrottled(t *testing.T) {
	r := &blockingReceiver{
		calls:   make(chan int64, 4),
		unblock: make(chan struct{}),
	}

	p := &Processor{
		pr: &progress.Progress{},
		w: &Writer{
			receivers: []WriteReceiver{r},
		},
	}

	atomic.StoreInt64(&p.throttle, 1)

	wi := newWriteIterator(p, nil, nil, nil)

	require.NoError(t, wi.lineStarted(1))

	done1 := make(chan error, 1)
	go func() {
		done1 <- wi.lineFinished(1)
	}()

	select {
	case seq := <-r.calls:
		require.Equal(t, int64(1), seq)
	case <-time.After(time.Second):
		t.Fatal("first row did not reach writer")
	}

	require.NoError(t, wi.lineStarted(2))

	done2 := make(chan error, 1)
	go func() {
		done2 <- wi.lineFinished(2)
	}()

	require.NoError(t, wi.lineStarted(3))

	done3 := make(chan error, 1)
	go func() {
		done3 <- wi.lineFinished(3)
	}()

	time.Sleep(50 * time.Millisecond)

	finishedCount := 0

	wi.finished.Range(func(_, _ any) bool {
		finishedCount++

		return true
	})

	require.Zero(t, finishedCount)
	require.Equal(t, int64(1), atomic.LoadInt64(&wi.seqExpected))
	require.Len(t, r.calls, 0)

	close(r.unblock)

	require.NoError(t, <-done1)
	require.NoError(t, <-done2)
	require.NoError(t, <-done3)

	require.Equal(t, int64(4), atomic.LoadInt64(&wi.seqExpected))
	require.Equal(t, int64(0), atomic.LoadInt64(&wi.inProgress))
}

func TestReaderRead_stallsWhenAllWorkersWaitBehindBlockedExpectedWrite(t *testing.T) {
	r := &blockingReceiver{
		calls:   make(chan int64, 8),
		unblock: make(chan struct{}),
	}

	p := &Processor{
		pr: &progress.Progress{},
		w: &Writer{
			receivers: []WriteReceiver{r},
		},
	}

	rd := &Reader{
		Concurrency: 3,
		Processor:   p,
		Progress:    &progress.Progress{},
		Buf:         make([]byte, 0, 1024),
	}

	wi := newWriteIterator(p, nil, nil, nil)

	sess, err := rd.session(Input{
		Reader: newTestInputReader("{}\n{}\n{}\n{}\n{}\n{}\n"),
	}, "test")
	require.NoError(t, err)
	defer sess.Close()

	sess.setupWalker = wi.setupWalker
	sess.lineStarted = wi.lineStarted
	sess.lineFinished = wi.lineFinished

	stopThrottle := make(chan struct{})
	defer close(stopThrottle)

	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopThrottle:
				return
			case <-ticker.C:
				atomic.StoreInt64(&p.throttle, 1)
			}
		}
	}()

	readDone := make(chan error, 1)
	go func() {
		readDone <- rd.Read(sess)
	}()

	select {
	case seq := <-r.calls:
		require.Equal(t, int64(1), seq)
	case <-time.After(time.Second):
		t.Fatal("first row did not reach writer")
	}

	select {
	case err := <-readDone:
		require.NoError(t, err)
		t.Fatal("read unexpectedly finished while first write was blocked")
	case <-time.After(150 * time.Millisecond):
	}

	require.Equal(t, int64(1), atomic.LoadInt64(&wi.seqExpected))

	close(r.unblock)

	require.NoError(t, <-readDone)
	require.Equal(t, int64(7), atomic.LoadInt64(&wi.seqExpected))
}
