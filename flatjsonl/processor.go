package flatjsonl

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	_ "time/tzdata" // Loading timezones.

	xsync "github.com/puzpuzpuz/xsync/v2"
	"github.com/swaggest/assertjson"
)

// Processor reads JSONL files with Reader and passes flat rows to Writer.
type Processor struct {
	Log func(args ...any)

	cfg    Config
	f      Flags
	inputs []Input

	pr *Progress
	w  *Writer
	rd *Reader

	includeKeys  map[string]int
	includeRegex []*regexp.Regexp
	replaceRegex map[*regexp.Regexp]string
	constVals    map[int]string

	replaceKeys  map[string]string
	replaceByKey map[string]string

	// keys are ordered replaced column names, indexes match values of includeKeys.
	keys []flKey

	flKeys *xsync.MapOf[uint64, flKey]

	mu            sync.Mutex
	flKeysList    []string
	keyHierarchy  KeyHierarchy
	canonicalKeys map[string]flKey

	totalLines int
}

// NewProcessor creates an instance of Processor.
func NewProcessor(f Flags, cfg Config, inputs ...Input) *Processor { //nolint: funlen // Yeah, that's what she said.
	pr := &Progress{
		Interval: f.ProgressInterval,
	}

	if f.GetKey != "" {
		cfg.IncludeKeys = append(cfg.IncludeKeys, f.GetKey)
	}

	p := &Processor{
		Log: func(args ...any) {
			_, _ = fmt.Fprintln(os.Stderr, args...)
		},

		cfg:    cfg,
		f:      f,
		inputs: inputs,

		pr: pr,
		w: &Writer{
			Progress: pr,
		},
		rd: &Reader{
			Concurrency:    f.Concurrency,
			AddSequence:    f.AddSequence,
			Progress:       pr,
			Buf:            make([]byte, f.BufSize),
			ExtractStrings: f.ExtractStrings,
		},
		includeKeys:   map[string]int{},
		constVals:     map[int]string{},
		canonicalKeys: map[string]flKey{},

		flKeysList:   make([]string, 0),
		keyHierarchy: KeyHierarchy{Name: "."},

		flKeys: xsync.NewIntegerMapOf[uint64, flKey](),
	}

	p.rd.Processor = p

	switch f.Verbosity {
	case 0:
		pr.Print = func(status ProgressStatus) {}
	case 1:
		pr.Print = func(status ProgressStatus) {
			p.Log(DefaultStatus(status))
		}
	case 2:
		pr.Print = func(status ProgressStatus) {
			s := DefaultStatus(status)
			m := MetricsStatus(status)

			if m != "" {
				s += "\n" + m
			}

			p.Log(s)
		}
	}

	if f.MatchLinePrefix != "" {
		p.rd.MatchPrefix = regexp.MustCompile(f.MatchLinePrefix)
	}

	p.replaceRegex = map[*regexp.Regexp]string{}
	starRepl := strings.NewReplacer(
		".", "\\.",
		"[", "\\[",
		"]", "\\]",
		"{", "\\{",
		"}", "\\}",
		"*", "([^\\d][^.]+)",
	)

	for _, reg := range p.cfg.IncludeKeysRegex {
		if strings.Contains(reg, "*") {
			reg = "^" + starRepl.Replace(reg) + "$"
		}

		r, err := regexp.Compile(reg)
		if err != nil {
			p.Log(fmt.Sprintf("failed to parse regular expression %s: %s", reg, err.Error()))
		}

		p.includeRegex = append(p.includeRegex, r)
	}

	for reg, rep := range p.cfg.ReplaceKeysRegex {
		if strings.Contains(reg, "*") {
			reg = "^" + starRepl.Replace(reg) + "$"
		}

		r, err := regexp.Compile(reg)
		if err != nil {
			p.Log(fmt.Sprintf("failed to parse regular expression %s: %s", reg, err.Error()))
		}

		p.replaceRegex[r] = rep
	}

	return p
}

// Process dispatches data from Reader to Writer.
func (p *Processor) Process() error {
	if err := p.PrepareKeys(); err != nil {
		return err
	}

	if err := p.WriteOutput(); err != nil {
		return err
	}

	return p.maybeShowKeys()
}

// PrepareKeys runs first pass of reading if necessary to scan the keys.
func (p *Processor) PrepareKeys() error {
	if len(p.includeRegex) == 0 && len(p.cfg.IncludeKeys) != 0 {
		p.iterateIncludeKeys()
	} else {
		// Scan available keys.
		if err := p.scanAvailableKeys(); err != nil {
			return err
		}

		p.Log("lines:", p.pr.Lines(), ", keys:", len(p.includeKeys))
		p.totalLines = int(p.pr.Lines())
	}

	p.prepareKeys()

	return nil
}

// WriteOutput runs second pass of reading to create the output.
func (p *Processor) WriteOutput() error {
	defer func() {
		if err := p.w.Close(); err != nil {
			log.Fatalf("failed to close writer: %v", err)
		}
	}()

	if err := p.setupWriters(); err != nil {
		return err
	}

	if p.w.HasReceivers() {
		if err := p.iterateForWriters(); err != nil {
			return err
		}

		p.Log("lines:", p.pr.Lines(), ", keys:", len(p.includeKeys))
	}

	return nil
}

func (p *Processor) maybeShowKeys() error {
	if p.f.ShowKeysFlat {
		fmt.Println("keys:")

		for _, k := range p.flKeysList {
			fmt.Println(`"` + k + `",`)
		}
	}

	if p.f.ShowKeysInfo {
		fmt.Println("keys info:")

		for i, k := range p.keys {
			line := k.replaced + ", TYPE " + string(k.t)

			if k.replaced != k.original {
				line = k.original + " REPLACED WITH " + line
			}

			if k.transposeDst != "" {
				line += ", TRANSPOSED TO " + k.transposeDst
			}

			fmt.Println(strconv.Itoa(i)+":", line)
		}
	}

	if p.f.ShowKeysHier {
		b, err := assertjson.MarshalIndentCompact(p.keyHierarchy.Hierarchy().(map[string]interface{})["."], "", " ", 120)
		if err != nil {
			return err
		}

		fmt.Println(string(b))
	}

	return nil
}

func (p *Processor) setupWriters() error {
	if p.f.CSV != "" {
		cw, err := NewCSVWriter(p.f.CSV)
		if err != nil {
			return fmt.Errorf("failed to create CSV file: %w", err)
		}

		cw.b = &baseWriter{}
		cw.b.p = p
		p.w.Add(cw)
	}

	if p.f.SQLite != "" {
		cw, err := NewSQLiteWriter(p.f.SQLite, p.f.SQLTable, p)
		if err != nil {
			return fmt.Errorf("failed to open SQLite file: %w", err)
		}

		p.w.Add(cw)
	}

	if p.f.PGDump != "" {
		w, err := NewPGDumpWriter(p.f.PGDump, p.f.SQLTable, p)
		if err != nil {
			return fmt.Errorf("failed to open PostgreSQL dump file: %w", err)
		}

		p.w.Add(w)
	}

	if p.f.Raw != "" {
		rw, err := NewRawWriter(p.f.Raw, p.f.RawDelim)
		if err != nil {
			return fmt.Errorf("failed")
		}

		rw.b = &baseWriter{}
		rw.b.p = p

		p.w.Add(rw)
	}

	return nil
}

// ck coverts original key to canonical.
func (p *Processor) ck(k string) string {
	if p.f.CaseSensitiveKeys {
		return k
	}

	return strings.ToLower(k)
}

func (p *Processor) iterateForWriters() error {
	p.Log("flattening data...")

	p.rd.MaxLines = 0
	atomic.StoreInt64(&p.rd.Sequence, 0)

	if p.f.MaxLines > 0 {
		p.rd.MaxLines = int64(p.f.MaxLines)
	}

	includeKeys := make(map[string]int, len(p.includeKeys))
	for k, i := range p.includeKeys {
		includeKeys[p.ck(k)] = i
	}

	pkIndex := make(map[uint64]int)
	pkDst := make(map[uint64]string)
	pkTimeFmt := make(map[uint64]string)

	p.flKeys.Range(func(key uint64, value flKey) bool {
		if i, ok := includeKeys[value.canonical]; ok {
			pkIndex[key] = i
			if value.transposeDst != "" {
				pkDst[key] = value.transposeDst
			}
		}

		if f, ok := p.cfg.ParseTime[value.original]; ok {
			pkTimeFmt[key] = f
		}

		return true
	})

	wi := newWriteIterator(p, pkIndex, pkDst, pkTimeFmt)

	if err := p.w.SetupKeys(p.keys); err != nil {
		return err
	}

	for _, input := range p.inputs {
		sess, err := p.rd.session(input, "flattening data")
		if err != nil {
			return err
		}

		if sess == nil {
			continue
		}

		sess.lineStarted = wi.lineStarted
		sess.setupWalker = wi.setupWalker
		sess.lineFinished = wi.lineFinished

		err = p.rd.Read(sess)
		if err != nil {
			sess.Close()

			return fmt.Errorf("failed to process file %s: %w", input, err)
		}

		sess.Close()
	}

	return wi.waitPending()
}

type lineBuf struct {
	h      *hasher
	values []Value
}

func newWriteIterator(p *Processor, pkIndex map[uint64]int, pkDst map[uint64]string, pkTimeFmt map[uint64]string) *writeIterator {
	wi := writeIterator{}
	wi.pending = xsync.NewIntegerMapOf[int64, *lineBuf]()
	wi.finished = &sync.Map{}

	if len(p.includeKeys) == 1 {
		for _, i := range p.includeKeys {
			kk := p.keys[i]
			wi.singleKeyHash = newHasher().hashBytes([]byte("." + strings.Join(kk.path, ".")))
		}
	}

	wi.lineBufPool = sync.Pool{
		New: func() interface{} {
			return &lineBuf{
				h:      newHasher(),
				values: make([]Value, len(p.keys)),
			}
		},
	}
	wi.seqExpected = 1
	wi.pkIndex = pkIndex
	wi.pkDst = pkDst
	wi.pkTimeFmt = pkTimeFmt
	wi.p = p
	wi.fieldLimit = p.f.FieldLimit
	wi.outTimeFmt = p.cfg.OutputTimeFormat

	if wi.outTimeFmt == "" {
		wi.outTimeFmt = time.RFC3339
	}

	if p.cfg.OutputTimezone != "" {
		tz, err := time.LoadLocation(p.cfg.OutputTimezone)
		if err == nil {
			wi.outputTZ = tz
		} else {
			p.Log("failed to load timezone:", err.Error())
		}
	}

	p.pr.AddMetrics(
		ProgressMetric{
			Name:  "rows in progress",
			Type:  ProgressGauge,
			Value: &wi.inProgress,
		},
	)

	return &wi
}

type writeIterator struct {
	// Read-only under concurrency.
	pkIndex    map[uint64]int
	pkDst      map[uint64]string
	pkTimeFmt  map[uint64]string
	p          *Processor
	fieldLimit int
	outTimeFmt string
	outputTZ   *time.Location

	// Read-write under concurrency.
	lineBufPool sync.Pool
	seqExpected int64
	pending     *xsync.MapOf[int64, *lineBuf]

	// Finished was originally *xsync.MapOf[int64, *lineBuf], but for unclear, hardly reproducible reason
	// it was leading to eventually failing "waiting pending".
	// Seems Store with eventual LoadAndDelete can have eviction-style inconsistency under heavy concurrent load.
	// E.g. LoadAndDelete would not find the entry that was Stored.
	// However, making a minimal reproducer isn't feasible.
	finished *sync.Map

	inProgress int64

	singleKeyHash uint64
}

func (wi *writeIterator) setupWalker(w *FastWalker) {
	w.ExtractStrings = wi.p.f.ExtractStrings
	w.FnString = func(seq int64, flatPath []byte, path []string, value []byte) {
		if wi.fieldLimit != 0 && len(value) > wi.fieldLimit {
			value = value[0:wi.fieldLimit]
		}

		wi.setValue(seq, Value{
			Type:   TypeString,
			String: string(value),
		}, flatPath)
	}
	w.FnNumber = func(seq int64, flatPath []byte, path []string, value float64, raw []byte) {
		wi.setValue(seq, Value{
			Type:      TypeFloat,
			Number:    value,
			RawNumber: string(raw),
		}, flatPath)
	}
	w.FnBool = func(seq int64, flatPath []byte, path []string, value bool) {
		wi.setValue(seq, Value{
			Type: TypeBool,
			Bool: value,
		}, flatPath)
	}
	w.FnNull = func(seq int64, flatPath []byte, path []string) {
		wi.setValue(seq, Value{
			Type: TypeNull,
		}, flatPath)
	}
}

func (wi *writeIterator) setValue(seq int64, v Value, flatPath []byte) {
	l, _ := wi.pending.Load(seq)
	pk := l.h.hashBytes(flatPath)

	if wi.singleKeyHash != 0 && pk != wi.singleKeyHash {
		return
	}

	i, ok := wi.pkIndex[pk]
	if !ok {
		return
	}

	if v.Type == TypeString { //nolint:nestif
		// Reformat time.
		tf, ok := wi.pkTimeFmt[pk]
		if ok && tf != "RAW" {
			var (
				t   time.Time
				err error
			)

			if wi.outputTZ != nil {
				t, err = time.ParseInLocation(tf, v.String, wi.outputTZ)
			} else {
				t, err = time.Parse(tf, v.String)
			}

			if err != nil {
				v.String = fmt.Sprintf("failed to parse time %s: %s", v.String, err)
			} else {
				v.String = t.Format(wi.outTimeFmt)
			}
		}
	}

	v.Dst = wi.pkDst[pk]

	ev := l.values[i]
	t := ev.Type

	if t == TypeAbsent {
		l.values[i] = v

		return
	}

	if wi.p.cfg.ConcatDelimiter != nil && v.Type != TypeAbsent {
		cv := Value{
			Type:   TypeString,
			String: ev.Format() + *wi.p.cfg.ConcatDelimiter + v.Format(),
		}

		l.values[i] = cv
	}
}

var throttle int64

func init() {
	go func() {
		for {
			m := runtime.MemStats{}
			runtime.ReadMemStats(&m)

			// 1 GB soft limit to start delays.
			if m.HeapInuse > 1e9 {
				atomic.StoreInt64(&throttle, 1)

				return
			}

			time.Sleep(time.Second / 10)
		}
	}()
}

func (wi *writeIterator) lineStarted(seq int64) error {
	inp := atomic.AddInt64(&wi.inProgress, 1)
	if inp > int64(5*wi.p.f.Concurrency) {
		if atomic.LoadInt64(&throttle) == 1 {
			time.Sleep(10 * time.Millisecond)
		}
	} else if inp > int64(20*wi.p.f.Concurrency) {
		if atomic.LoadInt64(&throttle) == 1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	l := wi.lineBufPool.Get().(*lineBuf) //nolint: errcheck
	wi.pending.Store(seq, l)

	return nil
}

func (wi *writeIterator) lineFinished(seq int64) error {
	l, ok := wi.pending.LoadAndDelete(seq)
	if !ok {
		panic("BUG: could not find pending line to finish")
	}

	wi.finished.Store(seq, l)

	return wi.checkCompleted()
}

func (wi *writeIterator) complete(seq int64, l *lineBuf) error {
	defer func() {
		atomic.AddInt64(&wi.inProgress, -1)
	}()

	for i, v := range wi.p.constVals {
		val := Value{
			Type:   TypeString,
			String: v,
		}

		l.values[i] = val
	}

	err := wi.p.w.ReceiveRow(seq, l.values)

	atomic.AddInt64(&wi.seqExpected, 1)

	for i := range l.values {
		l.values[i] = Value{}
	}

	wi.lineBufPool.Put(l)

	return err
}

func (wi *writeIterator) checkCompleted() error {
	for {
		seqExpected := atomic.LoadInt64(&wi.seqExpected)

		l, ok := wi.finished.LoadAndDelete(seqExpected)

		if !ok {
			break
		}

		if err := wi.complete(seqExpected, l.(*lineBuf)); err != nil {
			return err
		}
	}

	return nil
}

func (wi *writeIterator) waitPending() error {
	i := 0

	for {
		cnt := 0
		min := int64(-1)
		max := int64(-1)

		wi.finished.Range(func(key any, value any) bool {
			cnt++

			k, ok := key.(int64)
			if !ok {
				panic(fmt.Sprintf("BUG: int64 expected, %T received", key))
			}

			if min == -1 || k < min {
				min = k
			}

			if max == -1 || k > max {
				max = k
			}

			return true
		})

		if cnt == 0 {
			return nil
		}

		if err := wi.checkCompleted(); err != nil {
			return err
		}

		wi.p.Log(fmt.Sprintf("waiting pending: %d, reading in progress %d", cnt, wi.pending.Size()))
		time.Sleep(time.Second)

		i++

		if i > 10 {
			return fmt.Errorf("could not wait for lines %v (%d - %d), expected seq %d, in progress %d",
				cnt, min, max, atomic.LoadInt64(&wi.seqExpected), wi.pending.Size())
		}
	}
}
