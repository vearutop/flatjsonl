package flatjsonl

import (
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	_ "time/tzdata" // Loading timezones.

	"github.com/puzpuzpuz/xsync/v2"
	"github.com/swaggest/assertjson"
)

// Input can be either a file name or a reader.
type Input struct {
	FileName string
	Reader   interface {
		io.Reader
		Size() int64
		Reset()
		IsGzip() bool
	}
}

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
}

// NewProcessor creates an instance of Processor.
func NewProcessor(f Flags, cfg Config, inputs ...Input) *Processor { //nolint: funlen // Yeah, that's what she said.
	pr := &Progress{
		Interval: f.ProgressInterval,
	}

	p := &Processor{
		Log: func(args ...any) {
			_, _ = fmt.Fprintln(os.Stderr, args...)
		},

		cfg:    cfg,
		f:      f,
		inputs: inputs,

		pr: pr,
		w:  &Writer{},
		rd: &Reader{
			Concurrency: f.Concurrency,
			AddSequence: f.AddSequence,
			Progress:    pr,
			Buf:         make([]byte, f.BufSize),
		},
		includeKeys:   map[string]int{},
		constVals:     map[int]string{},
		canonicalKeys: map[string]flKey{},

		flKeysList:   make([]string, 0),
		keyHierarchy: KeyHierarchy{Name: "."},

		flKeys: xsync.NewIntegerMapOf[uint64, flKey](),
	}

	if f.HideProgress {
		pr.Print = func(status ProgressStatus) {}
	} else {
		pr.Print = func(status ProgressStatus) {
			p.Log(DefaultStatus(status))
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
		"*", "([^.]+)",
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
			line := strconv.Itoa(i) + ": " + k.replaced + ", TYPE " + string(k.t)

			if k.replaced != k.original {
				line = k.original + " REPLACED WITH " + line
			}

			if k.transposeDst != "" {
				line += ", TRANSPOSED TO " + k.transposeDst
			}

			fmt.Println(line)
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

		p.w.Add(cw)
	}

	if p.f.SQLite != "" {
		cw, err := NewSQLiteWriter(p.f.SQLite, p.f.SQLTable, p)
		if err != nil {
			return fmt.Errorf("failed to open SQLite file: %w", err)
		}

		p.w.Add(cw)
	}

	if p.f.Raw != "" {
		rw, err := NewRawWriter(p.f.Raw, p.f.RawDelim)
		if err != nil {
			return fmt.Errorf("failed")
		}

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
	wi.finished = xsync.NewIntegerMapOf[int64, *lineBuf]()

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
	finished    *xsync.MapOf[int64, *lineBuf]
	pending     *xsync.MapOf[int64, *lineBuf]
}

func (wi *writeIterator) setupWalker(w *FastWalker) {
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

	i, ok := wi.pkIndex[pk]
	if !ok {
		return
	}

	if v.Type == TypeString { //nolint:nestif
		// Reformat time.
		tf, ok := wi.pkTimeFmt[pk]
		if ok {
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

func (wi *writeIterator) lineStarted(seq int64) error {
	l := wi.lineBufPool.Get().(*lineBuf) //nolint: errcheck
	wi.pending.Store(seq, l)

	return nil
}

func (wi *writeIterator) lineFinished(seq int64) error {
	l, _ := wi.pending.LoadAndDelete(seq)

	wi.finished.Store(seq, l)

	return wi.checkCompleted()
}

func (wi *writeIterator) complete(seq int64, l *lineBuf) error {
	for i, v := range wi.p.constVals {
		val := Value{
			Type:   TypeString,
			String: v,
		}

		l.values[i] = val
	}

	err := wi.p.w.ReceiveRow(seq, l.values)

	for i := range l.values {
		l.values[i] = Value{}
	}

	wi.lineBufPool.Put(l)

	atomic.AddInt64(&wi.seqExpected, 1)

	return err
}

func (wi *writeIterator) checkCompleted() error {
	for {
		seqExpected := atomic.LoadInt64(&wi.seqExpected)

		l, ok := wi.finished.LoadAndDelete(seqExpected)

		if !ok {
			break
		}

		if err := wi.complete(seqExpected, l); err != nil {
			return err
		}
	}

	return nil
}

func (wi *writeIterator) waitPending() error {
	i := 0

	for {
		var seqLeft []int64

		if wi.finished.Size() == 0 {
			return nil
		}

		if err := wi.checkCompleted(); err != nil {
			return err
		}

		wi.p.Log("waiting pending", fmt.Sprintf("%v", seqLeft))
		time.Sleep(time.Second)

		i++

		if i > 10 {
			return fmt.Errorf("could not wait for lines %v", seqLeft)
		}
	}
}
