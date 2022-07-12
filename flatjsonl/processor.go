package flatjsonl

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync"
	"github.com/swaggest/assertjson"
)

// Processor reads JSONL files with Reader and passes flat rows to Writer.
type Processor struct {
	cfg    Config
	f      Flags
	inputs []string

	pr *Progress
	w  *Writer
	rd *Reader

	includeKeys  map[string]int
	includeRegex []*regexp.Regexp
	replaceRegex map[*regexp.Regexp]string

	replaceKeys  map[string]string
	replaceByKey map[string]string

	// keys are ordered replaced column names, indexes match values of includeKeys.
	keys []string

	// types are ordered types of respective keys.
	types []Type

	flKeys *xsync.MapOf[flKey]

	mu            sync.Mutex
	flKeysList    []string
	keyHierarchy  Key
	canonicalKeys map[string]flKey
}

// NewProcessor creates an instance of Processor.
func NewProcessor(f Flags, cfg Config, inputs []string) *Processor { //nolint: funlen // Yeah, that's what she said.
	pr := &Progress{}

	p := &Processor{
		cfg:    cfg,
		f:      f,
		inputs: inputs,

		pr: pr,
		w:  &Writer{},
		rd: &Reader{
			AddSequence: f.AddSequence,
			OnError: func(err error) {
				println(err.Error())
			},
			Progress: pr,
			Buf:      make([]byte, 1e7),
		},
		includeKeys: map[string]int{},

		flKeysList:   make([]string, 0),
		keyHierarchy: Key{Name: "."},

		flKeys: xsync.NewMapOf[flKey](),
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
			println(fmt.Sprintf("failed to parse regular expression %s: %s", reg, err.Error()))
		}

		p.includeRegex = append(p.includeRegex, r)
	}

	for reg, rep := range p.cfg.ReplaceKeysRegex {
		if strings.Contains(reg, "*") {
			reg = "^" + starRepl.Replace(reg) + "$"
		}

		r, err := regexp.Compile(reg)
		if err != nil {
			println(fmt.Sprintf("failed to parse regular expression %s: %s", reg, err.Error()))
		}

		p.replaceRegex[r] = rep
	}

	return p
}

// Process dispatches data from Reader to Writer.
func (p *Processor) Process() error {
	defer func() {
		if err := p.w.Close(); err != nil {
			log.Fatalf("failed to close writer: %v", err)
		}
	}()

	if err := p.setupWriters(); err != nil {
		return err
	}

	if len(p.includeRegex) == 0 && len(p.cfg.IncludeKeys) != 0 {
		p.iterateIncludeKeys()
	} else {
		// Scan available keys.
		if err := p.scanAvailableKeys(); err != nil {
			return err
		}
	}

	p.prepareKeys()

	if p.w.HasReceivers() {
		if err := p.iterateForWriters(); err != nil {
			return err
		}
	}

	println("lines:", p.pr.Lines(), ", keys:", len(p.includeKeys))

	return p.maybeShowKeys()
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

		keyByIndex := map[int]string{}

		for k, i := range p.includeKeys {
			keyByIndex[i] = k
		}

		for i, k := range p.keys {
			orig := keyByIndex[i]
			t := p.types[i]

			line := k + ", TYPE " + string(t)
			if orig != k {
				line = orig + " REPLACED WITH " + line
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
	p.rd.MaxLines = 0
	atomic.StoreInt64(&p.rd.Sequence, 0)

	if p.f.MaxLines > 0 {
		p.rd.MaxLines = int64(p.f.MaxLines)
	}

	includeKeys := make(map[string]int, len(p.includeKeys))
	for k, i := range p.includeKeys {
		includeKeys[p.ck(k)] = i
	}

	type lineBuf struct {
		h      *hasher
		values []Value
	}

	mu := sync.RWMutex{}
	pending := map[int64]*lineBuf{}
	finished := map[int64]bool{}

	lineBufPool := sync.Pool{
		New: func() interface{} {
			return &lineBuf{
				h:      newHasher(),
				values: make([]Value, len(p.includeKeys)),
			}
		},
	}
	seqCompleted := int64(1)

	pkIndex := make(map[string]int)
	p.flKeys.Range(func(key string, value flKey) bool {
		if i, ok := includeKeys[value.ck]; ok {
			pkIndex[key] = i
		}

		return true
	})

	for _, input := range p.inputs {
		err := func() error {
			sess, err := p.rd.session(input, "flattening data")
			if err != nil {
				return err
			}

			sess.lineStarted = func(seq, n int64) error {
				mu.Lock()
				defer mu.Unlock()

				pending[seq] = lineBufPool.Get().(*lineBuf)

				return nil
			}

			sess.setupWalker = func(w *FastWalker) {
				w.FnString = func(seq int64, path []string, value []byte) {
					mu.RLock()
					defer mu.RUnlock()

					l := pending[seq]

					if i, ok := pkIndex[l.h.hashString(path)]; ok {
						l.values[i] = Value{
							Seq:    seq,
							Type:   TypeString,
							String: string(value),
						}
					}
				}
				w.FnNumber = func(seq int64, path []string, value float64, raw []byte) {
					mu.RLock()
					defer mu.RUnlock()

					l := pending[seq]

					if i, ok := pkIndex[l.h.hashString(path)]; ok {
						l.values[i] = Value{
							Seq:       seq,
							Type:      TypeFloat,
							Number:    value,
							RawNumber: string(raw),
						}
					}
				}
				w.FnBool = func(seq int64, path []string, value bool) {
					mu.RLock()
					defer mu.RUnlock()

					l := pending[seq]

					if i, ok := pkIndex[l.h.hashString(path)]; ok {
						l.values[i] = Value{
							Seq:  seq,
							Type: TypeBool,
							Bool: value,
						}
					}
				}
				w.FnNull = func(seq int64, path []string) {
					mu.RLock()
					defer mu.RUnlock()

					l := pending[seq]

					if i, ok := pkIndex[l.h.hashString(path)]; ok {
						l.values[i] = Value{
							Seq:  seq,
							Type: TypeNull,
						}
					}
				}
			}

			sess.lineFinished = func(seq, n int64) error {
				mu.Lock()
				finished[seq] = true
				mu.Unlock()

				for {
					var l *lineBuf
					isReady := false
					mu.Lock()
					if finished[seqCompleted] {
						isReady = true
						l = pending[seqCompleted]
						delete(pending, seqCompleted)
						delete(finished, seqCompleted)
					}
					mu.Unlock()

					if !isReady {
						break
					}

					if err := p.w.ReceiveRow(p.keys, l.values); err != nil {
						return err
					}

					for i := range l.values {
						l.values[i] = Value{}
					}

					lineBufPool.Put(l)

					seqCompleted++
				}

				return nil
			}

			err = p.rd.Read(sess)
			if err != nil {
				return fmt.Errorf("failed to process file %s: %w", input, err)
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}
