package flatjsonl

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync/atomic"
	"unicode"

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

	includeKeys map[string]int

	flKeysMap    map[string]bool
	flKeysList   []string
	keyHierarchy Key
	nonZeroKeys  map[string]bool
}

// NewProcessor creates an instance of Processor.
func NewProcessor(f Flags, cfg Config, inputs []string) *Processor {
	pr := &Progress{}

	p := &Processor{
		cfg:    cfg,
		f:      f,
		inputs: inputs,

		pr: pr,
		w:  &Writer{},
		rd: &Reader{
			AddSequence: f.AddSequence,
			OnDecodeErr: func(err error) {
				println(err.Error())
			},
			Progress: pr,
			Buf:      make([]byte, 1e7),
		},
		includeKeys: map[string]int{},

		flKeysMap:    map[string]bool{},
		flKeysList:   make([]string, 0),
		keyHierarchy: Key{Name: "."},
		nonZeroKeys:  map[string]bool{},
	}

	if f.MatchLinePrefix != "" {
		p.rd.MatchPrefix = regexp.MustCompile(f.MatchLinePrefix)
	}

	return p
}

func (p *Processor) scanKey(k string, path []string, value interface{}) {
	if !p.flKeysMap[k] {
		p.flKeysMap[k] = true
		p.flKeysList = append(p.flKeysList, k)
	}

	switch v := value.(type) {
	case string:
		if v != "" {
			p.nonZeroKeys[k] = true
		}
	case float64:
		if v != 0 {
			p.nonZeroKeys[k] = true
		}
	case bool:
		if v {
			p.nonZeroKeys[k] = true
		}
	}

	p.keyHierarchy.Add(path)
}

func (p *Processor) scanAvailableKeys() error {
	println("scanning keys...")

	atomic.StoreInt64(&p.rd.Sequence, 0)

	if p.f.MaxLines > 0 {
		p.rd.MaxLines = int64(p.f.MaxLines)
	}

	if p.f.MaxLinesKeys > 0 && p.f.MaxLinesKeys < int(p.rd.MaxLines) {
		p.rd.MaxLines = int64(p.f.MaxLinesKeys)
	}

	for _, input := range p.inputs {
		err := p.rd.Read(input, func(path []string, value interface{}) {
			k := KeyFromPath(path)
			p.scanKey(k, path, value)
		}, nil)
		if err != nil {
			return fmt.Errorf("failed to read: %w", err)
		}
	}

	i := 0

	for _, k := range p.flKeysList {
		if !p.f.SkipZeroCols {
			p.includeKeys[k] = i
			i++
		} else if p.nonZeroKeys[k] {
			p.includeKeys[k] = i
			i++
		}
	}

	return nil
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

	if len(p.cfg.IncludeKeys) != 0 {
		for i, k := range p.cfg.IncludeKeys {
			p.includeKeys[k] = i
		}
	} else {
		// Scan available keys.
		if err := p.scanAvailableKeys(); err != nil {
			return err
		}
	}

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
		println("keys:")

		for _, k := range p.flKeysList {
			println(`"` + k + `",`)
		}
	}

	if p.f.ShowKeysHier {
		b, err := assertjson.MarshalIndentCompact(p.keyHierarchy.Hierarchy().(map[string]interface{})["."], "", " ", 120)
		if err != nil {
			return err
		}

		println(string(b))
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
		cw, err := NewSQLiteWriter(p.f.SQLite, p.f.SQLTable)
		if err != nil {
			return fmt.Errorf("failed to open SQLite file: %w", err)
		}

		p.w.Add(cw)
	}

	return nil
}

func (p *Processor) iterateForWriters() error {
	p.rd.MaxLines = 0
	atomic.StoreInt64(&p.rd.Sequence, 0)

	if p.f.MaxLines > 0 {
		p.rd.MaxLines = int64(p.f.MaxLines)
	}

	for _, input := range p.inputs {
		values := make([]interface{}, len(p.includeKeys))
		keys := p.prepareKeys()

		err := p.rd.Read(input, func(path []string, value interface{}) {
			k := KeyFromPath(path)

			if i, ok := p.includeKeys[k]; ok {
				values[i] = value
			}
		}, func(n int64) error {
			if err := p.w.ReceiveRow(keys, values); err != nil {
				return err
			}

			for i := range values {
				values[i] = nil
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to process file %s: %w", input, err)
		}
	}

	return nil
}

func (p *Processor) prepareKeys() []string {
	keys := make([]string, len(p.includeKeys))

	replaceByKey := make(map[string]string)
	for k, r := range p.cfg.ReplaceKeys {
		replaceByKey[r] = k
	}

	for k, i := range p.includeKeys {
		if rep, ok := p.cfg.ReplaceKeys[k]; ok {
			k = rep
		} else if p.f.ReplaceKeys {
			sk := strings.Split(k, ".")
			i := len(sk) - 1
			snk := strings.Trim(strings.ToLower(sk[i]), "[]")

			for {
				if _, ok := replaceByKey[snk]; !ok && (snk[0] == '_' || unicode.IsLetter(rune(snk[0]))) {
					replaceByKey[snk] = k
					k = snk

					break
				}
				i--

				if i == 0 {
					break
				}

				snk = strings.Trim(strings.ToLower(sk[i]), "[]") + "_" + snk
			}
		}

		keys[i] = k
	}

	return keys
}
