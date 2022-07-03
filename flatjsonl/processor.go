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

	includeKeys  map[string]int
	includeRegex []*regexp.Regexp
	replaceRegex map[*regexp.Regexp]string

	flKeysMap    map[string]bool
	flKeysList   []string
	keyHierarchy Key
	nonZeroKeys  map[string]bool

	// keyTypes contains cumulative types of data using Processor.k mutations as key.
	keyTypes map[string]Type

	replaceKeys  map[string]string
	replaceByKey map[string]string

	// keys are ordered replaced column names, indexes match values of includeKeys.
	keys []string

	// types are ordered types of respective keys.
	types []Type
}

// Type is a scalar type.
type Type string

// Type enumeration.
const (
	TypeString = "string"
	TypeInt    = "int"
	TypeFloat  = "float"
	TypeBool   = "bool"
)

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
		keyTypes:     map[string]Type{},
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

func (p *Processor) scanKey(k string, path []string, value interface{}) {
	mk := p.k(k)

	if !p.flKeysMap[mk] {
		p.flKeysMap[mk] = true
		p.flKeysList = append(p.flKeysList, k)

		p.keyHierarchy.Add(path)
	}

	t := p.keyTypes[mk]

	switch v := value.(type) {
	case string:
		p.keyTypes[mk] = t.Update(TypeString)
		if v != "" {
			p.nonZeroKeys[mk] = true
		}
	case float64:
		isInt := float64(int(v)) == v
		if isInt {
			p.keyTypes[mk] = t.Update(TypeInt)
		} else {
			p.keyTypes[mk] = t.Update(TypeFloat)
		}

		if v != 0 {
			p.nonZeroKeys[mk] = true
		}
	case bool:
		p.keyTypes[mk] = t.Update(TypeBool)

		if v {
			p.nonZeroKeys[mk] = true
		}
	}
}

// Update merges original type with updated.
func (t Type) Update(u Type) Type {
	// Undefined type is replaced by update.
	if t == "" {
		return u
	}

	// Same type is not updated.
	if t == u {
		return t
	}

	// String replaces any type.
	if u == TypeString || t == TypeString {
		return TypeString
	}

	// Bool and non-bool make unconstrained type: string.
	if t == TypeBool && u != TypeBool {
		return TypeString
	}

	// Float overrides Int.
	if t == TypeInt && u == TypeFloat {
		return TypeFloat
	}

	if t == TypeFloat && u == TypeInt {
		return TypeFloat
	}

	panic("don't know how to update " + t + " with " + u)
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

	for _, k := range p.cfg.IncludeKeys {
		p.includeKeys[k] = i
		i++
	}

	for _, k := range p.flKeysList {
		if _, ok := p.includeKeys[k]; ok {
			continue
		}

		if len(p.includeRegex) > 0 && len(p.cfg.IncludeKeys) > 0 {
			for _, r := range p.includeRegex {
				if r.MatchString(k) {
					p.includeKeys[k] = i
					i++

					break
				}
			}
		} else {
			if !p.f.SkipZeroCols {
				p.includeKeys[k] = i
				i++
			} else if p.nonZeroKeys[k] {
				p.includeKeys[k] = i
				i++
			}
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

	if len(p.includeRegex) == 0 && len(p.cfg.IncludeKeys) != 0 {
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

func (p *Processor) k(k string) string {
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
		includeKeys[p.k(k)] = i
	}

	for _, input := range p.inputs {
		values := make([]interface{}, len(p.includeKeys))
		p.prepareKeys()

		err := p.rd.Read(input, func(path []string, value interface{}) {
			k := KeyFromPath(path)
			mk := p.k(k)

			if i, ok := includeKeys[mk]; ok {
				values[i] = value
			}
		}, func(n int64) error {
			if err := p.w.ReceiveRow(p.keys, values); err != nil {
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

func (p *Processor) prepareKeys() {
	p.keys = make([]string, len(p.includeKeys))
	p.types = make([]Type, len(p.includeKeys))

	p.replaceKeys = make(map[string]string)
	p.replaceByKey = make(map[string]string)

	for k, r := range p.cfg.ReplaceKeys {
		mk := p.k(k)

		p.replaceByKey[r] = mk
		p.replaceKeys[mk] = r
	}

	for k, i := range p.includeKeys {
		p.keys[i], p.types[i] = p.prepareKey(k)
	}
}

func (p *Processor) prepareKey(k string) (string, Type) {
	mk := p.k(k)
	t := p.keyTypes[mk]

	if rep, ok := p.replaceKeys[mk]; ok {
		return rep, t
	}

	for reg, rep := range p.replaceRegex {
		kr := reg.ReplaceAllString(k, rep)
		if kr != k {
			if strings.HasSuffix(kr, "|to_snake_case") {
				kr = toSnakeCase(strings.TrimSuffix(kr, "|to_snake_case"))
			}

			return kr, t
		}
	}

	if !p.f.ReplaceKeys {
		return k, t
	}

	sk := strings.Split(k, ".")
	i := len(sk) - 1
	ski := toSnakeCase(sk[i])
	snk := strings.Trim(ski, "[]")

	for {
		if _, ok := p.replaceByKey[snk]; !ok && (snk[0] == '_' || unicode.IsLetter(rune(snk[0]))) {
			p.replaceByKey[snk] = k
			k = snk

			break
		}
		i--

		if i == 0 {
			break
		}

		ski := toSnakeCase(sk[i])
		snk = strings.Trim(ski, "[]") + "_" + snk
	}

	return k, t
}

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")

	return strings.ToLower(snake)
}
