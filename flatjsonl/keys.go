package flatjsonl

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode"

	"github.com/cespare/xxhash/v2"
)

type flKey struct {
	path             []string
	isZero           bool
	t                Type
	original         string
	canonical        string
	replaced         string
	transposeDst     string
	transposeKey     intOrString
	transposeTrimmed string
	par              uint64
}

type intOrString struct {
	t Type
	i int
	s string
}

func (is intOrString) Value() Value {
	if is.t == TypeString {
		return Value{
			Type:   TypeString,
			String: is.s,
		}
	}

	return Value{
		Type:   TypeFloat,
		Number: float64(is.i),
	}
}

func (is intOrString) String() string {
	if is.s != "" {
		return is.s
	}

	return strconv.Itoa(is.i)
}

func (p *Processor) initKey(pk, par uint64, path []string, t Type, isZero bool) flKey {
	k, ok := p.flKeys.Load(pk)
	if ok {
		return k
	}

	pp := make([]string, len(path))
	copy(pp, path)

	key := KeyFromPath(path)

	k.t = k.t.Update(t)
	k.isZero = k.isZero && isZero
	k.path = pp
	k.original = key
	k.canonical = p.ck(key)
	k.par = par

	for tk, dst := range p.cfg.Transpose {
		if strings.HasPrefix(k.original, tk) {
			scanTransposedKey(dst, tk, &k)

			break
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	parentCardinality := p.parentCardinality[par]
	parentCardinality++
	if parentCardinality > 50 {
		p.parentHighCardinality.Store(par, true)
	} else {
		p.parentCardinality[par] = parentCardinality
	}

	if _, ok := p.canonicalKeys[k.canonical]; !ok {
		p.flKeysList = append(p.flKeysList, k.original)
		p.keyHierarchy.Add(path)
		p.canonicalKeys[k.canonical] = k
	}

	p.flKeys.Store(pk, k)
	atomic.AddInt64(&p.totalKeys, 1)

	return k
}

func (p *Processor) scanKey(pk, par uint64, path []string, t Type, isZero bool) {
	if _, phc := p.parentHighCardinality.Load(par); phc {
		return
	}

	k, ok := p.flKeys.Load(pk)

	if !ok {
		k = p.initKey(pk, par, path, t, isZero)
	}

	updType := false

	if k.t != t {
		k.t = k.t.Update(t)
		updType = true
	}

	if updType || (k.isZero && !isZero) {
		p.mu.Lock()
		defer p.mu.Unlock()

		k, _ = p.flKeys.Load(pk)

		k.t = k.t.Update(t)
		k.isZero = k.isZero && isZero

		p.flKeys.Store(pk, k)
	}
}

func scanTransposedKey(dst string, tk string, k *flKey) {
	trimmed := strings.TrimPrefix(k.original, tk)
	if trimmed[0] == '.' {
		trimmed = trimmed[1:]
	}

	// Array.
	if trimmed[0] == '[' {
		pos := strings.Index(trimmed, "]")
		idx := trimmed[1:pos]

		i, err := strconv.Atoi(idx)
		if err != nil {
			panic("BUG: failed to parse idx " + idx + ": " + err.Error())
		}

		trimmed = trimmed[pos+1:]
		k.transposeKey = intOrString{t: TypeInt, i: i}
	} else {
		if pos := strings.Index(trimmed, "."); pos > 0 {
			k.transposeKey = intOrString{t: TypeString, s: trimmed[0:pos]}
			trimmed = trimmed[pos:]
		} else {
			k.transposeKey = intOrString{t: TypeString, s: trimmed}
			trimmed = ""
		}
	}

	if trimmed == "" {
		trimmed = "._value"
	}

	k.transposeDst = dst
	k.transposeTrimmed = trimmed
}

type hasher struct {
	digest *xxhash.Digest
}

func newHasher() *hasher {
	return &hasher{
		digest: xxhash.New(),
	}
}

func (h hasher) hashBytes(flatPath []byte) uint64 {
	h.digest.Reset()

	_, err := h.digest.Write(flatPath)
	if err != nil {
		panic("hashing failed: " + err.Error())
	}

	return h.digest.Sum64()
}

// hashParentBytes takes flat path to element as a parent path and last segment.
// It returns hashes for parent path and for full path.
func (h hasher) hashParentBytes(flatPath []byte, pl int) (pk uint64, par uint64) {
	h.digest.Reset()

	p1 := flatPath[:pl]

	_, err := h.digest.Write(p1)
	if err != nil {
		panic("hashing failed: " + err.Error())
	}

	par = h.digest.Sum64()

	p2 := flatPath[pl:]

	_, err = h.digest.Write(p2)
	if err != nil {
		panic("hashing failed: " + err.Error())
	}

	return h.digest.Sum64(), par
}

func (p *Processor) scanAvailableKeys() error {
	p.Log("scanning keys...")

	atomic.StoreInt64(&p.rd.Sequence, 0)

	if p.f.MaxLines > 0 {
		p.rd.MaxLines = int64(p.f.MaxLines)
	}

	if p.f.MaxLinesKeys > 0 && (p.rd.MaxLines == 0 || p.f.MaxLinesKeys < int(p.rd.MaxLines)) {
		p.rd.MaxLines = int64(p.f.MaxLinesKeys)
	}

	p.rd.OffsetLines = int64(p.f.OffsetLines)

	for _, input := range p.inputs {
		err := func() error {
			sess, err := p.rd.session(input, "scanning keys")
			if err != nil {
				if errors.Is(err, errEmptyFile) {
					return nil
				}

				return err
			}

			defer sess.Close()

			sess.setupWalker = func(w *FastWalker) {
				h := newHasher()

				w.WantPath = true

				w.FnString = func(seq int64, flatPath []byte, pl int, path []string, value []byte) {
					pk, par := h.hashParentBytes(flatPath, pl)
					p.scanKey(pk, par, path, TypeString, len(value) == 0)
				}
				w.FnNumber = func(seq int64, flatPath []byte, pl int, path []string, value float64, _ []byte) {
					pk, par := h.hashParentBytes(flatPath, pl)

					isInt := float64(int(value)) == value
					if isInt {
						p.scanKey(pk, par, path, TypeInt, value == 0)
					} else {
						p.scanKey(pk, par, path, TypeFloat, value == 0)
					}
				}
				w.FnBool = func(seq int64, flatPath []byte, pl int, path []string, value bool) {
					pk, par := h.hashParentBytes(flatPath, pl)
					p.scanKey(pk, par, path, TypeBool, !value)
				}
				w.FnNull = func(seq int64, flatPath []byte, pl int, path []string) {
					pk, par := h.hashParentBytes(flatPath, pl)
					p.scanKey(pk, par, path, TypeNull, true)
				}
			}

			err = p.rd.Read(sess)
			if err != nil {
				return fmt.Errorf("failed to read: %w", err)
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	p.iterateIncludeKeys()

	return nil
}

func (p *Processor) flKeysInit() {
	if p.flKeys.Size() == 0 && len(p.includeKeys) > 0 {
		h := newHasher()

		for k := range p.includeKeys {
			if strings.HasPrefix(k, "const:") {
				continue
			}

			path := strings.Split(strings.TrimPrefix(k, "."), ".")
			flatPath := []byte(k)
			pk := h.hashBytes(flatPath)
			p.flKeys.Store(pk, flKey{
				path:      path,
				isZero:    false,
				t:         TypeString,
				original:  k,
				canonical: p.ck(k),
			})
		}
	}

	p.flKeys.Range(func(key uint64, value flKey) bool {
		if _, phc := p.parentHighCardinality.Load(value.par); phc {
			// Skip keys with high cardinality parents.
			return true
		}

		v := p.canonicalKeys[value.canonical]
		value.isZero = value.isZero && v.isZero
		value.t = v.t.Update(value.t)

		p.canonicalKeys[value.canonical] = value

		return true
	})

	for _, k := range p.flKeysList {
		if len(p.cfg.Transpose) > 0 {
			for tk := range p.cfg.Transpose {
				if strings.HasPrefix(k, tk) {
					break
				}
			}
		}
	}
}

func (p *Processor) iterateIncludeKeys() {
	i := 0

	for _, k := range p.cfg.IncludeKeys {
		p.includeKeys[k] = i
		i++
	}

	p.flKeysInit()

	canonicalIncludes := make(map[string]bool)

	for _, k := range p.flKeysList {
		if _, ok := p.includeKeys[k]; ok {
			continue
		}

		ck := p.ck(k)

		if canonicalIncludes[ck] {
			continue
		}

		if len(p.includeRegex) > 0 && len(p.cfg.IncludeKeys) > 0 {
			for _, r := range p.includeRegex {
				if r.MatchString(k) {
					p.includeKeys[k] = i
					canonicalIncludes[k] = true
					i++

					break
				}
			}
		} else {
			if !p.f.SkipZeroCols {
				p.includeKeys[k] = i
				canonicalIncludes[k] = true

				i++
			} else if !p.canonicalKeys[ck].isZero {
				p.includeKeys[k] = i
				canonicalIncludes[k] = true

				i++
			}
		}
	}
}

func (p *Processor) prepareKeys() {
	p.keys = make([]flKey, len(p.includeKeys))

	p.replaceKeys = make(map[string]string)
	p.replaceByKey = make(map[string]string)

	for k, r := range p.cfg.ReplaceKeys {
		mk := p.ck(k)

		p.replaceByKey[r] = mk
		p.replaceKeys[mk] = r
	}

	type ik struct {
		orig string
		idx  int
	}

	sorted := make([]ik, 0, len(p.includeKeys))
	for origKey, i := range p.includeKeys {
		sorted = append(sorted, ik{orig: origKey, idx: i})
	}
	// Sorting by index first to have deterministic order.
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].idx < sorted[j].idx
	})

	// Sorting to put shorter keys first to have better replaces.
	sort.SliceStable(sorted, func(i, j int) bool {
		return len(sorted[i].orig) < len(sorted[j].orig)
	})

	for _, v := range sorted {
		origKey := v.orig

		ck, ok := p.canonicalKeys[p.ck(origKey)]

		switch {
		case !ok: // Can happen for meta keys like `const:X`.
			ck.replaced = p.prepareKey(origKey)
		case ck.transposeDst == "":
			ck.replaced = p.prepareKey(ck.original)
		default:
			ck.replaced = p.prepareKey(ck.transposeTrimmed)
		}

		p.keys[v.idx] = ck
	}

	keys := make([]flKey, 0, len(p.keys))
	keyExists := make(map[string]int)
	keyMap := make(map[int]int)

	for i, pk := range p.keys {
		if pk.transposeDst == "" {
			if j, ok := keyExists[pk.replaced]; ok {
				pk.t = pk.t.Update(p.keys[i].t)
				p.keys[i] = pk
				keyMap[i] = j

				continue
			}

			keyExists[pk.replaced] = len(keys)
		}

		keyMap[i] = len(keys)
		keys = append(keys, pk)
	}

	if len(keyMap) > 0 {
		for k, i := range p.includeKeys {
			if j, ok := keyMap[i]; ok {
				p.includeKeys[k] = j

				if strings.HasPrefix(k, "const:") {
					p.constVals[j] = strings.TrimPrefix(k, "const:")
				}
			}
		}
	}

	p.keys = keys
}

func (p *Processor) prepareKey(origKey string) (kk string) {
	ck := p.ck(origKey)

	if rep, ok := p.replaceKeys[ck]; ok {
		return rep
	}

	defer func() {
		if p.f.KeyLimit > 0 && len(kk) > p.f.KeyLimit {
			i := p.includeKeys[kk]
			is := strconv.Itoa(i)

			kk = kk[0:(p.f.KeyLimit-len(is))] + is

			p.replaceKeys[ck] = kk
		}
	}()

	for reg, rep := range p.replaceRegex {
		kr := reg.ReplaceAllString(origKey, rep)
		if kr != origKey {
			if strings.HasSuffix(kr, "|to_snake_case") {
				kr = toSnakeCase(strings.TrimSuffix(kr, "|to_snake_case"))
			}

			return kr
		}
	}

	if !p.f.ReplaceKeys {
		return origKey
	}

	sk := strings.Split(strings.TrimRight(origKey, "."), ".")
	i := len(sk) - 1
	ski := toSnakeCase(sk[i])
	snk := strings.Trim(ski, "[]")

	for {
		if len(snk) == 0 {
			panic("BUG: empty snk for " + origKey)
		}

		if stored, ok := p.replaceByKey[snk]; (!ok || origKey == stored) && (snk[0] == '_' || unicode.IsLetter(rune(snk[0]))) {
			p.replaceByKey[snk] = origKey
			origKey = snk

			break
		}
		i--

		if i == 0 {
			break
		}

		ski := toSnakeCase(sk[i])
		snk = strings.Trim(ski, "[]") + "_" + snk
	}

	return origKey
}

var (
	matchFirstCap        = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap          = regexp.MustCompile("([a-z0-9])([A-Z])")
	matchNonAlphaNumeric = regexp.MustCompile(`[^a-z0-9A-Z\s()]+`)
)

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	snake = matchNonAlphaNumeric.ReplaceAllString(snake, "_")

	return strings.ToLower(strings.Trim(strings.ReplaceAll(snake, "_ ", " "), "_"))
}
