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
	extractor        extractor
	parent           uint64
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

func (p *Processor) initKey(pk, parent uint64, path []string, t Type, isZero bool) flKey {
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
	k.parent = parent

	for tk, dst := range p.cfg.Transpose {
		if strings.HasPrefix(k.original, tk) {
			scanTransposedKey(dst, tk, &k)

			break
		}
	}

	for r, x := range p.extractRegex {
		if r.MatchString(key) {
			k.extractor = x

			break
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	existing, ok := p.flKeys.Load(pk)
	if ok {
		return existing
	}

	if p.f.ChildrenLimit > 0 && len(path) > 1 {
		parentCardinality := p.parentCardinality[parent]
		parentCardinality++

		if parentCardinality > p.f.ChildrenLimit {
			pp := k.path[0 : len(k.path)-1]
			parentKey := KeyFromPath(pp)
			grandParentKey := KeyFromPath(pp[:len(pp)-1])
			ppk, gpk := newHasher().hashParentBytes([]byte(parentKey), len(grandParentKey))

			p.mu.Unlock()
			// println("making parent key", parentKey, grandParentKey, ppk, gpk)
			p.initKey(ppk, gpk, pp, TypeJSON, false)
			p.mu.Lock()

			p.cfg.KeepJSON = append(p.cfg.KeepJSON, parentKey)
			p.parentHighCardinality.Store(parent, true)
		} else {
			p.parentCardinality[parent] = parentCardinality
		}
	}

	if _, ok := p.canonicalKeys[k.canonical]; !ok {
		p.flKeysList = append(p.flKeysList, k.original)
		p.canonicalKeys[k.canonical] = k
	}

	p.flKeys.Store(pk, k)
	atomic.AddInt64(&p.totalKeys, 1)

	return k
}

func (p *Processor) scanKey(pk, parent uint64, path []string, t Type, isZero bool) (_ extractor, stop bool) {
	if _, phc := p.parentHighCardinality.Load(parent); phc {
		return nil, true
	}

	k, ok := p.flKeys.Load(pk)

	if !ok {
		k = p.initKey(pk, parent, path, t, isZero)
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

	return k.extractor, false
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
func (h hasher) hashParentBytes(flatPath []byte, parentLen int) (pk uint64, par uint64) {
	h.digest.Reset()

	p1 := flatPath[:parentLen]

	_, err := h.digest.Write(p1)
	if err != nil {
		panic("hashing failed: " + err.Error())
	}

	par = h.digest.Sum64()

	p2 := flatPath[parentLen:]

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
			task := "scanning keys"
			if len(p.inputs) > 1 && input.FileName != "" {
				task = "scanning keys (" + input.FileName + ")"
			}

			sess, err := p.rd.session(input, task)
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

				w.FnObjectStop = func(_ int64, flatPath []byte, pl int, path []string) (stop bool) {
					if pl == 0 {
						return
					}

					pk, parent := h.hashParentBytes(flatPath, pl)

					_, stop = p.scanKey(pk, parent, path, TypeObject, false)

					return stop
				}
				w.FnArrayStop = func(_ int64, flatPath []byte, pl int, path []string) (stop bool) {
					if pl == 0 {
						return
					}

					pk, parent := h.hashParentBytes(flatPath, pl)

					_, stop = p.scanKey(pk, parent, path, TypeArray, false)

					return stop
				}
				w.FnString = func(_ int64, flatPath []byte, pl int, path []string, value []byte) extractor {
					pk, parent := h.hashParentBytes(flatPath, pl)

					x, _ := p.scanKey(pk, parent, path, TypeString, len(value) == 0)

					return x
				}
				w.FnNumber = func(_ int64, flatPath []byte, pl int, path []string, value float64, _ []byte) {
					pk, parent := h.hashParentBytes(flatPath, pl)
					isInt := float64(int(value)) == value

					if isInt {
						p.scanKey(pk, parent, path, TypeInt, value == 0)
					} else {
						p.scanKey(pk, parent, path, TypeFloat, value == 0)
					}
				}
				w.FnBool = func(_ int64, flatPath []byte, pl int, path []string, value bool) {
					pk, parent := h.hashParentBytes(flatPath, pl)
					p.scanKey(pk, parent, path, TypeBool, !value)
				}
				w.FnNull = func(_ int64, flatPath []byte, pl int, path []string) {
					pk, parent := h.hashParentBytes(flatPath, pl)
					p.scanKey(pk, parent, path, TypeNull, true)
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

	p.prepareScannedKeys()
	p.iterateIncludeKeys()

	return nil
}

func (p *Processor) prepareScannedKeys() {
	var jsonKeys map[string]bool
	if len(p.cfg.KeepJSON) > 0 {
		jsonKeys = make(map[string]bool)
		for _, key := range p.cfg.KeepJSON {
			jsonKeys[key] = true
		}
	}

	var (
		deleted = map[string]bool{}
		hcOrig  []string
	)

	p.parentHighCardinality.Range(func(key uint64, value bool) bool {
		k, ok := p.flKeys.Load(key)
		if !ok {
			println("BUG: high cardinality key not found")
			return true
		}

		hcOrig = append(hcOrig, k.original)
		k.t = TypeJSON

		p.flKeys.Store(key, k)

		return true
	})

	p.flKeys.Range(func(key uint64, k flKey) bool {
		if k.t == TypeObject || k.t == TypeArray {
			deleted[k.original] = true

			return true
		}

		for _, hc := range hcOrig {
			if len(k.original) > len(hc) && strings.HasPrefix(k.original, hc) {
				p.flKeys.Delete(key)

				deleted[k.original] = true

				return true
			}
		}

		p.keyHierarchy.Add(k.path)

		return true
	})

	var newFlKeys []string

	for _, key := range p.flKeysList {
		if !deleted[key] {
			newFlKeys = append(newFlKeys, key)
		}
	}

	p.flKeysList = newFlKeys
}

func (p *Processor) flKeysInit() {
	if p.flKeys.Size() == 0 && len(p.includeKeys) > 0 {
		h := newHasher()

		for key := range p.includeKeys {
			if strings.HasPrefix(key, "const:") {
				continue
			}

			path := strings.Split(strings.TrimPrefix(key, "."), ".")
			flatPath := []byte(key)
			pk := h.hashBytes(flatPath)

			k := flKey{
				path:      path,
				isZero:    false,
				t:         TypeString,
				original:  key,
				canonical: p.ck(key),
			}

			for r, x := range p.extractRegex {
				if r.MatchString(key) {
					k.extractor = x

					break
				}
			}

			p.flKeys.Store(pk, k)
		}
	}

	p.flKeys.Range(func(_ uint64, value flKey) bool {
		if _, phc := p.parentHighCardinality.Load(value.parent); phc {
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
	excludeKeys := make(map[string]bool, len(p.cfg.ExcludeKeys))
	for _, k := range p.cfg.ExcludeKeys {
		excludeKeys[p.ck(k)] = true
	}

	i := 0

	for _, k := range p.cfg.IncludeKeys {
		if excludeKeys[p.ck(k)] {
			continue
		}

		exclude := false

		for _, r := range p.excludeRegex {
			if r.MatchString(k) {
				exclude = true

				break
			}
		}

		if exclude {
			continue
		}

		p.addIncludeKey(k, &i)
	}

	p.flKeysInit()

	canonicalIncludes := make(map[string]bool)

	for _, k := range p.flKeysList {
		if _, ok := p.includeKeys[k]; ok {
			continue
		}

		ck := p.ck(k)

		if excludeKeys[ck] {
			continue
		}

		exclude := false

		for _, r := range p.excludeRegex {
			if r.MatchString(k) {
				exclude = true

				break
			}
		}

		if exclude {
			continue
		}

		if canonicalIncludes[ck] {
			continue
		}

		if len(p.includeRegex) > 0 {
			for _, r := range p.includeRegex {
				if r.MatchString(k) {
					canonicalIncludes[k] = true

					p.addIncludeKey(k, &i)

					break
				}
			}
		} else if len(p.cfg.IncludeKeys) == 0 {
			if !p.f.SkipZeroCols {
				canonicalIncludes[k] = true

				p.addIncludeKey(k, &i)
			} else if !p.canonicalKeys[ck].isZero {
				canonicalIncludes[k] = true

				p.addIncludeKey(k, &i)
			}
		}
	}
}

func (p *Processor) addIncludeKey(k string, i *int) {
	if _, found := p.includeKeys[k]; found {
		return
	}

	p.includeKeys[k] = *i
	*i++
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
		kr := origKey

		matches := reg.FindStringSubmatch(origKey)
		if matches != nil {
			kr = rep

			for i, m := range matches {
				if i == 0 {
					continue
				}

				kr = strings.ReplaceAll(kr, "${"+strconv.Itoa(i)+"}", trimSpaces.ReplaceAllString(strings.TrimSpace(m), "_"))
			}
		}

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
			panic("BUG: empty snk for '" + origKey + "'")
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
