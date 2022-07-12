package flatjsonl

import (
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"unicode"

	"github.com/cespare/xxhash/v2"
)

type flKey struct {
	path   []string
	isZero bool
	t      Type
	key    string
	ck     string
}

func (p *Processor) scanKey(pathHash []byte, path []string, t Type, isZero bool) {
	pk := string(pathHash)

	k, ok := p.flKeys.Load(pk)
	if !ok {
		p.mu.Lock()
		defer p.mu.Unlock()

		k, _ = p.flKeys.Load(pk)

		pp := make([]string, len(path))
		copy(pp, path)

		key := KeyFromPath(path)

		k.t = k.t.Update(t)
		k.isZero = k.isZero && isZero
		k.path = pp
		k.key = key
		k.ck = p.ck(key)

		p.flKeysList = append(p.flKeysList, k.key)
		p.keyHierarchy.Add(path)

		p.flKeys.Store(pk, k)

		return
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

type hasher struct {
	buf    []byte
	digest *xxhash.Digest
}

func newHasher() *hasher {
	return &hasher{
		buf:    make([]byte, 8),
		digest: xxhash.New(),
	}
}

func (h hasher) hash(path []string) []byte {
	h.digest.Reset()
	for _, s := range path {
		_, err := h.digest.WriteString(s)
		if err != nil {
			panic("hashing failed: " + err.Error())
		}
	}

	return h.digest.Sum(h.buf[:0])
}

func (h hasher) hashString(path []string) string {
	h.digest.Reset()
	for _, s := range path {
		_, err := h.digest.WriteString(s)
		if err != nil {
			panic("hashing failed: " + err.Error())
		}
	}

	return string(h.digest.Sum(h.buf[:0])) // TODO consider unsafe conversion.
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
		err := func() error {
			sess, err := p.rd.session(input, "scanning keys")
			if err != nil {
				return err
			}
			defer sess.Close()

			sess.setupWalker = func(w *FastWalker) {
				h := newHasher()

				w.FnString = func(seq int64, path []string, value []byte) {
					p.scanKey(h.hash(path), path, TypeString, len(value) == 0)
				}
				w.FnNumber = func(seq int64, path []string, value float64, _ []byte) {
					isInt := float64(int(value)) == value
					if isInt {
						p.scanKey(h.hash(path), path, TypeInt, value == 0)
					} else {
						p.scanKey(h.hash(path), path, TypeFloat, value == 0)
					}
				}
				w.FnBool = func(seq int64, path []string, value bool) {
					p.scanKey(h.hash(path), path, TypeBool, !value)
				}
				w.FnNull = func(seq int64, path []string) {
					p.scanKey(h.hash(path), path, TypeNull, true)
				}
			}

			sess.async = true

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

func (p *Processor) iterateIncludeKeys() {
	i := 0

	for _, k := range p.cfg.IncludeKeys {
		p.includeKeys[k] = i
		i++
	}

	p.canonicalKeys = make(map[string]flKey)
	if p.flKeys.Size() == 0 && len(p.includeKeys) > 0 {
		h := newHasher()
		for k := range p.includeKeys {
			path := strings.Split(strings.TrimPrefix(k, "."), ".")
			pk := h.hashString(path)
			p.flKeys.Store(pk, flKey{
				path:   path,
				isZero: false,
				t:      TypeString,
				key:    k,
				ck:     p.ck(k),
			})
		}
	}

	p.flKeys.Range(func(key string, value flKey) bool {
		v := p.canonicalKeys[value.ck]
		value.isZero = value.isZero && v.isZero
		value.t = v.t.Update(value.t)

		p.canonicalKeys[value.ck] = value

		return true
	})

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
	p.keys = make([]string, len(p.includeKeys))
	p.types = make([]Type, len(p.includeKeys))

	p.replaceKeys = make(map[string]string)
	p.replaceByKey = make(map[string]string)

	for k, r := range p.cfg.ReplaceKeys {
		mk := p.ck(k)

		p.replaceByKey[r] = mk
		p.replaceKeys[mk] = r
	}

	for k, i := range p.includeKeys {
		p.keys[i], p.types[i] = p.prepareKey(k)
	}
}

func (p *Processor) prepareKey(k string) (string, Type) {
	ck := p.ck(k)
	t := p.canonicalKeys[ck].t

	if rep, ok := p.replaceKeys[ck]; ok {
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
