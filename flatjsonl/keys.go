package flatjsonl

import (
	"fmt"
	"regexp"
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
	transposeIdx     int
	transposeTrimmed string
}

func (p *Processor) scanKey(pk string, flatPath []byte, path []string, t Type, isZero bool) {
	k, ok := p.flKeys.Load(pk)
	if !ok {
		p.mu.Lock()
		defer p.mu.Unlock()

		k, ok = p.flKeys.Load(pk)
		if !ok {
			pp := make([]string, len(path))
			copy(pp, path)

			key := KeyFromPath(path)

			k.t = k.t.Update(t)
			k.isZero = k.isZero && isZero
			k.path = pp
			k.original = key
			k.canonical = p.ck(key)

			for tk, dst := range p.cfg.Transpose {
				if strings.HasPrefix(k.original, tk) {
					trimmed := strings.TrimPrefix(k.original, tk)[1:]
					pos := strings.Index(trimmed, "]")
					idx := trimmed[0:pos]
					i, err := strconv.Atoi(idx)
					if err != nil {
						panic("BUG: failed to parse idx " + idx + ": " + err.Error())
					}
					trimmed = trimmed[pos+1:]
					if trimmed == "" {
						trimmed = "value"
					}

					k.transposeDst = dst
					k.transposeIdx = i
					k.transposeTrimmed = trimmed
					break
				}
			}

			p.flKeysList = append(p.flKeysList, k.original)
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

func (h hasher) hashString(path []string) string {
	h.digest.Reset()

	for _, s := range path {
		_, err := h.digest.WriteString(s)
		if err != nil {
			panic("hashing failed: " + err.Error())
		}
	}

	return string(h.digest.Sum(h.buf[:0]))
}

func (p *Processor) scanAvailableKeys() error {
	println("scanning keys...")

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
				return err
			}
			defer sess.Close()

			sess.setupWalker = func(w *FastWalker) {
				h := newHasher()

				w.FnString = func(seq int64, flatPath []byte, path []string, value []byte) {
					p.scanKey(h.hashString(path), flatPath, path, TypeString, len(value) == 0)
				}
				w.FnNumber = func(seq int64, flatPath []byte, path []string, value float64, _ []byte) {
					isInt := float64(int(value)) == value
					if isInt {
						p.scanKey(h.hashString(path), flatPath, path, TypeInt, value == 0)
					} else {
						p.scanKey(h.hashString(path), flatPath, path, TypeFloat, value == 0)
					}
				}
				w.FnBool = func(seq int64, flatPath []byte, path []string, value bool) {
					p.scanKey(h.hashString(path), flatPath, path, TypeBool, !value)
				}
				w.FnNull = func(seq int64, flatPath []byte, path []string) {
					p.scanKey(h.hashString(path), flatPath, path, TypeNull, true)
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

func (p *Processor) iterateIncludeKeys() {
	i := 0

	for _, k := range p.cfg.IncludeKeys {
		p.includeKeys[k] = i
		i++
	}

	if p.flKeys.Size() == 0 && len(p.includeKeys) > 0 {
		h := newHasher()

		for k := range p.includeKeys {
			path := strings.Split(strings.TrimPrefix(k, "."), ".")
			pk := h.hashString(path)
			p.flKeys.Store(pk, flKey{
				path:      path,
				isZero:    false,
				t:         TypeString,
				original:  k,
				canonical: p.ck(k),
			})
		}
	}

	p.flKeys.Range(func(key string, value flKey) bool {
		v := p.canonicalKeys[value.canonical]
		value.isZero = value.isZero && v.isZero
		value.t = v.t.Update(value.t)

		p.canonicalKeys[value.canonical] = value

		return true
	})

	canonicalIncludes := make(map[string]bool)

	for _, k := range p.flKeysList {
		if len(p.cfg.Transpose) > 0 {
			for tk := range p.cfg.Transpose {
				if strings.HasPrefix(k, tk) {
					break
				}
			}
		}
	}

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

	for origKey, i := range p.includeKeys {
		replaced := p.prepareKey(origKey)

		ck := p.canonicalKeys[p.ck(origKey)]
		ck.replaced = replaced

		p.keys[i] = ck
	}
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

	sk := strings.Split(origKey, ".")
	i := len(sk) - 1
	ski := toSnakeCase(sk[i])
	snk := strings.Trim(ski, "[]")

	for {
		if _, ok := p.replaceByKey[snk]; !ok && (snk[0] == '_' || unicode.IsLetter(rune(snk[0]))) {
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
