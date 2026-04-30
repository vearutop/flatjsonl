package flatjsonl

import (
	"sort"
	"strconv"
	"strings"
)

type transposeSpec struct {
	src  string
	dst  string
	path []string
}

type transposeMatcher struct {
	byRoot map[string][]transposeSpec
	bySrc  map[string]transposeSpec
	byDst  map[string]string
}

type transposeMatch struct {
	src        string
	dst        string
	srcPath    []string
	rowKey     intOrString
	trimmedSeg []string
}

func (p *Processor) matchTransposePath(path []string) (transposeMatch, bool) {
	if len(path) == 0 || len(p.transpose.byRoot) == 0 {
		return transposeMatch{}, false
	}

	for _, ts := range p.transpose.byRoot[path[0]] {
		if tm, ok := ts.match(path); ok {
			return tm, true
		}
	}

	return transposeMatch{}, false
}

func compileTransposeSpecs(cfg map[string]string) transposeMatcher {
	specs := make([]transposeSpec, 0, len(cfg))

	for src, dst := range cfg {
		path := strings.Split(strings.TrimPrefix(src, "."), ".")
		if len(path) == 1 && path[0] == "" {
			continue
		}

		specs = append(specs, transposeSpec{
			src:  src,
			dst:  dst,
			path: path,
		})
	}

	sort.Slice(specs, func(i, j int) bool {
		if len(specs[i].path) != len(specs[j].path) {
			return len(specs[i].path) > len(specs[j].path)
		}

		return specs[i].src < specs[j].src
	})

	m := transposeMatcher{
		byRoot: make(map[string][]transposeSpec),
		bySrc:  make(map[string]transposeSpec),
		byDst:  make(map[string]string),
	}

	for _, ts := range specs {
		m.byRoot[ts.path[0]] = append(m.byRoot[ts.path[0]], ts)
		m.bySrc[ts.src] = ts
		m.byDst[ts.dst] = ts.src
	}

	return m
}

func (m *transposeMatcher) add(src string, dst string) transposeSpec {
	ts := transposeSpec{
		src:  src,
		dst:  dst,
		path: strings.Split(strings.TrimPrefix(src, "."), "."),
	}

	root := ts.path[0]
	specs := append(m.byRoot[root], ts)
	sort.Slice(specs, func(i, j int) bool {
		if len(specs[i].path) != len(specs[j].path) {
			return len(specs[i].path) > len(specs[j].path)
		}

		return specs[i].src < specs[j].src
	})

	m.byRoot[root] = specs
	m.bySrc[src] = ts
	m.byDst[dst] = src

	return ts
}

func (p *Processor) autoTransposeDst(parentKey string) string {
	seg := parentKey
	if i := strings.LastIndex(seg, "."); i >= 0 {
		seg = seg[i+1:]
	}

	seg = strings.Trim(seg, "[]")
	dst := toSnakeCase(seg)
	if dst == "" {
		dst = "transpose"
	}

	if src, ok := p.transpose.byDst[dst]; !ok || src == parentKey {
		return dst
	}

	base := dst + "_transpose"
	if src, ok := p.transpose.byDst[base]; !ok || src == parentKey {
		return base
	}

	for i := 2; ; i++ {
		candidate := base + "_" + strconv.Itoa(i)
		if src, ok := p.transpose.byDst[candidate]; !ok || src == parentKey {
			return candidate
		}
	}
}

func (ts transposeSpec) match(path []string) (transposeMatch, bool) {
	if len(path) <= len(ts.path) {
		return transposeMatch{}, false
	}

	for i := range ts.path {
		if path[i] != ts.path[i] {
			return transposeMatch{}, false
		}
	}

	remainder := path[len(ts.path):]
	rowSeg := remainder[0]
	if rowSeg == "*" || rowSeg == "[*]" {
		return transposeMatch{}, false
	}
	fieldSegs := remainder[1:]

	m := transposeMatch{
		src:     ts.src,
		dst:     ts.dst,
		srcPath: ts.path,
	}

	if len(rowSeg) > 2 && rowSeg[0] == '[' && rowSeg[len(rowSeg)-1] == ']' {
		i, err := strconv.Atoi(rowSeg[1 : len(rowSeg)-1])
		if err != nil {
			panic("BUG: failed to parse idx " + rowSeg + ": " + err.Error())
		}

		m.rowKey = intOrString{t: TypeInt, i: i}
	} else {
		m.rowKey = intOrString{t: TypeString, s: rowSeg}
	}

	m.trimmedSeg = fieldSegs

	return m, true
}

func (tm transposeMatch) wildcardSegment() string {
	if tm.rowKey.t == TypeInt {
		return "[*]"
	}

	return "*"
}

func (tm transposeMatch) normalizedPath() []string {
	path := make([]string, 0, len(tm.srcPath)+1+len(tm.trimmedSeg))
	path = append(path, tm.srcPath...)
	path = append(path, tm.wildcardSegment())
	if len(tm.trimmedSeg) > 0 {
		path = append(path, tm.trimmedSeg...)
	}

	return path
}

func (tm transposeMatch) normalizedKey() string {
	var b strings.Builder
	b.Grow(len(tm.src) + 8)
	b.WriteString(tm.src)
	b.WriteByte('.')
	b.WriteString(tm.wildcardSegment())

	for _, seg := range tm.trimmedSeg {
		b.WriteByte('.')
		b.WriteString(seg)
	}

	return b.String()
}

func (tm transposeMatch) trimmedKey() string {
	if len(tm.trimmedSeg) == 0 {
		return "._value"
	}

	var b strings.Builder
	for _, seg := range tm.trimmedSeg {
		b.WriteByte('.')
		b.WriteString(seg)
	}

	return b.String()
}
