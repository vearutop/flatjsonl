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
}

type transposeMatch struct {
	src     string
	dst     string
	rowKey  intOrString
	trimmed string
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
	}

	for _, ts := range specs {
		m.byRoot[ts.path[0]] = append(m.byRoot[ts.path[0]], ts)
	}

	return m
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
	fieldSegs := remainder[1:]

	m := transposeMatch{
		src: ts.src,
		dst: ts.dst,
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

	if len(fieldSegs) == 0 {
		m.trimmed = "._value"
	} else {
		m.trimmed = "." + strings.Join(fieldSegs, ".")
	}

	return m, true
}
