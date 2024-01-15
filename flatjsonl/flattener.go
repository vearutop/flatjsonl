package flatjsonl

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/valyala/fastjson"
)

// KeyFromPath joins path elements into a dot-separated scalar key.
func KeyFromPath(path []string) string {
	return "." + strings.Join(path, ".")
}

var pl fastjson.ParserPool

// FastWalker walks JSON with fastjson.
type FastWalker struct {
	// These callbacks are invoked during JSON traversal.
	// Common arguments:
	// * seq is a sequence number of parent line,
	// * flatPath is a dot-separated path to the current element,
	// * parserPool is a length of parent prefix in flatPath,
	// * path holds a list of segments, it is nil if WantPath is false.
	FnNumber func(seq int64, flatPath []byte, path []string, value float64, raw []byte)
	FnString func(seq int64, flatPath []byte, path []string, value []byte) Extractor
	FnBool   func(seq int64, flatPath []byte, path []string, value bool)
	FnNull   func(seq int64, flatPath []byte, path []string)

	WantPath       bool
	ExtractStrings bool

	buf []byte
}

// GetKey walks into a single key.
func (fv *FastWalker) GetKey(seq int64, flatPath []byte, path []string, v *fastjson.Value) {
	vv := v.Get(path...)
	if vv != nil {
		fv.WalkFastJSON(seq, flatPath, path, vv)
	}
}

// WalkFastJSON iterates fastjson.Value JSON structure.
func (fv *FastWalker) WalkFastJSON(seq int64, flatPath []byte, path []string, v *fastjson.Value) {
	switch v.Type() {
	case fastjson.TypeObject:
		fv.walkFastJSONObject(seq, flatPath, path, v)
	case fastjson.TypeArray:
		fv.walkFastJSONArray(seq, flatPath, path, v)
	case fastjson.TypeString:
		fv.walkFastJSONString(seq, flatPath, path, v)
	case fastjson.TypeNumber:
		n, err := v.Float64()
		if err != nil {
			panic(fmt.Sprintf("BUG: failed to use JSON number %q at %d:%s: %v", v.String(), seq, string(flatPath), err))
		}

		fv.buf = fv.buf[:0]
		fv.buf = v.MarshalTo(fv.buf)

		fv.FnNumber(seq, flatPath, path, n, fv.buf)
	case fastjson.TypeFalse, fastjson.TypeTrue:
		b, err := v.Bool()
		if err != nil {
			panic(fmt.Sprintf("BUG: failed to use JSON bool: %v", err))
		}

		fv.FnBool(seq, flatPath, path, b)
	case fastjson.TypeNull:
		fv.FnNull(seq, flatPath, path)
	default:
		panic(fmt.Sprintf("BUG: don't know how to walk: %s", v.Type()))
	}
}

func (fv *FastWalker) walkFastJSONArray(seq int64, flatPath []byte, path []string, v *fastjson.Value) {
	a, err := v.Array()
	if err != nil {
		panic(fmt.Sprintf("BUG: failed to use JSON array: %v", err))
	}

	for i, v := range a {
		k := "[" + strconv.Itoa(i) + "]"

		flatPath := append(flatPath, '.')
		flatPath = append(flatPath, k...)

		if fv.WantPath {
			fv.WalkFastJSON(seq, flatPath, append(path, k), v)
		} else {
			fv.WalkFastJSON(seq, flatPath, nil, v)
		}
	}
}

func (fv *FastWalker) walkFastJSONObject(seq int64, flatPath []byte, path []string, v *fastjson.Value) {
	o, err := v.Object()
	if err != nil {
		panic(fmt.Sprintf("BUG: failed to use JSON object: %v", err))
	}

	o.Visit(func(key []byte, v *fastjson.Value) {
		flatPath := append(flatPath, '.')
		flatPath = append(flatPath, key...)
		if fv.WantPath {
			fv.WalkFastJSON(seq, flatPath, append(path, string(key)), v)
		} else {
			fv.WalkFastJSON(seq, flatPath, nil, v)
		}
	})
}

func (fv *FastWalker) walkFastJSONString(seq int64, flatPath []byte, path []string, v *fastjson.Value) {
	s, err := v.StringBytes()
	if err != nil {
		panic(fmt.Sprintf("BUG: failed to use JSON string: %v", err))
	}

	x := fv.FnString(seq, flatPath, path, s)

	if x != nil { //nolint:nestif
		xs, name, err := x(s)
		if err == nil {
			p := pl.Get()
			defer pl.Put(p)

			if v, err := p.ParseBytes(xs); err == nil {
				flatPath = append(flatPath, []byte("."+name)...)

				if fv.WantPath {
					fv.WalkFastJSON(seq, flatPath, append(path, string(name)), v)
				} else {
					fv.WalkFastJSON(seq, flatPath, nil, v)
				}

				return
			}
		}
	}

	if !fv.ExtractStrings || len(s) <= 2 {
		return
	}

	// Check if string has nested JSON or URL.
	if s[0] == '{' || s[0] == '[' {
		p := pl.Get()
		defer pl.Put(p)

		v, err := p.ParseBytes(s)
		if err == nil {
			flatPath = append(flatPath, []byte(".JSON")...)

			if fv.WantPath {
				fv.WalkFastJSON(seq, flatPath, append(path, "JSON"), v)
			} else {
				fv.WalkFastJSON(seq, flatPath, nil, v)
			}

			return
		}
	}

	if bytes.Contains(s, []byte("://")) { //nolint:nestif
		us, _, err := DecodeURL(s)
		if err == nil {
			p := pl.Get()
			defer pl.Put(p)

			v, err := p.ParseBytes(us)
			if err == nil {
				flatPath = append(flatPath, []byte(".URL")...)

				if fv.WantPath {
					fv.WalkFastJSON(seq, flatPath, append(path, "URL"), v)
				} else {
					fv.WalkFastJSON(seq, flatPath, nil, v)
				}

				return
			}
		}
	}
}

// Format turns value into a string.
func Format(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	switch val := v.(type) {
	case string:
		return val
	case float64:
		return strconv.FormatFloat(val, 'g', 5, 64)
	case bool:
		return strconv.FormatBool(val)
	default:
		panic(fmt.Sprintf("BUG: don't know how to format: %T", v))
	}
}

// KeyHierarchy collects structural relations.
type KeyHierarchy struct {
	Name string
	Sub  map[string]KeyHierarchy
}

// Add registers path to KeyHierarchy.
func (k *KeyHierarchy) Add(path []string) {
	if len(path) == 0 {
		return
	}

	if k.Sub == nil {
		k.Sub = map[string]KeyHierarchy{}
	}

	s := k.Sub[path[0]]
	s.Name = path[0]
	s.Add(path[1:])
	k.Sub[path[0]] = s
}

// Hierarchy exposes keys as tree hierarchy.
func (k KeyHierarchy) Hierarchy() interface{} {
	if len(k.Sub) == 0 {
		return k.Name
	}

	res := make([]interface{}, 0, len(k.Sub))

	for _, s := range k.Sub {
		res = append(res, s.Hierarchy())
	}

	return map[string]interface{}{k.Name: res}
}
