package flatjsonl

import (
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
	FnNumber func(seq int64, path []string, value float64, raw []byte)
	FnString func(seq int64, path []string, value []byte)
	FnBool   func(seq int64, path []string, value bool)
	FnNull   func(seq int64, path []string)

	buf []byte
}

// WalkFastJSON iterates fastjson.Value JSON structure.
func (fv *FastWalker) WalkFastJSON(seq int64, path []string, v *fastjson.Value) {
	switch v.Type() {
	case fastjson.TypeObject:
		fv.walkFastJSONObject(seq, path, v)
	case fastjson.TypeArray:
		fv.walkFastJSONArray(seq, path, v)
	case fastjson.TypeString:
		fv.walkFastJSONString(seq, path, v)
	case fastjson.TypeNumber:
		n, err := v.Float64()
		if err != nil {
			panic(fmt.Sprintf("BUG: failed to use JSON number: %v", err))
		}

		fv.buf = fv.buf[:0]
		fv.buf = v.MarshalTo(fv.buf)

		fv.FnNumber(seq, path, n, fv.buf)
	case fastjson.TypeFalse, fastjson.TypeTrue:
		b, err := v.Bool()
		if err != nil {
			panic(fmt.Sprintf("BUG: failed to use JSON bool: %v", err))
		}

		fv.FnBool(seq, path, b)
	case fastjson.TypeNull:
		fv.FnNull(seq, path)
	default:
		panic(fmt.Sprintf("BUG: don't know how to walk: %s", v.Type()))
	}
}

func (fv *FastWalker) walkFastJSONArray(seq int64, path []string, v *fastjson.Value) {
	a, err := v.Array()
	if err != nil {
		panic(fmt.Sprintf("BUG: failed to use JSON array: %v", err))
	}

	for i, v := range a {
		fv.WalkFastJSON(seq, append(path, "["+strconv.Itoa(i)+"]"), v)
	}
}

func (fv *FastWalker) walkFastJSONObject(seq int64, path []string, v *fastjson.Value) {
	o, err := v.Object()
	if err != nil {
		panic(fmt.Sprintf("BUG: failed to use JSON object: %v", err))
	}

	o.Visit(func(key []byte, v *fastjson.Value) {
		fv.WalkFastJSON(seq, append(path, string(key)), v)
	})
}

func (fv *FastWalker) walkFastJSONString(seq int64, path []string, v *fastjson.Value) {
	s, err := v.StringBytes()
	if err != nil {
		panic(fmt.Sprintf("BUG: failed to use JSON string: %v", err))
	}

	// Check if string has nested JSON.
	if len(s) > 2 && (s[0] == '{' || s[0] == '[') {
		p := pl.Get()
		defer pl.Put(p)

		v, err := p.ParseBytes(s)
		if err == nil {
			fv.WalkFastJSON(seq, append(path, "JSON"), v)

			return
		}
	}

	fv.FnString(seq, path, s)
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

// Key collects structural relations.
type Key struct {
	Name string
	Sub  map[string]Key
}

// Add registers path to Key Hierarchy.
func (k *Key) Add(path []string) {
	if len(path) == 0 {
		return
	}

	if k.Sub == nil {
		k.Sub = map[string]Key{}
	}

	s := k.Sub[path[0]]
	s.Name = path[0]
	s.Add(path[1:])
	k.Sub[path[0]] = s
}

// Hierarchy exposes keys as tree hierarchy.
func (k Key) Hierarchy() interface{} {
	if len(k.Sub) == 0 {
		return k.Name
	}

	res := make([]interface{}, 0, len(k.Sub))

	for _, s := range k.Sub {
		res = append(res, s.Hierarchy())
	}

	return map[string]interface{}{k.Name: res}
}
