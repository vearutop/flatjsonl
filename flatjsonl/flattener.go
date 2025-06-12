package flatjsonl

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/puzpuzpuz/xsync/v3"
	"github.com/vearutop/fastjson"
)

// KeyFromPath joins path elements into a dot-separated scalar key.
func KeyFromPath(path []string) string {
	return "." + strings.Join(path, ".")
}

var parserPool fastjson.ParserPool

// FastWalker walks JSON with fastjson.
type FastWalker struct {
	// These callbacks are invoked during JSON traversal.
	// Common arguments:
	// * seq is a sequence number of parent line,
	// * flatPath is a dot-separated path to the current element,
	// * parserPool is a length of parent prefix in flatPath,
	// * path holds a list of segments, it is nil if WantPath is false.
	FnObjectStop func(seq int64, flatPath []byte, pl int, path []string) (stop bool)
	FnArrayStop  func(seq int64, flatPath []byte, pl int, path []string) (stop bool)
	FnNumber     func(seq int64, flatPath []byte, pl int, path []string, value float64, raw []byte)
	FnString     func(seq int64, flatPath []byte, pl int, path []string, value []byte) extractor
	FnBool       func(seq int64, flatPath []byte, pl int, path []string, value bool)
	FnNull       func(seq int64, flatPath []byte, pl int, path []string)

	WantPath       bool
	ExtractStrings bool
	KeepJSON       map[string]bool
	KeepJSONRegex  []*regexp.Regexp

	buf []byte
}

func (fv *FastWalker) configure(p *Processor) {
	fv.ExtractStrings = p.f.ExtractStrings
	if len(p.cfg.KeepJSON) > 0 {
		fv.KeepJSON = make(map[string]bool)
		for _, v := range p.cfg.KeepJSON {
			fv.KeepJSON[v] = true
		}
	}

	for _, r := range p.cfg.KeepJSONRegex {
		reg, err := regex(r)
		if err != nil {
			panic(err)
		}

		fv.KeepJSONRegex = append(fv.KeepJSONRegex, reg)
	}
}

// GetKey walks into a single key.
func (fv *FastWalker) GetKey(seq int64, flatPath []byte, path []string, v *fastjson.Value) {
	vv := v.Get(path...)
	if vv != nil {
		fv.WalkFastJSON(seq, flatPath, 0, path, vv)
	}
}

// WalkFastJSON iterates fastjson.Value JSON structure.
func (fv *FastWalker) WalkFastJSON(seq int64, flatPath []byte, pl int, path []string, v *fastjson.Value) {
	switch v.Type() {
	case fastjson.TypeObject:
		fv.walkFastJSONObject(seq, flatPath, pl, path, v)
	case fastjson.TypeArray:
		fv.walkFastJSONArray(seq, flatPath, pl, path, v)
	case fastjson.TypeString:
		fv.walkFastJSONString(seq, flatPath, pl, path, v)
	case fastjson.TypeNumber:
		n, err := v.Float64()
		if err != nil {
			panic(fmt.Sprintf("BUG: failed to use JSON number %q at %d:%s: %v", v.String(), seq, string(flatPath), err))
		}

		fv.buf = fv.buf[:0]
		fv.buf = v.MarshalTo(fv.buf)

		fv.FnNumber(seq, flatPath, pl, path, n, fv.buf)
	case fastjson.TypeFalse, fastjson.TypeTrue:
		b, err := v.Bool()
		if err != nil {
			panic(fmt.Sprintf("BUG: failed to use JSON bool: %v", err))
		}

		fv.FnBool(seq, flatPath, pl, path, b)
	case fastjson.TypeNull:
		fv.FnNull(seq, flatPath, pl, path)
	default:
		panic(fmt.Sprintf("BUG: don't know how to walk: %s", v.Type()))
	}
}

func (fv *FastWalker) walkFastJSONArray(seq int64, flatPath []byte, pl int, path []string, v *fastjson.Value) {
	a, err := v.Array()
	if err != nil {
		panic(fmt.Sprintf("BUG: failed to use JSON array: %v", err))
	}

	if fv.FnArrayStop != nil {
		fv.FnArrayStop(seq, flatPath, pl, path)
	}

	pl = len(flatPath)

	if len(fv.KeepJSON) > 0 && fv.KeepJSON[string(flatPath)] {
		fv.buf = fv.buf[:0]
		fv.buf = v.MarshalTo(fv.buf)

		fv.FnString(seq, flatPath, pl, path, fv.buf)

		return
	}

	for _, r := range fv.KeepJSONRegex {
		if r.Match(flatPath) {
			fv.buf = fv.buf[:0]
			fv.buf = v.MarshalTo(fv.buf)

			fv.FnString(seq, flatPath, pl, path, fv.buf)

			return
		}
	}

	for i, v := range a {
		k := "[" + strconv.Itoa(i) + "]"

		flatPath := append(flatPath, '.')
		flatPath = append(flatPath, k...)

		if fv.WantPath {
			fv.WalkFastJSON(seq, flatPath, pl, append(path, k), v)
		} else {
			fv.WalkFastJSON(seq, flatPath, pl, nil, v)
		}
	}
}

func (fv *FastWalker) walkFastJSONObject(seq int64, flatPath []byte, pl int, path []string, v *fastjson.Value) {
	o, err := v.Object()
	if err != nil {
		panic(fmt.Sprintf("BUG: failed to use JSON object: %v", err))
	}

	if fv.FnObjectStop != nil {
		if fv.FnObjectStop(seq, flatPath, pl, path) {
			return
		}
	}

	pl = len(flatPath)

	if len(fv.KeepJSON) > 0 && fv.KeepJSON[string(flatPath)] {
		fv.buf = fv.buf[:0]
		fv.buf = v.MarshalTo(fv.buf)

		fv.FnString(seq, flatPath, pl, path, fv.buf)

		return
	}

	for _, r := range fv.KeepJSONRegex {
		if r.Match(flatPath) {
			fv.buf = fv.buf[:0]
			fv.buf = v.MarshalTo(fv.buf)

			fv.FnString(seq, flatPath, pl, path, fv.buf)

			return
		}
	}

	o.Visit(func(key []byte, v *fastjson.Value) {
		flatPath := append(flatPath, '.')
		flatPath = append(flatPath, key...)

		if fv.WantPath {
			fv.WalkFastJSON(seq, flatPath, pl, append(path, string(key)), v)
		} else {
			fv.WalkFastJSON(seq, flatPath, pl, nil, v)
		}
	})
}

func (fv *FastWalker) walkFastJSONString(seq int64, flatPath []byte, pl int, path []string, v *fastjson.Value) {
	s, err := v.StringBytes()
	if err != nil {
		panic(fmt.Sprintf("BUG: failed to use JSON string: %v", err))
	}

	x := fv.FnString(seq, flatPath, pl, path, s)

	if x != nil { //nolint:nestif
		xs, name, err := x.extract(s)
		if err == nil {
			p := parserPool.Get()
			p.AllowUnexpectedTail = true
			defer parserPool.Put(p)

			if v, err := p.ParseBytes(xs); err == nil {
				pl := len(flatPath)

				flatPath = append(flatPath, []byte("."+name)...)

				if fv.WantPath {
					fv.WalkFastJSON(seq, flatPath, pl, append(path, string(name)), v)
				} else {
					fv.WalkFastJSON(seq, flatPath, pl, nil, v)
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
		p := parserPool.Get()
		p.AllowUnexpectedTail = true
		defer parserPool.Put(p)

		v, err := p.ParseBytes(s)
		if err == nil {
			pl := len(flatPath)

			flatPath = append(flatPath, []byte(".JSON")...)

			if fv.WantPath {
				fv.WalkFastJSON(seq, flatPath, pl, append(path, "JSON"), v)
			} else {
				fv.WalkFastJSON(seq, flatPath, pl, nil, v)
			}

			return
		}
	}

	if bytes.Contains(s, []byte("://")) { //nolint:nestif
		us, _, err := (urlExtractor{}).extract(s)
		if err == nil {
			p := parserPool.Get()
			p.AllowUnexpectedTail = true
			defer parserPool.Put(p)

			v, err := p.ParseBytes(us)
			if err == nil {
				flatPath = append(flatPath, []byte(".URL")...)
				pl := len(flatPath)

				if fv.WantPath {
					fv.WalkFastJSON(seq, flatPath, pl, append(path, "URL"), v)
				} else {
					fv.WalkFastJSON(seq, flatPath, pl, nil, v)
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

type jsonSchema struct {
	Title      string                 `json:"title,omitempty"`
	Types      []string               `json:"type,omitempty"`
	Properties map[string]*jsonSchema `json:"properties,omitempty"`
	Items      *jsonSchema            `json:"items,omitempty"`
}

func (j *jsonSchema) AddType(t Type) {
	var tt string

	switch t {
	case TypeString:
		tt = "string"
	case TypeInt:
		tt = "integer"
	case TypeFloat:
		tt = "number"
	case TypeBool:
		tt = "boolean"
	case TypeArray:
		tt = "array"
	case TypeObject:
		tt = "object"
	case TypeJSON:
		return
	case TypeNull:
		tt = "null"
	case TypeAbsent:
		return
	default:
		panic(fmt.Sprintf("BUG: unknown type: %v", t))
	}

	for _, t := range j.Types {
		if t == tt {
			return
		}
	}

	j.Types = append(j.Types, tt)
}

func (j *jsonSchema) AddKey(k flKey, keys *xsync.MapOf[uint64, flKey]) {
	parents := []flKey{k}
	parent := k.parent

	for {
		if parent == 0 {
			break
		}

		pk, ok := keys.Load(parent)
		if !ok {
			println("BUG: failed to load parent key for JSON schema:", parent)

			return
		}

		parents = append(parents, pk)

		parent = pk.parent
	}

	parentSchema := j
	parentType := TypeObject

	for i := len(parents) - 1; i >= 0; i-- {
		pk := parents[i]
		name := pk.path[len(pk.path)-1]

		if i != 0 && pk.t == TypeString {
			pk.t = TypeObject
		}

		if parentType == TypeObject {
			if parentSchema.Properties == nil {
				parentSchema.Properties = make(map[string]*jsonSchema)
			}

			property := parentSchema.Properties[name]
			if property == nil {
				property = &jsonSchema{}
			}

			parentSchema.Properties[name] = property
			parentSchema = property

			parentType = pk.t
		} else if parentType == TypeArray {
			if parentSchema.Items == nil {
				parentSchema.Items = &jsonSchema{}
			}

			parentSchema = parentSchema.Items
			parentType = pk.t
		}
	}

	for _, t := range k.tt {
		parentSchema.AddType(t)
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
