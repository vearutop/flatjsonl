package flatjsonl

type transposeSchema struct {
	dst          string
	indexType    Type
	trimmedKeys  map[string]idxKey
	filteredKeys []flKey
}

func (p *Processor) prepareTransposeSchemas() {
	p.transposeSchemas = map[string]transposeSchema{}

	for _, key := range p.keys {
		if key.transposeDst == "" {
			continue
		}

		ts := p.transposeSchemas[key.transposeDst]
		ts.dst = key.transposeDst
		if ts.trimmedKeys == nil {
			ts.trimmedKeys = map[string]idxKey{
				"._sequence": {idx: 0, k: flKey{
					original: "._sequence",
					replaced: p.prepareKey("._sequence"),
				}},
				"._index": {idx: 1, k: flKey{
					original: "._index",
					replaced: p.prepareKey("._index"),
				}},
			}
		}

		ts.indexType = key.transposeKey.t

		if ik, ok := ts.trimmedKeys[key.transposeTrimmed]; !ok {
			kk := key
			kk.replaced = p.prepareKey(key.transposeTrimmed)
			ts.trimmedKeys[key.transposeTrimmed] = idxKey{
				idx: len(ts.trimmedKeys),
				k:   kk,
			}
		} else {
			kk := ik.k
			kk.UpdateType(key.t)
			ik.k = kk
			ts.trimmedKeys[key.transposeTrimmed] = ik
		}

		p.transposeSchemas[key.transposeDst] = ts
	}

	for dst, ts := range p.transposeSchemas {
		ts.filteredKeys = make([]flKey, len(ts.trimmedKeys))
		for _, ik := range ts.trimmedKeys {
			ts.filteredKeys[ik.idx] = ik.k
		}

		ts.filteredKeys[0].t = TypeInt
		ts.filteredKeys[1].t = ts.indexType

		p.transposeSchemas[dst] = ts
	}
}
