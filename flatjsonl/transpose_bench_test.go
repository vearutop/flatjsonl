package flatjsonl

import "testing"

func BenchmarkTransposeMatch(b *testing.B) {
	p := &Processor{
		transpose: compileTransposeSpecs(map[string]string{
			".flatMap":   "flat_map",
			".tokens":    "tokens",
			".deepArr":   "deep_arr",
			".meta.tags": "meta_tags",
		}),
	}

	b.Run("object", func(b *testing.B) {
		path := []string{"tokens", "foo", "a"}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tm, ok := p.matchTransposePath(path)
			if !ok || tm.dst != "tokens" || tm.trimmed != ".a" || tm.rowKey.s != "foo" {
				b.Fatal("unexpected transpose match")
			}
		}
	})

	b.Run("array", func(b *testing.B) {
		path := []string{"deepArr", "[12]", "foo", "a"}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tm, ok := p.matchTransposePath(path)
			if !ok || tm.dst != "deep_arr" || tm.trimmed != ".foo.a" || tm.rowKey.i != 12 {
				b.Fatal("unexpected transpose match")
			}
		}
	})

	b.Run("miss", func(b *testing.B) {
		path := []string{"notranspose", "foo", "a"}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, ok := p.matchTransposePath(path); ok {
				b.Fatal("unexpected transpose match")
			}
		}
	})
}
