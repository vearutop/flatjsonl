package flatjsonl

// Extract is the name of extractable format.
type Extract string

// Types of extractable content.
const (
	ExtractURL  = Extract("URL")
	ExtractJSON = Extract("JSON")
)

// Enum describes the type.
func (Extract) Enum() []any {
	return []any{
		ExtractURL,
		ExtractJSON,
	}
}

// Extractor is a factory.
func (e Extract) Extractor() Extractor {
	switch e {
	case ExtractURL:
		return urlExtractor{}
	case ExtractJSON:
		return jsonExtractor{}
	}

	return nil
}

// Extractor defines extractor function.
type Extractor interface {
	Name() Extract
	Extract(s []byte) (json []byte, name Extract, err error)
}

type jsonExtractor struct{}

func (jsonExtractor) Extract(s []byte) (json []byte, name Extract, err error) {
	return s, ExtractJSON, nil
}

// Name returns format name.
func (jsonExtractor) Name() Extract {
	return ExtractJSON
}
