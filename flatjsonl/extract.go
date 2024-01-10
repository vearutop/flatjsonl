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
		return DecodeURL
	case ExtractJSON:
		return func(s []byte) ([]byte, Extract, error) {
			return s, ExtractJSON, nil
		}
	}

	return nil
}

// Extractor defines extractor function.
type Extractor func(s []byte) (json []byte, name Extract, err error)
