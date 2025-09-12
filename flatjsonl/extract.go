package flatjsonl

// extract is the name of extractable format.
type extract string

// Types of extractable content.
const (
	extractURL   = extract("URL")
	extractJSON  = extract("JSON")
	extractGeoIP = extract("GEOIP")
)

// Enum describes the type.
func (extract) Enum() []any {
	return []any{
		extractURL,
		extractJSON,
		extractGeoIP,
	}
}

// Extractor is a factory.
func (e extract) Extractor() extractor {
	switch e {
	case extractURL:
		return urlExtractor{}
	case extractJSON:
		return jsonExtractor{}
	case extractGeoIP:
		return geoIPExtractor{}
	}

	return nil
}

// extractor defines extractor function.
type extractor interface {
	name() extract
	extract(s []byte) (json []byte, name extract, err error)
}

type jsonExtractor struct{}

func (jsonExtractor) extract(s []byte) (json []byte, name extract, err error) {
	return s, extractJSON, nil
}

// Name returns format name.
func (jsonExtractor) name() extract {
	return extractJSON
}
