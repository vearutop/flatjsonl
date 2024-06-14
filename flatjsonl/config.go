package flatjsonl

// Config describes processing options.
type Config struct {
	MatchLinePrefix    string             `json:"matchLinePrefix" yaml:"matchLinePrefix"`
	IncludeKeys        []string           `json:"includeKeys" yaml:"includeKeys"`
	IncludeKeysRegex   []string           `json:"includeKeysRegex" yaml:"includeKeysRegex"`
	ExcludeKeys        []string           `json:"excludeKeys" yaml:"excludeKeys" description:"List of keys remove from columns."`
	ExcludeKeysRegex   []string           `json:"excludeKeysRegex" yaml:"excludeKeysRegex" description:"List of key regex to remove keys from columns."`
	ReplaceKeys        map[string]string  `json:"replaceKeys" yaml:"replaceKeys"`
	ReplaceKeysRegex   map[string]string  `json:"replaceKeysRegex" yaml:"replaceKeysRegex"`
	ParseTime          map[string]string  `json:"parseTime" yaml:"parseTime" description:"Map of key to time format, RAW format means no processing of original value."`
	OutputTimeFormat   string             `json:"outputTimeFormat" yaml:"outputTimeFormat" example:"2006-01-02T15:04:05Z07:00" description:"See https://pkg.go.dev/time#pkg-constants."`
	OutputTimezone     string             `json:"outputTZ" yaml:"outputTZ" example:"UTC"`
	ConcatDelimiter    *string            `json:"concatDelimiter" yaml:"concatDelimiter" example:"," description:"In case multiple keys are replaced into one, their values would be concatenated."`
	Transpose          map[string]string  `json:"transpose" yaml:"transpose" description:"Map of key prefixes to transposed table names."`
	ExtractValuesRegex map[string]extract `json:"extractValuesRegex" yaml:"extractValuesRegex" description:"Map of key regex to extraction format, values can be 'URL', 'JSON'."`
}
