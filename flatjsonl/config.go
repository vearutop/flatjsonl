package flatjsonl

// Config describes processing options.
type Config struct {
	IncludeKeys      []string          `json:"includeKeys" yaml:"includeKeys"`
	IncludeKeysRegex []string          `json:"includeKeysRegex" yaml:"includeKeysRegex"`
	ReplaceKeys      map[string]string `json:"replaceKeys" yaml:"replaceKeys"`
	ReplaceKeysRegex map[string]string `json:"replaceKeysRegex" yaml:"replaceKeysRegex"`
	ParseTime        map[string]string `json:"parseTime" yaml:"parseTime" description:"Map of key to time format."`
	OutputTimeFormat string            `json:"outputTimeFormat" yaml:"outputTimeFormat" example:"2006-01-02T15:04:05Z07:00" description:"See https://pkg.go.dev/time#pkg-constants."`
	OutputTimezone   string            `json:"outputTZ" yaml:"outputTZ" example:"UTC"`
	ConcatDelimiter  *string           `json:"concatDelimiter" yaml:"concatDelimiter" example:"," description:"In case multiple keys are replaced into one, their values would be concatenated."`
	Transpose        map[string]string `json:"transpose" yaml:"transpose" description:"Map of key prefixes to transposed table names."`
}
