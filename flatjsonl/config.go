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
}
