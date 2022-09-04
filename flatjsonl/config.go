package flatjsonl

// Config describes processing options.
type Config struct {
	IncludeKeys      []string          `json:"includeKeys"`
	IncludeKeysRegex []string          `json:"includeKeysRegex"`
	ReplaceKeys      map[string]string `json:"replaceKeys"`
	ReplaceKeysRegex map[string]string `json:"replaceKeysRegex"`
	ParseTime        map[string]string `json:"parseTime" description:"Map of key to time format."`
	OutputTimeFormat string            `json:"outputTimeFormat" example:"2006-01-02T15:04:05Z07:00" description:"See https://pkg.go.dev/time#pkg-constants."`
	OutputTimezone   string            `json:"outputTZ" example:"UTC"`
	Transpose        map[string]string `json:"transpose"`
}
