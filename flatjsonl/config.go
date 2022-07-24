package flatjsonl

// Config describes processing options.
type Config struct {
	IncludeKeys      []string          `json:"includeKeys"`
	IncludeKeysRegex []string          `json:"includeKeysRegex"`
	ReplaceKeys      map[string]string `json:"replaceKeys"`
	ReplaceKeysRegex map[string]string `json:"replaceKeysRegex"`
	Transpose        map[string]string `json:"transpose"`
}
