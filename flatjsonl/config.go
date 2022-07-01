package flatjsonl

// Config describes processing options.
type Config struct {
	IncludeKeys      []string          `json:"includeKeys"`
	ReplaceKeys      map[string]string `json:"replaceKeys"`
	ReplaceKeysRegex map[string]string `json:"replaceKeysRegex"`
}
