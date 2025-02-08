package flatjsonl

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var starRepl = strings.NewReplacer(
	".", "\\.",
	"[", "\\[",
	"]", "\\]",
	"{", "\\{",
	"}", "\\}",
	"*", "([^.]+)",
)

// PrepareRegex converts * syntax to regex.
func PrepareRegex(reg string) string {
	if !strings.HasSuffix(reg, "$") && !strings.HasPrefix(reg, "^") && reg[0] == '.' {
		reg = "^" + starRepl.Replace(reg) + "$"
	}

	return reg
}

var trimSpaces = regexp.MustCompile(`\s+`)

func regex(reg string) (*regexp.Regexp, error) {
	if reg == "" {
		return nil, errors.New("empty regexp")
	}

	reg = PrepareRegex(reg)

	r, err := regexp.Compile(reg)
	if err != nil {
		return nil, fmt.Errorf("parse regular expression %s: %w", reg, err)
	}

	return r, nil
}
