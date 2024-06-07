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

var trimSpaces = regexp.MustCompile(`\s+`)

func regex(reg string) (*regexp.Regexp, error) {
	if reg == "" {
		return nil, errors.New("empty regexp")
	}

	if !strings.HasSuffix(reg, "$") && !strings.HasPrefix(reg, "^") && reg[0] == '.' {
		reg = "^" + starRepl.Replace(reg) + "$"
	}

	r, err := regexp.Compile(reg)
	if err != nil {
		return nil, fmt.Errorf("parse regular expression %s: %w", reg, err)
	}

	return r, nil
}
