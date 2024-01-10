package flatjsonl

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

// URL is a JSON representation of URL.
type URL struct {
	Scheme   string     `json:"scheme"`
	User     string     `json:"user,omitempty"`
	Pass     string     `json:"pass,omitempty"`
	Host     string     `json:"host,omitempty"`
	Port     string     `json:"port,omitempty"`
	Path     []string   `json:"path,omitempty"`
	Query    url.Values `json:"query,omitempty"`
	Fragment string     `json:"fragment,omitempty"`
}

var errInvalidURL = errors.New("invalid URL")

func decodeURL(s string) (URL, error) {
	if !strings.Contains(s, "://") {
		return URL{}, errInvalidURL
	}

	u, err := url.Parse(s)
	if err != nil {
		return URL{}, err
	}

	uv := URL{}
	uv.Scheme = u.Scheme
	uv.Port = u.Port()
	uv.Host = u.Hostname()
	uv.Query = u.Query()
	uv.Path = strings.Split(strings.Trim(u.Path, "/"), "/")
	uv.User = u.User.Username()
	uv.Pass, _ = u.User.Password()
	uv.Fragment = u.Fragment

	return uv, nil
}

// DecodeURL is an Extractor.
func DecodeURL(s []byte) ([]byte, Extract, error) {
	uv, err := decodeURL(string(s))
	if err != nil {
		return nil, "", err
	}

	j, err := json.Marshal(uv)

	return j, ExtractURL, err
}
