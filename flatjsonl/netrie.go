package flatjsonl

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/vearutop/netrie"
)

var netrieDatabases = map[string]netrie.IPLookuper{}

type netIPExtractor struct{}

// Name returns format name.
func (netIPExtractor) name() extract {
	return extractNetIP
}

// extract implements an extractor.
func (netIPExtractor) extract(s []byte) ([]byte, extract, error) {
	result := make(map[string]string, len(netrieDatabases))

	ip := net.ParseIP(string(s))
	if ip == nil {
		return nil, "", fmt.Errorf("invalid IP address: %s", s)
	}

	for name, db := range netrieDatabases {
		res := db.LookupIP(ip)

		result[name] = res
	}

	j, err := json.Marshal(result)

	return j, extractNetIP, err
}
