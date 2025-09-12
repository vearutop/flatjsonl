package flatjsonl

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/oschwald/maxminddb-golang"
)

var geoIPDatabases []*maxminddb.Reader

type geoIPExtractor struct{}

// Name returns format name.
func (geoIPExtractor) name() extract {
	return extractGeoIP
}

// extract implements an extractor.
func (geoIPExtractor) extract(s []byte) ([]byte, extract, error) {
	var result map[string]interface{}

	ip := net.ParseIP(string(s))
	if ip == nil {
		return nil, "", fmt.Errorf("invalid IP address: %s", s)
	}

	for _, db := range geoIPDatabases {
		if err := db.Lookup(ip, &result); err != nil {
			return nil, "", err
		}
	}

	j, err := json.Marshal(result)

	return j, extractGeoIP, err
}
