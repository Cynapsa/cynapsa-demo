package rank1webrtc

import (
	"net"
	"net/url"
	"strconv"
	"strings"
)

const (
	maximumPrivateServers    = 32
	maximumPrivateURLs       = 32
	maximumPrivateFieldBytes = 1024
)

func validPrivateServerURL(raw string) bool {
	_, valid := privateServerURLScheme(raw)
	return valid
}

func privateServerURLScheme(raw string) (string, bool) {
	if raw == "" || len(raw) > maximumPrivateFieldBytes || strings.ContainsAny(raw, "\r\n\t ") {
		return "", false
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil || parsed.Fragment != "" || parsed.Path != "" {
		return "", false
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "stun" && scheme != "stuns" && scheme != "turn" && scheme != "turns" {
		return "", false
	}
	hostPort := parsed.Opaque
	if hostPort == "" {
		hostPort = parsed.Host
	}
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil || host == "" || port == "" {
		return "", false
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil || parsedPort < 1 || parsedPort > 65535 {
		return "", false
	}
	if scheme == "stun" || scheme == "stuns" {
		return scheme, parsed.RawQuery == ""
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil || len(query) > 1 {
		return "", false
	}
	transportValues, present := query["transport"]
	valid := !present || (len(transportValues) == 1 && (transportValues[0] == "udp" || transportValues[0] == "tcp"))
	return scheme, valid
}
