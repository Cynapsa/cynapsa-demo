package model

import (
	"errors"
	"testing"
)

func TestMeshEndpointContract(t *testing.T) {
	valid := map[string][2]string{
		"Mesh-01.Example.TEST:5222":    {"mesh-01.example.test:5222", "mesh-01.example.test"},
		"localhost:00080":              {"localhost:80", "localhost"},
		"192.0.2.10:443":               {"192.0.2.10:443", "192.0.2.10"},
		"[2001:0DB8:0:0:0:0:0:1]:5222": {"[2001:db8::1]:5222", "2001:db8::1"},
	}
	for input, expected := range valid {
		endpoint, err := ParseMeshEndpoint(input)
		if err != nil || endpoint.DialAddress() != expected[0] || endpoint.TLSName() != expected[1] {
			t.Errorf("ParseMeshEndpoint(%q) = {%q,%q}, %v", input, endpoint.DialAddress(), endpoint.TLSName(), err)
		}
	}
	invalid := []string{"", "example.test", "example.test:0", "example.test:65536", "xmpp://example.test:5222", "user@example.test:5222", "example.test:5222/path", "example.test.:5222", "mésh.example:5222", "bad_name:5222", "[example.test]:5222", "[192.0.2.1]:5222", "2001:db8::1:5222", "[fe80::1%en0]:5222"}
	for _, input := range invalid {
		if endpoint, err := ParseMeshEndpoint(input); !errors.Is(err, ErrInvalidMeshEndpoint) || endpoint != (MeshEndpoint{}) {
			t.Errorf("ParseMeshEndpoint(%q) = %#v, %v", input, endpoint, err)
		}
	}
}
