package model

import (
	"errors"
	"testing"
)

func TestAddressURLContract(t *testing.T) {
	tests := []struct {
		input, key, path, query string
	}{
		{"HTTP://Agent.Example:80", "http://agent.example", "/", ""},
		{"https://Agent.Example:443/orders%2Fnew?x=1&x=2", "https://agent.example", "/orders%2Fnew", "x=1&x=2"},
		{"https://Agent.Example:8443", "https://agent.example:8443", "/", ""},
	}
	for _, test := range tests {
		address, err := ParseAddressURL(test.input)
		if err != nil || address.LookupKey() != test.key || address.Path() != test.path || address.Query() != test.query {
			t.Errorf("ParseAddressURL(%q) = {%q,%q,%q}, %v", test.input, address.LookupKey(), address.Path(), address.Query(), err)
		}
	}
	for _, input := range []string{"", "ftp://example", "https://user@example", "https://example/path#fragment", "https://example.:443", "https://mésh.example:443", "https://[2001:db8::1]:443", "https://example:0", "https://example:65536"} {
		if value, err := ParseAddressURL(input); !errors.Is(err, ErrInvalidAddressURL) || value != (AddressURL{}) {
			t.Errorf("ParseAddressURL(%q) = %#v, %v", input, value, err)
		}
	}
	if _, err := ParseAddressOrigin("https://example/path"); !errors.Is(err, ErrInvalidAddressURL) {
		t.Fatalf("origin path error = %v", err)
	}
}
