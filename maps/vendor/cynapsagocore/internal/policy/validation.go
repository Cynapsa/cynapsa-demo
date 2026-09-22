package policy

import (
	"path"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const (
	MaxRules     = 1_024
	MaxPathBytes = 2_048
)

func ValidateRule(rule Rule) error {
	if rule.Action != ActionAllow && rule.Action != ActionDeny {
		return ErrInvalidRule
	}
	if rule.Path != "" {
		if len(rule.Path) > MaxPathBytes || !utf8.ValidString(rule.Path) || !strings.HasPrefix(rule.Path, "/") || path.Clean(rule.Path) != rule.Path || strings.ContainsAny(rule.Path, "?#") {
			return ErrInvalidRule
		}
		for _, character := range rule.Path {
			if unicode.IsControl(character) {
				return ErrInvalidRule
			}
		}
	}
	if rule.AgentID != "" {
		if err := protocol.ValidateAgentIdentity(rule.AgentID); err != nil {
			return ErrInvalidRule
		}
	}
	return nil
}

func validateApplicationPath(value string) error {
	if value == "" || len(value) > MaxPathBytes || !utf8.ValidString(value) || !strings.HasPrefix(value, "/") || path.Clean(value) != value || strings.ContainsAny(value, "?#") {
		return ErrApplicationDenied
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return ErrApplicationDenied
		}
	}
	return nil
}
