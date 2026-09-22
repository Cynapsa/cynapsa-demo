package policy

type Action string

const (
	ActionAllow Action = "allow"
	ActionDeny  Action = "deny"
)

type Rule struct {
	Action  Action
	Path    string
	AgentID string
}

// Compiled is an immutable deterministic application ruleset.
type Compiled struct {
	rules []Rule
}

// Compile validates, copies, and rejects selectors with conflicting actions.
// Identical duplicate rules collapse to one rule.
func Compile(input []Rule) (*Compiled, error) {
	if len(input) > MaxRules {
		return nil, ErrRuleCapacity
	}
	selectors := make(map[string]Action, len(input))
	rules := make([]Rule, 0, len(input))
	for _, rule := range input {
		if err := ValidateRule(rule); err != nil {
			return nil, err
		}
		key := rule.AgentID + "\x00" + rule.Path
		if action, exists := selectors[key]; exists {
			if action != rule.Action {
				return nil, ErrAmbiguousRule
			}
			continue
		}
		selectors[key] = rule.Action
		rules = append(rules, rule)
	}
	return &Compiled{rules: rules}, nil
}

func (compiled *Compiled) Rules() []Rule {
	if compiled == nil {
		return nil
	}
	return append([]Rule(nil), compiled.rules...)
}

// Allows uses exact matching, with an empty selector as its whole-field
// wildcard. Any matching deny overrides every matching allow. No match denies.
func (compiled *Compiled) Allows(agentID, applicationPath string) bool {
	if compiled == nil || validateApplicationPath(applicationPath) != nil {
		return false
	}
	allowed := false
	for _, rule := range compiled.rules {
		if rule.AgentID != "" && rule.AgentID != agentID {
			continue
		}
		if rule.Path != "" && rule.Path != applicationPath {
			continue
		}
		if rule.Action == ActionDeny {
			return false
		}
		allowed = true
	}
	return allowed
}
