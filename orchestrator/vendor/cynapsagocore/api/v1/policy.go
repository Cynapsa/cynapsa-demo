package v1

const (
	CommandPolicySet  CommandName = "policy.set"
	CommandPolicyGet  CommandName = "policy.get"
	CommandPolicyTest CommandName = "policy.test"
)

type PolicyAction string

const (
	PolicyActionAllow PolicyAction = "allow"
	PolicyActionDeny  PolicyAction = "deny"
)

// PolicyRule is an application-level allowlisted policy rule.
type PolicyRule struct {
	Action PolicyAction
	// Path is either empty as the whole-field wildcard or a valid application
	// path under the 2,048-byte boundary contract.
	Path    string
	AgentID AgentID
}

type PolicySetCommand struct {
	CommandBase
	// Rules contains at most 1,024 policy rules.
	Rules []PolicyRule
}

func (PolicySetCommand) Name() CommandName { return CommandPolicySet }
func (PolicySetCommand) commandType()      {}

type PolicyGetCommand struct{ CommandBase }

func (PolicyGetCommand) Name() CommandName { return CommandPolicyGet }
func (PolicyGetCommand) commandType()      {}

type PolicyTestCommand struct {
	CommandBase
	Input PolicyTestInput
}

type PolicyTestInput struct {
	To      AgentID
	Payload Payload
}

func (PolicyTestCommand) Name() CommandName { return CommandPolicyTest }
func (PolicyTestCommand) commandType()      {}

type PolicyResult struct {
	// Rules contains at most 1,024 policy rules.
	Rules   []PolicyRule
	Allowed bool
}

func (PolicyResult) resultType() {}
