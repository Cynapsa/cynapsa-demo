package mesh

import (
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/policy"
)

// PolicyController is the shared pre/post-authentication policy owner. It
// publishes only immutable compiled rulesets and never partially applies an
// invalid update.
type PolicyController struct{ store *policy.Store }

func NewPolicyController(initial []model.PolicyRule) (*PolicyController, error) {
	rules, failure := modelRules(initial)
	if failure != nil {
		return nil, policy.ErrInvalidRule
	}
	store, err := policy.NewStore(rules)
	if err != nil {
		return nil, err
	}
	return &PolicyController{store: store}, nil
}

func (controller *PolicyController) Replace(input []model.PolicyRule) *Failure {
	if controller == nil || controller.store == nil {
		return &Failure{Code: FailureInternal}
	}
	rules, failure := modelRules(input)
	if failure != nil {
		return failure
	}
	if err := controller.store.Replace(rules); err != nil {
		return classifyPolicyError(err)
	}
	return nil
}

func (controller *PolicyController) Result() model.PolicyResult {
	if controller == nil || controller.store == nil {
		return model.PolicyResult{}
	}
	rules := controller.store.Rules()
	result := make([]model.PolicyRule, len(rules))
	for index, rule := range rules {
		result[index] = model.PolicyRule{Action: string(rule.Action), Path: rule.Path, AgentID: rule.AgentID}
	}
	return model.PolicyResult{Rules: result}
}

func (controller *PolicyController) Allows(agentID, applicationPath string) bool {
	return controller != nil && controller.store != nil && controller.store.Allows(agentID, applicationPath)
}

func modelRules(input []model.PolicyRule) ([]policy.Rule, *Failure) {
	if len(input) > policy.MaxRules {
		return nil, &Failure{Code: FailureCapacity}
	}
	rules := make([]policy.Rule, len(input))
	for index, rule := range input {
		rules[index] = policy.Rule{Action: policy.Action(rule.Action), Path: rule.Path, AgentID: rule.AgentID}
	}
	return rules, nil
}

func classifyPolicyError(err error) *Failure {
	switch err {
	case policy.ErrRuleCapacity:
		return &Failure{Code: FailureCapacity}
	case policy.ErrInvalidRule, policy.ErrAmbiguousRule, policy.ErrApplicationDenied:
		return &Failure{Code: FailureRejected}
	default:
		return &Failure{Code: FailureInternal}
	}
}
