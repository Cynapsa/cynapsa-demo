package ejabberd_test

import (
	"os"
	"strings"
	"testing"
)

func TestAuthorityDiscoveryFeatureHookContract(t *testing.T) {
	data, err := os.ReadFile("mod_cynapsa_mesh/src/mod_cynapsa_mesh.erl")
	if err != nil {
		t.Fatal(err)
	}
	production := string(data)
	if boundary := strings.LastIndex(production, "-ifdef(TEST)."); boundary >= 0 {
		production = production[:boundary]
	}
	for _, required := range []string{
		"-define(AUTHORITY_NAMESPACE, <<\"urn:cynapsa:mesh-authority:1\">>).",
		"ejabberd_hooks:add(disco_local_features, Host, ?MODULE,",
		"disco_local_features, ?HOOK_PRIORITY)",
		"ejabberd_hooks:delete(disco_local_features, Host, ?MODULE,",
		"disco_local_features(Acc, _From, _To, <<>>, _Lang) ->",
		"Feature =/= ?AUTHORITY_NAMESPACE",
	} {
		if !strings.Contains(production, required) {
			t.Errorf("authority discovery hook omits %q", required)
		}
	}
	hook := erlangReviewFunction(t, production,
		"disco_local_features({error, _} = Acc",
		"%% Runs after SASL")
	if count := strings.Count(hook, "?AUTHORITY_NAMESPACE |"); count != 1 {
		t.Fatalf("authority feature insertion count=%d, want exactly one", count)
	}
	if !strings.Contains(hook, "disco_local_features(Acc, _From, _To, _Node, _Lang) ->\n    Acc.") {
		t.Fatal("non-root disco nodes are not preserved unchanged")
	}
}
