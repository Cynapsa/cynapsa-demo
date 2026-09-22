package cynapsagocore

import (
	"context"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/rank2xmpp"
)

func TestBuiltInObjectServiceComposesWithoutIdentityKeys(t *testing.T) {
	connectivity := &builtInConnectivity{
		client: &rank2xmpp.Client{},
		clock:  meshCalibratedClockAdapter{source: transport.NewCalibratedClock(nil)},
	}
	profile, err := sessionkernel.DefaultOperationalProfile(sessionkernel.DeploymentConfig{QueueLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	service, failure := (builtInObjectFactory{}).Create(context.Background(), sessionkernel.ObjectBuildContext{
		Identity: sessionkernel.AuthenticatedIdentity{
			AgentID: "agent@example.test", BoundIdentity: "agent@example.test/mesh-one", MeshID: "mesh-one",
		},
		Profile: profile, Connectivity: connectivity,
	})
	if failure != nil || service == nil {
		t.Fatalf("create service=%#v failure=%#v", service, failure)
	}
	provider, ok := service.(sessionkernel.ObjectRuntimeService)
	if !ok || provider.ObjectRuntime() == nil {
		t.Fatalf("object runtime = %#v", service)
	}
	if failure = service.Start(context.Background()); failure != nil {
		t.Fatalf("start = %#v", failure)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if failure = service.Shutdown(ctx); failure != nil {
		t.Fatalf("shutdown = %#v", failure)
	}
}

func TestIdentityDomainRejectsNonBareIdentity(t *testing.T) {
	for _, value := range []string{"", "agent", "@example.test", "agent@", "agent@example.test/mesh"} {
		if domain, ok := identityDomain(value); ok || domain != "" {
			t.Fatalf("identityDomain(%q) = (%q, %v)", value, domain, ok)
		}
	}
	if domain, ok := identityDomain("agent@Example.Test"); !ok || domain != "example.test" {
		t.Fatalf("canonical domain = (%q, %v)", domain, ok)
	}
}
