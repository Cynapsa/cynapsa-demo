package cynapsagocore

import (
	"context"
	"testing"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/commandgate"
	"github.com/Cynapsa/cynapsagocore/internal/diagnostics"
	"github.com/Cynapsa/cynapsagocore/internal/mesh"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
	"github.com/Cynapsa/cynapsagocore/internal/sdkboundary"
	"github.com/Cynapsa/cynapsagocore/internal/sessionkernel"
)

type localDiagnosticServices struct{ status model.Status }

func (*localDiagnosticServices) Transition(model.LifecycleState) error { return nil }
func (*localDiagnosticServices) CommitAuthenticated(context.Context, coreruntime.AuthenticatedPublisher) error {
	return nil
}
func (*localDiagnosticServices) PublishEvent(context.Context, model.Event) error { return nil }
func (services *localDiagnosticServices) Status() model.Status                   { return services.status }
func (*localDiagnosticServices) Diagnostics() diagnostics.Snapshot {
	return diagnostics.Snapshot{Observation: diagnostics.Observation{CommandQueueDepth: 2, EventQueueDepth: 3}}
}

type fixedMessagingDiagnostics authenticatedDiagnosticSnapshot

func (source fixedMessagingDiagnostics) diagnosticSnapshot() authenticatedDiagnosticSnapshot {
	return authenticatedDiagnosticSnapshot(source)
}

func TestProviderMeshFailurePreservesLocalAuthorizationRejection(t *testing.T) {
	failure := providerMeshFailure(&mesh.Failure{Code: mesh.FailureAuthorization})
	if failure == nil || failure.Code != sessionkernel.ProviderAuthorizationRejected {
		t.Fatalf("authorization failure = %#v", failure)
	}
	if generic := providerMeshFailure(&mesh.Failure{Code: mesh.FailureRejected}); generic == nil || generic.Code != sessionkernel.ProviderRejected {
		t.Fatalf("generic rejection = %#v", generic)
	}
}

func TestProviderPolicyPayloadErrorPreservesLocalAuthorizationRejection(t *testing.T) {
	failure := providerPolicyPayloadError(payload.ErrAuthorization)
	if failure == nil || failure.Code != sessionkernel.ProviderAuthorizationRejected {
		t.Fatalf("authorization failure = %#v", failure)
	}
}

func TestLocalCapabilitiesIncludeLargePayloadsOnlyWhenConstructed(t *testing.T) {
	adapter, err := sdkboundary.New()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		available bool
	}{
		{name: "deferred", available: false},
		{name: "provider installed", available: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			services := &localServices{adapter: adapter, availability: localCapabilityAvailability{largePayloads: test.available}}
			result, failure := services.capabilities(context.Background(), nil, model.EmptyArgs{})
			if failure != nil {
				t.Fatalf("capabilities failure = %#v", failure)
			}
			found := false
			for _, feature := range result.Features {
				if feature == string(v1.CapabilityLargePayloads) {
					found = true
				}
			}
			if found != test.available {
				t.Fatalf("large-payload capability present=%t want=%t", found, test.available)
			}
		})
	}
}

func TestLocalDiagnosticsMergeAuthenticatedCountersWithoutChangingPublicShape(t *testing.T) {
	bridge := &messagingRuntimeBridge{}
	source := fixedMessagingDiagnostics{peerCount: 4, queuedMessages: 5, pendingRPC: 6, payloadTransfers: 7}
	if !bridge.registerDiagnostics(source) {
		t.Fatal("register diagnostics")
	}
	services := &localServices{diagnosticsFeed: bridge}
	runtimeServices := &localDiagnosticServices{status: model.Status{Lifecycle: model.LifecycleReady}}
	result, failure := services.diagnostics(context.Background(), runtimeServices, model.EmptyArgs{})
	if failure != nil {
		t.Fatalf("diagnostics failure = %#v", failure)
	}
	if result.CommandQueueDepth != 2 || result.EventQueueDepth != 3 || result.PeerCount != 4 || result.QueuedMessageCount != 5 || result.PendingRPCCount != 6 || result.PayloadTransferCount != 7 || result.Status.QueuedMessageCount != 5 {
		t.Fatalf("diagnostics result = %#v", result)
	}
	bridge.unregisterDiagnostics(source)
	if snapshot := bridge.diagnosticSnapshot(); snapshot != (authenticatedDiagnosticSnapshot{}) {
		t.Fatalf("unregistered diagnostics = %#v", snapshot)
	}
}

func TestLocalCommandCancelConsumesExactGateCapability(t *testing.T) {
	gate, err := commandgate.New(2)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := gate.Submit(context.Background(), model.Command{ID: "target", Name: "core.status", SessionID: "session", Args: model.EmptyArgs{}})
	if err != nil || !admission.Accepted {
		t.Fatalf("admission = %#v, %v", admission, err)
	}
	services := &localServices{gate: gate}
	result, failure := services.commandCancel(context.Background(), nil, model.CommandCancelArgs{CommandHandle: string(admission.CommandHandle)})
	if failure != nil || result != (model.EmptyResult{}) {
		t.Fatalf("cancel result = %#v, %#v", result, failure)
	}
	completion, err := gate.NextCompletion(context.Background())
	if err != nil || completion.CommandID != "target" || completion.Err == nil {
		t.Fatalf("target completion = %#v, %v", completion, err)
	}
	_, failure = services.commandCancel(context.Background(), nil, model.CommandCancelArgs{CommandHandle: string(admission.CommandHandle)})
	if failure == nil || failure.Code != sessionkernel.ProviderInvalidHandle {
		t.Fatalf("second cancel failure = %#v", failure)
	}
}

func TestLocalCommandCancelRejectsCrossCoreCapability(t *testing.T) {
	owner, err := commandgate.New(1)
	if err != nil {
		t.Fatal(err)
	}
	other, err := commandgate.New(1)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := owner.Submit(context.Background(), model.Command{ID: "target", Name: "core.status", SessionID: "session", Args: model.EmptyArgs{}})
	if err != nil {
		t.Fatal(err)
	}
	_, failure := (&localServices{gate: other}).commandCancel(context.Background(), nil, model.CommandCancelArgs{CommandHandle: string(admission.CommandHandle)})
	if failure == nil || failure.Code != sessionkernel.ProviderInvalidHandle {
		t.Fatalf("cross-core cancel failure = %#v", failure)
	}
}
