package sdkboundary

import (
	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// MapStatus will normalize every internal state into the bounded public vocabulary.
func (a *Adapter) MapStatus(status model.Status) v1.Status {
	public := v1.Status{
		Lifecycle:          publicLifecycle(status.Lifecycle),
		Connectivity:       publicConnectivity(status.ConnectivityDetail),
		Personality:        publicPersonality(status.Personality),
		AgentID:            v1.AgentID(status.AgentID),
		MeshID:             v1.MeshID(status.MeshID),
		MeshEndpoint:       status.MeshEndpoint,
		QueuedMessageCount: status.QueuedMessageCount,
	}
	if len(public.AgentID) > maxIdentifierLength || len(public.MeshID) > maxIdentifierLength || len(public.MeshEndpoint) > maxPublicString || public.QueuedMessageCount > maxQueueCapacity {
		public.AgentID = ""
		public.MeshID = ""
		public.MeshEndpoint = ""
		public.QueuedMessageCount = 0
		public.Lifecycle = v1.LifecycleFailed
	}
	return public
}

func publicLifecycle(state model.LifecycleState) v1.LifecycleState {
	switch state {
	case model.LifecycleCreated:
		return v1.LifecycleCreated
	case model.LifecycleAuthenticating, model.LifecycleMeshConnected, model.LifecycleDurableReady, model.LifecyclePeerLinkBuilding:
		return v1.LifecycleConnecting
	case model.LifecycleReady:
		return v1.LifecycleReady
	case model.LifecycleDegraded:
		return v1.LifecycleDegraded
	case model.LifecycleClosing:
		return v1.LifecycleClosing
	case model.LifecycleClosed:
		return v1.LifecycleClosed
	case model.LifecycleFailed:
		return v1.LifecycleFailed
	default:
		return v1.LifecycleFailed
	}
}

func publicConnectivity(detail string) v1.ConnectivityState {
	switch detail {
	case "available":
		return v1.ConnectivityAvailable
	case "degraded":
		return v1.ConnectivityDegraded
	case "unavailable":
		return v1.ConnectivityUnavailable
	default:
		return v1.ConnectivityUnknown
	}
}

func publicPersonality(personality string) v1.SDKPersonality {
	switch personality {
	case "http_bridge":
		return v1.SDKPersonalityHTTPBridge
	case "native":
		return v1.SDKPersonalityNative
	default:
		return v1.SDKPersonalityUnset
	}
}
