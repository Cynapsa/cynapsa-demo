package sdkboundary

import (
	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// MapDiagnostics will build a bounded support-safe public diagnostic projection.
func (a *Adapter) MapDiagnostics(snapshot model.DiagnosticSnapshot) (v1.DiagnosticSnapshot, error) {
	if snapshot.CommandQueueDepth > maxQueueCapacity || snapshot.EventQueueDepth > maxQueueCapacity || snapshot.PeerCount > maxQueueCapacity || snapshot.QueuedMessageCount > maxQueueCapacity || snapshot.PendingRPCCount > maxQueueCapacity || snapshot.PayloadTransferCount > maxQueueCapacity {
		return v1.DiagnosticSnapshot{}, malformed("diagnostic counter")
	}
	return v1.DiagnosticSnapshot{
		Status:               a.MapStatus(snapshot.Status),
		CommandQueueDepth:    snapshot.CommandQueueDepth,
		EventQueueDepth:      snapshot.EventQueueDepth,
		PeerCount:            snapshot.PeerCount,
		QueuedMessageCount:   snapshot.QueuedMessageCount,
		PendingRPCCount:      snapshot.PendingRPCCount,
		PayloadTransferCount: snapshot.PayloadTransferCount,
	}, nil
}
