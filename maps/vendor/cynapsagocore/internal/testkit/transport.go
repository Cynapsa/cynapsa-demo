package testkit

import (
	"errors"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"github.com/Cynapsa/cynapsagocore/internal/transport/loopback"
)

// Fault describes one deterministic private delivery fault.
type Fault struct {
	Kind  string
	Count int
}

// InjectFault will configure a test-only fault on a fake adapter.
func InjectFault(target any, fault Fault) error {
	controller, ok := target.(*loopback.Controller)
	if !ok || controller == nil || fault.Count < 1 || fault.Count > transport.MaximumReceiveQueue {
		return transport.ErrInvalidConfig
	}
	var mapped loopback.Fault
	switch fault.Kind {
	case "drop":
		mapped = loopback.FaultDrop
	case "duplicate":
		mapped = loopback.FaultDuplicate
	case "hold":
		mapped = loopback.FaultHold
	case "reject":
		mapped = loopback.FaultReject
	case "ambiguous":
		mapped = loopback.FaultAmbiguous
	case "block":
		mapped = loopback.FaultBlock
	default:
		return errors.New("testkit: unknown fault kind")
	}
	faults := make([]loopback.Fault, fault.Count)
	for i := range faults {
		faults[i] = mapped
	}
	return controller.Set(faults...)
}
