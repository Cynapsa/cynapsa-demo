package v1

// Capability names application behavior without describing its implementation.
type Capability string

const (
	CapabilityNativeMessaging          Capability = "native_messaging"
	CapabilityRPC                      Capability = "rpc"
	CapabilityHTTPBridge               Capability = "http_bridge"
	CapabilityOfflineDelivery          Capability = "offline_delivery"
	CapabilityLargePayloads            Capability = "large_payloads"
	CapabilityPayloadStreamingHandles  Capability = "payload_streaming_handles"
	CapabilityLocalCommandCancellation Capability = "local_command_cancellation"
	CapabilityBoundedQueues            Capability = "bounded_queues"
	CapabilityEventStream              Capability = "event_stream"
)

// Capabilities is an explicit public allowlist returned by the boundary adapter.
type Capabilities struct {
	Commands []CommandName
	Features []Capability
}

// resultType marks Capabilities as an allowlisted result.
func (Capabilities) resultType() {}
