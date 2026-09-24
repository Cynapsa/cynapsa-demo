package mesh

import "github.com/Cynapsa/cynapsagocore/internal/rpc"

// ReportServerRoutingFailure consumes one already-validated, server-originated
// XMPP message error. It never revokes the peer or closes the mesh session:
// only the exact rejected message and its outstanding RPC waiter are affected.
func (service *MessagingService) ReportServerRoutingFailure(messageID, condition string) {
	if service == nil || messageID == "" {
		return
	}
	var cause error
	switch condition {
	case "forbidden", "not-authorized", "not-allowed", "policy-violation", "registration-required":
		cause = rpc.ErrAuthorizationRejected
	case "resource-constraint":
		cause = rpc.ErrCapacity
	case "bad-request", "jid-malformed", "not-acceptable", "unexpected-request":
		cause = rpc.ErrInvalidResponse
	default:
		cause = rpc.ErrServerUnavailable
	}
	// Terminal server evidence must stop outbox replay even if Rank1 or Rank2
	// previously borrowed the same immutable message.
	_ = service.outbox.RetireServerRejected(messageID)
	correlation := service.outboundRPC.FailMessageID(messageID, cause)
	if correlation != "" {
		retained, _ := service.takeResponseValue(correlation)
		zeroModelPayload(retained.value)
	}
	service.wakeDrain()
}
