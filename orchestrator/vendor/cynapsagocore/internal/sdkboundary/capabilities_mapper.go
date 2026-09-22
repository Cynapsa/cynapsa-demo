package sdkboundary

import v1 "github.com/Cynapsa/cynapsagocore/api/v1"

// PublicCapabilities returns the maximum static V1 allowlist. The Core facade
// filters optional features through its closed construction-time availability
// state before returning core.capabilities; this method does not claim that an
// optional provider is installed in a particular Core.
func (a *Adapter) PublicCapabilities() v1.Capabilities {
	return v1.Capabilities{
		Commands: []v1.CommandName{
			v1.CommandCoreInit, v1.CommandCoreCapabilities, v1.CommandCoreStatus, v1.CommandCoreShutdown,
			v1.CommandConfigGet, v1.CommandConfigUpdate, v1.CommandChannelRegister, v1.CommandChannelClear,
			v1.CommandCancel, v1.CommandEventSinkRegister, v1.CommandEventSinkClear, v1.CommandEventSinkBind,
			v1.CommandAuthLogin, v1.CommandAuthConnect, v1.CommandAuthTokenLogin, v1.CommandAuthTokenConnect, v1.CommandAuthInstallationLogin, v1.CommandAuthInstallationConnect, v1.CommandAuthLogout, v1.CommandAuthAgentID,
			v1.CommandMeshList, v1.CommandMeshRefresh,
			v1.CommandAddressPut, v1.CommandAddressRemove, v1.CommandAddressList, v1.CommandAddressResolve,
			v1.CommandMessageSend, v1.CommandMessageRequest, v1.CommandMessageReply,
			v1.CommandDeliveryNext, v1.CommandDeliveryAccept, v1.CommandHandlerRegister, v1.CommandHandlerUnregister,
			v1.CommandPayloadOpen, v1.CommandPayloadWrite, v1.CommandPayloadFinish, v1.CommandPayloadCancel,
			v1.CommandPayloadRead, v1.CommandPayloadClose, v1.CommandPayloadRetain, v1.CommandPayloadRelease,
			v1.CommandDeliveryQueueStatus, v1.CommandDeliveryRetry, v1.CommandDeliveryPause,
			v1.CommandDeliveryResume, v1.CommandDeliveryDrop,
			v1.CommandConversationList, v1.CommandConversationStatus, v1.CommandConversationClose,
			v1.CommandPolicySet, v1.CommandPolicyGet, v1.CommandPolicyTest,
			v1.CommandDiagnosticsPeer, v1.CommandDiagnosticsConnectivity, v1.CommandDiagnosticsSnapshot,
			v1.CommandDiagnosticsLogs,
		},
		Features: []v1.Capability{
			v1.CapabilityNativeMessaging,
			v1.CapabilityRPC,
			v1.CapabilityHTTPBridge,
			v1.CapabilityOfflineDelivery,
			v1.CapabilityLargePayloads,
			v1.CapabilityPayloadStreamingHandles,
			v1.CapabilityLocalCommandCancellation,
			v1.CapabilityBoundedQueues,
			v1.CapabilityEventStream,
		},
	}
}
