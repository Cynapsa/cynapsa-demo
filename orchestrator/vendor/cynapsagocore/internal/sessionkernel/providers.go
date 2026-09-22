package sessionkernel

import (
	"context"
	"io"
	"time"

	coreclock "github.com/Cynapsa/cynapsagocore/internal/clock"
	"github.com/Cynapsa/cynapsagocore/internal/enrollment"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/payload"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
)

// Personality is selected exactly once by successful authentication.
type Personality string

const (
	PersonalityUnset      Personality = ""
	PersonalityHTTPBridge Personality = "http_bridge"
	PersonalityNative     Personality = "native"
)

// ProviderErrorCode is the complete error vocabulary accepted from injected
// services. It carries no dependency text or arbitrary cause.
type ProviderErrorCode uint8

const (
	ProviderCancelled ProviderErrorCode = iota + 1
	ProviderDeadline
	ProviderAuthenticationRejected
	ProviderUnavailable
	ProviderRejected
	ProviderAuthorizationRejected
	ProviderCapacity
	ProviderInvalidHandle
	ProviderPayloadTooLarge
	ProviderPayloadIntegrity
	ProviderPayloadTransfer
	ProviderInternal
)

// ProviderError is a closed, text-free dependency outcome.
type ProviderError struct{ Code ProviderErrorCode }

func (failure *ProviderError) valid() bool {
	return failure != nil && failure.Code >= ProviderCancelled && failure.Code <= ProviderInternal
}

// Authentication is short-lived provider input. A provider must honor ctx and
// must not retain the password. The kernel clears its own copy when the auth
// operation returns and clears the isolated provider copy when Create returns.
type Authentication struct {
	MeshEndpoint    string
	Username        string
	Password        []byte
	MeshID          string
	AgentInstanceID string
	// SessionResource is the server-issued full-JID resource. Empty preserves
	// the legacy mesh-as-resource binding during migration.
	SessionResource string
	// TokenAuthentication is selected only by the dedicated token commands.
	// Password then contains the extracted server credential, never its wrapper.
	TokenAuthentication bool
	// CredentialUsableUntil is set only for token authentication and is the
	// conservative absolute deadline after which no connection may present it.
	CredentialUsableUntil         time.Time
	CredentialSource              CredentialSource
	SessionExpiryMode             string
	ProfileID                     string
	OfflineStartDeadline          time.Time
	OfflineColdStartTargetSeconds int64
	OfflineTargetSatisfied        bool
	PolicyRevision                int64
	PreparationStatus             string
}

type CredentialSnapshot struct {
	Password    []byte
	UsableUntil time.Time
}

type CredentialSource interface {
	Snapshot(context.Context) (CredentialSnapshot, *ProviderError)
}

// AuthenticatedIdentity separates the application-visible bare identity from
// the private server-bound session identity.
type AuthenticatedIdentity struct {
	AgentID       string
	BoundIdentity string
	MeshID        string
}

// Service is one typed graph component. Calls must honor ctx and return only a
// closed ProviderError. A failed Start cleans its own partial work.
type Service interface {
	Name() string
	Start(context.Context) *ProviderError
	Shutdown(context.Context) *ProviderError
}

// Operation gives one command an exact argument/result pair at compile time.
type Operation[A any, R model.ResultValue] func(context.Context, coreruntime.Services, A) (R, *ProviderError)

// CoreInitOperation additionally receives the boundary-owned SDK session ID.
// It cannot recover that value from a generic context or untyped property bag.
type CoreInitOperation func(context.Context, coreruntime.Services, string, model.EmptyArgs) (model.CoreInitResult, *ProviderError)

// LocalOperations is the closed pre-authentication service partition. A nil
// field means that command is unavailable; no generic handler can be inserted.
type LocalOperations struct {
	CoreInit         CoreInitOperation
	CoreCapabilities Operation[model.EmptyArgs, model.CapabilitiesResult]
	CoreStatus       Operation[model.EmptyArgs, model.StatusResult]
	CoreShutdown     Operation[model.EmptyArgs, model.EmptyResult]
	ConfigGet        Operation[model.EmptyArgs, model.ConfigResult]
	ConfigUpdate     Operation[model.ConfigUpdateArgs, model.ConfigResult]

	CommandChannelRegister Operation[model.ChannelRegisterArgs, model.CompletionChannelResult]
	CommandChannelClear    Operation[model.ChannelIDArgs, model.EmptyResult]
	CommandCancel          Operation[model.CommandCancelArgs, model.EmptyResult]
	EventSinkRegister      Operation[model.EventSinkRegisterArgs, model.EventSinkResult]
	EventSinkClear         Operation[model.EventSinkIDArgs, model.EmptyResult]
	EventSinkBind          Operation[model.EventSinkIDArgs, model.EmptyResult]

	AddressPut     Operation[model.AddressPutArgs, model.EmptyResult]
	AddressRemove  Operation[model.AddressRemoveArgs, model.EmptyResult]
	AddressList    Operation[model.EmptyArgs, model.AddressMappingsResult]
	AddressResolve Operation[model.AddressResolveArgs, model.AddressResolution]

	DeliveryNext        Operation[model.EmptyArgs, model.EventResult]
	DeliveryAccept      Operation[model.DeliveryAcceptArgs, model.EmptyResult]
	DeliveryQueueStatus Operation[model.EmptyArgs, model.DeliveryQueueStatus]
	DeliveryPause       Operation[model.EmptyArgs, model.EmptyResult]
	DeliveryResume      Operation[model.EmptyArgs, model.EmptyResult]
	HandlerRegister     Operation[model.HandlerPathArgs, model.EmptyResult]
	HandlerUnregister   Operation[model.HandlerPathArgs, model.EmptyResult]

	PayloadOpen    Operation[model.EmptyArgs, model.PayloadHandleResult]
	PayloadWrite   Operation[model.PayloadWriteArgs, model.PayloadHandleResult]
	PayloadFinish  Operation[model.PayloadHandleArgs, model.PayloadHandleResult]
	PayloadCancel  Operation[model.PayloadHandleArgs, model.EmptyResult]
	PayloadRead    Operation[model.PayloadReadArgs, model.PayloadHandleResult]
	PayloadClose   Operation[model.PayloadHandleArgs, model.EmptyResult]
	PayloadRetain  Operation[model.PayloadHandleArgs, model.PayloadHandleResult]
	PayloadRelease Operation[model.PayloadHandleArgs, model.EmptyResult]

	PolicySet           Operation[model.PolicySetArgs, model.PolicyResult]
	PolicyGet           Operation[model.EmptyArgs, model.PolicyResult]
	PolicyTest          Operation[model.PolicyTestArgs, model.PolicyResult]
	DiagnosticsSnapshot Operation[model.EmptyArgs, model.DiagnosticSnapshot]
	DiagnosticsLogs     Operation[model.DiagnosticsLogsArgs, model.EmptyResult]
}

// AuthenticatedOperations is the closed post-authentication partition. It is
// not reachable until the graph and runtime readiness commit together.
type AuthenticatedOperations struct {
	Logout         Operation[model.EmptyArgs, model.EmptyResult]
	MeshList       Operation[model.EmptyArgs, model.MeshListResult]
	MeshRefresh    Operation[model.EmptyArgs, model.EmptyResult]
	MessageSend    Operation[model.MessageSendArgs, model.SendResult]
	MessageRequest Operation[model.MessageRequestArgs, model.ResponseResult]
	MessageReply   Operation[model.MessageReplyArgs, model.SendResult]
	DeliveryRetry  Operation[model.MessageIDArgs, model.EmptyResult]
	DeliveryDrop   Operation[model.MessageIDArgs, model.EmptyResult]

	ConversationList   Operation[model.EmptyArgs, model.ConversationListResult]
	ConversationStatus Operation[model.ConversationIDArgs, model.ConversationStatus]
	ConversationClose  Operation[model.ConversationIDArgs, model.EmptyResult]
	DiagnosticsPeer    Operation[model.DiagnosticsPeerArgs, model.PeerStatus]
	DiagnosticsNetwork Operation[model.EmptyArgs, model.ConnectivityStatus]
}

// ConnectivityService is the graph's mandatory authentication and durable
// connectivity owner.
type ConnectivityService interface {
	Service
	AuthenticatedIdentity() (AuthenticatedIdentity, *ProviderError)
	Operations() AuthenticatedOperations
}

// BuildContext passes approved immutable configuration and typed providers to
// the connectivity constructor.
type BuildContext struct {
	Auth         Authentication
	Profile      OperationalProfile
	Clock        coreclock.Clock
	Random       io.Reader
	IdentityKeys IdentityKeyProvider
}

type ConnectivityFactory interface {
	Create(context.Context, BuildContext) (ConnectivityService, *ProviderError)
}

// AuthenticatedBuildContext is available only after connectivity has started
// and its exact authenticated identity has been validated against the request.
// It contains no password or untyped dependency registry.
type AuthenticatedBuildContext struct {
	Identity     AuthenticatedIdentity
	Profile      OperationalProfile
	Clock        coreclock.Clock
	Random       io.Reader
	Connectivity ConnectivityService
	// IdentityKeys is the optional authenticated key-wrapping provider. It is
	// nil in the V1 transport-encrypted plaintext payload mode.
	IdentityKeys IdentityKeyProvider
	// Objects is present only when the started object service exposes the exact
	// prepared-upload, download, and receiver reachability capability.
	Objects ObjectRuntime
}

// AuthenticatedServiceFactory constructs the remaining post-authentication
// graph as one typed service and one closed operation partition. The returned
// service owns any factory resources even before Start and must tolerate
// Shutdown when Create returns a simultaneous failure.
type AuthenticatedServiceFactory interface {
	Create(context.Context, AuthenticatedBuildContext) (Service, AuthenticatedOperations, *ProviderError)
}

// IdentityKeyProvider is exactly compatible with Pod 5's authenticated key
// wrapping seam. Password material is structurally absent.
type IdentityKeyProvider interface {
	payload.KeySealer
	payload.KeyResolver
}

type ObjectBuildContext struct {
	Identity     AuthenticatedIdentity
	Profile      OperationalProfile
	Clock        coreclock.Clock
	Random       io.Reader
	IdentityKeys IdentityKeyProvider
	Connectivity ConnectivityService
}

// ObjectRuntime is the closed payload capability exposed by a started object
// service. Slot discovery and concrete HTTP transport remain provider-owned.
type ObjectRuntime interface {
	payload.PreparedObjectStore
	ProbeDownloadReference(context.Context, string) error
}

// ObjectRuntimeService optionally exposes object payload capability in
// addition to lifecycle ownership. Legacy lifecycle-only providers remain
// valid but cannot enable authenticated offload.
type ObjectRuntimeService interface {
	Service
	ObjectRuntime() ObjectRuntime
}

type ObjectServiceFactory interface {
	Create(context.Context, ObjectBuildContext) (Service, *ProviderError)
}

// Dependencies are private root-facade injection seams.
type Dependencies struct {
	Clock           coreclock.Clock
	Random          io.Reader
	Connectivity    ConnectivityFactory
	Enrollment      enrollment.Provider
	EnrollmentState enrollment.StateStore
	Authenticated   AuthenticatedServiceFactory
	IdentityKeys    IdentityKeyProvider
	Objects         ObjectServiceFactory
	Local           LocalOperations
}

type Snapshot struct {
	Personality      Personality
	AgentID          string
	MeshID           string
	MeshEndpoint     string
	AgentInstanceID  string
	OffloadAvailable bool
	Authenticated    bool
}
