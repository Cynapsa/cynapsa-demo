package v1

const (
	CommandDiagnosticsPeer         CommandName = "diagnostics.peer_status"
	CommandDiagnosticsConnectivity CommandName = "diagnostics.connectivity_status"
	CommandDiagnosticsSnapshot     CommandName = "diagnostics.snapshot"
	CommandDiagnosticsLogs         CommandName = "diagnostics.logs.subscribe"
)

type DiagnosticsPeerCommand struct {
	CommandBase
	Peer AgentID
}

func (DiagnosticsPeerCommand) Name() CommandName { return CommandDiagnosticsPeer }
func (DiagnosticsPeerCommand) commandType()      {}

type DiagnosticsConnectivityCommand struct{ CommandBase }

func (DiagnosticsConnectivityCommand) Name() CommandName { return CommandDiagnosticsConnectivity }
func (DiagnosticsConnectivityCommand) commandType()      {}

type DiagnosticsSnapshotCommand struct{ CommandBase }

func (DiagnosticsSnapshotCommand) Name() CommandName { return CommandDiagnosticsSnapshot }
func (DiagnosticsSnapshotCommand) commandType()      {}

type DiagnosticsLogsCommand struct {
	CommandBase
	Enabled bool
}

func (DiagnosticsLogsCommand) Name() CommandName { return CommandDiagnosticsLogs }
func (DiagnosticsLogsCommand) commandType()      {}

// PeerStatus is the normalized public peer view.
type PeerStatus struct {
	Peer               AgentID
	Connectivity       ConnectivityState
	Reachable          bool
	RecoveryInProgress bool
}

func (PeerStatus) resultType() {}

// ConnectivityStatus contains normalized local connectivity only.
type ConnectivityStatus struct {
	State ConnectivityState
}

func (ConnectivityStatus) resultType() {}

// DiagnosticSnapshot contains bounded support-safe public counters.
type DiagnosticSnapshot struct {
	Status            Status
	CommandQueueDepth uint64
	EventQueueDepth   uint64
	// PeerCount is the number of remote members in Core's complete current
	// synchronized membership snapshot, excluding the authenticated local
	// member. It is neither a server account-roster count nor a connectivity or
	// reachability count.
	PeerCount            uint64
	QueuedMessageCount   uint64
	PendingRPCCount      uint64
	PayloadTransferCount uint64
}

func (DiagnosticSnapshot) resultType() {}
