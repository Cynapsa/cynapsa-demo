package diagnostics

// Observation contains one point-in-time set of bounded resource counters.
type Observation struct {
	CommandQueueDepth      int
	CompletionQueueDepth   int
	CommandRegistryEntries int
	EventQueueDepth        int
	EventQueueCapacity     int
	WorkerCount            int
	TimerCount             int
	PeerCount              int
	QueuedMessageCount     uint64
	PendingRPCCount        uint64
	PayloadTransferCount   uint64
	Private                map[string]any
}

// Snapshot contains detailed private runtime observations.
type Snapshot struct {
	Observation
	Metrics map[Metric]float64
}

// Capture copies and redacts one point-in-time private diagnostic snapshot.
func Capture(observation Observation, metrics *Metrics) Snapshot {
	observation.Private = Redact(observation.Private)
	snapshot := Snapshot{Observation: observation, Metrics: map[Metric]float64{}}
	if metrics != nil {
		snapshot.Metrics = metrics.Snapshot()
	}
	return snapshot
}
