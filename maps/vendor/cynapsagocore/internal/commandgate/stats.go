package commandgate

// Stats is a race-safe bounded-resource snapshot for private diagnostics.
type Stats struct {
	Capacity             int
	ByteCapacity         int64
	OwnedBytes           int64
	CommandQueueDepth    int
	CompletionQueueDepth int
	RegistryEntries      int
	Closing              bool
	Closed               bool
}

// Stats returns gate-owned queue and registry accounting.
func (g *Gate) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	completionDepth := int(g.completionDepth.Load())
	return Stats{
		Capacity:             cap(g.commands),
		ByteCapacity:         g.maximumOwnedBytes,
		OwnedBytes:           g.ownedBytes.Load(),
		CommandQueueDepth:    len(g.commands),
		CompletionQueueDepth: completionDepth,
		RegistryEntries:      g.registry.Len(),
		Closing:              g.state != gateOpen,
		Closed:               g.state == gateClosed,
	}
}
