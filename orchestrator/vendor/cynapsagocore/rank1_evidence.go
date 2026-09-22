package cynapsagocore

// rank1EvidenceRecord is identity-free, credential-free qualification data.
// Its emitter is a no-op unless the test-only cynapsa_test_evidence tag is set.
type rank1EvidenceRecord struct {
	Event           string `json:"event"`
	Reason          string `json:"reason,omitempty"`
	ControlReady    bool   `json:"control_ready"`
	DataReady       bool   `json:"data_ready"`
	Suspended       bool   `json:"suspended"`
	Blocked         bool   `json:"blocked"`
	EndpointCount   int    `json:"endpoint_count,omitempty"`
	Connectivity    string `json:"connectivity,omitempty"`
	Reachable       bool   `json:"reachable"`
	RecoveryPending bool   `json:"recovery_pending"`
}
