package rank1webrtc

// healthEvidenceRecord is bounded, identity-free qualification data. Its
// emitter is a no-op unless the test-only cynapsa_test_evidence tag is set.
type healthEvidenceRecord struct {
	Event             string `json:"event"`
	Outcome           string `json:"outcome,omitempty"`
	ChannelState      string `json:"channel_state,omitempty"`
	ClockReady        bool   `json:"clock_ready"`
	AuthorityAdmitted bool   `json:"authority_admitted"`
	Matched           bool   `json:"matched"`
	ProgressAgeMS     int64  `json:"progress_age_ms"`
}
