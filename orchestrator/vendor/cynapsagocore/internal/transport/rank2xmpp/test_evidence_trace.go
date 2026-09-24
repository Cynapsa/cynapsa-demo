package rank2xmpp

// rank2EvidenceRecord is compiled into every build so call sites stay typed,
// but its emitter is a no-op unless the production qualification agent is
// built with cynapsa_test_evidence. It contains no credentials or payloads.
type rank2EvidenceRecord struct {
	Event            string `json:"event"`
	Source           string `json:"source,omitempty"`
	Stage            string `json:"stage,omitempty"`
	Caller           string `json:"caller,omitempty"`
	Deadline         string `json:"deadline,omitempty"`
	ElementNamespace string `json:"element_namespace,omitempty"`
	ElementLocal     string `json:"element_local,omitempty"`
	StanzaID         string `json:"stanza_id,omitempty"`
	StanzaType       string `json:"stanza_type,omitempty"`
	From             string `json:"from,omitempty"`
	To               string `json:"to,omitempty"`
	ClientGeneration uint64 `json:"client_generation,omitempty"`
	StateEpoch       uint64 `json:"state_epoch,omitempty"`
	SessionEpoch     uint64 `json:"session_epoch,omitempty"`
	WireGeneration   uint64 `json:"wire_generation,omitempty"`
	Attempt          int    `json:"attempt,omitempty"`
	State            uint8  `json:"state,omitempty"`
	Current          bool   `json:"current"`
	Admitted         bool   `json:"admitted"`
	Reconnect        bool   `json:"reconnect"`
	Resumed          bool   `json:"resumed"`
	Correlated       bool   `json:"correlated,omitempty"`
	Handled          bool   `json:"handled,omitempty"`
}
