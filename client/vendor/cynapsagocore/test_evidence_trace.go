package cynapsagocore

type coreEvidenceRecord struct {
	Event         string `json:"event"`
	MessageID     string `json:"message_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	ReplyTo       string `json:"reply_to,omitempty"`
	Mode          string `json:"mode,omitempty"`
	FailureCode   uint8  `json:"failure_code,omitempty"`
	Accepted      bool   `json:"accepted"`
}
