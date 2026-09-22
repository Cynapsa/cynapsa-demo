//go:build cynapsa_test_evidence

package rank1webrtc

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

var healthEvidenceMu sync.Mutex

func emitHealthEvidence(record healthEvidenceRecord) {
	value := struct {
		healthEvidenceRecord
		At string `json:"at"`
	}{healthEvidenceRecord: record, At: time.Now().UTC().Format(time.RFC3339Nano)}
	encoded, err := json.Marshal(value)
	if err != nil {
		return
	}
	healthEvidenceMu.Lock()
	_, _ = fmt.Fprintf(os.Stderr, "CYNAPSA_RANK1_EVIDENCE %s\n", encoded)
	healthEvidenceMu.Unlock()
}
