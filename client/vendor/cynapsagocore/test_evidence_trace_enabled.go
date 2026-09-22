//go:build cynapsa_test_evidence

package cynapsagocore

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

var coreEvidenceOutputMu sync.Mutex

func emitCoreEvidence(record coreEvidenceRecord) {
	output := struct {
		coreEvidenceRecord
		At string `json:"at"`
	}{coreEvidenceRecord: record, At: time.Now().UTC().Format(time.RFC3339Nano)}
	encoded, err := json.Marshal(output)
	if err != nil {
		return
	}
	coreEvidenceOutputMu.Lock()
	_, _ = fmt.Fprintf(os.Stderr, "CYNAPSA_CORE_EVIDENCE %s\n", encoded)
	coreEvidenceOutputMu.Unlock()
}
