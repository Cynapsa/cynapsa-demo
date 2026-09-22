//go:build cynapsa_test_evidence

package cynapsagocore

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

var rank1EvidenceMu sync.Mutex

func emitRank1Evidence(record rank1EvidenceRecord) {
	output := struct {
		rank1EvidenceRecord
		At string `json:"at"`
	}{rank1EvidenceRecord: record, At: time.Now().UTC().Format(time.RFC3339Nano)}
	encoded, err := json.Marshal(output)
	if err != nil {
		return
	}
	rank1EvidenceMu.Lock()
	_, _ = fmt.Fprintf(os.Stderr, "CYNAPSA_RANK1_EVIDENCE %s\n", encoded)
	rank1EvidenceMu.Unlock()
}
