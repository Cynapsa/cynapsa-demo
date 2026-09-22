package rank2xmpp

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"sort"
	"unsafe"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

const maximumReplayStanzas = 65536

type replayDigestFunc func(Stanza) [sha256.Size]byte

type replayDataRange struct {
	start uintptr
	end   uintptr
}

// cloneStanzas is retained for test fixtures that need an independent owner.
// Production replay transforms below consume their inputs instead.
func cloneStanzas(stanzas []Stanza) []Stanza {
	result := make([]Stanza, len(stanzas))
	for i := range stanzas {
		result[i] = stanzas[i].clone()
	}
	return result
}

// applicationReplay consumes stanzas. Retained entries are moved in place and
// session-local control records are zeroized before they are discarded.
func applicationReplay(stanzas []Stanza) ([]Stanza, error) {
	if err := validateReplayGraph(stanzas, nil); err != nil {
		clearStanzas(stanzas)
		return nil, err
	}
	return applicationReplayValidated(stanzas), nil
}

func applicationReplayValidated(stanzas []Stanza) []Stanza {
	write := 0
	for read := range stanzas {
		if isSessionControlStanza(stanzas[read].Kind) {
			clearStanzaOwned(&stanzas[read])
			continue
		}
		if write != read {
			stanzas[write] = stanzas[read]
			stanzas[read] = Stanza{}
		}
		write++
	}
	return stanzas[:write]
}

// filterReplayMesh consumes stanzas. It preserves exact order while
// moving accepted records in place and zeroizing every rejected record.
func filterReplayMesh(stanzas []Stanza, meshID string) ([]Stanza, error) {
	if err := validateReplayGraph(stanzas, nil); err != nil {
		clearStanzas(stanzas)
		return nil, err
	}
	return filterReplayMeshValidated(stanzas, meshID), nil
}

func filterReplayMeshValidated(stanzas []Stanza, meshID string) []Stanza {
	write := 0
	for read := range stanzas {
		if !replayMeshAllowed(stanzas[read], meshID) {
			clearStanzaOwned(&stanzas[read])
			continue
		}
		if write != read {
			stanzas[write] = stanzas[read]
			stanzas[read] = Stanza{}
		}
		write++
	}
	return stanzas[:write]
}

// filterReplayAuthority consumes clean-session replay after the replacement
// snapshot has been installed. Session-local controls were already removed;
// every retained application record must originate at the exact local
// resource and target a peer still present in that snapshot.
func filterReplayAuthority(stanzas []Stanza, members []string, local, meshID string) ([]Stanza, error) {
	if err := validateReplayGraph(stanzas, nil); err != nil {
		clearStanzas(stanzas)
		return nil, err
	}
	if protocol.ValidateAgentIdentity(local) != nil || protocol.ValidateMeshID(meshID) != nil {
		clearStanzas(stanzas)
		return nil, ErrStreamManagement
	}
	current := make(map[string]struct{}, len(members))
	localFound := false
	for _, member := range members {
		if protocol.ValidateAgentIdentity(member) != nil {
			clearStanzas(stanzas)
			return nil, ErrStreamManagement
		}
		current[member] = struct{}{}
		localFound = localFound || member == local
	}
	if !localFound {
		clearStanzas(stanzas)
		return nil, ErrStreamManagement
	}
	write := 0
	for read := range stanzas {
		record := &stanzas[read]
		_, peerCurrent := current[record.To]
		if record.From != local || !peerCurrent || !replayMeshAllowed(*record, meshID) {
			clearStanzaOwned(record)
			continue
		}
		if write != read {
			stanzas[write] = *record
			*record = Stanza{}
		}
		write++
	}
	return stanzas[:write], nil
}

func filterOwnedReplayAuthority(owned []outbox.Rank2Metadata, members []string) []outbox.Rank2Metadata {
	current := make(map[string]struct{}, len(members))
	for _, member := range members {
		current[member] = struct{}{}
	}
	write := 0
	for read := range owned {
		if _, ok := current[owned[read].Recipient]; !ok {
			continue
		}
		if write != read {
			owned[write] = owned[read]
		}
		write++
	}
	return owned[:write]
}

// mergeReplay consumes both inputs. Their aggregate payload, variable fields,
// and conservative per-record retention charge share one 256 MiB/65,536-
// record budget. Payload bytes are moved, never cloned. Fixed-size digest and
// index metadata is separately count-bounded by that same record ceiling;
// exact comparison within each digest chain makes collisions harmless.
func mergeReplay(primary, extra []Stanza) ([]Stanza, error) {
	return mergeReplayDigest(primary, extra, stanzaReplayDigest)
}

func mergeReplayDigest(primary, extra []Stanza, digest replayDigestFunc) ([]Stanza, error) {
	if digest == nil {
		clearStanzas(primary)
		clearStanzas(extra)
		return nil, ErrStreamManagement
	}
	if err := validateReplayGraph(primary, extra); err != nil {
		clearStanzas(primary)
		clearStanzas(extra)
		return nil, err
	}
	total := len(primary) + len(extra)

	seen := make(map[[sha256.Size]byte]int, total)
	previous := make([]int, 0, total)
	result := make([]Stanza, 0, total)
	for _, group := range [][]Stanza{primary, extra} {
		for i := range group {
			value := &group[i]
			key := digest(*value)
			duplicate := false
			for link := seen[key]; link != 0; link = previous[link-1] {
				if sameManagedStanza(result[link-1], *value) {
					duplicate = true
					break
				}
			}
			if duplicate {
				clearStanzaOwned(value)
				continue
			}
			previous = append(previous, seen[key])
			result = append(result, *value)
			*value = Stanza{}
			seen[key] = len(result)
		}
	}
	return result, nil
}

// validateReplayGraph validates every owner before a transform can
// selectively clear or move even one record. It applies the one aggregate
// replay budget and rejects overlapping payload or inbound-lease ownership.
func validateReplayGraph(primary, extra []Stanza) error {
	if len(primary) > maximumReplayStanzas || len(extra) > maximumReplayStanzas {
		return ErrQueueFull
	}
	total := len(primary) + len(extra)
	if total > maximumReplayStanzas {
		return ErrQueueFull
	}
	var workspaceBytes int64
	for _, group := range [][]Stanza{primary, extra} {
		for i := range group {
			if group[i].Kind == StanzaEnvelope && len(group[i].Data) != 0 {
				return ErrStreamManagement
			}
			charge, ok := retainedStanzaBytes(group[i])
			if !ok || charge > MaximumStreamManagementBytes-workspaceBytes {
				return ErrQueueFull
			}
			workspaceBytes += charge
		}
	}
	if !validReplayOwnership(primary, extra) {
		return ErrStreamManagement
	}
	return nil
}

func stanzaReplayDigest(stanza Stanza) [sha256.Size]byte {
	digest := sha256.New()
	writeDigestUint64(digest, uint64(stanza.Kind))
	writeDigestString(digest, stanza.From)
	writeDigestString(digest, stanza.To)
	writeDigestString(digest, stanza.MeshID)
	writeDigestUint64(digest, stanza.Ordinal)
	writeDigestString(digest, stanza.AttemptID)
	writeDigestString(digest, stanza.TransferID)
	writeDigestString(digest, stanza.MessageID)
	if stanza.Kind != StanzaEnvelope {
		writeDigestBytes(digest, stanza.Data)
	}
	writeDigestString(digest, stanza.Evidence.TransferID)
	writeDigestString(digest, stanza.Evidence.MessageID)
	writeDigestBytes(digest, stanza.Evidence.Digest[:])
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result
}

func writeDigestUint64(digest hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = digest.Write(encoded[:])
}

func writeDigestString(digest hash.Hash, value string) {
	writeDigestUint64(digest, uint64(len(value)))
	if len(value) != 0 {
		// Hash the immutable string storage directly; converting a maximum-size
		// metadata field to []byte would recreate the amplification this path is
		// specifically intended to avoid.
		_, _ = digest.Write(unsafe.Slice(unsafe.StringData(value), len(value)))
	}
}

func writeDigestBytes(digest hash.Hash, value []byte) {
	writeDigestUint64(digest, uint64(len(value)))
	_, _ = digest.Write(value)
}

// validReplayOwnership rejects overlapping source storage before any move or
// selective clear. Production snapshots satisfy this unique-owner contract;
// rejecting an injected alias prevents clearing one record from corrupting a
// different accepted record.
func validReplayOwnership(primary, extra []Stanza) bool {
	if stanzaSlicesOverlap(primary, extra) {
		return false
	}
	ranges := make([]replayDataRange, 0, len(primary)+len(extra))
	var leases map[*transport.InboundLease]struct{}
	for _, group := range [][]Stanza{primary, extra} {
		for i := range group {
			if lease := group[i].inboundLease; lease != nil {
				if leases == nil {
					leases = make(map[*transport.InboundLease]struct{})
				}
				if _, duplicate := leases[lease]; duplicate {
					return false
				}
				leases[lease] = struct{}{}
			}
			data := group[i].Data
			if len(data) == 0 {
				continue
			}
			start := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
			end := start + uintptr(len(data))
			if end < start {
				return false
			}
			ranges = append(ranges, replayDataRange{start: start, end: end})
		}
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].start < ranges[j].start })
	for i := 1; i < len(ranges); i++ {
		if ranges[i].start < ranges[i-1].end {
			return false
		}
	}
	return true
}

func stanzaSlicesOverlap(primary, extra []Stanza) bool {
	if len(primary) == 0 || len(extra) == 0 {
		return false
	}
	primaryStart := uintptr(unsafe.Pointer(unsafe.SliceData(primary)))
	extraStart := uintptr(unsafe.Pointer(unsafe.SliceData(extra)))
	primaryEnd := primaryStart + uintptr(len(primary))*unsafe.Sizeof(Stanza{})
	extraEnd := extraStart + uintptr(len(extra))*unsafe.Sizeof(Stanza{})
	return primaryEnd >= primaryStart && extraEnd >= extraStart && primaryStart < extraEnd && extraStart < primaryEnd
}
