package rank2xmpp

import (
	"context"
	"encoding/xml"
	"errors"

	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

// Mellium hands an IQ reply to Serve when its SendIQ caller was canceled.
// Retain exact, locally generated authorization IDs so such a reply can be
// validated and discarded without granting peer authority or killing XMPP.
// The ledger is bounded like the issued-Jingle tombstone ledger.
const issuedPeerAuthorizationCapacity = 4096

type issuedPeerAuthorization struct {
	requested string
	record    Stanza
}

func (s *melliumSession) rememberIssuedPeerAuthorization(session *xmpp.Session, management *StreamManagement, requested string, record Stanza) error {
	if s == nil || session == nil || management == nil || record.Kind != StanzaPeerAuthorization ||
		record.MessageID == "" || !canonicalPeerRequest(requested) {
		return ErrInvalidConfig
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.suspended || s.session != session || s.management != management {
		return ErrUnavailable
	}
	if s.issuedPeerAuthorizations == nil {
		s.issuedPeerAuthorizations = make(map[string]issuedPeerAuthorization)
	}
	if _, exists := s.issuedPeerAuthorizations[record.MessageID]; exists {
		return ErrProtocol
	}
	s.issuedPeerAuthorizations[record.MessageID] = issuedPeerAuthorization{requested: requested, record: record}
	s.issuedPeerAuthorizationIDs = append(s.issuedPeerAuthorizationIDs, record.MessageID)
	if len(s.issuedPeerAuthorizationIDs) > issuedPeerAuthorizationCapacity {
		oldest := s.issuedPeerAuthorizationIDs[0]
		s.issuedPeerAuthorizationIDs[0] = ""
		s.issuedPeerAuthorizationIDs = s.issuedPeerAuthorizationIDs[1:]
		delete(s.issuedPeerAuthorizations, oldest)
	}
	return nil
}

func (s *melliumSession) lookupIssuedPeerAuthorization(id string) (Stanza, string, bool) {
	if s == nil || id == "" {
		return Stanza{}, "", false
	}
	s.mu.Lock()
	issued, ok := s.issuedPeerAuthorizations[id]
	s.mu.Unlock()
	if !ok || issued.record.Kind != StanzaPeerAuthorization || issued.record.MessageID != id ||
		!canonicalPeerRequest(issued.requested) {
		return Stanza{}, "", false
	}
	return issued.record, issued.requested, true
}

func (s *melliumSession) clearIssuedPeerAuthorizationsLocked() {
	clear(s.issuedPeerAuthorizations)
	s.issuedPeerAuthorizations = nil
	clear(s.issuedPeerAuthorizationIDs)
	s.issuedPeerAuthorizationIDs = nil
}

func (s *melliumSession) handleLatePeerAuthorizationResult(ctx context.Context, source xml.TokenReader, outer xml.StartElement, iq stanza.IQ, owner bareServerResultOwnership) error {
	if s == nil || ctx == nil || source == nil || owner.kind != bareServerResultPeerAuthorization ||
		owner.management == nil || owner.record.Kind != StanzaPeerAuthorization || owner.record.MessageID != iq.ID {
		return ErrInvalidConfig
	}
	server, serverErr := jid.Parse(owner.record.To)
	local, localErr := jid.Parse(owner.record.From)
	if serverErr != nil || localErr != nil {
		return ErrProtocol
	}
	// A result may arrive after the original waiter has returned. The exact
	// issued ID and original requested peer are both checked; this path never
	// publishes the decoded peer or treats the response as a fresh handshake.
	_, correlated, handled, decodeErr := decodeAuthorizedPeer(
		&prefixedTokenReader{first: outer, source: source}, iq.ID, server, local,
		owner.requested, s.config.StanzaBudgetBytes, true,
	)
	if !correlated || !handled || decodeErr != nil &&
		!errors.Is(decodeErr, ErrAuthentication) && !errors.Is(decodeErr, ErrUnavailable) {
		return ErrProtocol
	}
	s.mu.Lock()
	current := !s.closed && !s.suspended && s.generation == owner.generation && s.management == owner.management
	s.mu.Unlock()
	if !current {
		return ErrUnavailable
	}
	ordinal, count, err := owner.management.ConfirmCorrelatedHandledRecord(owner.record)
	if err != nil {
		return err
	}
	owner.management.MarkHandledInbound()
	if count > 0 {
		if err = s.emit(ctx, Event{Kind: EventHandled, HandledThrough: ordinal, HandledCount: count}); err != nil {
			return err
		}
	}
	emitRank2Evidence(rank2EvidenceRecord{Event: "late_peer_authorization_consumed", Source: "control", Stage: "exact_issued_id", StanzaID: iq.ID, Handled: true}, nil)
	return nil
}
