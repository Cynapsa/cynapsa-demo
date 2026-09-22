package rank2xmpp

import (
	"context"
	"errors"
	"math"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type inboundBudgetSession interface {
	bindInboundBudget(*transport.InboundBudget) error
}

func inboundStanzaBytes(stanza Stanza) (int, bool) {
	total := int64(len(stanza.From)) + int64(len(stanza.To)) + int64(len(stanza.MeshID)) +
		int64(len(stanza.AttemptID)) + int64(len(stanza.TransferID)) + int64(len(stanza.MessageID)) +
		int64(len(stanza.Data)) + int64(len(stanza.Evidence.TransferID)) + int64(len(stanza.Evidence.MessageID))
	if total < 0 || total > math.MaxInt {
		return 0, false
	}
	return int(total), true
}

func clearStanzaOwned(stanza *Stanza) {
	if stanza == nil {
		return
	}
	clear(stanza.Data)
	stanza.inboundAccept.reject()
	if stanza.inboundLease != nil {
		stanza.inboundLease.Release()
	}
	*stanza = Stanza{}
}

func clearEventOwned(event *Event) {
	if event == nil {
		return
	}
	clearStanzaOwned(&event.Stanza)
	if event.inboundLease != nil {
		event.inboundLease.Release()
	}
	*event = Event{}
}

func moveEventStanza(event *Event) Stanza {
	stanza := event.Stanza
	event.Stanza = Stanza{}
	if stanza.inboundLease == nil {
		stanza.inboundLease = event.inboundLease
		event.inboundLease = nil
	}
	return stanza
}

func leaseMailbox(ctx context.Context, stanzas []Stanza, budget *transport.InboundBudget) error {
	if budget == nil {
		return ErrInvalidConfig
	}
	sizes := make([]int, len(stanzas))
	for i, stanza := range stanzas {
		size, ok := inboundStanzaBytes(stanza)
		if !ok {
			return ErrProtocol
		}
		sizes[i] = size
	}
	leases, err := budget.AcquireBatch(ctx, sizes)
	if errors.Is(err, transport.ErrProtocol) {
		return ErrProtocol
	}
	if err != nil {
		return err
	}
	for i := range stanzas {
		stanzas[i].inboundLease = leases[i]
	}
	return nil
}
