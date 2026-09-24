package rank2xmpp

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"time"

	"mellium.im/xmlstream"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	clientIdlePingInterval = 10 * time.Second
	clientPingTimeout      = 10 * time.Second
)

type sessionLivenessProbe interface {
	PingServer(context.Context) error
}

type queuedSessionLivenessProbe interface {
	PingServerAfterQueue(context.Context, time.Duration) error
}

// keepaliveLoop is owned by Go Core, never Python. A healthy idle stream is
// probed after 10 seconds; an absent authenticated answer for 10 more seconds
// closes the exact session and starts normal recovery. The server separately
// owns its own half-open detection and XEP-0198 resume grace period.
func (c *Client) keepaliveLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-c.ctx.Done():
			return
		}
		c.mu.Lock()
		if !c.config.DynamicPeerAuthority || !c.started || c.closed || c.state != DurableLive || !c.membershipReady || c.ingress == nil || c.session == nil || c.clock.Now().Sub(c.progress) < clientIdlePingInterval {
			c.mu.Unlock()
			continue
		}
		ingress, session := c.ingress, c.session
		c.mu.Unlock()
		probe, ok := session.(sessionLivenessProbe)
		if !ok {
			continue // injected legacy sessions do not implement wire probing
		}
		var err error
		if queued, ok := session.(queuedSessionLivenessProbe); ok {
			// A busy correlated-IQ lane is not evidence of a dead server. Start
			// the reply deadline only once this probe owns that lane.
			err = queued.PingServerAfterQueue(ingress.ctx, clientPingTimeout)
		} else {
			operation, cancel := context.WithTimeout(ingress.ctx, clientPingTimeout)
			err = probe.PingServer(operation)
			cancel()
		}
		if !c.ingressCurrent(ingress) {
			continue
		}
		if err != nil {
			_ = c.transitionIngressFailure(session, ingress, DurablePending, true)
			c.interruptFailedIngress(session, ingress)
			continue
		}
		c.mu.Lock()
		if !c.closed && c.session == session && c.ingress == ingress {
			c.progress = c.clock.Now()
		}
		c.mu.Unlock()
	}
}

// PingServer sends one strictly correlated XEP-0199 request to the server
// domain. Successful XEP-0198 stanza ACK alone is not a liveness answer.
func (s *melliumSession) PingServer(ctx context.Context) error {
	if s == nil || ctx == nil {
		return ErrInvalidConfig
	}
	if err := s.acquireCorrelated(ctx); err != nil {
		return err
	}
	defer s.releaseCorrelated()
	return s.pingServerLocked(ctx)
}

// PingServerAfterQueue gives the correlated-IQ gate an independent queue
// wait. The response timeout starts only after the request can be sent.
func (s *melliumSession) PingServerAfterQueue(ctx context.Context, responseTimeout time.Duration) error {
	if s == nil || ctx == nil || responseTimeout <= 0 {
		return ErrInvalidConfig
	}
	if err := s.acquireCorrelated(ctx); err != nil {
		return err
	}
	defer s.releaseCorrelated()
	operation, cancel := context.WithTimeout(ctx, responseTimeout)
	defer cancel()
	return s.pingServerLocked(operation)
}

func (s *melliumSession) pingServerLocked(ctx context.Context) error {
	s.mu.Lock()
	session, management, closed, suspended := s.session, s.management, s.closed, s.suspended
	s.mu.Unlock()
	if closed || suspended || session == nil || management == nil {
		return ErrUnavailable
	}
	local, server := session.LocalAddr(), session.LocalAddr().Domain()
	if local.Localpart() == "" || local.Resourcepart() != s.boundResource() || server.Domainpart() == "" {
		return ErrIdentityBinding
	}
	random := rand.Text()
	if len(random) < privateIQRandomLength {
		return ErrUnavailable
	}
	id := "cynapsa-idle-ping-" + random[:privateIQRandomLength]
	record := Stanza{Kind: StanzaSessionPingQuery, From: local.String(), To: server.String(), MeshID: s.meshID, MessageID: id}
	payload := xml.StartElement{Name: xml.Name{Space: xep0199PingNamespace, Local: "ping"}}
	iq := stanza.IQ{XMLName: xml.Name{Space: stanza.NSClient, Local: "iq"}, ID: id, To: server, Type: stanza.GetIQ}
	response, sequence, err := s.sendTrackedIQElement(ctx, session, management, record, xmlstream.Wrap(nil, payload), iq)
	if err != nil {
		return err
	}
	if response == nil {
		return ErrProtocol
	}
	defer response.Close()
	correlated, handled, err := decodeServerPingResult(response, id, server, local, s.config.StanzaBudgetBytes, true)
	if correlated {
		ordinal, count, confirmErr := management.ConfirmCorrelatedHandled(sequence)
		if confirmErr != nil {
			return confirmErr
		}
		if count > 0 {
			if emitErr := s.emit(ctx, Event{Kind: EventHandled, HandledThrough: ordinal, HandledCount: count}); emitErr != nil {
				return emitErr
			}
		}
	}
	if handled {
		management.MarkHandledInbound()
	}
	return err
}

func decodeServerPingResult(source xml.TokenReader, expectedID string, server, local jid.JID, budget int, mellium bool) (bool, bool, error) {
	if source == nil || expectedID == "" || budget <= 0 {
		return false, false, ErrProtocol
	}
	token, err := source.Token()
	outer, ok := token.(xml.StartElement)
	if err != nil || !ok || outer.Name.Local != "iq" || outer.Name.Space != "" && outer.Name.Space != stanza.NSClient {
		return false, false, ErrProtocol
	}
	iqType, ok := validCorrelatedIQAttrs(outer.Attr, expectedID, server, local)
	if !ok {
		return false, false, ErrProtocol
	}
	bounded, err := newStanzaBudget(source, outer, min(budget, maximumPrivateIQBytes))
	if err != nil {
		return true, false, ErrProtocol
	}
	reader := &privateIQTokenReader{source: bounded, remaining: maximumPrivateIQTokens}
	if iqType == stanza.ErrorIQ {
		failure, valid := decodePrivateIQError(reader, outer, mellium)
		return true, valid, failure
	}
	if consumePrivateIQEnd(reader, outer, mellium) != nil {
		return true, false, ErrProtocol
	}
	return true, true, nil
}
