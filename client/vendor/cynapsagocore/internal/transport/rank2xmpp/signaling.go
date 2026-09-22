package rank2xmpp

import (
	"context"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type jingleResult struct {
	from   string
	signal Jingle
	err    error
	lease  *transport.InboundLease
}

type jingleWaiter struct {
	recipient string
	result    chan jingleResult
}

const MaximumSignalBytes = 1 << 20

func (c *Client) SendSignal(ctx context.Context, recipient, attemptID string, data []byte) error {
	if c == nil || ctx == nil || protocol.ValidateAgentIdentity(recipient) != nil || attemptID == "" || len(data) == 0 || len(data) > MaximumSignalBytes {
		return ErrProtocol
	}
	c.mu.Lock()
	identity := c.identity.BoundIdentity
	c.mu.Unlock()
	return normalize(c.sendStanza(ctx, Stanza{Kind: StanzaSignal, From: identity, To: recipient, MeshID: c.config.Auth.MeshID, AttemptID: attemptID, Data: data}), ctx, ErrUnavailable)
}

func (c *Client) ReceiveSignal(ctx context.Context) (Stanza, error) {
	if c == nil || ctx == nil {
		return Stanza{}, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return Stanza{}, err
	}
	c.mu.Lock()
	lifetime, generation, started, closed := c.ctx, c.generation, c.started, c.closed
	if !closed && started && lifetime != nil {
		c.receiveWG.Add(1)
	}
	c.mu.Unlock()
	if closed {
		return Stanza{}, ErrClosed
	}
	if !started || lifetime == nil {
		return Stanza{}, ErrUnavailable
	}
	defer c.receiveWG.Done()
	select {
	case stanza := <-c.signals:
		c.mu.Lock()
		stale := c.closed || c.generation != generation || !c.started
		c.mu.Unlock()
		if stale {
			clearStanzaOwned(&stanza)
			return Stanza{}, ErrClosed
		}
		if stanza.inboundLease != nil {
			stanza.inboundLease.Release()
			stanza.inboundLease = nil
		}
		return stanza, nil
	case <-ctx.Done():
		return Stanza{}, ctx.Err()
	case <-lifetime.Done():
		return Stanza{}, ErrClosed
	}
}

func (c *Client) SendJingle(ctx context.Context, recipient string, signal Jingle) error {
	c.mu.Lock()
	local := c.identity.BoundIdentity
	c.mu.Unlock()
	if !validJingleRoute(signal, local, recipient) {
		return ErrProtocol
	}
	encoded, err := EncodeJingle(signal)
	if err != nil {
		return err
	}
	defer clear(encoded)
	return c.SendSignal(ctx, recipient, signal.SID, encoded)
}

func (c *Client) ReceiveJingle(ctx context.Context) (string, Jingle, error) {
	stanza, err := c.ReceiveSignal(ctx)
	if err != nil {
		return "", Jingle{}, err
	}
	defer clearStanzaOwned(&stanza)
	signal, err := DecodeJingle(stanza.Data)
	if err != nil || signal.SID != stanza.AttemptID || !validJingleRoute(signal, stanza.From, stanza.To) {
		return "", Jingle{}, ErrProtocol
	}
	return stanza.From, signal, nil
}

func validJingleRoute(signal Jingle, from, to string) bool {
	switch signal.Action {
	case "session-initiate":
		return signal.Initiator == from && signal.Responder == to
	case "session-accept":
		return signal.Responder == from && signal.Initiator == to
	default:
		return signal.Initiator == from && signal.Responder == to || signal.Responder == from && signal.Initiator == to
	}
}

// ExchangeJingle sends one request and waits for the matching SID response.
// It is private and bounded by the caller's handshake deadline.
func (c *Client) ExchangeJingle(ctx context.Context, recipient string, signal Jingle) (Jingle, error) {
	if c == nil || ctx == nil || protocol.ValidateAgentIdentity(recipient) != nil || signal.SID == "" {
		return Jingle{}, ErrInvalidConfig
	}
	waiter := &jingleWaiter{recipient: recipient, result: make(chan jingleResult, 1)}
	c.mu.Lock()
	if c.closed || c.jingle[signal.SID] != nil {
		c.mu.Unlock()
		return Jingle{}, ErrUnavailable
	}
	c.jingle[signal.SID] = waiter
	c.receiveWG.Add(1)
	c.mu.Unlock()
	defer c.receiveWG.Done()
	defer func() {
		c.mu.Lock()
		if c.jingle[signal.SID] == waiter {
			delete(c.jingle, signal.SID)
		}
		c.mu.Unlock()
		select {
		case abandoned := <-waiter.result:
			if abandoned.lease != nil {
				abandoned.lease.Release()
			}
		default:
		}
	}()
	if err := c.SendJingle(ctx, recipient, signal); err != nil {
		return Jingle{}, err
	}
	select {
	case result := <-waiter.result:
		if result.lease != nil {
			result.lease.Release()
			result.lease = nil
		}
		if result.err != nil {
			return Jingle{}, result.err
		}
		c.mu.Lock()
		local := c.identity.BoundIdentity
		c.mu.Unlock()
		if result.from != recipient || result.signal.SID != signal.SID || !validJingleRoute(result.signal, result.from, local) {
			return Jingle{}, ErrProtocol
		}
		return result.signal, nil
	case <-ctx.Done():
		return Jingle{}, ctx.Err()
	case <-c.closedChan():
		return Jingle{}, ErrClosed
	}
}

func (c *Client) handleSignal(ctx context.Context, stanza *Stanza) bool {
	if c == nil || ctx == nil || stanza == nil {
		return false
	}
	signal, err := DecodeJingle(stanza.Data)
	if err == nil && signal.SID == stanza.AttemptID {
		c.mu.Lock()
		waiter := c.jingle[signal.SID]
		if waiter != nil && waiter.recipient == stanza.From && validJingleRoute(signal, stanza.From, stanza.To) {
			delete(c.jingle, signal.SID)
		} else {
			waiter = nil
		}
		c.mu.Unlock()
		if waiter != nil {
			select {
			case waiter.result <- jingleResult{from: stanza.From, signal: signal, lease: stanza.inboundLease}:
				clear(stanza.Data)
				*stanza = Stanza{}
				return true
			default:
				return false
			}
		}
	}
	select {
	case c.signals <- *stanza:
		*stanza = Stanza{}
		return true
	case <-ctx.Done():
		return false
	}
}

// failJingleExchange atomically claims one exact active ExchangeJingle waiter.
// A server error can never cancel a later exchange which reuses the same SID,
// and delivery is non-blocking because every waiter owns one buffered result.
func (c *Client) failJingleExchange(attemptID string) bool {
	if c == nil || attemptID == "" {
		return false
	}
	c.mu.Lock()
	waiter := c.jingle[attemptID]
	if waiter != nil && validJingleBoundIdentity(waiter.recipient, c.config.Auth.MeshID) {
		delete(c.jingle, attemptID)
	} else {
		waiter = nil
	}
	c.mu.Unlock()
	if waiter == nil {
		return false
	}
	select {
	case waiter.result <- jingleResult{err: ErrUnavailable}:
		return true
	default:
		return false
	}
}
