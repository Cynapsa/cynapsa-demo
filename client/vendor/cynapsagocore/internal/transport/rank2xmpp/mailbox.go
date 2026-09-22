package rank2xmpp

import "context"

// CatchUp retrieves bounded offline/history inputs through the same validation
// pipeline as live input. Rank2a/b/c are observations, not routing choices.
func (c *Client) CatchUp(ctx context.Context) error {
	if c == nil || ctx == nil {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	ingress, ready := c.ingress, c.started && !c.closed
	owner := c.stateTransitionOwnerLocked()
	c.mu.Unlock()
	if !ready || ingress == nil {
		return ErrUnavailable
	}
	stanzas, err := catchUpSession(ingress.session, ctx, c.config.MailboxLimit)
	if err != nil {
		return normalize(err, ctx, ErrUnavailable)
	}
	if err = leaseMailbox(ctx, stanzas, c.inboundBudget); err != nil {
		clearStanzas(stanzas)
		return err
	}
	mailboxOwner, err := c.publishIngressStateOwned(owner, ingress, DurableMailbox)
	if err != nil {
		clearStanzas(stanzas)
		return err
	}
	for i := range stanzas {
		stanza := stanzas[i]
		stanzas[i] = Stanza{}
		c.handleIngressEvent(ingress, Event{Kind: EventStanza, Stanza: stanza}, ctx)
		if err = ctx.Err(); err != nil {
			clearStanzas(stanzas)
			return err
		}
	}
	clearStanzas(stanzas)
	return c.setIngressStateOwned(mailboxOwner, ingress, DurableLive)
}
