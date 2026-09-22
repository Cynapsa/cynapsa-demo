package commandgate

import "errors"

// Cancel cancels the one local command admission authorized by handle without
// promising remote revocation. CommandID is not cancellation authority.
// Cancellation races completion through the registry's single terminal-state
// transition. The winner publishes the command's only completion.
func (g *Gate) Cancel(handle CommandHandle) error {
	g.transitionMu.Lock()
	result, err := g.registry.cancelCommand(handle, ErrCommandCancelled)
	if err == nil {
		g.registry.takeDisposition(result.CommandID).resolveResult(result, false)
		g.publishCompletion(result)
	}
	g.transitionMu.Unlock()
	if errors.Is(err, ErrCommandNotFound) {
		g.mu.Lock()
		closed := g.state == gateClosed
		g.mu.Unlock()
		if closed {
			return ErrGateClosed
		}
	}
	return err
}
