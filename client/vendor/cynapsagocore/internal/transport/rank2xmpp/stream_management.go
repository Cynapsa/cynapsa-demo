package rank2xmpp

import "context"

// Resume attempts XEP-0198 resumption. Clean-session replay remains the
// receive loop's bounded reconnect responsibility.
func (c *Client) Resume(ctx context.Context) (bool, error) {
	if c == nil || ctx == nil {
		return false, ErrInvalidConfig
	}
	c.mu.Lock()
	lifetime, generation, ready := c.ctx, c.generation, c.started && !c.closed
	owner := c.stateTransitionOwnerLocked()
	c.mu.Unlock()
	if !ready || lifetime == nil {
		return false, ErrUnavailable
	}
	operation, cancel := context.WithTimeout(ctx, c.config.ReconnectOperationTimeout)
	stopLifetime := context.AfterFunc(lifetime, cancel)
	defer func() {
		stopLifetime()
		cancel()
	}()
	if err := c.acquireRecovery(operation); err != nil {
		if stateErr := c.recoveryGenerationError(generation); stateErr != nil {
			return false, stateErr
		}
		return false, err
	}
	defer c.releaseRecovery()
	if err := c.sendMu.LockContext(operation); err != nil {
		if stateErr := c.recoveryGenerationError(generation); stateErr != nil {
			return false, stateErr
		}
		return false, err
	}
	defer c.sendMu.Unlock()
	c.mu.Lock()
	session, ready := c.session, c.started && !c.closed && c.generation == generation && c.stateTransitionOwnerCurrentLocked(owner)
	c.mu.Unlock()
	if !ready {
		if stateErr := c.recoveryGenerationError(generation); stateErr != nil {
			return false, stateErr
		}
		return false, ErrUnavailable
	}
	resumed, prepared, err := c.prepareResume(session, operation)
	// A dependency may ignore cancellation and return a late benign rejection.
	// Revalidate both the client generation and the bounded operation before
	// treating (false, nil) as an authoritative resume outcome.
	if stateErr := c.recoveryGenerationError(generation); stateErr != nil {
		return false, stateErr
	}
	failAndRecover := func(retainData bool) error {
		_ = c.hardInvalidateAuthorityOwned(owner, retainData)
		if stateErr := c.setRecoveryStateOwned(owner, generation, DurablePending); stateErr != nil {
			return stateErr
		}
		// The Pending publication may advance stateEpoch. Capture that exact new
		// owner so the background clean-bind request cannot be discarded as stale.
		c.requestReconnect()
		return nil
	}
	if err != nil {
		if stateErr := failAndRecover(resumeFailureRetainsData(resumed, err)); stateErr != nil {
			return false, stateErr
		}
		return false, normalize(err, operation, ErrUnavailable)
	}
	if !resumed {
		if operationErr := operation.Err(); operationErr != nil {
			if stateErr := failAndRecover(resumeFailureRetainsData(false, operationErr)); stateErr != nil {
				return false, stateErr
			}
			return false, operationErr
		}
		if stateErr := failAndRecover(false); stateErr != nil {
			return false, stateErr
		}
		return false, nil
	}
	// A successful <resumed/> commits this Session object to the replacement
	// socket. Give its authority barrier, replay, and calibration a fresh
	// bounded phase while retaining the caller and Client lifetime parents.
	stopLifetime()
	cancel()
	operation, cancel = context.WithTimeout(ctx, c.config.ReconnectOperationTimeout)
	stopLifetime = context.AfterFunc(lifetime, cancel)
	if err = c.completeCommittedResume(operation, session, prepared, owner, generation); err != nil {
		if stateErr := failAndRecover(resumeFailureRetainsData(true, err)); stateErr != nil {
			return false, stateErr
		}
		return false, normalize(err, operation, ErrUnavailable)
	}
	if err = c.setRecoveryStateOwned(owner, generation, DurableLive); err != nil {
		return false, err
	}
	return true, nil
}
