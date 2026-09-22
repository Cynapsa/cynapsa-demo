package rank2xmpp

import (
	"context"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
)

const timeCalibrationTimeout = 5 * time.Second

type serverTimeSession interface {
	QueryServerTime(context.Context) (time.Time, error)
}

func dialSession(dialer Dialer, ctx context.Context) (session Session, err error) {
	defer func() {
		if recover() != nil {
			err = ErrUnavailable
		}
	}()
	return dialer.Dial(ctx)
}
func connectSession(session Session, ctx context.Context, endpoint string) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrUnavailable
		}
	}()
	return session.ConnectTLS(ctx, endpoint)
}
func authenticateSession(session Session, ctx context.Context, user string, password []byte) (bare string, proof []byte, err error) {
	defer func() {
		if recover() != nil {
			err = ErrAuthentication
		}
	}()
	return session.Authenticate(ctx, user, password)
}
func bindSession(session Session, ctx context.Context, resource string) (full string, err error) {
	defer func() {
		if recover() != nil {
			err = ErrIdentityBinding
		}
	}()
	return session.BindResource(ctx, resource)
}
func enableSMSession(session Session, ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrStreamManagement
		}
	}()
	return session.EnableStreamManagement(ctx, true)
}
func discoverAuthoritySession(session Session, ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrUnavailable
		}
	}()
	discovery, ok := session.(authorityDiscoverySession)
	if !ok {
		return ErrUnavailable
	}
	return discovery.DiscoverAuthority(ctx)
}
func queryServerTimeSession(session Session, ctx context.Context) (server time.Time, err error) {
	defer func() {
		if recover() != nil {
			server = time.Time{}
			err = ErrUnavailable
		}
	}()
	source, ok := session.(serverTimeSession)
	if !ok {
		return time.Time{}, ErrUnavailable
	}
	return source.QueryServerTime(ctx)
}
func sendSession(session Session, ctx context.Context, stanza Stanza) (err error) {
	if session == nil || ctx == nil {
		return ErrInvalidConfig
	}
	// Session.Send may inspect this dependency-owned snapshot only for the
	// duration of the call. Implementations that retain it (including stream
	// management) must clone it before returning.
	owned := stanza.clone()
	defer clearStanzaOwned(&owned)
	var (
		watchdogDone chan struct{}
		stopWatchdog func() bool
	)
	if ctx.Done() != nil {
		watchdogDone = make(chan struct{})
		stopWatchdog = context.AfterFunc(ctx, func() {
			defer close(watchdogDone)
			emitRank2Evidence(rank2EvidenceRecord{
				Event:  "socket_interrupt_requested",
				Source: "session_calls",
				Stage:  "send_context_done",
			}, ctx.Err())
			interruptSession(session)
		})
	}
	defer func() {
		if stopWatchdog != nil && !stopWatchdog() {
			<-watchdogDone
		}
	}()
	defer func() {
		if recover() != nil {
			err = ErrUnavailable
		}
	}()
	err = session.Send(ctx, owned)
	if contextErr := exactContextError(ctx); contextErr != nil {
		return contextErr
	}
	return err
}

func interruptSession(session Session) {
	defer func() { _ = recover() }()
	if interrupter, ok := session.(sessionInterrupter); ok && interrupter != nil {
		interrupter.interruptIO()
	}
}

func receiveSession(session Session, ctx context.Context) (event Event, err error) {
	defer func() {
		if recover() != nil {
			err = ErrUnavailable
		}
	}()
	return session.Receive(ctx)
}
func resumeSession(session Session, ctx context.Context) (resumed bool, err error) {
	defer func() {
		if recover() != nil {
			err = ErrUnavailable
		}
	}()
	return session.Resume(ctx)
}

// pendingForReplaySession owns every stanza returned by the dependency. An
// unsuccessful handoff is cleared here so reconnect never has to reason about
// a partial replay result or a dependency panic.
func pendingForReplaySession(session pendingSession, ctx context.Context) (stanzas []Stanza, err error) {
	defer func() {
		if recover() != nil {
			clearStanzas(stanzas)
			stanzas = nil
			err = ErrUnavailable
		}
	}()
	stanzas, err = session.PendingForReplay(ctx)
	if err != nil {
		clearStanzas(stanzas)
		stanzas = nil
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	return stanzas, nil
}
func catchUpSession(session Session, ctx context.Context, limit int) (stanzas []Stanza, err error) {
	defer func() {
		if recover() != nil {
			clearStanzas(stanzas)
			stanzas = nil
			err = ErrUnavailable
		}
	}()
	stanzas, err = session.CatchUp(ctx, limit)
	if err != nil {
		clearStanzas(stanzas)
		return nil, err
	}
	if limit <= 0 || len(stanzas) > limit {
		clearStanzas(stanzas)
		return nil, ErrProtocol
	}
	return stanzas, nil
}
func closeSession(session Session, ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrClosed
		}
	}()
	return session.Close(ctx)
}
func consumeProof(sink func(context.Context, string, []byte) error, ctx context.Context, meshID string, proof []byte) (err error) {
	defer clear(proof)
	defer func() {
		if recover() != nil {
			err = ErrAuthentication
		}
	}()
	return sink(ctx, meshID, proof)
}
func callTransferReceiver(receiver TransferReceiver, ctx context.Context, route payload.CarrierRoute, stanza Stanza) (evidence *payload.CompletionEvidence, err error) {
	defer clearStanzaOwned(&stanza)
	defer func() {
		if recover() != nil {
			err = ErrUnavailable
		}
	}()
	return receiver.HandleStanza(ctx, route, stanza)
}
