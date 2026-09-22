package mesh

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestRPCRequestAcceptsFortyFiveSecondConfiguredAndExplicitTTL(t *testing.T) {
	for _, test := range []struct {
		name       string
		configured time.Duration
		explicit   time.Duration
	}{
		{name: "configured", configured: 45 * time.Second},
		{name: "explicit", configured: time.Second, explicit: 45 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			notify := make(chan protocol.Envelope, 1)
			service, now := newTestService(t, &testCarrier{notify: notify}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
			service.config.RPCTimeout = fixedRPCTimeout(test.configured)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan *Failure, 1)
			go func() {
				_, failure := service.MessageRequest(ctx, model.MessageRequestArgs{To: testPeerBare, TTL: test.explicit, Payload: nativePayload("/rpc", "question")})
				done <- failure
			}()

			request := <-notify
			wantCreatedAt := now.Truncate(time.Millisecond)
			wantExpiresAt := wantCreatedAt.Add(45 * time.Second)
			if !request.CreatedAt.Equal(wantCreatedAt) || !request.ExpiresAt.Equal(wantExpiresAt) {
				t.Fatalf("request window = %s..%s, want %s..%s", request.CreatedAt, request.ExpiresAt, wantCreatedAt, wantExpiresAt)
			}
			cancel()
			if failure := <-done; failure == nil || failure.Code != FailureCancelled {
				t.Fatalf("cancelled accepted request = %#v", failure)
			}
		})
	}
}

func TestRPCRequestRejectsInvalidResolvedTTL(t *testing.T) {
	for _, test := range []struct {
		name       string
		configured time.Duration
		explicit   time.Duration
	}{
		{name: "configured zero"},
		{name: "configured negative", configured: -time.Millisecond},
		{name: "explicit negative", configured: time.Second, explicit: -time.Millisecond},
		{name: "positive below wire resolution", configured: time.Second, explicit: time.Nanosecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			carrier := &testCarrier{}
			service, _ := newTestService(t, carrier, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
			service.config.RPCTimeout = fixedRPCTimeout(test.configured)
			_, failure := service.MessageRequest(context.Background(), model.MessageRequestArgs{To: testPeerBare, TTL: test.explicit, Payload: nativePayload("/rpc", "question")})
			if failure == nil || failure.Code != FailureRejected {
				t.Fatalf("invalid TTL failure = %#v", failure)
			}
			if got := len(carrier.envelopes()); got != 0 {
				t.Fatalf("invalid TTL published %d envelopes", got)
			}
		})
	}
}

func TestRPCExpirationUsesGoDurationAndWireDomains(t *testing.T) {
	startedAt := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	expiresAt, ok := rpcExpiresAt(startedAt, time.Duration(math.MaxInt64))
	if !ok || !expiresAt.Equal(startedAt.Add(time.Duration(math.MaxInt64)).Truncate(time.Millisecond)) {
		t.Fatalf("maximum Go duration expiry = %s, accepted=%t", expiresAt, ok)
	}

	nearWireMaximum := time.Date(9999, 12, 31, 23, 59, 59, 999_000_000, time.UTC)
	if expiresAt, ok = rpcExpiresAt(nearWireMaximum, time.Millisecond); ok || !expiresAt.IsZero() {
		t.Fatalf("expiry beyond wire maximum = %s, accepted=%t", expiresAt, ok)
	}
}
