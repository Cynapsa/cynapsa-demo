package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

func TestServerRoutingDenialCompletesRPCBeforeItsTimeout(t *testing.T) {
	for _, test := range []struct {
		condition string
		want      FailureCode
	}{
		{"policy-violation", FailureAuthorization},
		{"resource-constraint", FailureCapacity},
		{"recipient-unavailable", FailureUnavailable},
	} {
		t.Run(test.condition, func(t *testing.T) {
			notify := make(chan protocol.Envelope, 1)
			service, _ := newTestService(t, &testCarrier{notify: notify}, &testDeliverySink{}, []model.PolicyRule{{Action: "allow", Path: "/rpc", AgentID: testPeerBare}})
			t.Cleanup(func() { _ = service.Shutdown(context.Background()) })
			done := make(chan *Failure, 1)
			go func() {
				_, failure := service.MessageRequest(context.Background(), model.MessageRequestArgs{To: testPeerBare, TTL: 5 * time.Second, Payload: nativePayload("/rpc", "question")})
				done <- failure
			}()
			request := <-notify
			service.ReportServerRoutingFailure(request.MessageID, test.condition)
			select {
			case failure := <-done:
				if failure == nil || failure.Code != test.want {
					t.Fatalf("request failure = %#v", failure)
				}
			case <-time.After(time.Second):
				t.Fatal("server denial did not complete the pending RPC")
			}
			if service.outbox.OwnsRank2(request.MessageID) {
				t.Fatal("rejected request retained Rank2 ownership")
			}
		})
	}
}
