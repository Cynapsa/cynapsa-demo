package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "bounded timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestXMPPBlackholeProbeRequiresExactBoundedTimeout(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		dial func(string, string, time.Duration) (net.Conn, error)
		want string
	}{
		{name: "drop timeout", dial: func(network, address string, timeout time.Duration) (net.Conn, error) {
			if network != "tcp" || address != "mesh.test:5222" || timeout != 250*time.Millisecond {
				t.Fatalf("dial=(%s,%s,%s)", network, address, timeout)
			}
			return nil, timeoutError{}
		}},
		{name: "reachable", dial: func(string, string, time.Duration) (net.Conn, error) {
			left, right := net.Pipe()
			_ = right.Close()
			return left, nil
		}, want: "remained reachable"},
		{name: "immediate rejection", dial: func(string, string, time.Duration) (net.Conn, error) {
			return nil, errors.New("connection refused")
		}, want: "did not produce a bounded timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := proveXMPPBlackholeWithDial("mesh.test:5222", test.dial)
			if test.want == "" && err != nil {
				t.Fatal(err)
			}
			if test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("error=%v, want %q", err, test.want)
			}
		})
	}
}

func TestEstablishSmokeSenderAlwaysSendsBeforeWaitingForHealth(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		sendErr   error
		healthErr error
		wantCalls []string
		wantError string
	}{
		{name: "success", wantCalls: []string{"send", "health"}},
		{name: "send failure", sendErr: errors.New("send failed"), wantCalls: []string{"send"}, wantError: "send failed"},
		{name: "health failure", healthErr: errors.New("health failed"), wantCalls: []string{"send", "health"}, wantError: "health failed"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var calls []string
			err := establishSmokeSender(
				context.Background(),
				func(context.Context) error {
					calls = append(calls, "send")
					return test.sendErr
				},
				func(context.Context) error {
					calls = append(calls, "health")
					return test.healthErr
				},
			)
			if test.wantError == "" && err != nil {
				t.Fatal(err)
			}
			if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Fatalf("error=%v, want %q", err, test.wantError)
			}
			if strings.Join(calls, ",") != strings.Join(test.wantCalls, ",") {
				t.Fatalf("calls=%v, want %v", calls, test.wantCalls)
			}
		})
	}
}

func TestEstablishSmokeReceiverStrictBootstrapValidation(t *testing.T) {
	t.Parallel()
	const (
		peer  = "agent-a@mesh.test"
		nonce = "fresh-run"
	)
	message := func() v1.MessageReceivedEvent {
		return v1.MessageReceivedEvent{
			FromAgentID:   peer,
			Mode:          v1.MessageModeOneWay,
			RequestHandle: "",
			Payload:       nativePayload("/remote-smoke/establish/"+nonce, "rank1-bootstrap"),
		}
	}
	for _, test := range []struct {
		name      string
		mutate    func(*v1.MessageReceivedEvent)
		acceptErr error
		wantCalls []string
		wantError string
	}{
		{name: "exact bootstrap", wantCalls: []string{"read", "accept", "health"}},
		{name: "wrong payload type", mutate: func(message *v1.MessageReceivedEvent) {
			message.Payload = v1.Payload{}
		}, wantCalls: []string{"read"}, wantError: "mismatch"},
		{name: "wrong peer", mutate: func(message *v1.MessageReceivedEvent) {
			message.FromAgentID = "agent-c@mesh.test"
		}, wantCalls: []string{"read"}, wantError: "mismatch"},
		{name: "wrong mode", mutate: func(message *v1.MessageReceivedEvent) {
			message.Mode = v1.MessageModeRequest
		}, wantCalls: []string{"read"}, wantError: "mismatch"},
		{name: "wrong path", mutate: func(message *v1.MessageReceivedEvent) {
			message.Payload = nativePayload("/remote-smoke/establish/stale-run", "rank1-bootstrap")
		}, wantCalls: []string{"read"}, wantError: "mismatch"},
		{name: "wrong body", mutate: func(message *v1.MessageReceivedEvent) {
			message.Payload = nativePayload("/remote-smoke/establish/"+nonce, "wrong-bootstrap")
		}, wantCalls: []string{"read"}, wantError: "mismatch"},
		{name: "non-empty request handle", mutate: func(message *v1.MessageReceivedEvent) {
			message.RequestHandle = "reqh_unexpected"
		}, wantCalls: []string{"read"}, wantError: "mismatch"},
		{name: "accept failure", acceptErr: errors.New("accept failed"), wantCalls: []string{"read", "accept"}, wantError: "accept smoke bootstrap: accept failed"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			bootstrap := message()
			if test.mutate != nil {
				test.mutate(&bootstrap)
			}
			var calls []string
			err := establishSmokeReceiver(
				context.Background(),
				peer,
				nonce,
				func(context.Context) (v1.Event, v1.MessageReceivedEvent, error) {
					calls = append(calls, "read")
					return v1.Event{ID: "event-bootstrap"}, bootstrap, nil
				},
				func(_ context.Context, eventID v1.EventID) error {
					calls = append(calls, "accept")
					if eventID != "event-bootstrap" {
						t.Fatalf("accepted event=%q", eventID)
					}
					return test.acceptErr
				},
				func(context.Context) error {
					calls = append(calls, "health")
					return nil
				},
			)
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error=%v, want %q", err, test.wantError)
			}
			if strings.Join(calls, ",") != strings.Join(test.wantCalls, ",") {
				t.Fatalf("calls=%v, want %v", calls, test.wantCalls)
			}
		})
	}
}

func TestAwaitSmokeRequestStrictValidation(t *testing.T) {
	t.Parallel()
	const (
		peer  = "agent-a@mesh.test"
		nonce = "fresh-run"
	)
	message := func() v1.MessageReceivedEvent {
		return v1.MessageReceivedEvent{
			FromAgentID:   peer,
			Mode:          v1.MessageModeRequest,
			RequestHandle: "reqh_test",
			Payload:       nativePayload("/remote-smoke/request/"+nonce+"-a", "request-a-to-b"),
		}
	}
	for _, test := range []struct {
		name      string
		mutate    func(*v1.MessageReceivedEvent)
		wantError string
	}{
		{name: "exact request"},
		{name: "wrong payload type", mutate: func(message *v1.MessageReceivedEvent) {
			message.Payload = v1.Payload{}
		}, wantError: "mismatch"},
		{name: "wrong peer", mutate: func(message *v1.MessageReceivedEvent) {
			message.FromAgentID = "agent-c@mesh.test"
		}, wantError: "mismatch"},
		{name: "wrong mode", mutate: func(message *v1.MessageReceivedEvent) {
			message.Mode = v1.MessageModeOneWay
		}, wantError: "mismatch"},
		{name: "wrong path", mutate: func(message *v1.MessageReceivedEvent) {
			message.Payload = nativePayload("/remote-smoke/request/stale-a", "request-a-to-b")
		}, wantError: "mismatch"},
		{name: "wrong body", mutate: func(message *v1.MessageReceivedEvent) {
			message.Payload = nativePayload("/remote-smoke/request/"+nonce+"-a", "wrong-request")
		}, wantError: "mismatch"},
		{name: "missing request handle", mutate: func(message *v1.MessageReceivedEvent) {
			message.RequestHandle = ""
		}, wantError: "mismatch"},
		{name: "bootstrap rejected", mutate: func(message *v1.MessageReceivedEvent) {
			message.Mode = v1.MessageModeOneWay
			message.RequestHandle = ""
			message.Payload = nativePayload("/remote-smoke/establish/"+nonce, "rank1-bootstrap")
		}, wantError: "mismatch"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := message()
			if test.mutate != nil {
				test.mutate(&request)
			}
			reads := 0
			event, got, err := awaitSmokeRequest(context.Background(), peer, nonce, func(context.Context) (v1.Event, v1.MessageReceivedEvent, error) {
				reads++
				return v1.Event{ID: "event-request"}, request, nil
			})
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				if event.ID != "event-request" || got.RequestHandle != "reqh_test" {
					t.Fatalf("event=%#v request=%#v", event, got)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error=%v, want %q", err, test.wantError)
			}
			if reads != 1 {
				t.Fatalf("reads=%d, want 1", reads)
			}
		})
	}
}
