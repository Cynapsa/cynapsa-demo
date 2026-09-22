package cynapsagocore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func TestABIBridgeUsesSoleAdapterAndDeterministicDocuments(t *testing.T) {
	core, err := NewFromABI([]byte(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":4,"payload_limit":1048576}`))
	if err != nil {
		t.Fatalf("NewFromABI: %v", err)
	}
	if err := core.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	admission, err := core.SubmitABI(context.Background(), []byte(`{"abi_version":1,"command_id":"command-1","command_name":"core.init","sdk_session_id":"session-1","args":{}}`))
	if err != nil {
		t.Fatalf("SubmitABI: %v", err)
	}
	if len(admission) == 0 || string(admission[:17]) != `{"abi_version":1,` {
		t.Fatalf("admission = %s", admission)
	}
	completion, err := core.NextCompletionABI(context.Background())
	if err != nil {
		t.Fatalf("NextCompletionABI: %v", err)
	}
	want := `{"abi_version":1,"command_id":"command-1","ok":true,"result_type":"core_init","result":{"sdk_session_id":"session-1"}}`
	if string(completion) != want {
		t.Fatalf("completion\n got %s\nwant %s", completion, want)
	}
	status, err := core.StatusABI(context.Background())
	if err != nil {
		t.Fatalf("StatusABI: %v", err)
	}
	want = `{"abi_version":1,"status":{"lifecycle":"created","connectivity":"unknown","personality":"unset","agent_id":"","mesh_id":"","mesh_endpoint":"","queued_message_count":0}}`
	if string(status) != want {
		t.Fatalf("status\n got %s\nwant %s", status, want)
	}
	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
}

func TestABICoreCreationPayloadLimitExactBoundary(t *testing.T) {
	atLimit := fmt.Sprintf(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":%d}`, v1.MaximumPayloadBytes)
	core, err := NewFromABI([]byte(atLimit))
	if err != nil {
		t.Fatalf("exact maximum rejected: %v", err)
	}
	if err := core.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	shutdownAndDrain(t, core)
	if err := core.Destroy(); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	overLimit := fmt.Sprintf(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":1,"payload_limit":%d}`, v1.MaximumPayloadBytes+1)
	if core, err := NewFromABI([]byte(overLimit)); core != nil || publicCode(err) != v1.ErrorCodeMalformedInput {
		t.Fatalf("maximum + 1 = (%v, %v), want malformed_input", core, err)
	}
}

func TestEncodeABIErrorDropsUnknownCause(t *testing.T) {
	encoded, err := EncodeABIError(errors.New("secret private canary"))
	if err != nil {
		t.Fatalf("EncodeABIError: %v", err)
	}
	want := `{"abi_version":1,"error":{"code":"core_error","message":"The AZTM core could not complete the operation","retryable":false,"stage":"command","local_or_remote":"local"}}`
	if string(encoded) != want {
		t.Fatalf("error\n got %s\nwant %s", encoded, want)
	}
	if encoded, err := EncodeABIError(nil); err != nil || encoded != nil {
		t.Fatalf("nil error = %q, %v", encoded, err)
	}
	public := &v1.Error{Code: v1.ErrorCodeInvalidHandle, Message: "The local handle is invalid", Stage: v1.ErrorStageSDK, Location: v1.ErrorLocationLocal}
	if encoded, err := EncodeABIError(public); err != nil || len(encoded) == 0 {
		t.Fatalf("public error = %q, %v", encoded, err)
	}
}

func TestABIBridgeSerializesCommandAndPayloadDeadlinesWithoutMislabeling(t *testing.T) {
	core, err := NewFromABI([]byte(`{"abi_version":1,"command_timeout_ms":0,"rpc_timeout_ms":0,"queue_limit":4,"payload_limit":1048576}`))
	if err != nil {
		t.Fatalf("NewFromABI: %v", err)
	}
	if err := core.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		shutdownAndDrain(t, core)
		if err := core.Destroy(); err != nil {
			t.Fatalf("Destroy: %v", err)
		}
	})

	expired, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancel()
	_, commandErr := core.StatusABI(expired)
	_, payloadErr := core.PayloadOpenABI(expired)
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{
			name: "command_status",
			err:  commandErr,
			want: `{"abi_version":1,"error":{"code":"core_error","message":"The AZTM core could not complete the operation","retryable":false,"stage":"command","local_or_remote":"local"}}`,
		},
		{
			name: "payload_open",
			err:  payloadErr,
			want: `{"abi_version":1,"error":{"code":"core_error","message":"The AZTM core could not complete the operation","retryable":false,"stage":"payload","local_or_remote":"local"}}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.err == nil {
				t.Fatal("expired ABI operation returned no error")
			}
			encoded, encodeErr := EncodeABIError(test.err)
			if encodeErr != nil {
				t.Fatalf("EncodeABIError: %v", encodeErr)
			}
			if string(encoded) != test.want {
				t.Fatalf("serialized deadline\n got %s\nwant %s", encoded, test.want)
			}
			if strings.Contains(string(encoded), "deadline") || strings.Contains(string(encoded), "private") {
				t.Fatalf("serialized deadline leaked a private cause: %s", encoded)
			}
		})
	}
}
