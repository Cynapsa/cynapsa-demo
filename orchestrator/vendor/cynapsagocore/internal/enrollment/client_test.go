package enrollment

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func validRequestFixture() Request {
	return Request{
		Token: []byte(testToken[5:]), MeshID: "mesh-one",
		InstallationID:     "11111111-1111-4111-9111-111111111111",
		InstallationSecret: []byte("ERERERERERERERERERERERERERERERERERERERERERE"),
	}
}

func validResponseFixture(request Request) responseWire {
	return responseWire{
		Version: "e1", InstallationID: request.InstallationID,
		AgentID: "22222222-2222-4222-a222-222222222222", MeshID: request.MeshID,
		SessionResource: "installation-11111111", MeshEndpoint: "mesh.example.test:5222",
		Username: "agent@example.test", Server: "mesh.example.test",
		AccessToken: "header.payload.signature", ExpiresIn: 86400,
		Wrapper: "aztm_wrapper-value", Display: "Friendly Agent",
	}
}

func successServer(t *testing.T, mutate func(*responseWire)) (*httptest.Server, *atomic.Bool) {
	t.Helper()
	called := &atomic.Bool{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		called.Store(true)
		if request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" ||
			request.Header.Get("Accept") != "application/json" || request.Header.Get("Cache-Control") != "no-store" {
			t.Errorf("request metadata = %s %#v", request.Method, request.Header)
		}
		data, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var wire requestWire
		if err := json.Unmarshal(data, &wire); err != nil {
			t.Error(err)
		}
		fixture := validRequestFixture()
		if wire.Token != string(fixture.Token) || strings.HasPrefix(wire.Token, "cpsa_") ||
			wire.MeshID != fixture.MeshID || wire.InstallationID != fixture.InstallationID ||
			wire.InstallationSecret != string(fixture.InstallationSecret) {
			t.Errorf("request body does not match private contract")
		}
		result := validResponseFixture(fixture)
		if mutate != nil {
			mutate(&result)
		}
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		response.Header().Set("Cache-Control", "private, no-store")
		_ = json.NewEncoder(response).Encode(result)
	}))
	return server, called
}

func TestClientMapsStrictSuccessfulBundle(t *testing.T) {
	server, called := successServer(t, nil)
	defer server.Close()
	client, err := NewTestClient(server.URL+"/v1/enroll", server.Client().Transport)
	if err != nil {
		t.Fatal(err)
	}
	request := validRequestFixture()
	bundle, failure := client.Enroll(context.Background(), request)
	if failure != nil || !called.Load() || !ValidateBundle(request, bundle) {
		t.Fatalf("bundle=%#v failure=%v", bundle, failure)
	}
	if string(bundle.AccessToken) != "header.payload.signature" || string(bundle.Wrapper) != "aztm_wrapper-value" {
		t.Fatal("private credentials did not map")
	}
	bundle.Clear()
}

func TestClientRenewUsesFixedPathAndInstallationCredentialOnly(t *testing.T) {
	fixture := validRequestFixture()
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/renew" {
			t.Errorf("renew path=%q", request.URL.Path)
		}
		data, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		if bytes.Contains(data, []byte(`"token"`)) || bytes.Contains(data, fixture.Token) {
			t.Error("renew request exposed enrollment token")
		}
		var wire renewRequestWire
		if err := json.Unmarshal(data, &wire); err != nil || wire.MeshID != fixture.MeshID ||
			wire.InstallationID != fixture.InstallationID || wire.InstallationSecret != string(fixture.InstallationSecret) {
			t.Errorf("renew request=%s err=%v", data, err)
		}
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(response).Encode(validResponseFixture(fixture))
	}))
	defer server.Close()
	client, err := NewTestClient(server.URL+"/v1/enroll", server.Client().Transport)
	if err != nil {
		t.Fatal(err)
	}
	bundle, failure := client.Renew(context.Background(), RenewRequest{
		MeshID: fixture.MeshID, InstallationID: fixture.InstallationID,
		InstallationSecret: fixture.InstallationSecret,
	})
	if failure != nil || bundle.InstallationID != fixture.InstallationID {
		t.Fatalf("bundle=%#v failure=%v", bundle, failure)
	}
	bundle.Clear()
}

func TestClientRejectsRedirectWithoutFollowing(t *testing.T) {
	var followed atomic.Bool
	target := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed.Store(true) }))
	defer target.Close()
	source := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		http.Redirect(response, &http.Request{}, target.URL, http.StatusFound)
	}))
	defer source.Close()
	client, err := NewTestClient(source.URL+"/v1/enroll", source.Client().Transport)
	if err != nil {
		t.Fatal(err)
	}
	if _, failure := client.Enroll(context.Background(), validRequestFixture()); failure == nil || failure.Code != FailureInvalidResponse {
		t.Fatalf("failure = %#v", failure)
	}
	if followed.Load() {
		t.Fatal("redirect was followed")
	}
}

func TestClientRequiresVerifiedTLSOutsideLoopbackSeam(t *testing.T) {
	if _, err := NewTestClient("http://enrollment.example.test/v1/enroll", http.DefaultTransport); err == nil {
		t.Fatal("accepted non-TLS non-loopback endpoint")
	}
	server, _ := successServer(t, nil)
	defer server.Close()
	client, err := NewTestClient(server.URL+"/v1/enroll", http.DefaultTransport)
	if err != nil {
		t.Fatal(err)
	}
	if _, failure := client.Enroll(context.Background(), validRequestFixture()); failure == nil || failure.Code != FailureUnavailable {
		t.Fatalf("untrusted TLS failure = %#v", failure)
	}
}

func TestClientRejectsResponseFramingAndBounds(t *testing.T) {
	fixture := validRequestFixture()
	valid, _ := json.Marshal(validResponseFixture(fixture))
	cases := []struct {
		name        string
		contentType string
		cache       string
		body        []byte
	}{
		{"content type", "text/plain", "no-store", valid},
		{"cache", "application/json", "private", valid},
		{"duplicate", "application/json", "no-store", bytes.Replace(valid, []byte(`"version":"e1"`), []byte(`"version":"e1","version":"e1"`), 1)},
		{"unknown", "application/json", "no-store", bytes.Replace(valid, []byte("{"), []byte(`{"extra":true,`), 1)},
		{"trailing", "application/json", "no-store", append(append([]byte(nil), valid...), []byte("{}")...)},
		{"oversize", "application/json", "no-store", bytes.Repeat([]byte("x"), maximumResponseBytes+1)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", test.contentType)
				response.Header().Set("Cache-Control", test.cache)
				_, _ = response.Write(test.body)
			}))
			defer server.Close()
			client, err := NewTestClient(server.URL+"/v1/enroll", server.Client().Transport)
			if err != nil {
				t.Fatal(err)
			}
			if _, failure := client.Enroll(context.Background(), fixture); failure == nil || failure.Code != FailureInvalidResponse {
				t.Fatalf("failure = %#v", failure)
			}
		})
	}
}

func TestClientNormalizesHTTPAndCancellation(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusGone} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(status)
			_, _ = response.Write([]byte(testToken))
		}))
		client, err := NewTestClient(server.URL+"/v1/enroll", server.Client().Transport)
		if err != nil {
			t.Fatal(err)
		}
		_, failure := client.Enroll(context.Background(), validRequestFixture())
		server.Close()
		if failure == nil || failure.Code != FailureRejected || strings.Contains(failure.Error(), "cpsa_") {
			t.Fatalf("status %d failure = %#v", status, failure)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := NewProductionClient()
	if _, failure := client.Enroll(ctx, validRequestFixture()); failure == nil || failure.Code != FailureCancelled {
		t.Fatalf("cancel failure = %#v", failure)
	}
}
