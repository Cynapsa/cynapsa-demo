package sessionkernel

import (
	"bytes"
	"context"
	"crypto/tls"
	"testing"
	"time"
)

func TestDefaultOperationalProfileFreezesApprovedValues(t *testing.T) {
	profile, err := DefaultOperationalProfile(DeploymentConfig{QueueLimit: MaximumQueueCapacity})
	if err != nil {
		t.Fatal(err)
	}
	if profile.ReconnectAttempts != 8 || profile.ReconnectInitial != 250*time.Millisecond || profile.ReconnectMaximum != 30*time.Second || profile.ReconnectOperationTimeout != 30*time.Second || profile.StanzaBudgetBytes != 4<<20 || profile.TransferWorkers != 4 || profile.QueueCapacity != 65_536 || !profile.UseSystemTLSRoots || !profile.VerifyEndpointHostname {
		t.Fatalf("profile = %+v", profile)
	}
	mechanisms := profile.SASLMechanisms()
	if len(mechanisms) != 2 || mechanisms[0].Name != "SCRAM-SHA-256-PLUS" || mechanisms[1].Name != "SCRAM-SHA-256" {
		t.Fatalf("mechanisms = %+v", mechanisms)
	}
	mechanisms[0].Name = "mutated"
	if fresh := profile.SASLMechanisms(); fresh[0].Name != "SCRAM-SHA-256-PLUS" {
		t.Fatalf("mechanism storage was shared: %+v", fresh)
	}
	tlsConfig, err := profile.TLSConfig("mesh.example.test")
	if err != nil || tlsConfig.RootCAs == nil || tlsConfig.ServerName != "mesh.example.test" || tlsConfig.InsecureSkipVerify || tlsConfig.MinVersion != tls.VersionTLS13 {
		t.Fatalf("TLSConfig = (%+v, %v)", tlsConfig, err)
	}
}

func TestOperationalProfileBoundsAndDeadlines(t *testing.T) {
	for _, limit := range []int{0, MaximumQueueCapacity + 1} {
		if _, err := DefaultOperationalProfile(DeploymentConfig{QueueLimit: limit}); err != ErrInvalidConfig {
			t.Fatalf("queue limit %d error = %v", limit, err)
		}
	}
	profile, err := DefaultOperationalProfile(DeploymentConfig{QueueLimit: 17})
	if err != nil {
		t.Fatal(err)
	}
	if profile.QueueCapacity != 17 {
		t.Fatalf("capacity = %d", profile.QueueCapacity)
	}
	now := time.Unix(100, 0)
	deadline, err := profile.OperationDeadline(context.Background(), now)
	if err != nil || !deadline.Equal(now.Add(30*time.Second)) {
		t.Fatalf("deadline = (%v, %v)", deadline, err)
	}
	callerContext, cancel := context.WithDeadline(context.Background(), now.Add(3*time.Second))
	defer cancel()
	deadline, err = profile.OperationDeadline(callerContext, now)
	if err != nil || !deadline.Equal(now.Add(3*time.Second)) {
		t.Fatalf("caller deadline = (%v, %v)", deadline, err)
	}
	want := []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second}
	for attempt, expected := range want {
		got, err := profile.BackoffCeiling(attempt + 1)
		if err != nil || got != expected {
			t.Fatalf("attempt %d = (%v, %v), want %v", attempt+1, got, err, expected)
		}
	}
}

func TestCryptoJitterUsesInjectedEntropyAndBounds(t *testing.T) {
	got, err := CryptoJitter(bytes.NewReader(make([]byte, 8)), time.Second)
	if err != nil || got != 0 {
		t.Fatalf("zero entropy = (%v, %v)", got, err)
	}
	if _, err := CryptoJitter(bytes.NewReader(nil), time.Second); err != ErrInvalidConfig {
		t.Fatalf("short entropy error = %v", err)
	}
	if _, err := CryptoJitter(bytes.NewReader(make([]byte, 8)), -1); err != ErrInvalidConfig {
		t.Fatalf("negative ceiling error = %v", err)
	}
}
