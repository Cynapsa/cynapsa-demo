package rank2xmpp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/outbox"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
	"mellium.im/sasl"
)

// TestProductionClientCalibratesAgainstPinnedEJabberd is skipped unless the
// disposable Pod6 fixture environment is active. It exercises Client.Start,
// not a standalone XML control implementation.
func TestProductionClientCalibratesAgainstPinnedEJabberd(t *testing.T) {
	endpoint := os.Getenv("CYNAPSA_EJABBERD_ENDPOINT")
	caPath := os.Getenv("CYNAPSA_EJABBERD_CA")
	username := os.Getenv("CYNAPSA_EJABBERD_AGENT_A")
	password := os.Getenv("CYNAPSA_EJABBERD_PASSWORD_A")
	if endpoint == "" || caPath == "" || username == "" || password == "" {
		t.Skip("disposable ejabberd environment is not active")
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("fixture CA is invalid")
	}
	dialer, err := NewMelliumDialer(endpoint, MelliumConfig{
		TLSConfig:                    &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13},
		SASLMechanisms:               []sasl.Mechanism{sasl.ScramSha256Plus, sasl.ScramSha256},
		ReceiveCapacity:              8,
		StreamManagementCapacity:     8,
		StreamManagementByteCapacity: 8 << 20,
		MaximumFrameBytes:            transport.MaximumControlFrameBytes,
		StanzaBudgetBytes:            512 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	probe, err := dialer.Dial(probeCtx)
	if err != nil {
		probeCancel()
		t.Fatal(err)
	}
	if err = probe.ConnectTLS(probeCtx, endpoint); err == nil {
		_, _, err = probe.Authenticate(probeCtx, username, []byte(password))
	}
	if err == nil {
		_, err = probe.BindResource(probeCtx, "time-probe")
	}
	if err == nil {
		err = probe.EnableStreamManagement(probeCtx, true)
	}
	if err == nil {
		_, err = queryServerTimeSession(probe, probeCtx)
	}
	_ = probe.Close(context.Background())
	probeCancel()
	if err != nil {
		t.Fatalf("direct production time query: %v", err)
	}
	clock := transport.NewCalibratedClock(nil)
	pending, err := outbox.New(outbox.Config{MessageCapacity: 8, ByteCapacity: 1 << 20, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(Config{
		Endpoint: endpoint, Auth: Authentication{Username: username, Password: []byte(password), MeshID: "time-calibration"},
		ReceiveCapacity: 8, TransferWorkers: 1, TransferQueue: 8, MailboxLimit: 8,
		TransferByteCapacity: 1 << 20, UnresolvedTransferCapacity: 8,
		UnresolvedTransferByteCapacity: 1 << 20, UnresolvedTransferLifetime: 5 * time.Second,
		ReconnectAttempts: 1, ReconnectInitial: 10 * time.Millisecond, ReconnectMaximum: 10 * time.Millisecond,
		ReconnectOperationTimeout: 5 * time.Second, Clock: clock,
	}, dialer, pending, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	snapshot, ok := client.TimeCalibration()
	if !ok || snapshot.UTC.IsZero() || snapshot.UTC.Location() != time.UTC || snapshot.Uncertainty <= 0 || snapshot.Uncertainty > transport.MaximumClockUncertainty {
		t.Fatalf("invalid calibration snapshot: %#v, ready=%t", snapshot, ok)
	}
}
