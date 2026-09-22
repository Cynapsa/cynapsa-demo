package rank1webrtc

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

type ownershipCaptureChannel struct {
	maximum  int
	seen     []byte
	retained Frame
	retain   bool
	result   error
	panic    bool
}

func (channel *ownershipCaptureChannel) MaximumFrameBytes() int { return channel.maximum }
func (channel *ownershipCaptureChannel) Send(_ context.Context, frame Frame) error {
	channel.seen = frame.Data
	if channel.retain {
		channel.retained = frame.Clone()
	}
	if channel.panic {
		panic("dependency panic")
	}
	return channel.result
}
func (*ownershipCaptureChannel) Receive(ctx context.Context) (Frame, error) {
	<-ctx.Done()
	return Frame{}, ctx.Err()
}
func (*ownershipCaptureChannel) Observe() transport.Observation {
	return transport.Observation{State: transport.HealthHealthy}
}
func (*ownershipCaptureChannel) Close(context.Context) error { return nil }

type ownershipCapturePC struct{ channel DataChannel }

func (pc ownershipCapturePC) OpenDataChannel(context.Context, DataChannelConfig) (DataChannel, error) {
	return pc.channel, nil
}
func (ownershipCapturePC) ChannelBinding() ([32]byte, bool) { return [32]byte{1}, true }
func (ownershipCapturePC) Close(context.Context) error      { return nil }

func TestReviewRank1SendClearsDependencyTemporary(t *testing.T) {
	channel := &ownershipCaptureChannel{maximum: 1024, retain: true}
	link, err := NewLink(Config{
		MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 1, TransferWorkers: 1,
		TransferQueue: 1, Clock: transport.ClockFunc(func() time.Time { return time.Unix(1, 0).UTC() }),
	}, ownershipCapturePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = link.Close(context.Background())
		clearOwnedFrame(&channel.retained)
	})

	caller := []byte("rank-one-private-payload")
	want := append([]byte(nil), caller...)
	transferID := "xfer_" + base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef"))
	if err := link.sendFrame(context.Background(), Frame{Kind: FrameTransferChunk, TransferID: transferID, Data: caller}); err != nil {
		t.Fatal(err)
	}
	assertRank1Zero(t, channel.seen)
	if !bytes.Equal(caller, want) {
		t.Fatalf("caller bytes changed: %q", caller)
	}
	if !bytes.Equal(channel.retained.Data, want) {
		t.Fatalf("dependency's explicit retained clone changed: %q", channel.retained.Data)
	}
}

func TestSendChannelClearsDependencySnapshotOnErrorAndPanic(t *testing.T) {
	privateErr := errors.New("private dependency error")
	for _, test := range []struct {
		name    string
		channel *ownershipCaptureChannel
		want    error
	}{
		{name: "error", channel: &ownershipCaptureChannel{retain: true, result: privateErr}, want: privateErr},
		{name: "panic", channel: &ownershipCaptureChannel{retain: true, panic: true}, want: transport.ErrSendAmbiguous},
	} {
		t.Run(test.name, func(t *testing.T) {
			caller := []byte("private payload")
			wantCaller := append([]byte(nil), caller...)
			err := sendChannel(test.channel, context.Background(), Frame{Data: caller})
			if !errors.Is(err, test.want) {
				t.Fatalf("sendChannel error = %v, want %v", err, test.want)
			}
			assertRank1Zero(t, test.channel.seen)
			if !bytes.Equal(caller, wantCaller) {
				t.Fatalf("caller bytes changed: %q", caller)
			}
			if !bytes.Equal(test.channel.retained.Data, wantCaller) {
				t.Fatalf("retained clone changed: %q", test.channel.retained.Data)
			}
			clearOwnedFrame(&test.channel.retained)
		})
	}
}

func TestSendChannelAllocationIsOneOwnedPayloadClone(t *testing.T) {
	channel := &ownershipCaptureChannel{}
	caller := make([]byte, 256)
	allocations := testing.AllocsPerRun(1000, func() {
		if err := sendChannel(channel, context.Background(), Frame{Data: caller}); err != nil {
			panic(err)
		}
	})
	if allocations > 1 {
		t.Fatalf("allocations per send = %.2f, want one dependency snapshot", allocations)
	}
}

func TestPayloadTransferPreservesCallerBytesAndClearsWireSnapshots(t *testing.T) {
	channel := &ownershipCaptureChannel{maximum: 1024, retain: true}
	link, err := NewLink(Config{
		MeshID: "mesh", LocalIdentity: "a", PeerID: "b", MaximumFrameBytes: 1024, ReceiveCapacity: 1, TransferWorkers: 1,
		TransferQueue: 1, Clock: transport.ClockFunc(func() time.Time { return time.Unix(1, 0).UTC() }),
	}, ownershipCapturePC{channel: channel}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := link.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		clearOwnedFrame(&channel.retained)
		_ = link.Close(context.Background())
	}()
	carrier, err := NewPayloadTransfer(link, PayloadTransferConfig{MaximumChunkBytes: 256, InFlightChunks: 1, MaximumTransfers: 1})
	if err != nil {
		t.Fatal(err)
	}
	route := qaLiveRoute()
	transferID := "xfer_" + base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef"))

	manifest := []byte("private manifest")
	wantManifest := append([]byte(nil), manifest...)
	if err := carrier.Begin(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameBinary, Data: manifest}); err != nil {
		t.Fatal(err)
	}
	assertRank1Zero(t, channel.seen)
	if !bytes.Equal(manifest, wantManifest) || !bytes.Equal(channel.retained.Data, wantManifest) {
		t.Fatal("manifest ownership was not preserved")
	}
	clearOwnedFrame(&channel.retained)

	chunk := []byte("private chunk")
	wantChunk := append([]byte(nil), chunk...)
	if err := carrier.SendChunk(context.Background(), route, payload.CarrierFrame{TransferID: transferID, Encoding: payload.FrameBinary, Data: chunk}); err != nil {
		t.Fatal(err)
	}
	assertRank1Zero(t, channel.seen)
	if !bytes.Equal(chunk, wantChunk) || !bytes.Equal(channel.retained.Data, wantChunk) {
		t.Fatal("chunk ownership was not preserved")
	}
	clearOwnedFrame(&channel.retained)
	_ = carrier.Abort(context.Background(), route, transferID)
}

func assertRank1Zero(t *testing.T, data []byte) {
	t.Helper()
	if len(data) == 0 {
		t.Fatal("dependency did not receive payload bytes")
	}
	for index, value := range data {
		if value != 0 {
			t.Fatalf("dependency temporary remained non-zero at byte %d", index)
		}
	}
}
