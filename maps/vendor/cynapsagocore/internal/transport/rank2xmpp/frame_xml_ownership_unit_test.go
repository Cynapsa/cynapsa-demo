package rank2xmpp

import (
	"errors"
	"reflect"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/transport"
)

func TestFrameToStanzaMovesOwnedDataWithoutClone(t *testing.T) {
	data := []byte("decoded private control frame")
	wantPointer := &data[0]
	frame := transport.ControlFrame{Kind: transport.FrameEnvelope, Data: data}
	stanza, err := frameToStanza(&frame, "sender", "recipient", "mesh")
	if err != nil {
		t.Fatal(err)
	}
	if len(stanza.Data) == 0 || &stanza.Data[0] != wantPointer {
		t.Fatal("frame data was cloned instead of moved")
	}
	if !reflect.DeepEqual(frame, transport.ControlFrame{}) {
		t.Fatalf("source frame retained ownership after move: %+v", frame)
	}
	clearStanzaOwned(&stanza)
}

func TestFrameToStanzaClearsMalformedObjectSubtype(t *testing.T) {
	data := []byte("malformed private object publication")
	retained := data[:]
	frame := transport.ControlFrame{
		Kind:       transport.FrameObjectTransfer,
		TransferID: "xfer_AAAAAAAAAAAAAAAAAAAAAA",
		Data:       data,
	}
	stanza, err := frameToStanza(&frame, "sender", "recipient", "mesh")
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("frameToStanza() error = %v", err)
	}
	if len(stanza.Data) != 0 || !reflect.DeepEqual(frame, transport.ControlFrame{}) {
		t.Fatalf("failed conversion retained state: stanza=%+v frame=%+v", stanza, frame)
	}
	assertRank2Zero(t, retained)
}

func TestFrameToStanzaClearsSourceAndPartialOutputOnMetadataPanic(t *testing.T) {
	source := []byte("decoded private frame")
	partial := []byte("partial private stanza")
	sourceCanary, partialCanary := source[:], partial[:]
	frame := transport.ControlFrame{Kind: transport.FrameEnvelope, Data: source}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("metadata decoder panic was not preserved")
			}
		}()
		_, _ = frameToStanzaWithMetadata(&frame, "sender", "recipient", "mesh", func(_ transport.ControlFrame, stanza *Stanza) error {
			stanza.Data = partial
			panic("metadata panic")
		})
	}()
	if !reflect.DeepEqual(frame, transport.ControlFrame{}) {
		t.Fatalf("panic retained source frame: %+v", frame)
	}
	assertRank2Zero(t, sourceCanary)
	assertRank2Zero(t, partialCanary)
}

func TestFrameToStanzaMoveAllocationBound(t *testing.T) {
	data := []byte("decoded private control frame")
	allocations := testing.AllocsPerRun(1000, func() {
		frame := transport.ControlFrame{Kind: transport.FrameEnvelope, Data: data}
		stanza, err := frameToStanza(&frame, "sender", "recipient", "mesh")
		if err != nil || len(stanza.Data) != len(data) {
			panic("frame move failed")
		}
		data = stanza.Data
	})
	if allocations > 1 {
		t.Fatalf("frame move allocated %.2f times per conversion", allocations)
	}
}
