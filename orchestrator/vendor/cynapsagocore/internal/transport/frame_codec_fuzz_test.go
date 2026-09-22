package transport

import (
	"bytes"
	"testing"
)

func FuzzQADecodeControlFrame(f *testing.F) {
	valid, err := EncodeControlFrame(ControlFrame{Kind: FrameHealthProbe, ProbeNonce: bytes.Repeat([]byte{1}, 16)}, 512)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid, uint16(512))
	f.Add([]byte{0xbf, 0xff}, uint16(512))
	f.Add(bytes.Repeat([]byte{0xff}, 513), uint16(512))

	f.Fuzz(func(t *testing.T, input []byte, requested uint16) {
		maximum := int(requested)
		if maximum == 0 {
			maximum = 1
		}
		decoded, err := DecodeControlFrame(input, maximum)
		if err != nil {
			return
		}
		reencoded, err := EncodeControlFrame(decoded, maximum)
		if err != nil {
			t.Fatalf("accepted value cannot re-encode: %v", err)
		}
		if !bytes.Equal(input, reencoded) {
			t.Fatalf("accepted non-canonical input: %x != %x", input, reencoded)
		}
	})
}
