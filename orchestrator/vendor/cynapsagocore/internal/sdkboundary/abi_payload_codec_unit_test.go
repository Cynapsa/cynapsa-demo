package sdkboundary

import (
	"errors"
	"testing"
)

func TestDirectPayloadABICodecsAreStrictAndDeterministic(t *testing.T) {
	adapter, _ := New()
	handleJSON := `{"abi_version":1,"handle":"` + validPayloadHandle + `"}`
	handle, err := adapter.DecodeABIPayloadHandle([]byte(handleJSON))
	if err != nil || string(handle) != validPayloadHandle {
		t.Fatalf("handle = %q, %v", handle, err)
	}

	handle, chunk, err := adapter.DecodeABIPayloadWrite([]byte(`{"abi_version":1,"handle":"` + validPayloadHandle + `","chunk":"AP8="}`))
	if err != nil || string(handle) != validPayloadHandle || len(chunk) != 2 || chunk[0] != 0 || chunk[1] != 0xff {
		t.Fatalf("write = %q %x, %v", handle, chunk, err)
	}
	chunk[0] = 9

	handle, offset, limit, err := adapter.DecodeABIPayloadRead([]byte(`{"abi_version":1,"handle":"` + validPayloadHandle + `","offset":7,"limit":11}`))
	if err != nil || string(handle) != validPayloadHandle || offset != 7 || limit != 11 {
		t.Fatalf("read = %q %d %d, %v", handle, offset, limit, err)
	}

	assertGolden := func(name string, got []byte, err error, want string) {
		t.Helper()
		if err != nil || string(got) != want {
			t.Fatalf("%s = %s, %v; want %s", name, got, err, want)
		}
	}
	encoded, err := adapter.EncodeABIPayloadOpen(handle)
	assertGolden("open", encoded, err, `{"abi_version":1,"payload_handle":"`+validPayloadHandle+`"}`)
	encoded, err = adapter.EncodeABIPayloadWrite(2)
	assertGolden("write", encoded, err, `{"abi_version":1,"accepted":2}`)
	encoded, err = adapter.EncodeABIPayloadFinish(handle, 19)
	assertGolden("finish", encoded, err, `{"abi_version":1,"payload_handle":"`+validPayloadHandle+`","size":19}`)
	read := []byte{0, 0xff}
	encoded, err = adapter.EncodeABIPayloadRead(read, true)
	assertGolden("read", encoded, err, `{"abi_version":1,"chunk":"AP8=","eof":true}`)
	read[0] = 9
	if string(encoded) != `{"abi_version":1,"chunk":"AP8=","eof":true}` {
		t.Fatal("encoded read aliased input")
	}

	for _, input := range []string{
		`{"abi_version":1,"handle":"` + validPayloadHandle + `","extra":true}`,
		`{"abi_version":1,"handle":"` + validPayloadHandle + `","handle":"` + validPayloadHandle + `"}`,
		`{"abi_version":2,"handle":"` + validPayloadHandle + `"}`,
	} {
		if _, err := adapter.DecodeABIPayloadHandle([]byte(input)); err == nil {
			t.Fatalf("accepted strict-invalid handle input %s", input)
		}
	}
	if _, _, _, err := adapter.DecodeABIPayloadRead([]byte(`{"abi_version":1,"handle":"` + validPayloadHandle + `","offset":0,"limit":0}`)); !errors.Is(err, ErrMalformedInput) {
		t.Fatalf("zero read limit = %v", err)
	}
}
