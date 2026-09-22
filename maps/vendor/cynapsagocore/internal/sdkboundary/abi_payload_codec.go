package sdkboundary

import (
	"fmt"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

type abiHandleWire struct {
	ABIVersion uint32 `json:"abi_version"`
	Handle     string `json:"handle"`
}

type abiPayloadWriteWire struct {
	ABIVersion uint32 `json:"abi_version"`
	Handle     string `json:"handle"`
	Chunk      []byte `json:"chunk"`
}

type abiPayloadReadWire struct {
	ABIVersion uint32 `json:"abi_version"`
	Handle     string `json:"handle"`
	Offset     uint64 `json:"offset"`
	Limit      uint32 `json:"limit"`
}

type abiPayloadOpenResultWire struct {
	ABIVersion uint32 `json:"abi_version"`
	Handle     string `json:"payload_handle"`
}
type abiPayloadWriteResultWire struct {
	ABIVersion uint32 `json:"abi_version"`
	Accepted   uint64 `json:"accepted"`
}
type abiPayloadFinishResultWire struct {
	ABIVersion uint32 `json:"abi_version"`
	Handle     string `json:"payload_handle"`
	Size       uint64 `json:"size"`
}
type abiPayloadReadResultWire struct {
	ABIVersion uint32 `json:"abi_version"`
	Chunk      []byte `json:"chunk"`
	EOF        bool   `json:"eof"`
}

// DecodeABICommandHandle strictly decodes one cancellation authority.
func (a *Adapter) DecodeABICommandHandle(data []byte) (v1.CommandHandle, error) {
	var wire abiHandleWire
	if err := strictDecode(data, &wire); err != nil {
		return "", err
	}
	if err := requireABIVersion(wire.ABIVersion); err != nil {
		return "", err
	}
	handle := v1.CommandHandle(wire.Handle)
	if err := validateCommandHandle(handle); err != nil {
		return "", err
	}
	return handle, nil
}

// DecodeABIPayloadHandle strictly decodes one payload-handle operation.
func (a *Adapter) DecodeABIPayloadHandle(data []byte) (v1.PayloadHandle, error) {
	var wire abiHandleWire
	if err := strictDecode(data, &wire); err != nil {
		return "", err
	}
	if err := requireABIVersion(wire.ABIVersion); err != nil {
		return "", err
	}
	handle := v1.PayloadHandle(wire.Handle)
	if err := validatePayloadHandle(handle); err != nil {
		return "", err
	}
	return handle, nil
}

// DecodeABIPayloadWrite strictly decodes a bounded copied payload chunk.
func (a *Adapter) DecodeABIPayloadWrite(data []byte) (v1.PayloadHandle, []byte, error) {
	var wire abiPayloadWriteWire
	if err := strictDecode(data, &wire); err != nil {
		return "", nil, err
	}
	if err := requireABIVersion(wire.ABIVersion); err != nil {
		return "", nil, err
	}
	handle := v1.PayloadHandle(wire.Handle)
	if err := validatePayloadHandle(handle); err != nil {
		return "", nil, err
	}
	if len(wire.Chunk) > maxPayloadChunkBytes {
		return "", nil, ErrInputTooLarge
	}
	return handle, cloneBytes(wire.Chunk), nil
}

// DecodeABIPayloadRead strictly decodes one bounded payload range.
func (a *Adapter) DecodeABIPayloadRead(data []byte) (v1.PayloadHandle, uint64, uint32, error) {
	var wire abiPayloadReadWire
	if err := strictDecode(data, &wire); err != nil {
		return "", 0, 0, err
	}
	if err := requireABIVersion(wire.ABIVersion); err != nil {
		return "", 0, 0, err
	}
	handle := v1.PayloadHandle(wire.Handle)
	if err := validatePayloadHandle(handle); err != nil {
		return "", 0, 0, err
	}
	if wire.Limit == 0 || wire.Limit > maxPayloadChunkBytes || wire.Offset > maxCanonicalPayloadSize || wire.Offset+uint64(wire.Limit) < wire.Offset || wire.Offset+uint64(wire.Limit) > maxCanonicalPayloadSize {
		return "", 0, 0, malformed("payload range")
	}
	return handle, wire.Offset, wire.Limit, nil
}

func (a *Adapter) EncodeABIPayloadOpen(handle v1.PayloadHandle) ([]byte, error) {
	if err := validatePayloadHandle(handle); err != nil {
		return nil, err
	}
	return stableMarshal(abiPayloadOpenResultWire{ABIVersion: v1.CurrentSchemaVersion, Handle: string(handle)})
}

func (a *Adapter) EncodeABIPayloadWrite(accepted uint64) ([]byte, error) {
	if accepted > maxPayloadChunkBytes {
		return nil, fmt.Errorf("%w: payload write", ErrInputTooLarge)
	}
	return stableMarshal(abiPayloadWriteResultWire{ABIVersion: v1.CurrentSchemaVersion, Accepted: accepted})
}

func (a *Adapter) EncodeABIPayloadFinish(handle v1.PayloadHandle, size uint64) ([]byte, error) {
	if err := validatePayloadHandle(handle); err != nil || size > maxCanonicalPayloadSize {
		return nil, malformed("payload finish")
	}
	return stableMarshal(abiPayloadFinishResultWire{ABIVersion: v1.CurrentSchemaVersion, Handle: string(handle), Size: size})
}

func (a *Adapter) EncodeABIPayloadRead(chunk []byte, eof bool) ([]byte, error) {
	if len(chunk) > maxPayloadChunkBytes {
		return nil, ErrInputTooLarge
	}
	return stableMarshal(abiPayloadReadResultWire{ABIVersion: v1.CurrentSchemaVersion, Chunk: cloneBytes(chunk), EOF: eof})
}
