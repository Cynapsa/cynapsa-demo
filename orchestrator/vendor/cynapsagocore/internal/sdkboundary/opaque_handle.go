package sdkboundary

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
)

const (
	commandHandlePrefix = "cmdh_"
	requestHandlePrefix = "reqh_"
	payloadHandlePrefix = "payh_"

	commandHandleBytes = 40
	requestHandleBytes = 32
	payloadHandleBytes = 32
	diagnosticIDBytes  = 32
)

// NewCommandHandle encodes the issuing Core scope, admission generation, and a
// fresh nonce into the frozen opaque command-handle shape.
func NewCommandHandle(coreScope [16]byte, admissionGeneration uint64) (string, error) {
	data := make([]byte, commandHandleBytes)
	copy(data[:16], coreScope[:])
	binary.BigEndian.PutUint64(data[16:24], admissionGeneration)
	if _, err := rand.Read(data[24:]); err != nil {
		return "", err
	}
	return commandHandlePrefix + base64.RawURLEncoding.EncodeToString(data), nil
}

// NewRequestHandle creates a transport-neutral single-use request capability.
func NewRequestHandle() (string, error) {
	return newRandomClassHandle(requestHandlePrefix, requestHandleBytes)
}

// NewPayloadHandle creates a transport-neutral canonical-snapshot handle.
func NewPayloadHandle() (string, error) {
	return newRandomClassHandle(payloadHandlePrefix, payloadHandleBytes)
}

func newRandomClassHandle(prefix string, size int) (string, error) {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(data), nil
}
