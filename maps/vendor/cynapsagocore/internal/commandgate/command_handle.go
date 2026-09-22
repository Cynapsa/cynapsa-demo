package commandgate

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"io"
	"math"
)

// CommandHandle is the opaque, core-scoped authority for local cancellation of
// one specific command admission. It is distinct from the application-visible
// command identifier.
type CommandHandle string

const (
	commandHandlePrefix    = "cmdh_"
	commandHandleScopeSize = 16
	commandHandleNonceSize = 16
	commandHandleRawSize   = commandHandleScopeSize + 8 + commandHandleNonceSize
)

func (r *Registry) newCommandHandleLocked() (CommandHandle, error) {
	if err := r.ensureHandleScopeLocked(); err != nil {
		return "", err
	}
	if r.nextGeneration == math.MaxUint64 {
		return "", ErrCommandHandleExhausted
	}
	r.nextGeneration++

	raw := make([]byte, commandHandleRawSize)
	copy(raw, r.handleScope[:])
	binary.BigEndian.PutUint64(raw[commandHandleScopeSize:], r.nextGeneration)
	if _, err := io.ReadFull(rand.Reader, raw[commandHandleScopeSize+8:]); err != nil {
		return "", ErrCommandHandleGeneration
	}
	return CommandHandle(commandHandlePrefix + base64.RawURLEncoding.EncodeToString(raw)), nil
}

func (r *Registry) ensureHandleScopeLocked() error {
	if r.handleScopeReady {
		return nil
	}
	if _, err := io.ReadFull(rand.Reader, r.handleScope[:]); err != nil {
		clear(r.handleScope[:])
		return ErrCommandHandleGeneration
	}
	if _, err := io.ReadFull(rand.Reader, r.terminalKey[:]); err != nil {
		clear(r.handleScope[:])
		clear(r.terminalKey[:])
		return ErrCommandHandleGeneration
	}
	r.handleScopeReady = true
	return nil
}
