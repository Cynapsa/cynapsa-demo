package rank2xmpp

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	"github.com/fxamacker/cbor/v2"
)

const (
	objectReadinessVersion      uint16 = 2
	objectReadinessAttemptBytes        = 16
	maximumObjectReferenceBytes        = 16 << 10
	maximumObjectReadinessBytes        = 32 << 10
)

type wireObjectReadinessRequest struct {
	Version        uint16 `cbor:"0,keyasint"`
	AttemptID      string `cbor:"1,keyasint"`
	MessageID      string `cbor:"2,keyasint"`
	ExpiresAtMS    int64  `cbor:"4,keyasint"`
	Reference      string `cbor:"5,keyasint"`
	ManifestSHA256 []byte `cbor:"6,keyasint"`
	TransferID     string `cbor:"7,keyasint"`
}

type wireObjectReadinessResult struct {
	Version         uint16 `cbor:"0,keyasint"`
	AttemptID       string `cbor:"1,keyasint"`
	MessageID       string `cbor:"2,keyasint"`
	ExpiresAtMS     int64  `cbor:"4,keyasint"`
	ReferenceSHA256 []byte `cbor:"5,keyasint"`
	ManifestSHA256  []byte `cbor:"6,keyasint"`
	Ready           bool   `cbor:"7,keyasint"`
	TransferID      string `cbor:"8,keyasint"`
}

type objectReadinessRequest struct {
	AttemptID       string
	MessageID       string
	TransferID      string
	ExpiresAt       time.Time
	Reference       string
	ReferenceDigest [sha256.Size]byte
	ManifestDigest  [sha256.Size]byte
}

type objectReadinessResult struct {
	request objectReadinessRequest
	ready   bool
}

type objectReadinessOutcome struct {
	from   string
	result objectReadinessResult
	err    error
}

type objectReadinessWaiter struct {
	request    objectReadinessRequest
	transferID string
	peer       string
	local      string
	result     chan objectReadinessOutcome
}

type objectReadinessInbound struct {
	requestDigest [sha256.Size]byte
	request       objectReadinessRequest
	response      []byte
}

// BindPayloadRuntime atomically installs the one authenticated transfer
// runtime after the durable session is ready. It is single-use so a later
// graph cannot replace routing or probe authority underneath in-flight work.
func (c *Client) BindPayloadRuntime(receiver TransferReceiver, routes RouteResolver, probe ObjectReadinessProbe) error {
	if c == nil || receiver == nil || routes == nil {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.payloadBound {
		return ErrProtocol
	}
	c.receiver, c.routes, c.readinessProbe, c.payloadBound = receiver, routes, probe, true
	if admitter, ok := receiver.(AuthenticatedPayloadWorkAdmitter); ok {
		c.payloadWork = admitter
	}
	return nil
}

// ConfirmObjectReadiness performs the authenticated exact-reference exchange
// required before a prepared XEP-0363 upload can commit any ciphertext.
func (c *Client) ConfirmObjectReadiness(ctx context.Context, route payload.CarrierRoute, manifest payload.TransferManifest, reference string) error {
	if c == nil || ctx == nil || !validReadinessRoute(route, manifest) || manifest.ExpiresAt.IsZero() || manifest.ExpiresAt.Location() != time.UTC || len(reference) == 0 || len(reference) > maximumObjectReferenceBytes || !validReadinessText(reference) {
		return payload.ErrAuthentication
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	manifestBytes, err := payload.EncodeManifest(manifest)
	if err != nil {
		return payload.ErrAuthentication
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	clear(manifestBytes)
	attemptID, err := newObjectReadinessAttempt()
	if err != nil {
		return payload.ErrCarrierUnavailable
	}
	request := objectReadinessRequest{
		AttemptID: attemptID, MessageID: route.MessageID, TransferID: manifest.TransferID,
		ExpiresAt: manifest.ExpiresAt, Reference: reference,
		ReferenceDigest: sha256.Sum256([]byte(reference)), ManifestDigest: manifestDigest,
	}
	encoded, err := encodeObjectReadinessRequest(request)
	if err != nil {
		return payload.ErrAuthentication
	}
	defer clear(encoded)
	waiter := objectReadinessWaiter{request: request, transferID: manifest.TransferID, peer: route.PeerID, local: route.SenderID, result: make(chan objectReadinessOutcome, 1)}
	c.mu.Lock()
	_, remaining, validUntil := c.readinessWindowLocked(request.ExpiresAt)
	if c.closed || !c.started || c.state == DurableUnknown || !validUntil || len(c.readiness) >= c.config.TransferQueue || c.readiness[attemptID].result != nil {
		c.mu.Unlock()
		return payload.ErrCarrierUnavailable
	}
	c.readiness[attemptID] = waiter
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.readiness, attemptID)
		c.mu.Unlock()
	}()
	operation, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()
	stanza := Stanza{Kind: StanzaObjectReadinessRequest, From: route.SenderID, To: route.RecipientID, MeshID: route.MeshID, TransferID: manifest.TransferID, MessageID: route.MessageID, AttemptID: attemptID, Data: encoded}
	if err := c.sendStanza(operation, stanza); err != nil {
		return classifyReadinessError(operation, err)
	}
	select {
	case outcome := <-waiter.result:
		if outcome.err != nil {
			return classifyReadinessError(operation, outcome.err)
		}
		if outcome.from != route.PeerID || !sameObjectReadinessRequest(outcome.result.request, request) {
			return payload.ErrAuthentication
		}
		if !outcome.result.ready {
			return payload.ErrCarrierUnavailable
		}
		return nil
	case <-operation.Done():
		return operation.Err()
	case <-c.closedChan():
		return payload.ErrCarrierUnavailable
	}
}

func (c *Client) handleObjectReadinessResult(stanza Stanza) {
	result, err := decodeObjectReadinessResult(stanza.Data)
	if err != nil || result.request.AttemptID != stanza.AttemptID || result.request.MessageID != stanza.MessageID || result.request.TransferID != stanza.TransferID {
		return
	}
	c.mu.Lock()
	waiter := c.readiness[result.request.AttemptID]
	if waiter.result == nil || stanza.TransferID != waiter.transferID || stanza.From != waiter.peer || stanza.To != waiter.local || stanza.MeshID != c.config.Auth.MeshID || !sameObjectReadinessRequest(result.request, waiter.request) {
		c.mu.Unlock()
		return
	}
	select {
	case waiter.result <- objectReadinessOutcome{from: stanza.From, result: result}:
	default:
	}
	c.mu.Unlock()
}

func (c *Client) handleObjectReadinessRequest(ctx context.Context, stanza Stanza) {
	request, err := decodeObjectReadinessRequest(stanza.Data)
	if err != nil || request.AttemptID != stanza.AttemptID || request.MessageID != stanza.MessageID || request.TransferID != stanza.TransferID {
		return
	}
	c.mu.Lock()
	if c.closed || !c.started {
		c.mu.Unlock()
		return
	}
	local := c.identity.BoundIdentity
	routes, probe := c.routes, c.readinessProbe
	now, remaining, validUntil := c.readinessWindowLocked(request.ExpiresAt)
	c.expireReadinessLocked(now)
	prior, seen := c.readinessSeen[stanza.TransferID]
	requestDigest := sha256.Sum256(stanza.Data)
	if seen {
		response := append([]byte(nil), prior.response...)
		matching := prior.requestDigest == requestDigest && sameObjectReadinessRequest(prior.request, request)
		c.mu.Unlock()
		if matching && len(response) != 0 {
			c.sendObjectReadinessResult(ctx, stanza, response)
		}
		clear(response)
		return
	}
	if routes == nil || probe == nil || !validUntil || len(c.readinessSeen) >= c.config.TransferQueue {
		c.mu.Unlock()
		c.replyObjectReadiness(ctx, stanza, request, false, requestDigest)
		return
	}
	c.mu.Unlock()
	route, ok := routes.ResolveTransferRoute(stanza.TransferID)
	if !ok {
		if waiting, supportsWait := routes.(waitingRouteResolver); supportsWait {
			operation, cancel := context.WithTimeout(ctx, remaining)
			route, ok = waiting.WaitTransferRoute(operation, stanza.TransferID)
			cancel()
		}
	}
	if !ok || route.PeerID != stanza.From || route.MeshID != stanza.MeshID || route.SenderID != stanza.From || route.RecipientID != stanza.To || route.MessageID != request.MessageID || stanza.To != local {
		c.replyObjectReadiness(ctx, stanza, request, false, requestDigest)
		return
	}
	operation, cancel := context.WithTimeout(ctx, remaining)
	probeErr := probe.ProbeDownloadReference(operation, request.Reference)
	operationErr := operation.Err()
	cancel()
	ready := probeErr == nil && operationErr == nil && ctx.Err() == nil
	c.replyObjectReadiness(ctx, stanza, request, ready, requestDigest)
}

func (c *Client) replyObjectReadiness(ctx context.Context, stanza Stanza, request objectReadinessRequest, ready bool, requestDigest [sha256.Size]byte) {
	result := objectReadinessResult{request: request, ready: ready}
	encoded, err := encodeObjectReadinessResult(result)
	if err != nil {
		return
	}
	c.mu.Lock()
	current := !c.closed && c.started && c.membershipReady && ctx != nil && ctx.Err() == nil
	if current && len(c.readinessSeen) < c.config.TransferQueue {
		c.readinessSeen[stanza.TransferID] = objectReadinessInbound{requestDigest: requestDigest, request: request, response: append([]byte(nil), encoded...)}
	}
	c.mu.Unlock()
	if current {
		c.sendObjectReadinessResult(ctx, stanza, encoded)
	}
	clear(encoded)
}

func (c *Client) sendObjectReadinessResult(ctx context.Context, request Stanza, encoded []byte) {
	if ctx == nil || ctx.Err() != nil || len(encoded) == 0 {
		return
	}
	result, err := decodeObjectReadinessResult(encoded)
	if err != nil {
		return
	}
	_ = c.sendStanza(ctx, Stanza{
		Kind: StanzaObjectReadinessResult, From: request.To, To: request.From, MeshID: request.MeshID,
		TransferID: request.TransferID, MessageID: result.request.MessageID, AttemptID: result.request.AttemptID,
		Data: encoded,
	})
}

func (c *Client) readinessWindowLocked(expires time.Time) (time.Time, time.Duration, bool) {
	snapshot, ok := c.clock.Snapshot()
	if !ok || snapshot.UTC.IsZero() || snapshot.Uncertainty <= 0 {
		return time.Time{}, 0, false
	}
	conservativeNow := snapshot.UTC.UTC().Add(snapshot.Uncertainty)
	remaining := expires.Sub(conservativeNow)
	return conservativeNow, remaining, remaining > 0
}

func (c *Client) expireReadinessLocked(now time.Time) {
	if now.IsZero() {
		return
	}
	for transferID, state := range c.readinessSeen {
		if !state.request.ExpiresAt.After(now) {
			clear(state.response)
			delete(c.readinessSeen, transferID)
		}
	}
}

func (c *Client) retireObjectReadinessLocked(err error) {
	for attempt, waiter := range c.readiness {
		select {
		case waiter.result <- objectReadinessOutcome{err: err}:
		default:
		}
		delete(c.readiness, attempt)
	}
	for transferID, state := range c.readinessSeen {
		clear(state.response)
		delete(c.readinessSeen, transferID)
	}
}

func encodeObjectReadinessRequest(request objectReadinessRequest) ([]byte, error) {
	if !validObjectReadinessRequest(request) {
		return nil, ErrProtocol
	}
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, ErrProtocol
	}
	wire := wireObjectReadinessRequest{
		Version: objectReadinessVersion, AttemptID: request.AttemptID, MessageID: request.MessageID,
		ExpiresAtMS: request.ExpiresAt.UnixMilli(), Reference: request.Reference,
		ManifestSHA256: append([]byte(nil), request.ManifestDigest[:]...), TransferID: request.TransferID,
	}
	encoded, err := mode.Marshal(wire)
	clear(wire.ManifestSHA256)
	if err != nil || len(encoded) == 0 || len(encoded) > maximumObjectReadinessBytes {
		clear(encoded)
		return nil, ErrProtocol
	}
	return encoded, nil
}

func decodeObjectReadinessRequest(encoded []byte) (objectReadinessRequest, error) {
	if len(encoded) == 0 || len(encoded) > maximumObjectReadinessBytes {
		return objectReadinessRequest{}, ErrProtocol
	}
	mode, err := readinessDecMode()
	if err != nil {
		return objectReadinessRequest{}, ErrProtocol
	}
	var wire wireObjectReadinessRequest
	if err := mode.Unmarshal(encoded, &wire); err != nil || wire.Version != objectReadinessVersion || len(wire.ManifestSHA256) != sha256.Size {
		return objectReadinessRequest{}, ErrProtocol
	}
	request := objectReadinessRequest{
		AttemptID: wire.AttemptID, MessageID: wire.MessageID, TransferID: wire.TransferID,
		ExpiresAt: time.UnixMilli(wire.ExpiresAtMS).UTC(), Reference: wire.Reference,
		ReferenceDigest: sha256.Sum256([]byte(wire.Reference)),
	}
	copy(request.ManifestDigest[:], wire.ManifestSHA256)
	if !validObjectReadinessRequest(request) {
		return objectReadinessRequest{}, ErrProtocol
	}
	reencoded, err := encodeObjectReadinessRequest(request)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		clear(reencoded)
		return objectReadinessRequest{}, ErrProtocol
	}
	clear(reencoded)
	return request, nil
}

func encodeObjectReadinessResult(result objectReadinessResult) ([]byte, error) {
	if !validObjectReadinessResultRequest(result.request) {
		return nil, ErrProtocol
	}
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, ErrProtocol
	}
	wire := wireObjectReadinessResult{
		Version: objectReadinessVersion, AttemptID: result.request.AttemptID, MessageID: result.request.MessageID,
		ExpiresAtMS: result.request.ExpiresAt.UnixMilli(), Ready: result.ready,
		ReferenceSHA256: append([]byte(nil), result.request.ReferenceDigest[:]...), ManifestSHA256: append([]byte(nil), result.request.ManifestDigest[:]...), TransferID: result.request.TransferID,
	}
	encoded, err := mode.Marshal(wire)
	clear(wire.ReferenceSHA256)
	clear(wire.ManifestSHA256)
	if err != nil || len(encoded) == 0 || len(encoded) > maximumObjectReadinessBytes {
		clear(encoded)
		return nil, ErrProtocol
	}
	return encoded, nil
}

func decodeObjectReadinessResult(encoded []byte) (objectReadinessResult, error) {
	if len(encoded) == 0 || len(encoded) > maximumObjectReadinessBytes {
		return objectReadinessResult{}, ErrProtocol
	}
	mode, err := readinessDecMode()
	if err != nil {
		return objectReadinessResult{}, ErrProtocol
	}
	var wire wireObjectReadinessResult
	if err := mode.Unmarshal(encoded, &wire); err != nil || wire.Version != objectReadinessVersion || len(wire.ReferenceSHA256) != sha256.Size || len(wire.ManifestSHA256) != sha256.Size {
		return objectReadinessResult{}, ErrProtocol
	}
	request := objectReadinessRequest{AttemptID: wire.AttemptID, MessageID: wire.MessageID, TransferID: wire.TransferID, ExpiresAt: time.UnixMilli(wire.ExpiresAtMS).UTC()}
	copy(request.ReferenceDigest[:], wire.ReferenceSHA256)
	copy(request.ManifestDigest[:], wire.ManifestSHA256)
	if !validObjectReadinessResultRequest(request) {
		return objectReadinessResult{}, ErrProtocol
	}
	result := objectReadinessResult{request: request, ready: wire.Ready}
	reencoded, err := encodeObjectReadinessResult(result)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		clear(reencoded)
		return objectReadinessResult{}, ErrProtocol
	}
	clear(reencoded)
	return result, nil
}

func readinessDecMode() (cbor.DecMode, error) {
	return (cbor.DecOptions{
		DupMapKey: cbor.DupMapKeyEnforcedAPF, MaxNestedLevels: 4, MaxArrayElements: 16, MaxMapPairs: 16,
		IndefLength: cbor.IndefLengthForbidden, TagsMd: cbor.TagsForbidden,
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField, UTF8: cbor.UTF8RejectInvalid,
	}).DecMode()
}

func validObjectReadinessRequest(request objectReadinessRequest) bool {
	return validObjectReadinessResultRequest(request) && len(request.Reference) > 0 && len(request.Reference) <= maximumObjectReferenceBytes && validReadinessText(request.Reference) && request.ReferenceDigest == sha256.Sum256([]byte(request.Reference))
}

func validObjectReadinessResultRequest(request objectReadinessRequest) bool {
	return validObjectReadinessAttempt(request.AttemptID) && validReadinessTyped(request.MessageID, "msg_", 16) && validReadinessTyped(request.TransferID, "xfer_", 16) && !request.ExpiresAt.IsZero() && request.ExpiresAt.Location() == time.UTC && request.ExpiresAt.Nanosecond()%int(time.Millisecond) == 0 && request.ReferenceDigest != [sha256.Size]byte{} && request.ManifestDigest != [sha256.Size]byte{}
}

func validReadinessRoute(route payload.CarrierRoute, manifest payload.TransferManifest) bool {
	return route.PeerID == route.RecipientID && route.MessageID == manifest.MessageID && route.MeshID == manifest.MeshID && route.SenderID == manifest.SenderID && route.RecipientID == manifest.RecipientID && manifest.TransferID != "" && protocol.ValidateMeshID(route.MeshID) == nil && protocol.ValidateAgentIdentity(route.SenderID) == nil && protocol.ValidateAgentIdentity(route.RecipientID) == nil
}

func validReadinessText(value string) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func newObjectReadinessAttempt() (string, error) {
	raw := make([]byte, objectReadinessAttemptBytes)
	if _, err := io.ReadFull(rand.Reader, raw); err != nil {
		clear(raw)
		return "", err
	}
	value := "rdy_" + base64.RawURLEncoding.EncodeToString(raw)
	clear(raw)
	return value, nil
}

func validObjectReadinessAttempt(value string) bool {
	return validReadinessTyped(value, "rdy_", objectReadinessAttemptBytes)
}

func validReadinessTyped(value, prefix string, size int) bool {
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, prefix))
	valid := err == nil && len(raw) == size && prefix+base64.RawURLEncoding.EncodeToString(raw) == value
	clear(raw)
	return valid
}

func sameObjectReadinessRequest(a, b objectReadinessRequest) bool {
	return a.AttemptID == b.AttemptID && a.MessageID == b.MessageID && a.TransferID == b.TransferID && a.ExpiresAt.Equal(b.ExpiresAt) && a.ReferenceDigest == b.ReferenceDigest && a.ManifestDigest == b.ManifestDigest
}

func classifyReadinessError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, ErrQueueFull) {
		return payload.ErrQueueFull
	}
	if errors.Is(err, ErrProtocol) || errors.Is(err, ErrAuthentication) {
		return payload.ErrAuthentication
	}
	return payload.ErrCarrierUnavailable
}
