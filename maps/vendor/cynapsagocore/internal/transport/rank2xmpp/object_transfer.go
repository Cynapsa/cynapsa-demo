package rank2xmpp

import (
	"bytes"
	"context"
	"sync"

	"github.com/Cynapsa/cynapsagocore/internal/payload"
	"github.com/fxamacker/cbor/v2"
)

const (
	objectPublicationVersion = uint16(2)
	maximumObjectPublication = 48 << 10
)

type wireObjectPublication struct {
	Version          uint16 `cbor:"0,keyasint"`
	Manifest         []byte `cbor:"2,keyasint"`
	PrivateReference string `cbor:"3,keyasint"`
}

// ObjectPublication is an authenticated private control value. The raw object
// URL remains inside PrivateReference and is never projected through the SDK.
type ObjectPublication struct {
	Manifest         payload.TransferManifest
	PrivateReference string
}

type objectTransferState struct {
	route            payload.CarrierRoute
	manifest         payload.TransferManifest
	rawReference     string
	privateReference string
	done             <-chan completion
	ready            bool
}

// ObjectTransfer implements the authenticated object readiness,
// materialization publication, completion, and abort contract over Rank 2.
type ObjectTransfer struct {
	client  *Client
	maximum int
	mu      sync.Mutex
	active  map[string]objectTransferState
}

func NewObjectTransfer(client *Client, maximumTransfers int) (*ObjectTransfer, error) {
	if client == nil || maximumTransfers <= 0 || maximumTransfers > 65_536 {
		return nil, payload.ErrInvalidLimits
	}
	return &ObjectTransfer{client: client, maximum: maximumTransfers, active: make(map[string]objectTransferState)}, nil
}

func (transfer *ObjectTransfer) ConfirmObjectReadiness(ctx context.Context, route payload.CarrierRoute, manifest payload.TransferManifest, reference string) error {
	if transfer == nil || ctx == nil {
		return payload.ErrCarrierUnavailable
	}
	transfer.mu.Lock()
	if len(transfer.active) >= transfer.maximum || transfer.active[manifest.TransferID].rawReference != "" {
		transfer.mu.Unlock()
		return payload.ErrQueueFull
	}
	transfer.active[manifest.TransferID] = objectTransferState{route: route, manifest: cloneObjectManifest(manifest), rawReference: reference}
	transfer.mu.Unlock()
	if err := transfer.client.ConfirmObjectReadiness(ctx, route, manifest, reference); err != nil {
		transfer.remove(manifest.TransferID)
		return err
	}
	transfer.mu.Lock()
	state, ok := transfer.active[manifest.TransferID]
	matched := ok && state.route == route && state.rawReference == reference && objectManifestsEqual(state.manifest, manifest)
	if matched {
		state.ready = true
		transfer.active[manifest.TransferID] = state
	}
	transfer.mu.Unlock()
	if !matched {
		return payload.ErrCarrierUnavailable
	}
	return nil
}

func (transfer *ObjectTransfer) PublishObject(ctx context.Context, route payload.CarrierRoute, manifest payload.TransferManifest, privateReference string) error {
	if transfer == nil || ctx == nil {
		return payload.ErrCarrierUnavailable
	}
	transferID, rawReference, err := payload.ParseObjectReference(privateReference)
	if err != nil || transferID != manifest.TransferID {
		return payload.ErrAuthentication
	}
	transfer.mu.Lock()
	state, ok := transfer.active[transferID]
	if !ok || !state.ready || state.route != route || state.rawReference != rawReference || !objectManifestsEqual(state.manifest, manifest) || state.done != nil {
		transfer.mu.Unlock()
		return payload.ErrAuthentication
	}
	done, err := transfer.client.registerCompletion(transferID, route.PeerID, route.MessageID)
	if err != nil {
		transfer.mu.Unlock()
		return payload.ErrQueueFull
	}
	state.done, state.privateReference = done, privateReference
	transfer.active[transferID] = state
	transfer.mu.Unlock()
	encoded, err := EncodeObjectPublication(ObjectPublication{Manifest: manifest, PrivateReference: privateReference})
	if err != nil {
		transfer.remove(transferID)
		return payload.ErrAuthentication
	}
	defer clear(encoded)
	if err = transfer.client.sendStanza(ctx, Stanza{Kind: StanzaObjectTransfer, From: route.SenderID, To: route.RecipientID, MeshID: route.MeshID, TransferID: transferID, MessageID: route.MessageID, Data: encoded}); err != nil {
		transfer.remove(transferID)
		return classify(err)
	}
	return nil
}

func (transfer *ObjectTransfer) AwaitMaterialization(ctx context.Context, route payload.CarrierRoute, transferID string) (payload.CompletionEvidence, error) {
	if transfer == nil || ctx == nil {
		return payload.CompletionEvidence{}, payload.ErrCarrierUnavailable
	}
	transfer.mu.Lock()
	state, ok := transfer.active[transferID]
	transfer.mu.Unlock()
	if !ok || state.route != route || state.done == nil {
		return payload.CompletionEvidence{}, payload.ErrCarrierRejected
	}
	select {
	case result := <-state.done:
		transfer.remove(transferID)
		if result.err != nil {
			return payload.CompletionEvidence{}, classify(result.err)
		}
		if result.evidence.TransferID != transferID || result.evidence.MessageID != route.MessageID {
			return payload.CompletionEvidence{}, payload.ErrAuthentication
		}
		return result.evidence, nil
	case <-ctx.Done():
		transfer.client.terminalizeCompletion(transferID, completion{err: ctx.Err()})
		transfer.remove(transferID)
		return payload.CompletionEvidence{}, ctx.Err()
	}
}

func (transfer *ObjectTransfer) AbortMaterialization(ctx context.Context, route payload.CarrierRoute, transferID string) error {
	if transfer == nil || ctx == nil {
		return payload.ErrCarrierUnavailable
	}
	transfer.mu.Lock()
	state, ok := transfer.active[transferID]
	transfer.mu.Unlock()
	if !ok {
		return nil
	}
	if state.route != route {
		return payload.ErrAuthentication
	}
	if state.privateReference != "" {
		encoded, err := EncodeObjectPublication(ObjectPublication{Manifest: state.manifest, PrivateReference: state.privateReference})
		if err == nil {
			_ = transfer.client.sendStanza(ctx, Stanza{Kind: StanzaObjectTransferAbort, From: route.SenderID, To: route.RecipientID, MeshID: route.MeshID, TransferID: transferID, MessageID: route.MessageID, Data: encoded})
		}
		clear(encoded)
	}
	transfer.client.terminalizeCompletion(transferID, completion{err: ErrCompletionAmbiguous})
	transfer.remove(transferID)
	return nil
}

func (transfer *ObjectTransfer) remove(id string) {
	transfer.mu.Lock()
	delete(transfer.active, id)
	transfer.mu.Unlock()
	transfer.client.removeCompletion(id)
}

func EncodeObjectPublication(publication ObjectPublication) ([]byte, error) {
	if publication.PrivateReference == "" || len(publication.PrivateReference) > maximumObjectReferenceBytes {
		return nil, ErrProtocol
	}
	transferID, _, err := payload.ParseObjectReference(publication.PrivateReference)
	if err != nil || transferID != publication.Manifest.TransferID {
		return nil, ErrProtocol
	}
	manifest, err := payload.EncodeManifest(publication.Manifest)
	if err != nil {
		return nil, ErrProtocol
	}
	defer clear(manifest)
	mode, err := objectPublicationEncMode()
	if err != nil {
		return nil, ErrProtocol
	}
	encoded, err := mode.Marshal(wireObjectPublication{Version: objectPublicationVersion, Manifest: manifest, PrivateReference: publication.PrivateReference})
	if err != nil || len(encoded) > maximumObjectPublication {
		clear(encoded)
		return nil, ErrProtocol
	}
	return encoded, nil
}

func DecodeObjectPublication(encoded []byte) (ObjectPublication, error) {
	return decodeObjectPublication(encoded)
}

func decodeObjectPublication(encoded []byte) (ObjectPublication, error) {
	if len(encoded) == 0 || len(encoded) > maximumObjectPublication {
		return ObjectPublication{}, ErrProtocol
	}
	mode, err := objectPublicationDecMode()
	if err != nil {
		return ObjectPublication{}, ErrProtocol
	}
	var wire wireObjectPublication
	defer func() { clear(wire.Manifest) }()
	if err = mode.Unmarshal(encoded, &wire); err != nil || wire.Version != objectPublicationVersion || len(wire.Manifest) == 0 || len(wire.PrivateReference) == 0 || len(wire.PrivateReference) > maximumObjectReferenceBytes {
		return ObjectPublication{}, ErrProtocol
	}
	manifest, err := payload.DecodeManifest(wire.Manifest)
	if err != nil {
		return ObjectPublication{}, ErrProtocol
	}
	publication := ObjectPublication{Manifest: manifest, PrivateReference: wire.PrivateReference}
	valid := false
	defer func() {
		if !valid {
			clearObjectPublicationOwned(&publication)
		}
	}()
	transferID, _, err := payload.ParseObjectReference(wire.PrivateReference)
	if err != nil || transferID != manifest.TransferID {
		return ObjectPublication{}, ErrProtocol
	}
	reencoded, err := EncodeObjectPublication(publication)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		clear(reencoded)
		return ObjectPublication{}, ErrProtocol
	}
	clear(reencoded)
	valid = true
	return publication, nil
}

func clearObjectPublicationOwned(publication *ObjectPublication) {
	if publication == nil {
		return
	}
	clear(publication.Manifest.CanonicalDigest)
	clear(publication.Manifest.TransferredDigest)
	*publication = ObjectPublication{}
}

func objectPublicationEncMode() (cbor.EncMode, error) {
	options := cbor.CoreDetEncOptions()
	options.IndefLength = cbor.IndefLengthForbidden
	options.TagsMd = cbor.TagsForbidden
	return options.EncMode()
}

func objectPublicationDecMode() (cbor.DecMode, error) {
	options := cbor.DecOptions{DupMapKey: cbor.DupMapKeyEnforcedAPF, IndefLength: cbor.IndefLengthForbidden, TagsMd: cbor.TagsForbidden, MaxArrayElements: 16, MaxMapPairs: 16, ExtraReturnErrors: cbor.ExtraDecErrorUnknownField}
	return options.DecMode()
}

func objectManifestsEqual(left, right payload.TransferManifest) bool {
	leftBytes, leftErr := payload.EncodeManifest(left)
	rightBytes, rightErr := payload.EncodeManifest(right)
	equal := leftErr == nil && rightErr == nil && bytes.Equal(leftBytes, rightBytes)
	clear(leftBytes)
	clear(rightBytes)
	return equal
}

func cloneObjectManifest(manifest payload.TransferManifest) payload.TransferManifest {
	manifest.CanonicalDigest = append([]byte(nil), manifest.CanonicalDigest...)
	manifest.TransferredDigest = append([]byte(nil), manifest.TransferredDigest...)
	return manifest
}

func (c *Client) authorizeObjectPublication(stanza Stanza, publication ObjectPublication) bool {
	transferID, rawReference, err := payload.ParseObjectReference(publication.PrivateReference)
	if err != nil || transferID != stanza.TransferID || publication.Manifest.MessageID != stanza.MessageID || publication.Manifest.SenderID != stanza.From || publication.Manifest.RecipientID != stanza.To || publication.Manifest.MeshID != stanza.MeshID {
		return false
	}
	manifestBytes, err := payload.EncodeManifest(publication.Manifest)
	if err != nil {
		return false
	}
	manifestDigest := payload.Digest(manifestBytes)
	clear(manifestBytes)
	c.mu.Lock()
	prior, ok := c.readinessSeen[stanza.TransferID]
	c.mu.Unlock()
	if !ok || prior.request.MessageID != stanza.MessageID || prior.request.TransferID != stanza.TransferID || prior.request.Reference != rawReference || prior.request.ManifestDigest != manifestDigest || len(prior.response) == 0 {
		return false
	}
	result, err := decodeObjectReadinessResult(prior.response)
	return err == nil && result.ready && sameObjectReadinessRequest(result.request, prior.request)
}

func (c *Client) failObjectCompletion(stanza Stanza) {
	publication, err := decodeObjectPublication(stanza.Data)
	if err != nil {
		return
	}
	defer clearObjectPublicationOwned(&publication)
	if publication.Manifest.TransferID != stanza.TransferID || publication.Manifest.MessageID != stanza.MessageID {
		return
	}
	c.mu.Lock()
	waiter, ok := c.complete[stanza.TransferID]
	if !ok || waiter.peer != stanza.From || waiter.messageID != stanza.MessageID {
		c.mu.Unlock()
		return
	}
	delete(c.complete, stanza.TransferID)
	c.mu.Unlock()
	select {
	case waiter.result <- completion{err: ErrCompletionAmbiguous}:
	default:
	}
}

var _ payload.ObjectReadinessAcknowledger = (*ObjectTransfer)(nil)
