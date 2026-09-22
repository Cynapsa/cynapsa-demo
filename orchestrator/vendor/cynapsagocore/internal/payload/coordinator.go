package payload

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

// Send applies the exact direct, object, then durable-message ladder. It never
// changes application message identity and returns only authenticated terminal evidence.
func (c *Coordinator) Send(ctx context.Context, request TransferRequest) (TransferReceipt, error) {
	if c == nil || ctx == nil || !validMessageID(request.MessageID) || request.PeerID == "" || len(request.PeerID) > protocol.MaxAgentIdentityBytes || !validPrivateText(request.PeerID) || protocol.ValidateMeshID(request.MeshID) != nil || protocol.ValidateAgentIdentity(request.SenderID) != nil || protocol.ValidateAgentIdentity(request.RecipientID) != nil || protocol.ValidateConversationID(request.ConversationID) != nil || !validCanonicalProfile(request.Profile) || request.Mode.Validate() != nil {
		return TransferReceipt{}, ErrInvalidManifest
	}
	if err := ctx.Err(); err != nil {
		return TransferReceipt{}, err
	}
	operation, cancelOperation := context.WithTimeout(ctx, c.limits.TransferLifetime)
	defer cancelOperation()
	if int64(len(request.Canonical)) > c.limits.MaximumPayloadBytes {
		return TransferReceipt{}, ErrPayloadTooLarge
	}
	if err := validateCanonicalProfileOnly(request.Canonical, request.Profile, c.limits.MaximumPayloadBytes); err != nil {
		return TransferReceipt{}, err
	}
	large := int64(len(request.Canonical)) > c.limits.InlineBytes
	if large {
		c.activeLarge.Add(1)
		defer c.activeLarge.Add(-1)
	}
	route := routeFor(request)
	if int64(len(request.Canonical)) <= c.limits.InlineBytes {
		descriptor, err := protocol.NewInlinePayload(request.Profile, request.Canonical)
		if err != nil {
			return TransferReceipt{}, err
		}
		if err := c.publish(operation, request, route, descriptor); err != nil {
			return TransferReceipt{}, err
		}
		return TransferReceipt{MessageID: request.MessageID, Descriptor: descriptor}, nil
	}

	availability := CarrierAvailability{}
	if c.direct != nil {
		available, err := authorityValue(operation, func() (bool, error) { return c.direct.Available(operation, route) })
		if contextErr := operation.Err(); contextErr != nil {
			return TransferReceipt{}, contextErr
		}
		if contextErr := dependencyContextError(operation, err); contextErr != nil {
			return TransferReceipt{}, contextErr
		}
		availability.DirectChunks = err == nil && available
	}
	preparedObjects, canPrepareObject := c.objects.(PreparedObjectStore)
	objectReadiness, canConfirmObject := c.objectEvidence.(ObjectReadinessAcknowledger)
	if canPrepareObject && canConfirmObject {
		available, err := authorityValue(operation, func() (bool, error) { return c.objects.Available(operation) })
		if contextErr := operation.Err(); contextErr != nil {
			return TransferReceipt{}, contextErr
		}
		if contextErr := dependencyContextError(operation, err); contextErr != nil {
			return TransferReceipt{}, contextErr
		}
		availability.ObjectUpload = err == nil && available
	}
	if c.messages != nil {
		available, err := authorityValue(operation, func() (bool, error) { return c.messages.Available(operation, route) })
		if contextErr := operation.Err(); contextErr != nil {
			return TransferReceipt{}, contextErr
		}
		if contextErr := dependencyContextError(operation, err); contextErr != nil {
			return TransferReceipt{}, contextErr
		}
		availability.MessageChunks = err == nil && available
	}
	plan, err := BuildFallbackPlan(availability, c.limits)
	if err != nil {
		return TransferReceipt{}, err
	}

	// One transferred snapshot, transfer ID, and descriptor survive the entire
	// ladder. V1 uses the canonical bytes directly when no end-to-end cipher is
	// configured. This avoids changing signed logical-envelope content on
	// fallback while preserving the future encryption seam.
	transferID, err := NewTransferID(nil)
	if err != nil {
		return TransferReceipt{}, err
	}
	binding := bindingFor(request, transferID)
	var transferred []byte
	encryptionRef := ""
	if c.cipher != nil {
		type encryptedPayload struct {
			bytes []byte
			ref   string
		}
		var encrypted encryptedPayload
		encrypted, err = authorityValue(operation, func() (encryptedPayload, error) {
			value, ref, encryptErr := c.cipher.Encrypt(operation, binding, request.Canonical)
			return encryptedPayload{bytes: value, ref: ref}, encryptErr
		})
		transferred, encryptionRef = encrypted.bytes, encrypted.ref
		if contextErr := operation.Err(); contextErr != nil {
			zero(transferred)
			return TransferReceipt{}, contextErr
		}
		if err != nil || len(transferred) != len(request.Canonical)+encryptionFrameOverhead || int64(len(transferred)) > MaximumTransferredBytes {
			zero(transferred)
			if contextErr := dependencyContextError(operation, err); contextErr != nil {
				return TransferReceipt{}, contextErr
			}
			return TransferReceipt{}, ErrEncryption
		}
		wrapped, refErr := decodeEncryptionRef(encryptionRef)
		zero(wrapped)
		if refErr != nil {
			zero(transferred)
			return TransferReceipt{}, ErrEncryption
		}
	} else {
		transferred = clone(request.Canonical)
	}
	if len(transferred) == 0 || int64(len(transferred)) > MaximumTransferredBytes {
		zero(transferred)
		return TransferReceipt{}, ErrPayloadTooLarge
	}
	defer zero(transferred)
	manifestNow := c.now().UTC()
	manifest, err := BuildManifest(binding, request.Canonical, transferred, c.limits.ChunkBytes, encryptionRef, manifestNow.Add(c.limits.TransferLifetime), nil)
	if err != nil {
		return TransferReceipt{}, err
	}
	if err := ValidateTransferManifest(manifest, binding, c.limits, manifestNow); err != nil {
		return TransferReceipt{}, err
	}
	descriptor, err := referencedDescriptor(protocol.PayloadTransferReference, request.Profile, transferID, encryptionRef, request.Canonical)
	if err != nil {
		return TransferReceipt{}, err
	}
	// Registration happens before upload/chunks. Carrier completion therefore
	// cannot create an unmatchable completed transfer at the receiver.
	if err := c.publish(operation, request, route, descriptor); err != nil {
		return TransferReceipt{}, err
	}

	for _, carrier := range plan {
		if err := operation.Err(); err != nil {
			return TransferReceipt{}, err
		}
		switch carrier {
		case CarrierDirectChunks:
			_, err = sendChunks(operation, c.direct, route, manifest, transferred, FrameBinary, c.limits.MaximumFrameBytes, c.limits.CleanupTimeout)
			if contextErr := operation.Err(); contextErr != nil {
				return TransferReceipt{}, contextErr
			}
			if err == nil {
				return TransferReceipt{request.MessageID, transferID, carrier, descriptor}, nil
			}
			if ClassifyCarrierFailure(carrier, err) == FailureTerminal {
				return TransferReceipt{}, normalizedTransferError(err)
			}
		case CarrierObjectUpload:
			privateRef, uploadErr, contextErr, objectAttempted := func() (string, error, error, bool) {
				prepared, uploadErr := authorityValue(operation, func() (PreparedObjectUpload, error) {
					return preparedObjects.PrepareUpload(operation, int64(len(transferred)))
				})
				if prepared != nil {
					// PrepareUpload transfers every non-nil handle even when it also
					// returns an error. Retire that ownership exactly once on every
					// ordinary return and panic unwind before leaving this scope.
					defer abortPreparedObjectUpload(prepared)
				}
				if contextErr := operation.Err(); contextErr != nil {
					return "", uploadErr, contextErr, false
				}
				if contextErr := dependencyContextError(operation, uploadErr); contextErr != nil {
					return "", uploadErr, contextErr, false
				}
				if uploadErr == nil && prepared == nil {
					uploadErr = ErrCarrierRejected
				}
				if uploadErr != nil {
					return "", uploadErr, nil, false
				}

				reference, referenceErr := authorityValue(operation, func() (string, error) { return prepared.DownloadReference(), nil })
				if referenceErr != nil {
					return "", referenceErr, nil, true
				}
				privateRef, refErr := encodeObjectReference(manifest.TransferID, reference)
				if refErr != nil {
					return "", ErrCarrierRejected, nil, true
				}
				uploadErr = authorityError(operation, func() error { return objectReadiness.ConfirmObjectReadiness(operation, route, manifest, reference) })
				if contextErr := operation.Err(); contextErr != nil {
					return privateRef, uploadErr, contextErr, true
				}
				if contextErr := dependencyContextError(operation, uploadErr); contextErr != nil {
					return privateRef, uploadErr, contextErr, true
				}
				if uploadErr == nil {
					uploadErr = authorityError(operation, func() error { return prepared.Commit(operation, transferred) })
				}
				return privateRef, uploadErr, nil, true
			}()
			if contextErr != nil {
				if objectAttempted {
					c.abortObject(route, manifest.TransferID)
				}
				return TransferReceipt{}, contextErr
			}
			if objectAttempted {
				if uploadErr == nil {
					uploadErr = authorityError(operation, func() error { return c.objectEvidence.PublishObject(operation, route, manifest, privateRef) })
				}
				if contextErr := operation.Err(); contextErr != nil {
					c.abortObject(route, manifest.TransferID)
					return TransferReceipt{}, contextErr
				}
				if contextErr := dependencyContextError(operation, uploadErr); contextErr != nil {
					c.abortObject(route, manifest.TransferID)
					return TransferReceipt{}, contextErr
				}
				if uploadErr == nil {
					var evidence CompletionEvidence
					evidence, uploadErr = authorityValue(operation, func() (CompletionEvidence, error) {
						return c.objectEvidence.AwaitMaterialization(operation, route, manifest.TransferID)
					})
					if contextErr := operation.Err(); contextErr != nil {
						c.abortObject(route, manifest.TransferID)
						return TransferReceipt{}, contextErr
					}
					if contextErr := dependencyContextError(operation, uploadErr); contextErr != nil {
						c.abortObject(route, manifest.TransferID)
						return TransferReceipt{}, contextErr
					}
					if uploadErr == nil {
						uploadErr = verifyEvidence(manifest, evidence)
					}
				}
				if uploadErr == nil {
					return TransferReceipt{request.MessageID, transferID, carrier, descriptor}, nil
				}
				c.abortObject(route, manifest.TransferID)
			}
			if ClassifyCarrierFailure(carrier, uploadErr) == FailureTerminal {
				return TransferReceipt{}, normalizedTransferError(uploadErr)
			}
		case CarrierMessageChunks:
			_, sendErr := sendChunks(operation, c.messages, route, manifest, transferred, FrameText, c.limits.MaximumFrameBytes, c.limits.CleanupTimeout)
			if contextErr := operation.Err(); contextErr != nil {
				return TransferReceipt{}, contextErr
			}
			if sendErr == nil {
				return TransferReceipt{request.MessageID, transferID, carrier, descriptor}, nil
			}
			if ClassifyCarrierFailure(carrier, sendErr) == FailureTerminal {
				return TransferReceipt{}, normalizedTransferError(sendErr)
			}
		}
	}
	return TransferReceipt{}, ErrAllCarriersFailed
}

// abortPreparedObjectUpload contains dependency cleanup panics so retirement
// cannot replace the original carrier result or a panic already unwinding.
func abortPreparedObjectUpload(prepared PreparedObjectUpload) {
	if prepared == nil {
		return
	}
	defer func() { _ = recover() }()
	prepared.Abort()
}

func (c *Coordinator) publish(ctx context.Context, request TransferRequest, route CarrierRoute, descriptor protocol.PayloadDescriptor) error {
	descriptor.Inline = clone(descriptor.Inline)
	defer zero(descriptor.Inline)
	publication := EnvelopePublication{Route: route, ConversationID: request.ConversationID, Mode: request.Mode, CorrelationID: request.CorrelationID, ReplyTo: request.ReplyTo, CreatedAt: request.CreatedAt, ExpiresAt: request.ExpiresAt, ClockUncertainty: request.ClockUncertainty, Descriptor: descriptor}
	var err error
	if checkpoint, guarded := authorityCheckpointFromContext(ctx); guarded {
		err = publishPayloadEnvelopeGuarded(checkpoint, c.publisher, ctx, publication)
	} else {
		err = c.publisher.PublishPayloadEnvelope(ctx, publication)
	}
	if err != nil {
		if contextErr := dependencyContextError(ctx, err); contextErr != nil {
			return contextErr
		}
		if errors.Is(err, ErrCarrierUnavailable) || errors.Is(err, ErrPublicationRejected) || errors.Is(err, ErrAuthorization) || errors.Is(err, ErrQueueFull) || errors.Is(err, ErrInvalidHandle) || errors.Is(err, ErrPayloadTooLarge) || errors.Is(err, ErrIntegrity) || errors.Is(err, ErrEnvelopePublication) || errors.Is(err, ErrPublicationInternal) {
			return err
		}
		return ErrEnvelopePublication
	}
	return nil
}

func publishPayloadEnvelopeGuarded(checkpoint AuthorityCheckpoint, publisher EnvelopePublisher, ctx context.Context, publication EnvelopePublication) error {
	return checkpoint(ctx, func() error { return publisher.PublishPayloadEnvelope(ctx, publication) })
}

func (c *Coordinator) abortObject(route CarrierRoute, transferID string) {
	cleanup, cancel := context.WithTimeout(context.Background(), c.limits.CleanupTimeout)
	defer cancel()
	_ = c.objectEvidence.AbortMaterialization(cleanup, route, transferID)
}

func bindingFor(request TransferRequest, transferID string) TransferBinding {
	return TransferBinding{TransferID: transferID, MessageID: request.MessageID, MeshID: request.MeshID, SenderID: request.SenderID, RecipientID: request.RecipientID, Profile: request.Profile, CanonicalSize: int64(len(request.Canonical)), CanonicalDigest: Digest(request.Canonical)}
}

func routeFor(request TransferRequest) CarrierRoute {
	return CarrierRoute{PeerID: request.PeerID, MeshID: request.MeshID, SenderID: request.SenderID, RecipientID: request.RecipientID, MessageID: request.MessageID}
}

func sendChunks(ctx context.Context, carrier ChunkCarrier, route CarrierRoute, manifest TransferManifest, transferred []byte, encoding FrameEncoding, maximumFrameBytes int, cleanupTimeout time.Duration) (CompletionEvidence, error) {
	if carrier == nil {
		return CompletionEvidence{}, ErrCarrierUnavailable
	}
	manifestFrame, err := frameManifest(manifest, encoding, maximumFrameBytes)
	if err != nil {
		return CompletionEvidence{}, err
	}
	manifestFrameOwned := true
	complete := false
	// Keep scrubbing and dependency cleanup in one unwind guard so private
	// manifest bytes are gone before Abort can block or panic.
	defer func() {
		if manifestFrameOwned {
			zero(manifestFrame.Data)
		}
		if !complete {
			cleanup, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			defer cancel()
			func() {
				defer func() { _ = recover() }()
				_ = carrier.Abort(cleanup, route, manifest.TransferID)
			}()
		}
	}()
	beginErr := authorityError(ctx, func() error { return carrier.Begin(ctx, route, manifestFrame) })
	zero(manifestFrame.Data)
	manifestFrameOwned = false
	if beginErr != nil {
		if contextErr := dependencyContextError(ctx, beginErr); contextErr != nil {
			return CompletionEvidence{}, contextErr
		}
		return CompletionEvidence{}, beginErr
	}
	if err := ForEachChunk(manifest, transferred, func(chunk TransferChunk) error {
		frame, err := frameChunk(chunk, encoding, maximumFrameBytes)
		if err != nil {
			return err
		}
		defer zero(frame.Data)
		err = authorityError(ctx, func() error { return carrier.SendChunk(ctx, route, frame) })
		if contextErr := dependencyContextError(ctx, err); contextErr != nil {
			return contextErr
		}
		return err
	}); err != nil {
		return CompletionEvidence{}, err
	}
	evidence, err := authorityValue(ctx, func() (CompletionEvidence, error) { return carrier.Finish(ctx, route, manifest.TransferID) })
	if err != nil {
		if contextErr := dependencyContextError(ctx, err); contextErr != nil {
			return CompletionEvidence{}, contextErr
		}
		return CompletionEvidence{}, err
	}
	if err := verifyEvidence(manifest, evidence); err != nil {
		return CompletionEvidence{}, err
	}
	complete = true
	return evidence, nil
}

func frameManifest(manifest TransferManifest, encoding FrameEncoding, maximum int) (CarrierFrame, error) {
	var data []byte
	var err error
	switch encoding {
	case FrameBinary:
		data, err = EncodeManifest(manifest)
	case FrameText:
		var text string
		text, err = EncodeTextManifest(manifest)
		data = []byte(text)
	default:
		return CarrierFrame{}, ErrInvalidChunk
	}
	if err != nil || len(data) > maximum {
		zero(data)
		return CarrierFrame{}, ErrFrameTooLarge
	}
	return CarrierFrame{TransferID: manifest.TransferID, Encoding: encoding, Data: data}, nil
}
func frameChunk(chunk TransferChunk, encoding FrameEncoding, maximum int) (CarrierFrame, error) {
	var data []byte
	var err error
	switch encoding {
	case FrameBinary:
		data, err = EncodeChunk(chunk)
	case FrameText:
		var text string
		text, err = EncodeTextChunk(chunk)
		data = []byte(text)
	default:
		return CarrierFrame{}, ErrInvalidChunk
	}
	if err != nil || len(data) > maximum {
		zero(data)
		return CarrierFrame{}, ErrFrameTooLarge
	}
	return CarrierFrame{TransferID: chunk.TransferID, Index: chunk.Index, Encoding: encoding, Data: data}, nil
}

func verifyEvidence(manifest TransferManifest, evidence CompletionEvidence) error {
	if evidence.TransferID != manifest.TransferID || evidence.MessageID != manifest.MessageID || !bytes.Equal(evidence.Digest[:], manifest.CanonicalDigest) {
		return ErrAuthentication
	}
	return nil
}

func normalizedTransferError(err error) error {
	if contextErr := dependencyContextError(nil, err); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, ErrIntegrity) {
		return ErrIntegrity
	}
	if errors.Is(err, ErrAuthentication) {
		return ErrAuthentication
	}
	if errors.Is(err, ErrAuthorization) {
		return ErrAuthorization
	}
	return ErrAllCarriersFailed
}

func authorityError(ctx context.Context, call func() error) error {
	if call == nil {
		return ErrCarrierRejected
	}
	checkpoint, guarded := authorityCheckpointFromContext(ctx)
	if !guarded {
		return call()
	}
	return checkpoint(ctx, call)
}

func authorityValue[T any](ctx context.Context, call func() (T, error)) (T, error) {
	var value T
	if call == nil {
		return value, ErrCarrierRejected
	}
	checkpoint, guarded := authorityCheckpointFromContext(ctx)
	if !guarded {
		return call()
	}
	var callErr error
	err := checkpoint(ctx, func() error {
		value, callErr = call()
		return callErr
	})
	if err != nil {
		return value, err
	}
	return value, callErr
}
