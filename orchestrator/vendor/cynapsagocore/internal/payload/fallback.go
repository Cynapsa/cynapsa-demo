package payload

import (
	"context"
	"errors"
)

// CarrierAvailability reports private mechanisms currently permitted and usable.
type CarrierAvailability struct {
	DirectChunks  bool
	ObjectUpload  bool
	MessageChunks bool
}

type FailureClass uint8

const (
	FailureRetryable FailureClass = iota + 1
	FailureFallbackEligible
	FailureTerminal
)

// BuildFallbackPlan returns the exact ADR 0005 order without inventing a path.
func BuildFallbackPlan(availability CarrierAvailability, limits Limits) ([]CarrierKind, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	plan := make([]CarrierKind, 0, 3)
	if availability.DirectChunks {
		plan = append(plan, CarrierDirectChunks)
	}
	if availability.ObjectUpload {
		plan = append(plan, CarrierObjectUpload)
	}
	if availability.MessageChunks {
		plan = append(plan, CarrierMessageChunks)
	}
	if len(plan) == 0 {
		return nil, ErrAllCarriersFailed
	}
	return plan, nil
}

// ClassifyCarrierFailure identifies fallback eligibility without exposing a
// dependency error. Integrity, authentication, authorization, cancellation,
// and local safety failures are terminal.
func ClassifyCarrierFailure(kind CarrierKind, err error) FailureClass {
	if err == nil {
		return FailureTerminal
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrIntegrity) || errors.Is(err, ErrAuthentication) || errors.Is(err, ErrAuthorization) || errors.Is(err, ErrPayloadTooLarge) || errors.Is(err, ErrReassemblyQuota) {
		return FailureTerminal
	}
	if errors.Is(err, ErrFrameTooLarge) {
		if kind == CarrierDirectChunks {
			return FailureFallbackEligible
		}
		return FailureTerminal
	}
	if errors.Is(err, ErrCarrierUnavailable) || errors.Is(err, ErrCarrierUnsupported) || errors.Is(err, ErrCarrierRejected) || errors.Is(err, ErrCarrierTimeout) || errors.Is(err, ErrCarrierUpload) || errors.Is(err, ErrCarrierMaterialization) || errors.Is(err, ErrAmbiguousCompletion) {
		return FailureFallbackEligible
	}
	if kind == CarrierDirectChunks || kind == CarrierObjectUpload {
		return FailureFallbackEligible
	}
	return FailureTerminal
}
