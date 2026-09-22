package sessionkernel

import (
	"context"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/enrollment"
)

func TestEnrollmentRetryPolicyIsNarrowAndDeadlineBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if retryableEnrollmentFailure(ctx, &enrollment.Failure{Code: enrollment.FailureRejected}) ||
		retryableEnrollmentFailure(ctx, &enrollment.Failure{Code: enrollment.FailureInvalidResponse}) ||
		retryableEnrollmentFailure(ctx, &enrollment.Failure{Code: enrollment.FailureCancelled}) {
		t.Fatal("non-transient enrollment failure was retryable")
	}
	if !retryableEnrollmentFailure(ctx, &enrollment.Failure{Code: enrollment.FailureUnavailable}) ||
		!retryableEnrollmentFailure(ctx, &enrollment.Failure{Code: enrollment.FailureDeadline}) {
		t.Fatal("transient enrollment failure was not retryable with command budget")
	}
	nearDeadline, stopNearDeadline := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stopNearDeadline()
	if failure := waitForEnrollmentRetry(nearDeadline, 50*time.Millisecond); failure == nil || failure.Code != ProviderDeadline {
		t.Fatalf("retry wait exceeded command budget: %#v", failure)
	}
	cancel()
	if retryableEnrollmentFailure(ctx, &enrollment.Failure{Code: enrollment.FailureUnavailable}) {
		t.Fatal("expired command remained retryable")
	}
}
