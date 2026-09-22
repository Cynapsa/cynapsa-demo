package payload

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Cynapsa/cynapsagocore/internal/protocol"
)

const dependencySecretCanary = "https://user:password@private.invalid/object?token=secret"

type leakingDeadlineKeyProvider struct{}

func (leakingDeadlineKeyProvider) SealKey(context.Context, KeyContext, []byte) ([]byte, error) {
	return []byte("partial wrapped secret"), fmt.Errorf("key provider %s: %w", dependencySecretCanary, context.DeadlineExceeded)
}
func (leakingDeadlineKeyProvider) OpenKey(context.Context, KeyContext, []byte) ([]byte, error) {
	return make([]byte, dataKeyBytes), fmt.Errorf("key resolver %s: %w", dependencySecretCanary, context.DeadlineExceeded)
}

type leakingDeadlineObjects struct{}

func (leakingDeadlineObjects) Available(context.Context) (bool, error) { return true, nil }
func (leakingDeadlineObjects) Upload(context.Context, []byte) (string, error) {
	return "", fmt.Errorf("upload %s: %w", dependencySecretCanary, context.DeadlineExceeded)
}
func (leakingDeadlineObjects) Download(context.Context, string) ([]byte, error) {
	return nil, fmt.Errorf("download %s: %w", dependencySecretCanary, context.DeadlineExceeded)
}

func TestAcceptanceDependencyDeadlineErrorsPreserveClassWithoutPrivateCanary(t *testing.T) {
	assertRedactedDeadline := func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline class lost: %v", err)
		}
		if strings.Contains(err.Error(), dependencySecretCanary) || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "token=secret") {
			t.Fatalf("private dependency error leaked: %v", err)
		}
	}

	t.Run("key sealer", func(t *testing.T) {
		plaintext := testCanonical("secret error redaction")
		binding := cryptoBinding(plaintext)
		cipher, err := NewAEADCipher(leakingDeadlineKeyProvider{}, leakingDeadlineKeyProvider{}, &sequenceReader{})
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = cipher.Encrypt(context.Background(), binding, plaintext)
		assertRedactedDeadline(t, err)
	})

	t.Run("object download", func(t *testing.T) {
		canonical := testCanonical("secret object error redaction")
		binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
		binding.CanonicalSize = int64(len(canonical))
		binding.CanonicalDigest = Digest(canonical)
		privateReference, err := encodeObjectReference(binding.TransferID, "https://objects.example/blob")
		if err != nil {
			t.Fatal(err)
		}
		descriptor, err := referencedDescriptor(protocol.PayloadObjectReference, binding.Profile, privateReference, "enc1_AQ", canonical)
		if err != nil {
			t.Fatal(err)
		}
		materializer, err := NewMaterializer(1024, leakingDeadlineObjects{}, fakeCipher{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = materializer.Materialize(context.Background(), descriptor, binding)
		assertRedactedDeadline(t, err)
	})
}
