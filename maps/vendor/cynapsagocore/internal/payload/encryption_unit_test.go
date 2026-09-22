package payload

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type canaryKeyProvider struct {
	output []byte
	err    error
}

func (p *canaryKeyProvider) SealKey(context.Context, KeyContext, []byte) ([]byte, error) {
	p.output = []byte("private partial key output")
	return p.output, p.err
}
func (p *canaryKeyProvider) OpenKey(context.Context, KeyContext, []byte) ([]byte, error) {
	p.output = bytes.Repeat([]byte{9}, dataKeyBytes)
	return p.output, p.err
}

// authenticatedTestWrapper is deterministic test-only key wrapping. Production
// must inject the real authenticated identity-key provider documented by ADR 0005.
type authenticatedTestWrapper struct{ secret []byte }

func (w authenticatedTestWrapper) SealKey(_ context.Context, keyContext KeyContext, key []byte) ([]byte, error) {
	aad, err := encryptionAssociatedData(keyContext)
	if err != nil {
		return nil, err
	}
	mask := sha256.Sum256(append(clone(w.secret), aad...))
	wrapped := make([]byte, len(key)+sha256.Size)
	for i := range key {
		wrapped[i] = key[i] ^ mask[i%len(mask)]
	}
	mac := hmac.New(sha256.New, w.secret)
	_, _ = mac.Write(aad)
	_, _ = mac.Write(wrapped[:len(key)])
	copy(wrapped[len(key):], mac.Sum(nil))
	return wrapped, nil
}
func (w authenticatedTestWrapper) OpenKey(_ context.Context, keyContext KeyContext, wrapped []byte) ([]byte, error) {
	if len(wrapped) != dataKeyBytes+sha256.Size {
		return nil, ErrAuthentication
	}
	aad, err := encryptionAssociatedData(keyContext)
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha256.New, w.secret)
	_, _ = mac.Write(aad)
	_, _ = mac.Write(wrapped[:dataKeyBytes])
	expected := mac.Sum(nil)
	if subtle.ConstantTimeCompare(expected, wrapped[dataKeyBytes:]) != 1 {
		return nil, ErrAuthentication
	}
	mask := sha256.Sum256(append(clone(w.secret), aad...))
	key := make([]byte, dataKeyBytes)
	for i := range key {
		key[i] = wrapped[i] ^ mask[i%len(mask)]
	}
	return key, nil
}

type sequenceReader struct {
	mu   sync.Mutex
	next byte
}

func (r *sequenceReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range p {
		r.next++
		p[i] = r.next
	}
	return len(p), nil
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

type oversizedSealer struct{}

func (oversizedSealer) SealKey(context.Context, KeyContext, []byte) ([]byte, error) {
	return make([]byte, 256), nil
}
func (oversizedSealer) OpenKey(context.Context, KeyContext, []byte) ([]byte, error) {
	return make([]byte, 32), nil
}

type partialKeyProvider struct{ output []byte }

func (p *partialKeyProvider) SealKey(context.Context, KeyContext, []byte) ([]byte, error) {
	p.output = []byte("partial wrapped key")
	return p.output, context.DeadlineExceeded
}

type countingResolver struct{ calls int }

func (c *countingResolver) OpenKey(context.Context, KeyContext, []byte) ([]byte, error) {
	c.calls++
	return make([]byte, dataKeyBytes), nil
}

type blockingKeyProvider struct{}

func (blockingKeyProvider) SealKey(ctx context.Context, _ KeyContext, _ []byte) ([]byte, error) {
	<-ctx.Done()
	return []byte("partial"), errors.New("opaque provider error")
}
func (blockingKeyProvider) OpenKey(ctx context.Context, _ KeyContext, _ []byte) ([]byte, error) {
	<-ctx.Done()
	return bytes.Repeat([]byte{1}, dataKeyBytes), errors.New("opaque provider error")
}
func (p *partialKeyProvider) OpenKey(context.Context, KeyContext, []byte) ([]byte, error) {
	p.output = bytes.Repeat([]byte{7}, dataKeyBytes)
	return p.output, context.DeadlineExceeded
}

func cryptoBinding(value []byte) TransferBinding {
	binding := testBinding("xfer_AAAAAAAAAAAAAAAAAAAAAA")
	binding.CanonicalSize = int64(len(value))
	binding.CanonicalDigest = Digest(value)
	return binding
}

func TestAEADCipherRoundTripUniquenessAndVector(t *testing.T) {
	t.Parallel()
	wrapper := authenticatedTestWrapper{secret: []byte("test identity wrapping secret")}
	cipher, err := NewAEADCipher(wrapper, wrapper, &sequenceReader{})
	if err != nil {
		t.Fatal(err)
	}
	plaintext := []byte("canonical payload")
	binding := cryptoBinding(plaintext)
	first, ref, err := cipher.Encrypt(context.Background(), binding, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	second, secondRef, err := cipher.Encrypt(context.Background(), binding, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, second) || ref == secondRef {
		t.Fatal("ephemeral key/nonce reused")
	}
	if first[0] != encryptionVersion1 || len(first) != 1+nonceBytes+len(plaintext)+16 {
		t.Fatalf("frame length %d", len(first))
	}
	opened, err := cipher.Decrypt(context.Background(), binding, first, ref)
	if err != nil || !bytes.Equal(opened, plaintext) {
		t.Fatalf("open %q %v", opened, err)
	}
	aad, _ := encryptionAssociatedData(keyContextFromBinding(binding))
	const aadGolden = "43594e415053412d5041594c4f41442d414541442d563200000000046d6573680000000673656e64657200000009726563697069656e740000001a6d73675f414141414141414141414141414141414141414141410000001b786665725f4141414141414141414141414141414141414141414100000000000000114a64e29359c7d3f9be9aa5118f928b72226ff181b3123da6cea94b4ef8a1d993"
	got := hex.EncodeToString(aad)
	if aadGolden != "" && got != aadGolden {
		t.Fatalf("AAD vector changed: %s", got)
	} else if aadGolden == "" {
		t.Logf("aad=%s", got)
	}
}

func TestAEADCipherRejectsTamperWrongContextKeyAndMalformed(t *testing.T) {
	t.Parallel()
	wrapper := authenticatedTestWrapper{secret: []byte("secret one")}
	cipher, _ := NewAEADCipher(wrapper, wrapper, &sequenceReader{})
	plaintext := []byte("canonical payload")
	binding := cryptoBinding(plaintext)
	ciphertext, ref, _ := cipher.Encrypt(context.Background(), binding, plaintext)
	for _, index := range []int{0, 1, len(ciphertext) - 1} {
		tampered := clone(ciphertext)
		tampered[index] ^= 1
		if _, err := cipher.Decrypt(context.Background(), binding, tampered, ref); !errors.Is(err, ErrAuthentication) {
			t.Fatalf("tamper %d: %v", index, err)
		}
	}
	contexts := []TransferBinding{binding, binding, binding, binding, binding}
	contexts[0].MeshID = "other"
	contexts[1].SenderID = "other"
	contexts[2].RecipientID = "other"
	contexts[3].MessageID = "msg_AQEBAQEBAQEBAQEBAQEBAQ"
	contexts[4].TransferID = "xfer_AQEBAQEBAQEBAQEBAQEBAQ"
	for i, wrong := range contexts {
		if _, err := cipher.Decrypt(context.Background(), wrong, ciphertext, ref); !errors.Is(err, ErrAuthentication) {
			t.Fatalf("context %d: %v", i, err)
		}
	}
	wrongWrapper := authenticatedTestWrapper{secret: []byte("secret two")}
	wrongCipher, _ := NewAEADCipher(wrongWrapper, wrongWrapper, &sequenceReader{})
	if _, err := wrongCipher.Decrypt(context.Background(), binding, ciphertext, ref); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("wrong key: %v", err)
	}
	for _, bad := range []string{"", "enc1_", "enc1_***", ref + "="} {
		if _, err := cipher.Decrypt(context.Background(), binding, ciphertext, bad); !errors.Is(err, ErrAuthentication) {
			t.Fatalf("ref %q: %v", bad, err)
		}
	}
	if _, err := cipher.Decrypt(context.Background(), binding, ciphertext[:10], ref); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("short: %v", err)
	}
}

func TestAEADCipherFailuresAndConcurrency(t *testing.T) {
	t.Parallel()
	wrapper := authenticatedTestWrapper{secret: []byte("secret")}
	plaintext := []byte("canonical payload")
	binding := cryptoBinding(plaintext)
	cipher, _ := NewAEADCipher(wrapper, wrapper, failingReader{})
	if _, _, err := cipher.Encrypt(context.Background(), binding, plaintext); !errors.Is(err, ErrEncryption) {
		t.Fatalf("entropy: %v", err)
	}
	tooLarge, _ := NewAEADCipher(oversizedSealer{}, oversizedSealer{}, &sequenceReader{})
	if _, _, err := tooLarge.Encrypt(context.Background(), binding, plaintext); !errors.Is(err, ErrEncryption) {
		t.Fatalf("wrapped size: %v", err)
	}
	concurrent, _ := NewAEADCipher(wrapper, wrapper, &sequenceReader{})
	const count = 64
	refs := make(chan string, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, ref, err := concurrent.Encrypt(context.Background(), binding, plaintext)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := concurrent.Decrypt(context.Background(), binding, value, ref); err != nil {
				t.Error(err)
			}
			refs <- ref
		}()
	}
	wg.Wait()
	close(refs)
	seen := map[string]bool{}
	for ref := range refs {
		if seen[ref] {
			t.Fatal("duplicate wrapped key")
		}
		seen[ref] = true
	}
}

func TestAEADCipherZerosPartialProviderOutputsAndPreservesDeadline(t *testing.T) {
	plaintext := testCanonical("partial provider")
	binding := cryptoBinding(plaintext)
	provider := &partialKeyProvider{}
	cipher, _ := NewAEADCipher(provider, provider, &sequenceReader{})
	if _, _, err := cipher.Encrypt(context.Background(), binding, plaintext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("seal deadline %v", err)
	}
	if !bytes.Equal(provider.output, make([]byte, len(provider.output))) {
		t.Fatalf("wrapped key retained: %x", provider.output)
	}

	wrapper := authenticatedTestWrapper{secret: []byte("secret")}
	good, _ := NewAEADCipher(wrapper, wrapper, &sequenceReader{})
	ciphertext, ref, err := good.Encrypt(context.Background(), binding, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	provider = &partialKeyProvider{}
	cipher, _ = NewAEADCipher(wrapper, provider, &sequenceReader{})
	if _, err := cipher.Decrypt(context.Background(), binding, ciphertext, ref); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("open deadline %v", err)
	}
	if !bytes.Equal(provider.output, make([]byte, len(provider.output))) {
		t.Fatalf("resolved key retained: %x", provider.output)
	}
}

func TestAEADCipherRejectsWrongGeometryBeforeKeyResolution(t *testing.T) {
	plaintext := testCanonical("geometry")
	binding := cryptoBinding(plaintext)
	wrapper := authenticatedTestWrapper{secret: []byte("secret")}
	good, _ := NewAEADCipher(wrapper, wrapper, &sequenceReader{})
	ciphertext, ref, err := good.Encrypt(context.Background(), binding, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &countingResolver{}
	checked, _ := NewAEADCipher(wrapper, resolver, &sequenceReader{})
	attacks := [][]byte{
		clone(ciphertext[:len(ciphertext)-1]),
		append(clone(ciphertext), 0),
		make([]byte, 1<<20),
	}
	attacks[2][0] = encryptionVersion1
	for index, attack := range attacks {
		if _, err := checked.Decrypt(context.Background(), binding, attack, ref); !errors.Is(err, ErrAuthentication) {
			t.Errorf("attack %d: %v", index, err)
		}
	}
	if resolver.calls != 0 {
		t.Fatalf("resolver called %d times", resolver.calls)
	}
}

func TestAEADCipherKeySealIsBoundedByCallerDeadline(t *testing.T) {
	plaintext := testCanonical("blocking key provider")
	binding := cryptoBinding(plaintext)
	cipher, _ := NewAEADCipher(blockingKeyProvider{}, blockingKeyProvider{}, &sequenceReader{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, _, err := cipher.Encrypt(ctx, binding, plaintext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline %v", err)
	}
}

func TestAEADCipherNormalizesWrappedDependencyContextErrors(t *testing.T) {
	canonical := testCanonical("dependency canary")
	binding := cryptoBinding(canonical)
	for _, contextClass := range []error{context.Canceled, context.DeadlineExceeded} {
		wrapped := errors.Join(errors.New("PRIVATE_KEY_PROVIDER_CANARY"), contextClass)
		provider := &canaryKeyProvider{err: wrapped}
		cipher, _ := NewAEADCipher(provider, provider, &sequenceReader{})
		_, _, err := cipher.Encrypt(context.Background(), binding, canonical)
		if !errors.Is(err, contextClass) || strings.Contains(err.Error(), "CANARY") {
			t.Fatalf("seal error %q", err)
		}
		if !bytes.Equal(provider.output, make([]byte, len(provider.output))) {
			t.Fatal("seal partial output not zeroed")
		}

		wrapper := authenticatedTestWrapper{secret: []byte("test-only-secret")}
		encrypter, _ := NewAEADCipher(wrapper, wrapper, &sequenceReader{})
		ciphertext, ref, encryptErr := encrypter.Encrypt(context.Background(), binding, canonical)
		if encryptErr != nil {
			t.Fatal(encryptErr)
		}
		provider = &canaryKeyProvider{err: wrapped}
		cipher, _ = NewAEADCipher(wrapper, provider, &sequenceReader{})
		_, err = cipher.Decrypt(context.Background(), binding, ciphertext, ref)
		if !errors.Is(err, contextClass) || strings.Contains(err.Error(), "CANARY") {
			t.Fatalf("resolve error %q", err)
		}
		if !bytes.Equal(provider.output, make([]byte, len(provider.output))) {
			t.Fatal("resolver partial output not zeroed")
		}
	}
}
