package payload

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"sync"
	"testing"
)

func TestAcceptanceHandleConcurrentReadRetainReleaseAndShutdown(t *testing.T) {
	const owners = 128
	store, err := NewHandleStore(HandleLimits{
		MaximumHandles:    2,
		MaximumBytes:      1 << 20,
		MaximumPerHandle:  1 << 20,
		MaximumWriteBytes: 1 << 20,
		MaximumReadBytes:  1 << 20,
	}, &sequenceReader{})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	canonical := testCanonical("concurrent handle ownership")
	if err := store.Write(handle, canonical); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(handle); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < owners; i++ {
		if err := store.Retain(handle); err != nil {
			t.Fatalf("retain %d: %v", i, err)
		}
	}

	start := make(chan struct{})
	errs := make(chan error, owners*3)
	var wg sync.WaitGroup
	for i := 0; i < owners; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			value, digest, readErr := store.Snapshot(handle)
			if readErr != nil {
				errs <- readErr
				return
			}
			if !bytes.Equal(value, canonical) || digest != Digest(canonical) {
				errs <- errors.New("snapshot changed during ownership race")
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			value, done, readErr := store.Read(handle, 0, len(canonical))
			if readErr != nil {
				errs <- readErr
				return
			}
			if !done || !bytes.Equal(value, canonical) {
				errs <- errors.New("range read changed during ownership race")
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			if releaseErr := store.Release(handle); releaseErr != nil {
				errs <- releaseErr
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if err := store.Release(handle); err != nil {
		t.Fatalf("final release: %v", err)
	}
	if _, _, err := store.Snapshot(handle); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("stale handle accepted: %v", err)
	}
	store.Close()
	store.Close()
}

func TestAcceptanceAEADRejectsEveryFrameAndContextTamper(t *testing.T) {
	wrapper := authenticatedTestWrapper{secret: []byte("acceptance identity secret")}
	cipher, err := NewAEADCipher(wrapper, wrapper, &sequenceReader{})
	if err != nil {
		t.Fatal(err)
	}
	plaintext := testCanonical("tamper every ciphertext byte")
	binding := cryptoBinding(plaintext)
	ciphertext, ref, err := cipher.Encrypt(context.Background(), binding, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	for index := range ciphertext {
		tampered := clone(ciphertext)
		tampered[index] ^= 0x80
		opened, err := cipher.Decrypt(context.Background(), binding, tampered, ref)
		zero(opened)
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("ciphertext byte %d tamper error = %v", index, err)
		}
	}

	contexts := map[string]func(*TransferBinding){
		"mesh":      func(v *TransferBinding) { v.MeshID += "-other" },
		"sender":    func(v *TransferBinding) { v.SenderID += "-other" },
		"recipient": func(v *TransferBinding) { v.RecipientID += "-other" },
		"message":   func(v *TransferBinding) { v.MessageID = "msg_AQEBAQEBAQEBAQEBAQEBAQ" },
		"transfer":  func(v *TransferBinding) { v.TransferID = "xfer_AQEBAQEBAQEBAQEBAQEBAQ" },
		"size":      func(v *TransferBinding) { v.CanonicalSize++ },
		"digest":    func(v *TransferBinding) { v.CanonicalDigest[0] ^= 1 },
	}
	for name, mutate := range contexts {
		wrong := binding
		mutate(&wrong)
		opened, err := cipher.Decrypt(context.Background(), wrong, ciphertext, ref)
		zero(opened)
		if !errors.Is(err, ErrAuthentication) {
			t.Errorf("%s context tamper error = %v", name, err)
		}
	}

	decodedRef, err := decodeEncryptionRef(ref)
	if err != nil {
		t.Fatal(err)
	}
	for index := range decodedRef {
		tampered := clone(decodedRef)
		tampered[index] ^= 1
		tamperedRef := encryptionRefPrefix + base64.RawURLEncoding.EncodeToString(tampered)
		opened, err := cipher.Decrypt(context.Background(), binding, ciphertext, tamperedRef)
		zero(opened)
		zero(tampered)
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("wrapped-key byte %d tamper error = %v", index, err)
		}
	}
	zero(decodedRef)
}
