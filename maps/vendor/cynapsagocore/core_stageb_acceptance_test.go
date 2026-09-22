package cynapsagocore

import (
	"context"
	"encoding/hex"
	"strconv"
	"sync"
	"testing"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
)

func TestStageBAcceptancePublicPollingPayloadShutdownAndDestroyRaces(t *testing.T) {
	core, err := New(Config{QueueLimit: 64, PayloadLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	handle, err := core.PayloadOpen(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := hex.DecodeString("a500010100026003612f044178")
	if err != nil {
		t.Fatal(err)
	}
	if written, err := core.PayloadWrite(context.Background(), handle, canonical); err != nil || written != len(canonical) {
		t.Fatalf("PayloadWrite = %d, %v", written, err)
	}
	if size, err := core.PayloadFinish(context.Background(), handle); err != nil || size != int64(len(canonical)) {
		t.Fatalf("PayloadFinish = %d, %v", size, err)
	}
	const readers = 32
	for range readers {
		if err := core.PayloadRetain(context.Background(), handle); err != nil {
			t.Fatal(err)
		}
	}

	for index := 0; index < 32; index++ {
		command := v1.CoreInitCommand{CommandBase: v1.CommandBase{
			CommandID:    v1.CommandID("qa-command-" + strconv.Itoa(index)),
			SDKSessionID: "qa-session",
		}}
		if admission, err := core.Submit(context.Background(), command); err != nil || !admission.Accepted {
			t.Fatalf("Submit %d = %#v, %v", index, admission, err)
		}
	}

	stopPolling := make(chan struct{})
	pollErrors := make(chan error, 2)
	var pollers sync.WaitGroup
	for _, poll := range []func(context.Context) error{
		func(ctx context.Context) error { _, err := core.NextCompletion(ctx); return err },
		func(ctx context.Context) error { _, err := core.NextEvent(ctx); return err },
	} {
		pollers.Add(1)
		go func(poll func(context.Context) error) {
			defer pollers.Done()
			for {
				select {
				case <-stopPolling:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				err := poll(ctx)
				cancel()
				if err == nil {
					continue
				}
				code := publicCode(err)
				if code != v1.ErrorCodeDeliveryTimeout && code != v1.ErrorCodeShutdownInProgress {
					pollErrors <- err
					return
				}
			}
		}(poll)
	}

	start := make(chan struct{})
	operationErrors := make(chan error, readers*3)
	var operations sync.WaitGroup
	for range readers {
		operations.Add(3)
		go func() {
			defer operations.Done()
			<-start
			chunk, eof, err := core.PayloadRead(context.Background(), handle, 0, len(canonical))
			if err == nil && (!eof || string(chunk) != string(canonical)) {
				operationErrors <- &v1.Error{Code: v1.ErrorCodePayloadIntegrityFailed, Message: "payload changed during race"}
			} else if err != nil && publicCode(err) != v1.ErrorCodeShutdownInProgress && publicCode(err) != v1.ErrorCodeInvalidHandle {
				operationErrors <- err
			}
		}()
		go func() {
			defer operations.Done()
			<-start
			if err := core.PayloadCancel(context.Background(), handle); err != nil && publicCode(err) != v1.ErrorCodeShutdownInProgress && publicCode(err) != v1.ErrorCodeInvalidHandle {
				operationErrors <- err
			}
		}()
		go func() {
			defer operations.Done()
			<-start
			if err := core.PayloadRelease(context.Background(), handle); err != nil && publicCode(err) != v1.ErrorCodeShutdownInProgress && publicCode(err) != v1.ErrorCodeInvalidHandle {
				operationErrors <- err
			}
		}()
	}
	close(start)
	operations.Wait()
	close(operationErrors)
	for err := range operationErrors {
		t.Errorf("public operation race: %v", err)
	}

	shutdownAndDrain(t, core)
	close(stopPolling)
	pollers.Wait()
	close(pollErrors)
	for err := range pollErrors {
		t.Errorf("polling race: %v", err)
	}

	const destroyers = 64
	destroyErrors := make(chan error, destroyers)
	var destroys sync.WaitGroup
	for range destroyers {
		destroys.Add(1)
		go func() {
			defer destroys.Done()
			destroyErrors <- core.Destroy()
		}()
	}
	destroys.Wait()
	close(destroyErrors)
	for err := range destroyErrors {
		if err != nil {
			t.Errorf("concurrent Destroy: %v", err)
		}
	}
	if _, err := core.Status(context.Background()); publicCode(err) != v1.ErrorCodeInvalidHandle {
		t.Fatalf("Status after destruction = %v", err)
	}
}
