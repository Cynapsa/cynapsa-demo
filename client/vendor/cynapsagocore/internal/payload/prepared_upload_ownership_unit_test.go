package payload

import (
	"context"
	"errors"
	"testing"
	"time"
)

type ownershipPreparedUpload struct {
	reference     string
	downloadPanic any
	commitPanic   any
	abortPanic    any
	commitErr     error
	commits       int
	aborts        int
}

func (upload *ownershipPreparedUpload) DownloadReference() string {
	if upload.downloadPanic != nil {
		panic(upload.downloadPanic)
	}
	return upload.reference
}

func (upload *ownershipPreparedUpload) Commit(context.Context, []byte) error {
	upload.commits++
	if upload.commitPanic != nil {
		panic(upload.commitPanic)
	}
	return upload.commitErr
}

func (upload *ownershipPreparedUpload) Abort() {
	upload.aborts++
	if upload.abortPanic != nil {
		panic(upload.abortPanic)
	}
}

type ownershipPreparedStore struct {
	prepared PreparedObjectUpload
	result   error
	prepare  func()
}

func (ownershipPreparedStore) Available(context.Context) (bool, error) { return true, nil }
func (ownershipPreparedStore) Upload(context.Context, []byte) (string, error) {
	return "", ErrCarrierUpload
}
func (ownershipPreparedStore) Download(context.Context, string) ([]byte, error) {
	return nil, ErrCarrierMaterialization
}
func (store ownershipPreparedStore) PrepareUpload(context.Context, int64) (PreparedObjectUpload, error) {
	if store.prepare != nil {
		store.prepare()
	}
	return store.prepared, store.result
}

type ownershipObjectEvidence struct {
	confirmPanic any
	manifest     TransferManifest
}

func (e *ownershipObjectEvidence) ConfirmObjectReadiness(context.Context, CarrierRoute, TransferManifest, string) error {
	if e.confirmPanic != nil {
		panic(e.confirmPanic)
	}
	return nil
}
func (e *ownershipObjectEvidence) PublishObject(_ context.Context, _ CarrierRoute, manifest TransferManifest, _ string) error {
	e.manifest = cloneManifest(manifest)
	return nil
}
func (e *ownershipObjectEvidence) AwaitMaterialization(_ context.Context, route CarrierRoute, transferID string) (CompletionEvidence, error) {
	var digest [32]byte
	copy(digest[:], e.manifest.CanonicalDigest)
	return CompletionEvidence{TransferID: transferID, MessageID: e.manifest.MessageID, Digest: digest}, nil
}
func (*ownershipObjectEvidence) AbortMaterialization(context.Context, CarrierRoute, string) error {
	return nil
}

func TestPreparedUploadReturnedWithOrdinaryErrorIsAbortedExactlyOnce(t *testing.T) {
	upload := &ownershipPreparedUpload{reference: "https://objects.example/blob"}
	coordinator := newPreparedOwnershipCoordinator(t, ownershipPreparedStore{prepared: upload, result: ErrCarrierUpload}, &ownershipObjectEvidence{})
	if _, err := coordinator.Send(context.Background(), transferRequest()); !errors.Is(err, ErrAllCarriersFailed) {
		t.Fatalf("Send() error = %v, want fallback exhaustion", err)
	}
	if upload.aborts != 1 || upload.commits != 0 {
		t.Fatalf("upload calls = commits %d, aborts %d; want 0, 1", upload.commits, upload.aborts)
	}
}

func TestPreparedUploadContextErrorPrecedenceStillAbortsExactlyOnce(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	upload := &ownershipPreparedUpload{reference: "https://objects.example/blob"}
	store := ownershipPreparedStore{
		prepared: upload,
		result:   errors.Join(errors.New("private prepare error"), context.DeadlineExceeded),
		prepare:  cancel,
	}
	coordinator := newPreparedOwnershipCoordinator(t, store, &ownershipObjectEvidence{})
	if _, err := coordinator.Send(parent, transferRequest()); !errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send() error = %v, want exact caller cancellation precedence", err)
	}
	if upload.aborts != 1 {
		t.Fatalf("upload aborts = %d, want exactly 1", upload.aborts)
	}
}

func TestPreparedUploadNilValueAndNilErrorFailsClosed(t *testing.T) {
	coordinator := newPreparedOwnershipCoordinator(t, ownershipPreparedStore{}, &ownershipObjectEvidence{})
	if _, err := coordinator.Send(context.Background(), transferRequest()); !errors.Is(err, ErrAllCarriersFailed) {
		t.Fatalf("Send() error = %v, want fallback exhaustion", err)
	}
}

func TestPreparedUploadLaterPanicStillAbortsExactlyOnce(t *testing.T) {
	for _, test := range []struct {
		name  string
		phase func(*ownershipPreparedUpload, *ownershipObjectEvidence)
	}{
		{name: "download reference", phase: func(upload *ownershipPreparedUpload, _ *ownershipObjectEvidence) {
			upload.downloadPanic = "download panic"
		}},
		{name: "readiness confirmation", phase: func(_ *ownershipPreparedUpload, evidence *ownershipObjectEvidence) {
			evidence.confirmPanic = "confirm panic"
		}},
		{name: "commit", phase: func(upload *ownershipPreparedUpload, _ *ownershipObjectEvidence) { upload.commitPanic = "commit panic" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			upload := &ownershipPreparedUpload{reference: "https://objects.example/blob"}
			evidence := &ownershipObjectEvidence{}
			test.phase(upload, evidence)
			coordinator := newPreparedOwnershipCoordinator(t, ownershipPreparedStore{prepared: upload}, evidence)
			if recovered := capturePreparedOwnershipPanic(func() { _, _ = coordinator.Send(context.Background(), transferRequest()) }); recovered == nil {
				t.Fatal("dependency panic did not unwind")
			}
			if upload.aborts != 1 {
				t.Fatalf("upload aborts = %d, want exactly 1", upload.aborts)
			}
		})
	}
}

func TestPreparedUploadAbortPanicIsContainedAndNeverRetried(t *testing.T) {
	t.Run("ordinary result", func(t *testing.T) {
		upload := &ownershipPreparedUpload{reference: "https://objects.example/blob", abortPanic: "abort panic"}
		coordinator := newPreparedOwnershipCoordinator(t, ownershipPreparedStore{prepared: upload, result: ErrCarrierUpload}, &ownershipObjectEvidence{})
		if recovered := capturePreparedOwnershipPanic(func() {
			if _, err := coordinator.Send(context.Background(), transferRequest()); !errors.Is(err, ErrAllCarriersFailed) {
				t.Fatalf("Send() error = %v, want original fallback classification", err)
			}
		}); recovered != nil {
			t.Fatalf("Abort panic escaped: %v", recovered)
		}
		if upload.aborts != 1 {
			t.Fatalf("upload aborts = %d, want exactly 1", upload.aborts)
		}
	})

	t.Run("panic unwind", func(t *testing.T) {
		upload := &ownershipPreparedUpload{reference: "https://objects.example/blob", downloadPanic: "download panic", abortPanic: "abort panic"}
		coordinator := newPreparedOwnershipCoordinator(t, ownershipPreparedStore{prepared: upload}, &ownershipObjectEvidence{})
		if recovered := capturePreparedOwnershipPanic(func() { _, _ = coordinator.Send(context.Background(), transferRequest()) }); recovered != "download panic" {
			t.Fatalf("recovered panic = %v, want original dependency panic", recovered)
		}
		if upload.aborts != 1 {
			t.Fatalf("upload aborts = %d, want exactly 1", upload.aborts)
		}
	})
}

func TestPreparedUploadSuccessfulCommitAndEvidenceStillAbortExactlyOnce(t *testing.T) {
	upload := &ownershipPreparedUpload{reference: "https://objects.example/blob"}
	coordinator := newPreparedOwnershipCoordinator(t, ownershipPreparedStore{prepared: upload}, &ownershipObjectEvidence{})
	receipt, err := coordinator.Send(context.Background(), transferRequest())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Carrier != CarrierObjectUpload || receipt.TransferID == "" {
		t.Fatalf("receipt = %+v, want successful object evidence", receipt)
	}
	if upload.commits != 1 || upload.aborts != 1 {
		t.Fatalf("upload calls = commits %d, aborts %d; want 1, 1", upload.commits, upload.aborts)
	}
}

func newPreparedOwnershipCoordinator(t *testing.T, store PreparedObjectStore, evidence *ownershipObjectEvidence) *Coordinator {
	t.Helper()
	coordinator, err := NewCoordinator(testLimits(), nil, store, evidence, nil, nil, &fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.now = func() time.Time { return time.Unix(1000, 0).UTC() }
	return coordinator
}

func capturePreparedOwnershipPanic(run func()) (recovered any) {
	defer func() { recovered = recover() }()
	run()
	return nil
}
