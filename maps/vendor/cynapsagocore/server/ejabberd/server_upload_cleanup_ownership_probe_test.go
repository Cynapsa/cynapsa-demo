package ejabberd_test

import (
	"os"
	"strings"
	"testing"
)

func erlangReviewFunction(t *testing.T, source, start, next string) string {
	t.Helper()
	begin := strings.Index(source, start)
	if begin < 0 {
		t.Fatalf("missing %q", start)
	}
	end := strings.Index(source[begin+len(start):], next)
	if end < 0 {
		t.Fatalf("missing boundary %q after %q", next, start)
	}
	return source[begin : begin+len(start)+end]
}

func TestReviewRejectedUploadResponsePreservesRetainedPathOwner(t *testing.T) {
	data, err := os.ReadFile("mod_cynapsa_mesh/src/mod_cynapsa_mesh.erl")
	if err != nil {
		t.Fatal(err)
	}
	clause := erlangReviewFunction(t, string(data),
		"cancel_upload_response(#iq{type = result", "cancel_upload_response(_Packet")
	if strings.Contains(clause, "cleanup_upload_object(Host, Path, Slot)") &&
		!strings.Contains(clause, "mnesia:dirty_read") &&
		!strings.Contains(clause, "cancel_unowned_upload") {
		t.Fatal("a rejected duplicate/path-conflict response directly cancels the durable owner's retained path")
	}
}

func TestReviewFailedPUTKeepsOwnershipWhenPhysicalCleanupFails(t *testing.T) {
	data, err := os.ReadFile("mod_cynapsa_mesh/src/mod_cynapsa_mesh.erl")
	if err != nil {
		t.Fatal(err)
	}
	clause := erlangReviewFunction(t, string(data),
		"upload_http_delegate(Path, Request, Host, ObjectPath, Slot, 'PUT')", "upload_http_delegate(Path, Request, _Host")
	unsafe := "_ = cleanup_upload_object_locked(Host, ObjectPath, Slot),\n            _ = delete_upload_path_record(Host, ObjectPath)"
	if strings.Contains(clause, unsafe) {
		t.Fatal("failed PUT drops durable ownership even when physical cleanup fails, orphaning retained bytes outside retry/quota accounting")
	}
}
