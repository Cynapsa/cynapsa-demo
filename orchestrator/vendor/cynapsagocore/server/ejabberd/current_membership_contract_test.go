package ejabberd_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCurrentMembershipServerContract(t *testing.T) {
	sourcePath := filepath.Join("mod_cynapsa_mesh", "src", "mod_cynapsa_mesh.erl")
	sourceBytes, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	testBoundary := strings.LastIndex(source, "-ifdef(TEST).")
	if testBoundary < 0 {
		t.Fatal("embedded legacy-test boundary is missing")
	}
	production := source[:testBoundary]
	for _, forbidden := range []string{
		"cynapsa_mesh_generation", `<<"generation">>`, `<<"watermark">>`,
		"process_group_page", "urn:cynapsa:mesh-group:1", "mnesia_table_event",
		"cynapsa_purge_removed", "purge_resumption_state",
		"invalidate_c2s_resumable_stream",
	} {
		if strings.Contains(production, forbidden) {
			t.Errorf("production mesh module retains %q", forbidden)
		}
	}
	for _, required := range []string{
		"cynapsa_mesh_remove_many", `<<"membership-changed">>`,
		"authority_snapshot_page", "invalidate_mesh", "revoke_exact_sessions",
		"purge_removed_state", "offline_msg", "archive_msg",
		"offline_message_guard",
		"pending_snapshot_capacity", "MAX_PENDING_SNAPSHOT_BYTES",
		"pending_sync_members", "SNAPSHOT_PENDING_MILLISECONDS",
		"snapshot_sweep", "close_expired_snapshot",
		"snapshot-required", "ejabberd_sm:get_session_pid",
		"exact_session_epoch_current", "safe_policy_error_for_session",
		"c2s_copy_session", "c2s_session_resumed", "resume_transfers",
		"prepare_resume_transfer", "complete_resume_transfer",
		"mgmt_queue", "packet_removed_mesh",
		"cynapsa_mesh_upload_object", "purge_removed_uploads",
		"with_upload_path_lock", "upload_http_authorized", "upload_object_key",
		"MAX_RETAINED_UPLOAD_OBJECTS", "MAX_RETAINED_UPLOAD_BYTES",
		"MAX_OWNER_UPLOAD_OBJECTS", "MAX_OWNER_UPLOAD_BYTES",
		"UPLOAD_SLOT_MILLISECONDS", "upload_sweep", "prune_expired_uploads",
		"retained_upload_usage_tx", "complete_upload_object",
		"cleanup_pending", "finish_failed_upload_cleanup",
		"cancel_unowned_upload_object", "cancel_unowned_upload_path_locked",
		"cleanup_expired_upload_object", "cleanup_valid_expired_upload_object",
		"persisted_upload_cleanup_safe",
		"cleanup_removed_upload_record", "read_current_removed_upload",
		"delete_current_removed_upload",
		"archive_prefs", "enforce_mam_never", "request_activates_archiving",
	} {
		if !strings.Contains(production, required) {
			t.Errorf("production mesh module omits %q", required)
		}
	}
	if !strings.Contains(source, "pending_snapshot_count_available(map_size(Pending))") {
		t.Error("pending snapshot count must be rejected before snapshot construction")
	}
	if !strings.Contains(production, "-define(MAX_UPLOAD_BYTES, 134217760).") {
		t.Error("server upload ceiling must equal the V1 transferred-byte ceiling")
	}
	if strings.Contains(production, "delete_upload_records(Objects)") {
		t.Error("removed upload cleanup must delete the current locked row, not a stale snapshot")
	}
	if strings.Contains(production, `#message{type = set`) {
		t.Error("membership control must not use an offline-storable message stanza")
	}
	if strings.Contains(production, `<<"removed">>`) {
		t.Error("membership wakeup must be constant-size and snapshot-only")
	}
	if !strings.Contains(production, "-define(DEFAULT_CONTROL_ACK_TIMEOUT_SECONDS, 5).") {
		t.Error("membership-control processed-ack timeout must default to five seconds")
	}

	controlAck := erlangReviewFunction(t, production,
		"handle_call({control_ack, Pid, SID, BoundJID, ID}",
		"handle_call({mailbox_route")
	for _, required := range []string{
		"consume_control_ack(Pid, SID, BoundJID, ID, Pending)",
		"close_exact_session(Pid, authority_control_invalid)",
		"remove_control_owner(Pid, Pending)",
	} {
		if !strings.Contains(controlAck, required) {
			t.Errorf("invalid control acknowledgement path omits %q", required)
		}
	}
	queueControl := erlangReviewFunction(t, production,
		"queue_membership_controls(Sessions, Host, Pending, Now, Timeout, Maximum,",
		"control_owner_exists(Pid, Pending) ->")
	for _, required := range []string{
		"authority_control_capacity", "authority_control_enqueue_failed",
		"authority_control_session_replaced",
	} {
		if !strings.Contains(queueControl, required) {
			t.Errorf("control enqueue/backlog path omits %q", required)
		}
	}
	timeoutControl := erlangReviewFunction(t, production,
		"expire_control_acks(Now, #state{} = State) ->",
		"close_exact_session(Pid, Reason)")
	if !strings.Contains(timeoutControl, "authority_control_ack_timeout") ||
		!strings.Contains(timeoutControl, "close_control_owner(Pid") {
		t.Error("expired control acknowledgement does not close its exact owner")
	}

	for _, config := range []string{"ejabberd.yml.example", "ejabberd.integration.yml"} {
		data, err := os.ReadFile(config)
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		for _, required := range []string{
			"mod_mam:", "mod_offline:", "db_type: mnesia",
			"default: never", "request_activates_archiving: false",
			"mod_cynapsa_mesh:", "control_ack_timeout_seconds: 5",
			"control_pending_max: 65536",
		} {
			if !strings.Contains(text, required) {
				t.Errorf("%s omits %q", config, required)
			}
		}
	}

	example, err := os.ReadFile("ejabberd.yml.example")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(example), "/upload: mod_cynapsa_mesh") {
		t.Error("dedicated upload listener bypasses exact-path membership gate")
	}
	if !strings.Contains(string(example), "max_size: 134217760") {
		t.Error("example upload service ceiling diverges from the V1 contract")
	}
}
