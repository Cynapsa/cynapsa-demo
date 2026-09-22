package ejabberd_test

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/conversation"
)

func TestResourceMailboxContract(t *testing.T) {
	data, err := os.ReadFile("mod_cynapsa_mesh/src/mod_cynapsa_mesh.erl")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	testBoundary := strings.LastIndex(source, "-ifdef(TEST).")
	if testBoundary < 0 {
		t.Fatal("embedded test boundary is missing")
	}
	production := source[:testBoundary]

	for _, required := range []string{
		"cynapsa_mesh_mailbox", "cynapsa_mesh_mailbox_cursor",
		"{disc_copies, [node()]}", "{type, ordered_set}",
		"{index, [owner, sender, message_id]}", "mailbox_max_messages",
		"mailbox_max_bytes", "mailbox_retention_seconds",
		"MAX_MAILBOX_RETENTION_SECONDS",
		"mailbox_audit_log", "cynapsa_mailbox",
		"store_mailbox_packet_tx", "dispatch_mailbox_packet",
		"mnesia:index_read(cynapsa_mesh_mailbox,", "message_id",
		"drain_mailbox", "acknowledged_mailbox_keys",
		"c2s_authenticated_packet", "c2s_handle_send",
		"purge_removed_mailbox", "urn:cynapsa:aztm:1",
		"xmpp:err_resource_constraint",
	} {
		if !strings.Contains(production, required) {
			t.Errorf("resource mailbox implementation omits %q", required)
		}
	}

	maxMailboxSeconds := erlangIntegerMacro(t, production, "MAX_MAILBOX_RETENTION_SECONDS")
	defaultMailboxSeconds := erlangIntegerMacro(t, production, "DEFAULT_MAILBOX_RETENTION_SECONDS")
	if defaultMailboxSeconds > maxMailboxSeconds {
		t.Fatalf("default mailbox retention %d exceeds configured maximum %d", defaultMailboxSeconds, maxMailboxSeconds)
	}
	maxMailboxRetention := time.Duration(maxMailboxSeconds) * time.Second
	if maxMailboxRetention > conversation.MaximumMailboxReplayRetention {
		t.Fatalf("server mailbox retention %s exceeds Core replay contract %s", maxMailboxRetention, conversation.MaximumMailboxReplayRetention)
	}
	if conversation.DefaultDedupeRetention <= maxMailboxRetention {
		t.Fatalf("Core dedupe retention %s does not outlive server mailbox maximum %s", conversation.DefaultDedupeRetention, maxMailboxRetention)
	}

	rowStart := strings.Index(production, "-record(cynapsa_mesh_mailbox,")
	rowEnd := strings.Index(production[rowStart:], ").")
	if rowStart < 0 || rowEnd < 0 {
		t.Fatal("resource mailbox record is missing")
	}
	row := production[rowStart : rowStart+rowEnd]
	for _, forbidden := range []string{"session", "sid", "resume", "pid"} {
		if strings.Contains(strings.ToLower(row), forbidden) {
			t.Errorf("durable mailbox row is session-bound through %q", forbidden)
		}
	}

	filter := erlangReviewFunction(t, production,
		"filter_agent_packet(Packet) ->", "%% Capture the exact c2s process")
	if !strings.Contains(filter, "{mailbox_route, Packet") ||
		!strings.Contains(filter, "stored -> drop") {
		t.Fatal("eligible frames are not synchronously intercepted before ejabberd_sm routing")
	}

	classifier := erlangReviewFunction(t, production,
		"cynapsa_message_kind(#message{id = MessageID, type = chat,",
		"exact_aztm_frame(#xmlel")
	if !strings.Contains(classifier, "canonical_message_id(MessageID)") ||
		!strings.Contains(classifier, "true -> eligible") ||
		!strings.Contains(classifier, "false -> transient") {
		t.Fatal("outer message identity does not select envelope custody versus transient routing")
	}
	if strings.Contains(classifier, "exact_aztm_frame_kind") {
		t.Fatal("server classifier must not decode Core-owned CBOR frame kinds")
	}
	store := erlangReviewFunction(t, production,
		"store_mailbox_packet(#message{id = MessageID, from = From, to = To} = Packet,",
		"store_mailbox_packet(_, _Host, _MaxMessages, _MaxBytes, _Retention) ->")
	if !strings.Contains(store, "Owner = {To#jid.luser, Host, To#jid.lresource}") ||
		!strings.Contains(store, "Sender = {From#jid.luser, Host, From#jid.lresource}") {
		t.Fatal("mailbox custody is not keyed by exact sender and destination resources")
	}
	for _, forbidden := range []string{"sid", "resume", "session_meta"} {
		if strings.Contains(strings.ToLower(store), forbidden) {
			t.Fatalf("mailbox persistence depends on session state through %q", forbidden)
		}
	}
	if !strings.Contains(filter, "strip_transient_transport_id(Packet)") ||
		!strings.Contains(production, "Packet#message{id = <<>>}") {
		t.Fatal("transport-local transient stanza IDs are not stripped before recipient routing")
	}

	custody := erlangReviewFunction(t, production,
		"dispatch_custody_accepted(#message{id = MessageID} = Packet,",
		"custody_receipt_for_session(")
	if !strings.Contains(custody, "xmpp:get_meta(Packet, ?SESSION_META, undefined)") ||
		!strings.Contains(custody, "{true, {Pid, SID, Sender}}") ||
		!strings.Contains(custody, "exact_session_epoch_current(") {
		t.Fatal("custody acceptance is not fenced to the exact authenticated sender session that committed the packet")
	}

	ack := erlangReviewFunction(t, production,
		"c2s_authenticated_packet(#{lserver := Host,",
		"%% XEP-0198 counters are uint32 serial numbers")
	if !strings.Contains(ack, "acknowledged_mailbox_keys(Queue, Handled)") ||
		!strings.Contains(ack, "mailbox_audit_ack(") {
		t.Fatal("destination SM acknowledgement is not auditable at the deletion boundary")
	}

	remove := erlangReviewFunction(t, production,
		"remove_members_tx(Host, Users, Group, MeshID) ->", "read_snapshot(")
	if !strings.Contains(remove, "purge_removed_mailbox_resources_tx(") {
		t.Fatal("membership removal and exact-resource mailbox purge do not share one Mnesia transaction")
	}

	purge := erlangReviewFunction(t, production,
		"purge_removed_mailbox(Removed, Host, MeshID) ->", "purge_removed_uploads(")
	if !strings.Contains(purge, "mailbox_row_matches_removed_resource(") ||
		!strings.Contains(purge, "lists:member(Owner, RemovedResources)") ||
		!strings.Contains(purge, "lists:member(Sender, RemovedResources)") {
		t.Fatal("membership removal does not purge both recipient and sender rows")
	}

	mutation := erlangReviewFunction(t, production,
		"mutation_reply({ok, changed, MeshID, Members, Removed}",
		"cleanup_reply([]) -> ok;")
	for _, forbidden := range []string{
		"p1_queue:clear", "p1_queue:drop", "cynapsa_purge_removed",
		"purge_resumption_state", "invalidate_c2s_resumable_stream",
	} {
		if strings.Contains(mutation, forbidden) {
			t.Fatalf("membership mutation corrupts or destroys resumability through %q", forbidden)
		}
	}
	if !strings.Contains(mutation, "Updated0 = invalidate_mesh(MeshID, State)") ||
		!strings.Contains(mutation, "notify_membership_sessions(Retained") ||
		!strings.Contains(mutation, "revoke_exact_sessions(Removed") {
		t.Fatal("membership mutation does not fence centrally, notify retained sessions, and revoke only removed resources")
	}
}

func erlangIntegerMacro(t *testing.T, source, name string) int64 {
	t.Helper()
	pattern := regexp.MustCompile(`-define\(` + regexp.QuoteMeta(name) + `,\s*([0-9]+)\)\.`)
	match := pattern.FindStringSubmatch(source)
	if len(match) != 2 {
		t.Fatalf("integer macro %s is missing", name)
	}
	value, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil {
		t.Fatalf("parse integer macro %s: %v", name, err)
	}
	return value
}
