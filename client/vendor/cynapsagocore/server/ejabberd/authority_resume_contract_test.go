package ejabberd_test

import (
	"os"
	"strings"
	"testing"
)

func TestAuthorityContinuityAcrossXEP0198Resume(t *testing.T) {
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

	for _, hook := range []string{
		"ejabberd_hooks:add(c2s_copy_session",
		"ejabberd_hooks:add(c2s_session_resumed",
		"ejabberd_hooks:delete(c2s_copy_session",
		"ejabberd_hooks:delete(c2s_session_resumed",
	} {
		if !strings.Contains(production, hook) {
			t.Fatalf("ejabberd 26.04 authority-resume hook is missing: %q", hook)
		}
	}

	copyHook := erlangReviewFunction(t, production,
		"c2s_copy_session(State, OldState) ->",
		"%% mod_stream_mgmt calls this only after")
	for _, required := range []string{
		"resume_handoff_identity(CleanState, OldState, self())",
		"{prepare_resume, OldPid, OldSID, self(),",
		"CleanState#{?RESUME_META =>",
	} {
		if !strings.Contains(copyHook, required) {
			t.Fatalf("copy hook does not reserve server-owned old readiness through %q", required)
		}
	}

	resumedHook := erlangReviewFunction(t, production,
		"c2s_session_resumed(#{lserver := Host, sid := NewSID,",
		"resume_handoff_identity(")
	for _, required := range []string{
		"{complete_resume, self(), NewSID", "ready -> ready", "_ -> not_ready",
		"catch _:_ -> not_ready", "send_resume_authority_result(CleanState",
		"ejabberd_c2s:send(State, IQ)", "resume_authority_result(Host, User, MeshID, Status)",
		"name = <<\"resume-authority\">>", "{<<\"status\">>, EncodedStatus}",
		"from = jid:make(<<>>, Host, <<>>)", "to = jid:make(User, Host, MeshID)",
	} {
		if !strings.Contains(resumedHook, required) {
			t.Fatalf("resume authority result contract omits %q", required)
		}
	}
	completeAt := strings.Index(resumedHook, "{complete_resume, self(), NewSID")
	sendAt := strings.Index(resumedHook, "send_resume_authority_result(CleanState")
	if completeAt < 0 || sendAt < 0 || completeAt >= sendAt {
		t.Fatal("authority result is not sent strictly after resume completion")
	}
	if strings.Contains(production, "approved_service_name(<<\"resume-authority\">>") {
		t.Fatal("server-only resume authority result was exposed as a client service command")
	}

	prepare := erlangReviewFunction(t, production,
		"handle_call({prepare_resume, OldPid, OldSID, NewPid, User, MeshID}",
		"handle_call({complete_resume, NewPid")
	if !strings.Contains(prepare, "member_authorized(Host, MeshID, {User, Host})") ||
		!strings.Contains(prepare, "maps:remove(OldPid, Ready)") {
		t.Fatal("resume preparation does not validate current membership and reserve old readiness fail-closed")
	}

	complete := erlangReviewFunction(t, production,
		"handle_call({complete_resume, NewPid, NewSID, User, MeshID, Token}",
		"handle_call({mailbox_route")
	if !strings.Contains(complete, "member_authorized(Host, MeshID, {User, Host})") ||
		!strings.Contains(complete, "UpdatedReady = case Status of") ||
		!strings.Contains(complete, "ready -> Ready#{NewPid =>") ||
		!strings.Contains(complete, "not_ready -> Ready") ||
		!strings.Contains(complete, "{reply, Status,") {
		t.Fatal("resume completion does not distinguish retained readiness from a control-only replacement")
	}

	invalidate := erlangReviewFunction(t, production,
		"invalidate_mesh(MeshID,",
		"replace_all_pending(Pending, State) ->")
	if !strings.Contains(invalidate, "KeepTransfers = maps:filter(") ||
		!strings.Contains(invalidate, "resume_transfers = KeepTransfers") {
		t.Fatal("membership mutation does not invalidate in-flight resume handoffs")
	}

	bind := erlangReviewFunction(t, production,
		"c2s_handle_bind({Resource, {ok, #{user := User,",
		"c2s_handle_bind(Acc) ->")
	if strings.Contains(bind, "ready") || strings.Contains(bind, "resume") {
		t.Fatal("fresh resource bind must authorize identity without inheriting readiness")
	}

	mutation := erlangReviewFunction(t, production,
		"mutation_reply({ok, changed, MeshID, Members, Removed}",
		"cleanup_reply([]) -> ok;")
	fenceAt := strings.Index(mutation, "Updated0 = invalidate_mesh(MeshID, State)")
	notifyAt := strings.Index(mutation, "notify_membership_sessions(Retained")
	if fenceAt < 0 || notifyAt < 0 || fenceAt >= notifyAt {
		t.Fatal("membership readiness must be invalidated before notification")
	}
	if !strings.Contains(prepare, "pending_controls = UpdatedControls") ||
		!strings.Contains(prepare, "Pending, OldPid, OldSID, NewPid, OldSID") ||
		!strings.Contains(prepare, "not_ready when UpdatedControls =/= Pending") ||
		!strings.Contains(complete, "Pending, NewPid, OldSID, NewPid, NewSID") {
		t.Fatal("pending membership-control ownership is not preserved across resume handoff")
	}
	for _, forbidden := range []string{
		"p1_queue:clear", "p1_queue:drop", "p1_queue:out", "p1_queue:in",
		"ejabberd_c2s:stop_async(self())", "cynapsa_purge_removed",
	} {
		if strings.Contains(mutation, forbidden) {
			t.Fatalf("retained stream continuity is violated through %q", forbidden)
		}
	}
}

func TestAuthorityResumeDocumentationContract(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(data)
	for _, required := range []string{
		"Every fresh bind starts unsynchronized",
		"Successful XEP-0198 resumption",
		"one indivisible FIFO",
		"server-generated one-use handoff",
		"resume-authority",
		"post-resume `<r/>`",
		"Removed resources are still kicked",
	} {
		if !strings.Contains(doc, required) {
			t.Errorf("server authority-resume documentation omits %q", required)
		}
	}
}
