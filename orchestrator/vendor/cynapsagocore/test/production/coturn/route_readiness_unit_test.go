package coturn_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRouteUsesGateway(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, route, gateway string
		want                 bool
	}{
		{name: "exact", route: "default via 172.20.0.2 dev eth0\n172.20.0.0/16 dev eth0", gateway: "172.20.0.2", want: true},
		{name: "metric", route: "default via 172.20.0.2 dev eth0 proto static metric 100", gateway: "172.20.0.2", want: true},
		{name: "wrong gateway", route: "default via 172.20.0.1 dev eth0", gateway: "172.20.0.2"},
		{name: "multiple defaults", route: "default via 172.20.0.2 dev eth0\ndefault via 172.20.0.2 dev eth1", gateway: "172.20.0.2"},
		{name: "second wrong default", route: "default via 172.20.0.2 dev eth0\ndefault via 172.20.0.1 dev eth1", gateway: "172.20.0.2"},
		{name: "non-default", route: "172.20.0.2 via 172.20.0.1 dev eth0", gateway: "172.20.0.2"},
		{name: "substring is not an address", route: "default via 172.20.0.20 dev eth0", gateway: "172.20.0.2"},
		{name: "malformed", route: "default 172.20.0.2", gateway: "172.20.0.2"},
		{name: "empty", gateway: "172.20.0.2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := routeUsesGateway(test.route, test.gateway); got != test.want {
				t.Fatalf("routeUsesGateway(%q, %q)=%t, want %t", test.route, test.gateway, got, test.want)
			}
		})
	}
}

func TestDecodeReportsIgnoresRank2EvidenceLines(t *testing.T) {
	t.Parallel()
	output := strings.Join([]string{
		`CYNAPSA_RANK1_EVIDENCE {"event":"health_probe_send","outcome":"accepted"}`,
		`CYNAPSA_RANK2_EVIDENCE {"event":"mellium_serve_return","error":"EOF"}`,
		`CYNAPSA_CORE_EVIDENCE {"event":"rank2_inbound_disposition","accepted":false}`,
		`{"event":"terminal-ready","role":"sender"}`,
	}, "\n")
	reports := decodeReports(t, output)
	if len(reports) != 1 || reports[0].Event != "terminal-ready" || reports[0].Role != "sender" {
		t.Fatalf("decodeReports()=%#v, want one terminal-ready sender report", reports)
	}
}

func TestTimedOutRouteVerified(t *testing.T) {
	t.Parallel()
	const (
		gateway   = "172.20.0.2"
		agentID   = "agent-id"
		gatewayID = "gateway-id"
	)
	validRoute := commandResult{output: "default via 172.20.0.2 dev eth0\n172.20.0.0/16 dev eth0\ncontext deadline exceeded", exitCode: -1, timedOut: true}
	validAgent := commandResult{output: agentID + "|true|0\n"}
	validGateway := commandResult{output: gatewayID + "|true|0\n"}
	tests := []struct {
		name         string
		route        commandResult
		agent        commandResult
		gatewayState commandResult
		want         bool
	}{
		{name: "deadline output plus fresh liveness", route: validRoute, agent: validAgent, gatewayState: validGateway, want: true},
		{name: "non-timeout failure", route: commandResult{output: validRoute.output, exitCode: -1}, agent: validAgent, gatewayState: validGateway},
		{name: "signaled without deadline", route: commandResult{output: validRoute.output, exitCode: 137}, agent: validAgent, gatewayState: validGateway},
		{name: "exit zero timeout still requires and passes fresh liveness", route: commandResult{output: validRoute.output, timedOut: true}, agent: validAgent, gatewayState: validGateway, want: true},
		{name: "exit zero timeout without agent liveness", route: commandResult{output: validRoute.output, timedOut: true}, agent: commandResult{output: validAgent.output, exitCode: -1}, gatewayState: validGateway},
		{name: "multiple defaults", route: commandResult{output: "default via 172.20.0.2 dev eth0\ndefault via 172.20.0.2 dev eth1", exitCode: -1, timedOut: true}, agent: validAgent, gatewayState: validGateway},
		{name: "wrong gateway", route: commandResult{output: "default via 172.20.0.1 dev eth0", exitCode: -1, timedOut: true}, agent: validAgent, gatewayState: validGateway},
		{name: "agent inspection failed", route: validRoute, agent: commandResult{output: validAgent.output, exitCode: -1}, gatewayState: validGateway},
		{name: "agent inspection timed out", route: validRoute, agent: commandResult{output: validAgent.output, exitCode: -1, timedOut: true}, gatewayState: validGateway},
		{name: "wrong agent identity", route: validRoute, agent: commandResult{output: "other|true|0"}, gatewayState: validGateway},
		{name: "agent stopped", route: validRoute, agent: commandResult{output: agentID + "|false|0"}, gatewayState: validGateway},
		{name: "agent stale exit", route: validRoute, agent: commandResult{output: agentID + "|true|1"}, gatewayState: validGateway},
		{name: "gateway inspection failed", route: validRoute, agent: validAgent, gatewayState: commandResult{output: validGateway.output, exitCode: -1}},
		{name: "wrong gateway identity", route: validRoute, agent: validAgent, gatewayState: commandResult{output: "other|true|0"}},
		{name: "gateway stopped", route: validRoute, agent: validAgent, gatewayState: commandResult{output: gatewayID + "|false|0"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := timedOutRouteVerified(test.route, gateway, test.agent, agentID, test.gatewayState, gatewayID); got != test.want {
				t.Fatalf("timedOutRouteVerified()=%t, want %t", got, test.want)
			}
		})
	}
}

func TestOrdinaryRouteVerifiedRejectsEveryTimedOutResult(t *testing.T) {
	t.Parallel()
	const gateway = "172.20.0.2"
	exact := "default via 172.20.0.2 dev eth0"
	for _, test := range []struct {
		name  string
		route commandResult
		want  bool
	}{
		{name: "exit zero exact route", route: commandResult{output: exact}, want: true},
		{name: "exit zero timed out", route: commandResult{output: exact, timedOut: true}},
		{name: "failure timed out", route: commandResult{output: exact, exitCode: -1, timedOut: true}},
		{name: "exit zero wrong route", route: commandResult{output: "default via 172.20.0.1 dev eth0"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := ordinaryRouteVerified(test.route, gateway); got != test.want {
				t.Fatalf("ordinaryRouteVerified()=%t, want %t", got, test.want)
			}
		})
	}
}

func TestParseContainerInspection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, input, id, exit string
		running, ok           bool
	}{
		{name: "running", input: "abc123|true|0\n", id: "abc123", running: true, exit: "0", ok: true},
		{name: "stopped", input: "abc123|false|70", id: "abc123", exit: "70", ok: true},
		{name: "missing id", input: "|true|0"},
		{name: "invalid boolean", input: "abc123|yes|0"},
		{name: "missing exit", input: "abc123|true|"},
		{name: "extra field", input: "abc123|true|0|extra"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			id, running, exit, ok := parseContainerInspection(test.input)
			if id != test.id || running != test.running || exit != test.exit || ok != test.ok {
				t.Fatalf("parseContainerInspection(%q)=(%q,%t,%q,%t), want (%q,%t,%q,%t)",
					test.input, id, running, exit, ok, test.id, test.running, test.exit, test.ok)
			}
		})
	}
}

func TestExactXMPPFaultCounter(t *testing.T) {
	t.Parallel()
	const address = "172.20.0.9"
	for _, test := range []struct {
		name, rules string
		want        bool
	}{
		{name: "positive exact", rules: "[2:120] -A CYNAPSA_XMPP -d 172.20.0.9/32 -p tcp -m tcp --dport 5222 -j DROP\n", want: true},
		{name: "zero packets", rules: "[0:0] -A CYNAPSA_XMPP -d 172.20.0.9/32 -p tcp -m tcp --dport 5222 -j DROP\n"},
		{name: "wrong destination", rules: "[2:120] -A CYNAPSA_XMPP -d 172.20.0.8/32 -p tcp -m tcp --dport 5222 -j DROP\n"},
		{name: "wrong port", rules: "[2:120] -A CYNAPSA_XMPP -d 172.20.0.9/32 -p tcp -m tcp --dport 5223 -j DROP\n"},
		{name: "port substring", rules: "[2:120] -A CYNAPSA_XMPP -d 172.20.0.9/32 -p tcp -m tcp --dport 52220 -j DROP\n"},
		{name: "destination substring", rules: "[2:120] -A CYNAPSA_XMPP -d 172.20.0.90/32 -p tcp -m tcp --dport 5222 -j DROP\n"},
		{name: "cross-line composition", rules: "[2:120] -A CYNAPSA_XMPP -d 172.20.0.9/32 -p tcp\n[2:120] -A OTHER -m tcp --dport 5222 -j DROP\n"},
		{name: "broad tcp drop", rules: "[2:120] -A CYNAPSA_XMPP -p tcp -j DROP\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := hasExactXMPPFaultCounter(test.rules, address); got != test.want {
				t.Fatalf("hasExactXMPPFaultCounter()=%t, want %t", got, test.want)
			}
		})
	}
}

func TestIPTablesRuleMatchesRequiredTokensIndependentOfOrdering(t *testing.T) {
	t.Parallel()
	const exact = "[0:0] -A POSTROUTING -s 172.27.0.0/16 -d 172.26.0.3/32 -o eth1 -p udp -m udp --dport 3478 -j SNAT --to-source 172.26.0.5:40000\n"
	if !hasIPTablesRule(exact, "--to-source 172.26.0.5:40000", "-p udp", "-A POSTROUTING", "--dport 3478", "-d 172.26.0.3/32") {
		t.Fatal("exact rule tokens were not matched across normalized option ordering")
	}
	for name, rules := range map[string]string{
		"missing token":          exact,
		"port substring":         "[0:0] -A POSTROUTING -d 172.26.0.3/32 -p udp --dport 34780 -j SNAT --to-source 172.26.0.5:40000\n",
		"destination substring":  "[0:0] -A POSTROUTING -d 172.26.0.30/32 -p udp --dport 3478 -j SNAT --to-source 172.26.0.5:40000\n",
		"target substring":       "[0:0] -A POSTROUTING -d 172.26.0.3/32 -p udp --dport 3478 -j SNAT --to-source 172.26.0.5:400000\n",
		"cross-line composition": "[0:0] -A POSTROUTING -d 172.26.0.3/32 -p udp --dport 3478\n[0:0] -A OTHER -j SNAT --to-source 172.26.0.5:40000\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			port := "--dport 3478"
			if name == "missing token" {
				port = "--dport 3479"
			}
			if hasIPTablesRule(rules, "-A POSTROUTING", "-d 172.26.0.3/32", "-p udp", port, "-j SNAT", "--to-source 172.26.0.5:40000") {
				t.Fatal("rule matcher accepted a partial or cross-line requirement")
			}
		})
	}
}

func TestPositiveIPTablesCounterRequiresExactSameLineRule(t *testing.T) {
	t.Parallel()
	const requirements = "-A POSTROUTING"
	for _, test := range []struct {
		name, rules string
		want        bool
	}{
		{name: "positive exact", rules: "[2:120] -A POSTROUTING -p udp -j SNAT --to-source 172.26.0.5:40001-40999 --random-fully\n", want: true},
		{name: "requirement groups reordered", rules: "[2:120] -A POSTROUTING --random-fully -p udp -j SNAT --to-source 172.26.0.5:40001-40999\n", want: true},
		{name: "zero packets", rules: "[0:120] -A POSTROUTING -p udp -j SNAT --to-source 172.26.0.5:40001-40999 --random-fully\n"},
		{name: "zero bytes", rules: "[2:0] -A POSTROUTING -p udp -j SNAT --to-source 172.26.0.5:40001-40999 --random-fully\n"},
		{name: "malformed counter", rules: "[2:120]extra -A POSTROUTING -p udp -j SNAT --to-source 172.26.0.5:40001-40999 --random-fully\n"},
		{name: "target substring", rules: "[2:120] -A POSTROUTING -p udp -j SNAT --to-source 172.26.0.5:40001-409990 --random-fully\n"},
		{name: "cross-line composition", rules: "[2:120] -A POSTROUTING -p udp\n[3:180] -A OTHER -j SNAT --to-source 172.26.0.5:40001-40999 --random-fully\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := hasPositiveIPTablesCounter(test.rules, requirements, "-p udp", "-j SNAT", "--to-source 172.26.0.5:40001-40999", "--random-fully")
			if got != test.want {
				t.Fatalf("hasPositiveIPTablesCounter()=%t, want %t", got, test.want)
			}
		})
	}
}

func TestNATPacketEvidenceSummaryReportsPreNATProtocolCounters(t *testing.T) {
	t.Parallel()
	rules := strings.Join([]string{
		`[3:240] -A CYNAPSA_EVIDENCE -s 172.27.0.0/16 -o eth1 -p udp -m comment --comment cynapsa-egress-stun-turn`,
		`[7:560] -A CYNAPSA_EVIDENCE -s 172.27.0.0/16 -o eth1 -p udp -m comment --comment cynapsa-egress-udp`,
		`[11:880] -A CYNAPSA_EVIDENCE -s 172.27.0.0/16 -o eth1 -p tcp -m comment --comment cynapsa-egress-xmpp`,
		`[13:1040] -A CYNAPSA_EVIDENCE -s 172.27.0.0/16 -o eth1 -p tcp -m comment --comment cynapsa-egress-tcp`,
		`[20:1600] -A CYNAPSA_EVIDENCE -s 172.27.0.0/16 -o eth1 -m comment --comment cynapsa-egress-any`,
		`[2:160] -A CYNAPSA_EVIDENCE -d 172.27.0.0/16 -i eth1 -p udp -m comment --comment cynapsa-ingress-stun-turn`,
		`[5:400] -A CYNAPSA_EVIDENCE -d 172.27.0.0/16 -i eth1 -p udp -m comment --comment cynapsa-ingress-udp`,
		`[9:720] -A CYNAPSA_EVIDENCE -d 172.27.0.0/16 -i eth1 -p tcp -m comment --comment cynapsa-ingress-xmpp`,
		`[12:960] -A CYNAPSA_EVIDENCE -d 172.27.0.0/16 -i eth1 -p tcp -m comment --comment cynapsa-ingress-tcp`,
		`[17:1360] -A CYNAPSA_EVIDENCE -d 172.27.0.0/16 -i eth1 -m comment --comment cynapsa-ingress-any`,
	}, "\n")
	want := "cynapsa-egress-stun-turn=3:240 cynapsa-egress-udp=7:560 cynapsa-egress-xmpp=11:880 cynapsa-egress-tcp=13:1040 cynapsa-egress-any=20:1600 " +
		"cynapsa-ingress-stun-turn=2:160 cynapsa-ingress-udp=5:400 cynapsa-ingress-xmpp=9:720 cynapsa-ingress-tcp=12:960 cynapsa-ingress-any=17:1360"
	if got := natPacketEvidenceSummary(rules); got != want {
		t.Fatalf("natPacketEvidenceSummary()=%q, want %q", got, want)
	}
}

func TestIPTablesCommentCounterRejectsMalformedOrCrossLineEvidence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, rules string
		wantFound   bool
	}{
		{name: "missing", rules: `[1:80] -A CYNAPSA_EVIDENCE -p udp`},
		{name: "zero is valid", rules: `[0:0] -A CYNAPSA_EVIDENCE -p udp -m comment --comment cynapsa-egress-udp`, wantFound: true},
		{name: "malformed counter", rules: `[1:x] -A CYNAPSA_EVIDENCE -p udp -m comment --comment cynapsa-egress-udp`},
		{name: "cross line", rules: "[1:80] -A CYNAPSA_EVIDENCE -p udp\n[2:160] -A OTHER -m comment --comment cynapsa-egress-udp"},
		{name: "label substring", rules: `[1:80] -A CYNAPSA_EVIDENCE -p udp -m comment --comment cynapsa-egress-udp-extra`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			packets, bytes, found := iptablesCommentCounter(test.rules, "cynapsa-egress-udp")
			if found != test.wantFound || packets != 0 || bytes != 0 {
				t.Fatalf("counter=(%d,%d,%t), want (0,0,%t)", packets, bytes, found, test.wantFound)
			}
		})
	}
}

func TestNATTranslationEvidenceSummaryReportsExactRules(t *testing.T) {
	t.Parallel()
	profile := "profile=symmetric\nwan_address=172.26.0.5\nstun_address=172.26.0.10\n"
	rules := strings.Join([]string{
		`[1097:65820] -A POSTROUTING -s 172.27.0.0/16 -o eth1 -j SNAT --to-source 172.26.0.5`,
		`[3:240] -A POSTROUTING -s 172.27.0.0/16 -d 172.26.0.10/32 -o eth1 -p udp -m udp --dport 3478 -j SNAT --to-source 172.26.0.5:40000`,
		`[7:560] -A POSTROUTING -s 172.27.0.0/16 -o eth1 -p udp -j SNAT --to-source 172.26.0.5:40001-40999 --random-fully`,
	}, "\n")
	want := "generic-snat=1097:65820 stun-snat=3:240 non-stun-udp-snat=7:560"
	if got := natTranslationEvidenceSummary(rules, profile); got != want {
		t.Fatalf("natTranslationEvidenceSummary()=%q, want %q", got, want)
	}
}

func TestNATTranslationEvidenceSummaryDoesNotComposeRules(t *testing.T) {
	t.Parallel()
	profile := "profile=symmetric\nwan_address=172.26.0.5\nstun_address=172.26.0.10\n"
	rules := strings.Join([]string{
		`[3:240] -A POSTROUTING -p udp --dport 3478`,
		`[7:560] -A OTHER -j SNAT --to-source 172.26.0.5:40000`,
		`[9:720] -A POSTROUTING -p udp --to-source 172.26.0.5:40001-40999`,
		`[11:880] -A OTHER --random-fully`,
	}, "\n")
	want := "generic-snat=missing stun-snat=missing non-stun-udp-snat=missing"
	if got := natTranslationEvidenceSummary(rules, profile); got != want {
		t.Fatalf("natTranslationEvidenceSummary()=%q, want %q", got, want)
	}
}

func TestFailureEvidenceFormattingIsBoundedAndRedacted(t *testing.T) {
	t.Parallel()
	logs := "first\nsecret-value\nallocate processed, success\nlast\n"
	redacted := redactEvidence(logs, []string{"", "secret-value"})
	if strings.Contains(redacted, "secret-value") || !strings.Contains(redacted, "[REDACTED]") {
		t.Fatalf("redacted evidence=%q", redacted)
	}
	if got := boundedLogTail(redacted, 2, 1<<10); got != "allocate processed, success\nlast" {
		t.Fatalf("bounded line tail=%q", got)
	}
	if got := boundedLogTail("0123456789", 10, 4); got != "[truncated to trailing bytes]\n6789" {
		t.Fatalf("bounded byte tail=%q", got)
	}
}

func TestServiceProtocolSummaryCountsExactEvidence(t *testing.T) {
	t.Parallel()
	logs := strings.Join([]string{
		"ALLOCATE processed, success",
		"allocate processed, error 442",
		"create_permission processed, success",
		"channel_bind processed, success",
		"refresh processed, error",
		"Jingle negotiation",
		"Accepted c2s SCRAM-SHA-256-PLUS authentication",
		"Opened c2s session",
		"Closing c2s connection; waiting 300 seconds for stream resumption",
		"Replaced by new connection (conflict)",
		"cynapsa_mailbox admission",
		"cynapsa_mailbox ack",
		"warning: sample",
	}, "\n")
	summary := serviceProtocolSummary(logs)
	for _, expected := range []string{
		"turn_allocate_success=1",
		"turn_allocate_error=1",
		"turn_permission_success=1",
		"turn_channel_success=1",
		"turn_refresh_error=1",
		"xmpp_auth_accepted=1",
		"xmpp_session_opened=1",
		"xmpp_connection_failed=1",
		"xmpp_replaced_conflict=1",
		"xmpp_resume_wait=1",
		"mailbox_admission=1",
		"mailbox_ack=1",
		"jingle=1",
		"warning=1",
	} {
		if !strings.Contains(summary, expected) {
			t.Fatalf("service summary %q lacks %q", summary, expected)
		}
	}
}

func TestPrivateAgentEnvironmentFileHasExactPrivateMode(t *testing.T) {
	path := writePrivateAgentEnvironmentFile(t, []string{
		"CYNAPSA_AGENT_ID=agent-a@mesh.test",
		"CYNAPSA_AGENT_PASSWORD=synthetic-private-value",
	})
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("private agent environment mode=%v, want 0600", info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "CYNAPSA_AGENT_ID=agent-a@mesh.test\nCYNAPSA_AGENT_PASSWORD=synthetic-private-value\n" {
		t.Fatalf("private agent environment contents=%q", data)
	}
}

func TestExternalServiceReadinessUsesObservedCredentialLifetime(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, readiness string
		want            bool
	}{
		{name: "one second", readiness: "xmpp=authenticated extdisco=valid turn=authenticated-allocation lifetime=1s\n", want: true},
		{name: "observed upper bound", readiness: "stun=ready\nxmpp=authenticated extdisco=valid turn=authenticated-allocation lifetime=120s\n", want: true},
		{name: "zero", readiness: "xmpp=authenticated extdisco=valid turn=authenticated-allocation lifetime=0s\n"},
		{name: "over upper bound", readiness: "xmpp=authenticated extdisco=valid turn=authenticated-allocation lifetime=121s\n"},
		{name: "fabricated label", readiness: "xmpp=authenticated extdisco=valid turn=authenticated-allocation lifetime=validated\n"},
		{name: "trailing data", readiness: "xmpp=authenticated extdisco=valid turn=authenticated-allocation lifetime=119s extra\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := validExternalServiceReadiness(test.readiness); got != test.want {
				t.Fatalf("validExternalServiceReadiness(%q)=%t, want %t", test.readiness, got, test.want)
			}
		})
	}
}

func TestReportedCoturnStateIsExactOwnedDirectory(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "cynapsa-p2p.valid")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	if !validCoturnState(state) {
		t.Fatal("exact owned state rejected")
	}
	for _, invalid := range []string{"", "relative/cynapsa-p2p.valid", root, filepath.Join(root, "foreign.valid"), filepath.Join(root, "cynapsa-p2p.missing")} {
		if validCoturnState(invalid) {
			t.Fatalf("invalid state accepted: %q", invalid)
		}
	}
	link := filepath.Join(root, "cynapsa-p2p.link")
	if err := os.Symlink(state, link); err != nil {
		t.Fatal(err)
	}
	if validCoturnState(link) {
		t.Fatal("symlink state accepted")
	}
}
