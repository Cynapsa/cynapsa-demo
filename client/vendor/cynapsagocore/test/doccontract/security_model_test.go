package doccontract_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

type carrierContract struct {
	row      string
	required []string
}

var carrierContracts = []carrierContract{
	{row: "Rank 1 WebRTC RTCDataChannel", required: []string{"authenticated peer-to-peer dtls", "cannot recover rtcdatachannel plaintext", "dtls ciphertext"}},
	{row: "Rank 2 ordinary XMPP messages and controls", required: []string{"xmpp over mandatory tls", "xmpp/ejabberd service can", "not end-to-end encryption"}},
	{row: "Rank 2 XEP-0363 object transfer", required: []string{"https", "http upload/download service can", "payload plaintext"}},
	{row: "Rank 2 XMPP fallback chunks", required: []string{"xmpp over mandatory tls", "xmpp/ejabberd service can", "payload plaintext"}},
}

var (
	xepCarrierPattern  = regexp.MustCompile(`\b(?:xep[- ]?0363|upload (?:server|service)|uploaded object|object offload|http upload)\b`)
	xmppCarrierPattern = regexp.MustCompile(`\b(?:xmpp(?:[- ]tls)?(?: fallback)?(?:[- ]| )?(?:chunks?|messages?)|fallback chunks?|ejabberd(?: service)?)\b`)
	secrecyPattern     = regexp.MustCompile(`\b(?:encrypt(?:ed|ion|s)?|ciphertext|server[- ]opaque|opaque (?:blob|object|payload)|end[- ]to[- ]end encrypt(?:ed|ion)?|e2ee|hidden from)\b`)
	listItemPattern    = regexp.MustCompile(`^\d+\.\s`)
	safeClaimPatterns  = []*regexp.Regexp{
		regexp.MustCompile(`\b(?:server[- ]visible|payload plaintext|plaintext (?:object|chunks?|bytes?|binary)|can (?:see|observe|inspect|retain))\b`),
		regexp.MustCompile(`\b(?:does not|do not|is not|are not|must not|cannot|never)\b.{0,100}\b(?:encrypt|ciphertext|e2ee|opaque|end[- ]to[- ]end)\b`),
		regexp.MustCompile(`\b(?:reject|unsupported)\b.{0,100}\bencrypt`),
		regexp.MustCompile(`\b(?:future|deferred|inactive|optional future)\b.{0,140}\b(?:encrypt(?:ed|ion)?|ciphertext|e2ee|identitykeyprovider|key provider)\b`),
		regexp.MustCompile(`\b(?:encrypt(?:ed|ion)?|ciphertext|e2ee)\b.{0,140}\b(?:future work|deferred|future provider|identitykeyprovider)\b`),
		regexp.MustCompile(`\bencryption\b.{0,20}\b(?:null|empty|absent|omitted)\b`),
	}
	semanticDenyPatterns = []*regexp.Regexp{
		regexp.MustCompile(`\braw encrypted\b.{0,120}\b(?:xep[- ]?0363|upload|object)\b`),
		regexp.MustCompile(`\b(?:xep[- ]?0363|upload|object)\b.{0,120}\braw encrypted\b`),
		regexp.MustCompile(`\b(?:upload (?:server|service)|ejabberd)\b.{0,100}\b(?:receives? an? opaque|sees? ciphertext only|cannot (?:see|observe)|hidden from)\b`),
		regexp.MustCompile(`\b(?:xep[- ]?0363|xmpp|fallback chunks?)\b.{0,120}\b(?:contains?|carries?|sends?|stores?|uploads?)\b.{0,60}\b(?:encrypted|ciphertext|opaque)\b`),
		regexp.MustCompile(`\b(?:encrypted|ciphertext|opaque)\b.{0,100}\b(?:xep[- ]?0363|xmpp|upload|fallback chunks?)\b`),
		regexp.MustCompile(`\b(?:xep[- ]?0363|xmpp|object offload|fallback chunks?)\b.{0,80}\b(?:is|are)\b.{0,40}\b(?:end[- ]to[- ]end encrypted|ciphertext|opaque)\b`),
	}
	serverOpacitySafePatterns = []*regexp.Regexp{
		regexp.MustCompile(`\b(?:future|deferred|optional future)\b.{0,180}\b(?:e2ee|end[- ]to[- ]end encrypt(?:ed|ion)?|identitykeyprovider|key provider)\b`),
		regexp.MustCompile(`\b(?:does not|do not|must not|cannot|never)\b.{0,40}\b(?:claim|imply|promise|guarantee)\b.{0,100}\b(?:cannot|unable to)\b.{0,30}\b(?:inspect|access|observe|read|see)\b`),
	}
	serverOpacityDenyPatterns = []*regexp.Regexp{
		regexp.MustCompile(`\b(?:unable to|cannot|can not|can't|does not|do not|never)\b\s+(?:inspect|access|observe|read|see)\b`),
		regexp.MustCompile(`\b(?:cannot|can not|can't)\b\s+be\s+(?:inspected|accessed|observed|read|seen)\b`),
		regexp.MustCompile(`\blacks?\s+access\b`),
		regexp.MustCompile(`\binaccessible\s+to\b`),
		regexp.MustCompile(`\bprevented\s+from\s+(?:inspecting|accessing|observing|reading|seeing)\b`),
		regexp.MustCompile(`\b(?:has|have|with)\s+no\s+visibility\b|\bno\s+visibility\s+(?:into|of)\b`),
		regexp.MustCompile(`\bconcealed\s+from\b`),
	}
	serverOpacityMetaNegationPatterns = []*regexp.Regexp{
		regexp.MustCompile(`^(?:[-*]\s+|\d+\.\s+)?it is false that\b[^.!?]{0,320}[.!]?$`),
		regexp.MustCompile(`^(?:[-*]\s+|\d+\.\s+)?the claim that\b[^.!?]{0,260}\bis incorrect\b[.!]?$`),
	}
	serverOpacityNotInaccessiblePattern = regexp.MustCompile(`^[^.!?]{0,320}\b(?:is|are|was|were)\s+not\s+inaccessible\s+to\b[^.!?]{0,160}[.!]?$`)
	serverOpacitySentenceBoundary       = regexp.MustCompile(`[!?]+\s*|\.\s+|\.$`)
	serverOpacityClauseBoundary         = regexp.MustCompile(`;\s*|(?:,\s*)?(?:but|yet|however|while)\s+`)
)

func TestV1SecurityModelIsAuthoritativeAndConsistent(t *testing.T) {
	repository := repositoryRoot(t)
	authorityPath := filepath.Join(repository, "docs", "security", "V1_SECURITY_MODEL.md")
	authority, err := os.ReadFile(authorityPath)
	if err != nil {
		t.Fatalf("read authoritative V1 security model: %v", err)
	}
	for _, required := range []string{
		"This document is the authoritative confidentiality and trust-boundary statement",
		"Rank 1 is therefore **infrastructure-confidential** in V1",
		"Rank 2 is **transport protected but server visible** in V1",
		"The stored V1 object is payload plaintext protected in transit by HTTPS",
		"The V1 chunk body contains payload plaintext",
		"`IdentityKeyProvider` and production payload E2EE are future work",
		"The public V1 `large_payloads` capability means bounded multi-carrier",
		"not end-to-end encrypted large-payload delivery",
	} {
		if !strings.Contains(string(authority), required) {
			t.Errorf("authoritative model is missing normative statement %q", required)
		}
	}
	assertCarrierTable(t, string(authority))

	// These were the concrete contradictory formulations in repository design,
	// qualification, and test documents. Keep the scan deliberately textual:
	// documentation must name V1 server-visible plaintext directly rather than
	// relying on an ambiguous use of "encrypted".
	forbidden := []string{
		"encrypted xep-0363",
		"encrypted bounded xmpp",
		"xmpp encrypted chunk",
		"encrypted xmpp chunk",
		"encrypted-object http transfer",
		"upload server sees ciphertext only",
		"upload server should see only ciphertext",
		"encrypt complete canonical payload snapshot",
		"encrypts the complete canonical payload snapshot",
		"split the encrypted canonical snapshot",
		"encrypted payload as bounded text-safe xmpp",
		"xmpp fallback chunks contain encrypted payload bytes",
		"raw encrypted binary for xep-0363",
		"xep-0363 service receives an opaque blob",
	}
	err = filepath.WalkDir(repository, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.ToLower(filepath.Ext(path)) != ".md" {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		lower := strings.ToLower(string(data))
		for _, phrase := range forbidden {
			if strings.Contains(lower, phrase) {
				relative, _ := filepath.Rel(repository, path)
				t.Errorf("%s retains contradictory V1 security claim %q", relative, phrase)
			}
		}
		for lineNumber, statement := range markdownSecurityStatements(string(data)) {
			if carrier, contradictory := contradictoryCarrierClaim(statement); contradictory {
				relative, _ := filepath.Rel(repository, path)
				t.Errorf("%s:%d makes a contradictory active-V1 %s secrecy claim: %q", relative, lineNumber, carrier, statement)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan repository Markdown: %v", err)
	}

	for _, linkedDocument := range []string{"README.md", "SECURITY.md", filepath.Join("docs", "adr", "0005-multi-carrier-large-payload-transfer.md")} {
		data, readErr := os.ReadFile(filepath.Join(repository, linkedDocument))
		if readErr != nil {
			t.Fatalf("read %s: %v", linkedDocument, readErr)
		}
		if !strings.Contains(string(data), "V1_SECURITY_MODEL.md") {
			t.Errorf("%s does not link the authoritative V1 security model", linkedDocument)
		}
	}
}

func TestCarrierClaimSemanticMutants(t *testing.T) {
	for _, statement := range []string{
		"Use raw encrypted binary for XEP-0363, so the upload service receives an opaque blob.",
		"XEP-0363 stores an encrypted object and the upload server sees ciphertext only.",
		"XMPP fallback chunks contain encrypted payload bytes.",
		"Ciphertext is sent through XMPP fallback chunks and remains hidden from ejabberd.",
		"The XEP 0363 object offload is end-to-end encrypted.",
		"The XEP-0363 upload service is unable to inspect application payload contents.",
		"The upload server cannot access the XEP-0363 object bytes.",
		"Ejabberd does not observe plaintext in XMPP fallback chunks.",
		"XMPP fallback chunks cannot be read by the server.",
		"The XEP-0363 upload service lacks access to application payload contents.",
		"The XEP-0363 object is inaccessible to the upload service.",
		"Ejabberd is prevented from reading XMPP fallback chunks.",
		"The XMPP server has no visibility into fallback chunk plaintext.",
		"The XEP-0363 object is concealed from the upload service.",
		"It is false that the XEP-0363 service lacks access, but the payload is concealed from the upload service.",
		"The claim that XMPP fallback chunks are concealed from ejabberd is incorrect, yet ejabberd has no visibility into them.",
		"It is false that the upload service can inspect objects. The XEP-0363 payload is concealed from it.",
		"The claim that ejabberd can inspect messages is incorrect. It has no visibility into XMPP fallback chunks.",
		"The XEP-0363 service is inspectable. Its plaintext is not inaccessible to the upload service.",
		"XMPP fallback chunks are not inaccessible to ejabberd, but the upload service lacks access to XEP-0363 objects.",
		"The XEP-0363 object carries ciphertext and is not inaccessible to the upload service.",
		"V1 does not claim that the XEP-0363 service cannot inspect the payload, but the service lacks access to it.",
		"Future E2EE could conceal XMPP fallback chunks from ejabberd, but the V1 service lacks access to them.",
	} {
		if carrier, contradictory := contradictoryCarrierClaim(statement); !contradictory {
			t.Errorf("mutant was accepted for %s: %q", carrier, statement)
		}
	}

	for _, statement := range []string{
		"V1 XEP-0363 stores server-visible payload plaintext protected by HTTPS.",
		"V1 does not end-to-end encrypt the XEP-0363 object.",
		"Future E2EE may encrypt XMPP fallback chunks after IdentityKeyProvider is reviewed.",
		"XMPP rejects an unsupported encryption reference and otherwise carries plaintext chunks over TLS.",
		"Documentation must not describe V1 XEP-0363 objects as ciphertext.",
		"V1 does not claim that the XEP-0363 service cannot inspect the payload.",
		"Future E2EE with IdentityKeyProvider may make XMPP fallback chunks unable to be read by ejabberd.",
		"It is false that the XEP-0363 upload service lacks access to application payload contents.",
		"The claim that XMPP fallback chunks are concealed from ejabberd is incorrect.",
		"XEP-0363 plaintext is inspectable; it is not inaccessible to the upload service.",
		"XMPP fallback chunk plaintext is not inaccessible to ejabberd.",
	} {
		if carrier, contradictory := contradictoryCarrierClaim(statement); contradictory {
			t.Errorf("valid %s statement was rejected: %q", carrier, statement)
		}
	}
}

func assertCarrierTable(t *testing.T, authority string) {
	t.Helper()
	rows := make(map[string]string)
	for _, line := range strings.Split(authority, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") || !strings.HasSuffix(line, "|") {
			continue
		}
		cells := strings.Split(line, "|")
		if len(cells) < 5 {
			continue
		}
		name := strings.TrimSpace(cells[1])
		if strings.HasPrefix(name, "Rank ") {
			if _, duplicate := rows[name]; duplicate {
				t.Errorf("authoritative carrier table repeats %q", name)
			}
			rows[name] = strings.ToLower(strings.Join(cells[2:len(cells)-1], " "))
		}
	}
	for _, contract := range carrierContracts {
		row, ok := rows[contract.row]
		if !ok {
			t.Errorf("authoritative carrier table is missing %q", contract.row)
			continue
		}
		for _, required := range contract.required {
			if !strings.Contains(row, required) {
				t.Errorf("authoritative carrier row %q is missing %q", contract.row, required)
			}
		}
	}
}

func contradictoryCarrierClaim(statement string) (string, bool) {
	normalized := strings.ToLower(strings.Join(strings.Fields(statement), " "))
	carrier := ""
	if xepCarrierPattern.MatchString(normalized) {
		carrier = "XEP-0363"
	}
	if xmppCarrierPattern.MatchString(normalized) {
		if carrier != "" {
			carrier += "/XMPP"
		} else {
			carrier = "XMPP"
		}
	}
	if carrier == "" {
		return carrier, false
	}
	if activeServerOpacityDenial(normalized) {
		return carrier, true
	}
	if !secrecyPattern.MatchString(normalized) {
		return carrier, false
	}
	for _, safe := range safeClaimPatterns {
		if safe.MatchString(normalized) {
			return carrier, false
		}
	}
	for _, deny := range semanticDenyPatterns {
		if deny.MatchString(normalized) {
			return carrier, true
		}
	}
	return carrier, false
}

func activeServerOpacityDenial(statement string) bool {
	sentences := make([]string, 0, 2)
	for _, sentence := range serverOpacitySentenceBoundary.Split(statement, -1) {
		sentence = strings.TrimSpace(sentence)
		if sentence != "" {
			sentences = append(sentences, sentence)
		}
	}
	wholeStatementException := len(sentences) == 1
	for _, sentence := range sentences {
		sentenceNamesCarrier := xepCarrierPattern.MatchString(sentence) || xmppCarrierPattern.MatchString(sentence)
		for _, clause := range serverOpacityClauseBoundary.Split(sentence, -1) {
			clause = strings.TrimSpace(clause)
			if !hasServerOpacityDenial(clause) {
				continue
			}
			if !sentenceNamesCarrier {
				return true
			}
			wholeStatementSafe := wholeStatementException && (explicitServerOpacityMetaNegation(clause) || explicitServerOpacityNotInaccessible(clause))
			if wholeStatementSafe || matchesServerOpacitySafePattern(clause) {
				continue
			}
			return true
		}
	}
	return false
}

func hasServerOpacityDenial(statement string) bool {
	return serverOpacityDenialCount(statement) > 0
}

func serverOpacityDenialCount(statement string) int {
	denials := 0
	for _, pattern := range serverOpacityDenyPatterns {
		denials += len(pattern.FindAllStringIndex(statement, -1))
	}
	return denials
}

func matchesServerOpacitySafePattern(statement string) bool {
	if serverOpacityDenialCount(statement) != 1 {
		return false
	}
	for _, pattern := range serverOpacitySafePatterns {
		if pattern.MatchString(statement) {
			return true
		}
	}
	return false
}

func explicitServerOpacityNotInaccessible(statement string) bool {
	return serverOpacityDenialCount(statement) == 1 && serverOpacityNotInaccessiblePattern.MatchString(statement)
}

func explicitServerOpacityMetaNegation(statement string) bool {
	if serverOpacityDenialCount(statement) != 1 {
		return false
	}
	for _, pattern := range serverOpacityMetaNegationPatterns {
		if pattern.MatchString(statement) {
			return true
		}
	}
	return false
}

func markdownSecurityStatements(markdown string) map[int]string {
	statements := make(map[int]string)
	var paragraph []string
	paragraphStart := 0
	flush := func() {
		if len(paragraph) > 1 {
			statements[paragraphStart] = strings.Join(paragraph, " ")
		}
		paragraph = nil
		paragraphStart = 0
	}
	for index, line := range strings.Split(markdown, "\n") {
		lineNumber := index + 1
		trimmed := strings.TrimSpace(line)
		listItem := strings.HasPrefix(trimmed, "- ") || listItemPattern.MatchString(trimmed)
		if trimmed == "" || strings.HasPrefix(trimmed, "|") || strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "#") || listItem {
			flush()
			if trimmed != "" {
				statements[lineNumber] = trimmed
			}
			continue
		}
		statements[lineNumber] = trimmed
		if paragraphStart == 0 {
			paragraphStart = lineNumber
		}
		paragraph = append(paragraph, trimmed)
	}
	flush()
	return statements
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve security-model test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
