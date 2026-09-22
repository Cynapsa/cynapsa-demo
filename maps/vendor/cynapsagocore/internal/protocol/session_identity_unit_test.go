package protocol

import "testing"

func TestBoundSessionIdentityAcceptsLegacyAndRuntimeV2Resources(t *testing.T) {
	const mesh = "mesh-one"
	const installation = "01234567-89ab-4def-8123-456789abcdef"
	for _, identity := range []string{
		"agent@example.test/" + mesh,
		"agent@example.test/r2." + installation + ".AAAAAAAAAAAAAAAA",
	} {
		if err := ValidateBoundSessionIdentity(identity, mesh); err != nil {
			t.Fatalf("valid identity %q: %v", identity, err)
		}
	}
	for _, identity := range []string{
		"agent@example.test",
		"agent@example.test/other-mesh",
		"agent@example.test/r2.not-a-uuid.AAAAAAAA",
		"agent@example.test/r2." + installation + ".short",
		"agent@example.test/r2." + installation + ".bad/resource",
	} {
		if err := ValidateBoundSessionIdentity(identity, mesh); err == nil {
			t.Fatalf("invalid identity %q accepted", identity)
		}
	}
}
