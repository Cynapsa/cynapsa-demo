package rank1webrtc

import "testing"

func TestBoundToMeshAcceptsLegacyAndAuthorizedInstallationResources(t *testing.T) {
	const bare = "agent@example.test"
	const mesh = "mesh-one"
	const installation = "01234567-89ab-4def-8123-456789abcdef"
	for _, identity := range []string{
		bare + "/" + mesh,
		bare + "/r2." + installation + ".AAAAAAAAAAAAAAAA",
	} {
		if !boundToMesh(identity, mesh) {
			t.Fatalf("authorized identity %q was rejected", identity)
		}
	}
	for _, identity := range []string{
		bare,
		bare + "/r2.not-a-uuid.AAAAAAAAAAAAAAAA",
		bare + "/r2." + installation + ".short",
		bare + "/r2." + installation + ".bad/resource",
	} {
		if boundToMesh(identity, mesh) {
			t.Fatalf("invalid identity %q was accepted", identity)
		}
	}
}
