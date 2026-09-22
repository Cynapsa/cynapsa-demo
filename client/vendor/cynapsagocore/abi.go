package cynapsagocore

import v1 "github.com/Cynapsa/cynapsagocore/api/v1"

// CurrentABIVersion is the first version of the transport-opaque native SDK contract.
const CurrentABIVersion uint32 = v1.CurrentSchemaVersion

// ABIVersion returns the native SDK contract version implemented by this build.
func ABIVersion() uint32 {
	return CurrentABIVersion
}
