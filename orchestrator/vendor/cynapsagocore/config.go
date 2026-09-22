package cynapsagocore

import v1 "github.com/Cynapsa/cynapsagocore/api/v1"

// MaximumPayloadBytes is the V1 Core creation ceiling shared by every SDK
// binding and the native ABI.
const MaximumPayloadBytes = v1.MaximumPayloadBytes

// Config aliases the versioned, transport-opaque public runtime configuration.
type Config = v1.Config
