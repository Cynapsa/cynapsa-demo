package runtime

import (
	"time"

	"github.com/Cynapsa/cynapsagocore/internal/model"
)

// Config contains the boundary-mapped process settings plus private runtime
// ownership settings. CleanupTimeout is supplied by trusted composition; it is
// deliberately absent from the SDK-visible configuration.
type Config struct {
	model.RuntimeConfig
	CleanupTimeout time.Duration
}
