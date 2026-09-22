package sdkboundary

import (
	"fmt"
	"time"

	v1 "github.com/Cynapsa/cynapsagocore/api/v1"
	"github.com/Cynapsa/cynapsagocore/internal/model"
)

const defaultApplicationTimeout = 30 * time.Second

// MapConfig will copy allowlisted public configuration into a fresh internal config.
func (a *Adapter) MapConfig(config v1.Config) (model.RuntimeConfig, error) {
	if config.CommandTimeout < 0 || config.RPCTimeout < 0 || config.QueueLimit == 0 || config.QueueLimit > maxQueueCapacity || config.PayloadLimit == 0 || config.PayloadLimit > maxCanonicalPayloadSize {
		return model.RuntimeConfig{}, fmt.Errorf("%w: config limit", ErrMalformedInput)
	}
	if config.CommandTimeout == 0 {
		config.CommandTimeout = defaultApplicationTimeout
	}
	if config.RPCTimeout == 0 {
		config.RPCTimeout = defaultApplicationTimeout
	}
	return model.RuntimeConfig{
		CommandTimeout: config.CommandTimeout,
		RPCTimeout:     config.RPCTimeout,
		QueueLimit:     config.QueueLimit,
		PayloadLimit:   config.PayloadLimit,
	}, nil
}
