package model

import "time"

// RuntimeConfig contains private runtime configuration and is never decoded from SDK input.
type RuntimeConfig struct {
	CommandTimeout time.Duration
	RPCTimeout     time.Duration
	QueueLimit     uint32
	PayloadLimit   uint64
	Connectivity   ConnectivityConfig
}

// ConnectivityConfig contains low-level private configuration unavailable to SDKs.
type ConnectivityConfig struct {
	BootstrapData []byte
	HealthTimeout time.Duration
	RecoveryDelay time.Duration
}
