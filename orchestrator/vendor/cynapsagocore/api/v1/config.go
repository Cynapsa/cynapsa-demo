package v1

import "time"

// MaximumPayloadBytes is the largest complete canonical payload snapshot
// accepted by the V1 Core. Bindings must reject larger configured limits
// before creating a Core.
const MaximumPayloadBytes uint64 = 134_217_696

// Config contains process-local limits used to create one Core.
// Authentication credentials are supplied later by AuthLoginCommand or
// AuthConnectCommand and are never part of Core creation.
type Config struct {
	// Zero timeouts select the V1 default of 30 seconds.
	CommandTimeout time.Duration
	RPCTimeout     time.Duration
	// QueueLimit is always in the inclusive range 1..65,536.
	QueueLimit uint32
	// PayloadLimit is always in the inclusive range
	// 1 byte..MaximumPayloadBytes (134,217,696 bytes).
	PayloadLimit uint64
}

// ConfigUpdate contains the allowlisted runtime settings that may change safely.
type ConfigUpdate struct {
	CommandTimeout *time.Duration
	RPCTimeout     *time.Duration
	// QueueLimit, when present, is always in the inclusive range 1..65,536.
	QueueLimit *uint32
	// PayloadLimit, when present, is always in the inclusive range
	// 1 byte..MaximumPayloadBytes (134,217,696 bytes).
	PayloadLimit *uint64
}
