package rpc

import "errors"

var (
	ErrInvalidConfig         = errors.New("rpc: invalid configuration")
	ErrCapacity              = errors.New("rpc: table capacity exhausted")
	ErrDuplicate             = errors.New("rpc: duplicate identifier")
	ErrUnknown               = errors.New("rpc: unknown request")
	ErrExpired               = errors.New("rpc: request expired")
	ErrCancelled             = errors.New("rpc: request cancelled")
	ErrCompleted             = errors.New("rpc: request already completed")
	ErrInvalidResponse       = errors.New("rpc: invalid response")
	ErrInvalidHandle         = errors.New("rpc: invalid request handle")
	ErrHandleEntropy         = errors.New("rpc: request handle entropy unavailable")
	ErrHandleCollision       = errors.New("rpc: request handle collision limit reached")
	ErrInvalidCorrelation    = errors.New("rpc: invalid correlation identifier")
	ErrReplyInProgress       = errors.New("rpc: reply already in progress")
	ErrInvalidLease          = errors.New("rpc: invalid reply lease")
	ErrIssuanceExhausted     = errors.New("rpc: process-lifetime identity issuance exhausted")
	ErrAuthorizationRejected = errors.New("rpc: current mesh membership rejected")
)
