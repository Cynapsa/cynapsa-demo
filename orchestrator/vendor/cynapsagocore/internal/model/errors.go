package model

import "errors"

var (
	ErrInvalidMeshEndpoint = errors.New("model: invalid mesh endpoint")
	ErrInvalidAddressURL   = errors.New("model: invalid address URL")
)
