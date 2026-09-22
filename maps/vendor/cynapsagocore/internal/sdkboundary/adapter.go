// Package sdkboundary is the only translator between api/v1 and internal models.
package sdkboundary

// Adapter owns public validation, normalization, redaction, and projection.
type Adapter struct{}

// New constructs the sole SDK/Core boundary adapter.
func New() (*Adapter, error) {
	return &Adapter{}, nil
}
