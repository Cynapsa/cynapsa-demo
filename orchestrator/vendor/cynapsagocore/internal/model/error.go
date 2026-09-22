package model

// Error retains detailed internal failure data for private diagnostics.
type Error struct {
	Code         string
	Stage        string
	Cause        error
	Retryable    bool
	Location     string
	DiagnosticID string
}
