package v1

// Admission reports whether the local boundary accepted responsibility for a command.
type Admission struct {
	CommandID     CommandID
	CommandHandle CommandHandle
	Accepted      bool
	Error         *Error
}

// Completion contains one final normalized result for an admitted command.
type Completion struct {
	CommandID CommandID
	OK        bool
	Result    Result
	Error     *Error
}

// Result is the closed marker interface implemented by public result types.
type Result interface {
	resultType()
}

// EmptyResult represents a successful command with no additional public data.
type EmptyResult struct{}

// resultType prevents arbitrary external values from becoming command results.
func (EmptyResult) resultType() {
}
