package protocol

import "strings"

// ValidateBoundSessionIdentity validates the private full-identity forms that
// the Mesh Server can bind. The resource is only an exact-session address;
// callers must establish mesh authority independently from authenticated
// server state.
func ValidateBoundSessionIdentity(identity, meshID string) error {
	if ValidateAgentIdentity(identity) != nil || ValidateMeshID(meshID) != nil {
		return &FieldError{Field: "session_identity", Cause: ErrInvalidIdentifier}
	}
	slash := strings.LastIndexByte(identity, '/')
	if slash <= 0 || slash == len(identity)-1 || strings.Contains(identity[:slash], "/") || ValidateAgentIdentity(identity[:slash]) != nil {
		return &FieldError{Field: "session_identity", Cause: ErrInvalidIdentifier}
	}
	resource := identity[slash+1:]
	if resource != meshID && !ValidInstallationResource(resource) {
		return &FieldError{Field: "session_identity", Cause: ErrInvalidIdentifier}
	}
	return nil
}

// ValidInstallationResource recognizes the exact server-issued Runtime V2
// resource shape without treating it as membership evidence.
func ValidInstallationResource(resource string) bool {
	parts := strings.Split(resource, ".")
	if len(parts) != 3 || parts[0] != "r2" || len(parts[1]) != 36 || len(parts[2]) < 8 || len(parts[2]) > 128 {
		return false
	}
	for index, value := range parts[1] {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if value != '-' {
				return false
			}
			continue
		}
		if value < '0' || value > '9' && (value < 'a' || value > 'f') {
			return false
		}
	}
	for _, value := range parts[2] {
		if value < 'a' || value > 'z' {
			if value < 'A' || value > 'Z' {
				if value < '0' || value > '9' {
					if value != '-' && value != '_' {
						return false
					}
				}
			}
		}
	}
	return true
}
