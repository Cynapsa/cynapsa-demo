// Package stub provides the temporary sentinel used by scaffold-only functions.
package stub

import "errors"

// ErrNotImplemented identifies intentionally unimplemented scaffold behavior.
var ErrNotImplemented = errors.New("cynapsagocore internal scaffold: not implemented")
