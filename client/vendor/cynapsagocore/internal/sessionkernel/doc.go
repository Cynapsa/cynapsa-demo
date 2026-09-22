// Package sessionkernel is the private composition root between the stable
// runtime kernel and authenticated session services.
//
// Runtime startup installs one exhaustive closed typed handler adapter and starts
// this controller as a local lifecycle component. It does not create network
// clients. A later auth.login or auth.connect command constructs and starts a
// typed service graph, verifies its authenticated identity, rolls failures back
// in reverse order, advances the frozen runtime lifecycle, and only then makes
// the graph and immutable SDK personality reachable by other commands.
//
// Concrete connectivity and object implementations remain injected. Encrypted
// oversized-payload support is constructed only when an explicit provider
// implementing both payload.KeySealer and payload.KeyResolver is present.
// Password material is never passed to that provider and there is no local key
// fallback.
package sessionkernel
