# Executable regression contract

This directory maps GitHub issues
[#2](https://github.com/Cynapsa/cynapsagocore/issues/2) through
[#13](https://github.com/Cynapsa/cynapsagocore/issues/13) to tests that already
exist on the current tree. `manifest.json` contains no planned-success rows.
Its validator parses the exact package source and rejects loose or nonmatching
`-run` selectors.

Run from the repository root:

```sh
test/regression/run.sh manifest
test/regression/run.sh fast
test/regression/run.sh race
test/regression/run.sh ejabberd
test/regression/run.sh remote-mesh
```

`fast` and `race` dynamically run the same complete non-Docker package set as
hosted CI. The service stages require Docker and OpenSSL. They use the current
pinned ejabberd/Coturn fixtures and synthetic identities only.

## Independent-NAT smoke

`remote-mesh` runs only `TestRemoteMeshSmoke` in the existing
`test/production/coturn` package. It starts the same XEP-0215 authority, Coturn
service, NAT gateway, multiprocess public-Core agent, and production lock used
by the full qualification matrix; it owns no Compose or ejabberd configuration.

The server advertises two-minute TURN REST credentials over authenticated
XEP-0215. Neither client receives the REST secret or a local connectivity
profile. The clients live on separate private bridges with separate default
NAT gateways and no common client/service network. After both public peer
diagnostics report stable availability, each gateway blackholes only
ejabberd:5222. Fresh nonce-bound request/reply, reverse one-way traffic with
mandatory delivery acceptance, and post-health checks run behind explicit host
barriers with a fresh ten-second bound per phase. Positive XMPP DROP and
authenticated TURN allocation/channel-bind counters must be observed while the
blackhole remains installed. XMPP is restored before graceful shutdown.

This composition proves the exchange cannot silently use Rank 2 during the
barrier without exposing a private carrier label through the SDK. Package tests
remain the authority for carrier preference and ownership linearization. The
separate full Coturn matrix still owns direct traversal, UDP/TCP relay,
rejection, outage, and recovery profiles.

Every service, client, gateway, network, volume, generated credential, and
secret-bearing file is torn down by exact identity. Failure evidence is scanned
against generated secrets. Readiness uses protocol probes and bounded polling;
fixed sleeps are not used where a readiness condition is observable.

## CI routing

Hosted `ci.yml` retains its complete normal, race, static, vulnerability, and
ABI jobs and runs the manifest contract as an additional check. The protected
self-hosted `qualification.yml` runs the focused smoke before the separate full
Coturn matrix, preserving integration, E2E, fault, and release order. It also
runs Sundays at `17 2 * * 0`; the canonical-repository event guard admits only
protected-main pushes, approved manual dispatches, and that trusted schedule.
