# Regression test instructions

These rules apply to this directory and its descendants.

1. Read `README.md` and `manifest.json` before changing a mapped regression.
2. Preserve issue #2 through #13 coverage. Every entry must remain `existing`
   in the source tree and use one exact anchored test selector in its actual
   package. Never add a planned row, skip, ignored error, or selector that
   matches a different test.
3. Run the manifest contract and the exact issue command before broader gates.
   Concurrency and lifecycle changes require the race stage.
4. The `remote-mesh` stage must reuse `test/production/coturn`; do not create a
   duplicate ejabberd configuration, Compose file, agent, NAT implementation,
   or service lock.
5. Preserve authenticated XEP-0215 authority, short-lived TURN credentials,
   immutable images, least privilege, separate private client networks,
   explicit NAT gateways, bounded readiness, positive network evidence,
   sanitized artifacts, and unconditional exact-resource teardown.
6. Never mount or set `connectivity.json`,
   `CYNAPSA_PRIVATE_CONNECTIVITY_PROFILE`, a TURN REST secret in a client,
   production credentials, a Docker socket, broad host paths, host networking,
   or privileged client containers.
7. Keep the comprehensive Coturn matrix separate from the focused remote smoke.
   Functional smoke success is not production qualification.
