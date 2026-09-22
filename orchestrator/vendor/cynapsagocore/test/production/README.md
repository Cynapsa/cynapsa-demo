# Production Qualification Test Area

This folder is owned by Pod 7. Its required implementation is defined in:

- `docs/development/PRODUCTION_READINESS_NEXT_STEP.md`
- `docs/development/POD_07_PRODUCTION_QUALIFICATION.md`

Pod 7 will add pinned Coturn and fault-injection environments, multi-process runners, SDK conformance, security and chaos suites, performance benchmarks, soak workloads, compatibility matrices, clean-install tests, release-candidate build verification, and sanitized evidence collection here.

The first implemented slice is the [disposable NAT/STUN/TURN environment](coturn/README.md). It reuses the accepted disposable ejabberd authority, keeps STUN and authenticated TURN in separate pinned containers, and places each public-API agent behind its own disposable NAT gateway.

The second implemented slice is the [disposable TCP/HTTP fault environment](faults/README.md). It fronts production XMPP and XEP-0363 client traffic with a pinned proxy and proves latency, backpressure, half-open, reset, and proxy-restart behavior without impairing the host network.

Tests and fixtures must remain synthetic, disposable, isolated, resource-bounded, and free of production credentials or user data. Nothing in this folder authorizes publishing packages or contacting production services.
