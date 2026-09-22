# Black-Box E2E Runner

The Pod 6 runner builds two independent consumers:

- `go_agent` links only the root Go facade and `api/v1` and emits one JSON
  observation per scenario.
- `native_agent.c` compiles against `cynapsacore_v1.h` and loads the
  release-form shared library with its export-control file applied.

Run the implemented public-boundary slice with:

```bash
GOTOOLCHAIN=go1.26.6 go test -count=1 ./test/e2e -v
```

The suite exercises process-local controls, terminal lifecycle behavior, the
release-form native library, and real authenticated establishment against the
pinned disposable service. It never converts a required scenario into a skip.
Real service profiles are consumed from the sibling-owned `test/e2e/ejabberd`
infrastructure and are not replaced by mocks.

Set `CYNAPSA_E2E_ARTIFACT_DIR` to a private directory outside the tested Git
worktree to retain the sanitized JSONL observations and disposable-service
evidence. In-worktree paths are rejected so evidence cannot hide dirty or
ignored build inputs. The qualification workflow uses a unique runner-temporary
directory and uploads it even when the E2E step fails.
