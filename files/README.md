# Files agent

A standalone native Cynapsa identity with a local LangGraph. It answers questions
from UTF-8 `.txt`, `.md`, `.csv`, and `.json` documents in one directory. It does
not need Google Maps or a vector database. Other agents can run on other machines.

## Configure and run

Create a files-agent identity in the portal and add it to the same mesh as the
orchestrator and client. From this directory:

```sh
cp .env.example .env
# Fill .env, then:
./run.sh
```

Required configuration:

- `CYNAPSA_TOKEN`: this identity's enrollment token for first login.
- `CYNAPSA_GITHUB_TOKEN`: build-only read access to SDK/Core repositories.
- `DEMO_MESH_ID`: the actual mesh ID, not its display label.
- `DEMO_FILES_DIRECTORY`: an existing absolute host directory, e.g.
  `/Users/you/Documents/demo-database`. Spaces are supported; do not quote values
  in the Docker env file. Commas are unsupported. Symlink mount roots are rejected.
- `LITELLM_API_KEY`: the model gateway key, or an OpenAI key when using OpenAI
  directly. Alternatively use `.private/litellm_api_key` with mode `0600`.
- `LITELLM_BASE_URL`, `LITELLM_MODEL`: defaults are
  `https://litellm.eladrave.com` and `gpt-5.6-terra-high`. For direct OpenAI use
  `https://api.openai.com/v1` and a model available to your account.

The runner mounts the host directory at `/data` **read-only** and overrides the
container's `DEMO_FILES_DIRECTORY` accordingly. The non-root container user
(UID/GID 65532) must be able to read the directory/files; do not broadly relax
permissions on unrelated private documents. Mount only a dedicated database.
Documents are not copied into the image or Cynapsa state volume.

On startup, copy the printed bare agent JID into the orchestrator's `.env`:

```dotenv
DEMO_FILES_AGENT_ID=your-files-agent@connect.cynapsa.com
```

Rebuild/restart the orchestrator and client after updating their application code:

```sh
./run.sh --build
```

Start maps and files before the orchestrator, then start the client. Ask
“What is Guy's home address?” The orchestrator calls `ask_files_agent` over native
Cynapsa RPC `/files`; no inter-agent HTTP link or shared graph is used. The files
agent accepts `{"question": "..."}` on its wildcard endpoint, and returns
`answer`, verified tool-output `sources` (relative filenames and lines/excerpts),
and `attribution`. The client prints file citations. Documents are data, never
instructions. Not-found is a successful answer, not a transport error.

## Tools and limits

- `list_files()`: recursive supported filenames; skips hidden files/directories,
  symlinks, and special files. At most 4,000 entries, 1,000 files, and seven nested
  directory levels; reports truncation.
- `search_files(query)`: case-insensitive keyword matching, not semantic search
  or structured CSV/JSON queries. At most 20 matching lines, 500 characters each,
  and 4 MiB of file bytes per call (including binary/invalid text). Reports skipped files
  and truncation. Use a person's name as the query, then read adjacent context.
- `read_file(path, start_line, end_line)`: relative supported paths only; at most
  100 lines, 500 characters per line, 8,000 output characters. Reports truncation.
- A file must be regular UTF-8 text without NUL bytes, at most 1 MiB. Traversal,
  absolute paths, symlinks at every relative component, FIFOs, and unsupported
  extensions are rejected. Descriptor-relative opens avoid symlink-swap races.
- Up to three tool rounds, five calls per round, then one final model completion.
  Tool errors become bounded tool feedback; invalid model output becomes canonical
  502 `files_failed`. Bad input is 400 `bad_request`; model failure is 503
  `model_unavailable`. Model HTTP timeout is 25 seconds per completion, not a
  total deadline; a long workflow can exceed the orchestrator's 100-second RPC TTL.

There is no comprehensive-search guarantee for large directories. Empty evidence
forces a not-found answer; evidence-backed model interpretation can still be
wrong, so check the citations. Host updates are visible to subsequent tool reads,
but there is no transactional snapshot across reads.

## Privacy and state

File excerpts **leave the machine for the configured LLM provider** and travel
to the orchestrator over Cynapsa. Orchestrator conversation memory stores answer
text in its local SQLite volume. Every authorized mesh caller of this agent can
ask about its mounted documents; there is no per-document/user access control.
Never point this agent at your whole home directory or a secret store.

The entity directory is independent: its Dockerfile builds SDK/Core from GitHub
`main`, and includes them in the resulting image. No sibling files are required.
Read `SOURCE_DEPENDENCIES.md` for provenance. `./run.sh --build` refreshes the
image; `./run.sh --force-enroll` uses the genuine SDK CLI in the same state volume.
Default volume: `cynapsa-demo-files-state`; override with
`CYNAPSA_DEMO_FILES_VOLUME`. Other overrides: `CYNAPSA_DEMO_FILES_IMAGE` and
`CYNAPSA_DEMO_FILES_ENV_FILE`. Stop with Ctrl-C; the encrypted installation remains
in its volume. Real `.env`, keys, and documents must not be committed.

## Traffic logs

The app logs full incoming Cynapsa request bodies and handler return values,
including file excerpts returned to callers, with the peer's full ID. Set
`DEMO_ORCHESTRATOR_AGENT_ID` to label that incoming peer as `orchestrator`.
There is no redaction or truncation, so use only non-sensitive demo files.
Headers are not logged. LLM HTTP access logs are quiet; warnings/errors and
connection alerts remain. A handler return precedes SDK validation/transmission
and is not a delivery receipt. Rebuild with `./run.sh --build`.
