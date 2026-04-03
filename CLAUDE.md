<!-- Last reviewed: 2026-03 -->

## Project

Synapse Python Client — official Python SDK and CLI for Synapse (synapse.org), a collaborative science platform by Sage Bionetworks. Provides programmatic access to entities (projects, files, folders, tables, views), metadata, permissions, evaluations, and data curation workflows. Published to PyPI as `synapseclient`.

## Stack

- Python 3.10–3.14 (`setup.cfg`: `python_requires = >=3.10, <3.15`)
- HTTP: httpx (async), requests (sync/legacy)
- Models: stdlib dataclasses (NOT Pydantic)
- Tests: pytest 8.2, pytest-asyncio, pytest-socket, pytest-xdist
- Docs: MkDocs with Material theme, mkdocstrings
- Linting: ruff, black (line-length 88), isort (profile=black), bandit
- CI: GitHub Actions → SonarCloud, PyPI deploy on release
- Docker: `Dockerfile` at repo root, published to `ghcr.io/sage-bionetworks/synapsepythonclient`

## Commands

```bash
# Install for development
pip install -e ".[boto3,pandas,pysftp,tests,curator,dev]"

# Unit tests
pytest -sv tests/unit

# Integration tests (requires Synapse credentials, runs in parallel)
pytest -sv --reruns 3 tests/integration -n 8 --dist loadscope

# Pre-commit checks (ruff, black, isort, bandit)
pre-commit run --all-files

# Build docs locally
pip install -e ".[docs]" && mkdocs serve
```

## Conventions

### Async-first with generated sync wrappers
All new methods must be async with `_async` suffix. The `@async_to_sync` class decorator (`core/async_utils.py`) auto-generates sync counterparts at class definition time. Never write sync methods manually on model classes — the decorator handles it.

### `wrap_async_to_sync()` for standalone functions
Use `wrap_async_to_sync()` (not `@async_to_sync`) for free-standing async functions outside of classes — see `operations/` layer for the pattern. The class decorator only works on classes.

### Protocol classes for sync type hints
Each model in `models/` has a corresponding protocol in `models/protocols/` defining the sync method signatures. When adding a new async method to a model, add its sync signature to the protocol class so IDE type hints work.

### Dataclass models with `fill_from_dict()`
Models are `@dataclass` classes, NOT Pydantic. REST responses are deserialized via `fill_from_dict()` methods on each model. New models must follow this pattern.

### Concrete types are Java class names
`core/constants/concrete_types.py` maps Java class names (e.g., `org.sagebionetworks.repo.model.FileEntity`) for polymorphic entity deserialization. When adding new entity types, register the concrete type string here AND in `api/entity_factory.py` AND in `models/mixins/asynchronous_job.py` if it's an async job type.

### Options dataclass pattern
The `operations/` layer uses dataclass option objects (`StoreFileOptions`, `FileOptions`, `TableOptions`, etc.) to bundle type-specific configuration for CRUD operations. Follow this pattern for new entity-type-specific options.

### Mixin composition for shared behavior
Shared functionality lives in `models/mixins/` (AccessControllable, StorableContainer, AsynchronousJob, etc.). Prefer adding to existing mixins over duplicating logic across models.

### `synapse_client` parameter pattern
Most functions accept an optional `synapse_client` parameter. If omitted, `Synapse.get_client()` returns the cached singleton. Never pass `None` explicitly — omit the argument instead.

### Branch naming
Use `SYNPY-{issue_number}` or `synpy-{issue_number}` prefix for feature branches. PR titles follow `[SYNPY-XXXX] Description` format.

## Architecture

```
synapseclient/
├── client.py          # Synapse class — public entry point, REST methods, auth (9600+ lines)
├── api/               # REST API layer — one file per resource type (21 files)
│   └── entity_factory.py  # Polymorphic entity deserialization via concrete type dispatch
├── models/            # Dataclass entities (Project, File, Table, etc.) (28 files)
│   ├── protocols/     # Sync method type signatures for IDE hints (18 files)
│   ├── mixins/        # Shared behavior (ACL, containers, async jobs, tables) (7 files)
│   └── services/      # Model-level business logic (storable_entity, search)
├── operations/        # High-level CRUD: get(), store(), delete() — factory dispatch
├── core/              # Infrastructure: upload/download, retry, cache, creds, OTel
│   ├── upload/        # Multipart upload (sync + async)
│   ├── download/      # File download (sync + async)
│   ├── credentials/   # Auth chain (PAT, env var, config file, AWS SSM)
│   ├── constants/     # Concrete types, config keys, limits, method flags
│   ├── models/        # ACL, Permission, DictObject, custom JSON serialization
│   └── multithread_download/  # Threaded download manager
├── extensions/
│   └── curator/       # Schema curation (pandas, networkx, rdflib) — optional
├── services/          # JSON schema validation services
└── entity.py, table.py, ...  # Legacy classes (pre-OOP rewrite, read-only)

synapseutils/          # Legacy bulk utilities (copy, sync, migrate, walk) — sync-only
```

Data flow: User → `operations/` factory → model async methods → `api/` service functions → `client.py` REST calls → Synapse API. Responses deserialized via `fill_from_dict()` on model instances.

## Constraints

- Do not use Pydantic for models — the codebase uses stdlib dataclasses with custom serialization. Mixing would break the `@async_to_sync` decorator and `fill_from_dict()` pattern.
- For new tests, prefer async test modules. Existing synchronous unit tests under `tests/unit/` are retained and maintained; the `@async_to_sync` decorator is covered by a dedicated smoke test, so avoid adding duplicate sync/async test coverage.
- On non-Windows platforms, unit tests must not make external network calls — `pytest-socket` blocks internet-facing sockets while allowing Unix domain sockets. Socket blocking is skipped on Windows. Use `pytest-mock` for HTTP mocking.
- `develop` is the default/main branch, not `main` or `master`. PRs target `develop`.
- Legacy classes in root `synapseclient/` (entity.py, table.py, etc.) are kept for backwards compatibility. New features go in `models/` using the dataclass pattern.
- Avoid adding new methods to `client.py` (9600+ lines) — prefer the `api/` + `models/` layered pattern.
- `synapseutils/` is legacy sync-only (uses `requests`, NOT `httpx`). Do not add async methods there — new async equivalents go in `models/` or `operations/`.

## Testing

- `asyncio_mode = auto` in pytest.ini — no need for `@pytest.mark.asyncio`
- `asyncio_default_fixture_loop_scope = session` — all async tests share one event loop
- Unit test client fixture: session-scoped, `skip_checks=True`, `cache_client=False`
- Integration tests use `--reruns 3` for flaky retries and `-n 8 --dist loadscope` for parallelism
- Integration fixtures create per-worker Synapse projects; use `schedule_for_cleanup()` for teardown
- Auth env vars: `SYNAPSE_AUTH_TOKEN` (bearer token), `SYNAPSE_PROFILE` (config file profile, default: `"default"`), `SYNAPSE_TOKEN_AWS_SSM_PARAMETER_NAME` (AWS SSM path)
- CI runs integration tests only on Python 3.10 and 3.14 (oldest + newest) to limit Synapse server load

## Maintenance

Each CLAUDE.md file has a `<!-- Last reviewed: YYYY-MM -->` header. Update this when the file is reviewed or modified. If a code change invalidates guidance in a CLAUDE.md file, update the guidance in the same PR.
