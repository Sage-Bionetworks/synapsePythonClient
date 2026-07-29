<!-- Last reviewed: 2026-07 -->

## Project

Composable behavior mixins for model classes — ACL management, container operations, async job orchestration, table CRUD, form submissions, and JSON schema validation.

## Conventions

### access_control.py
Uses `BenefactorTracker` dataclass to track ACL cascade when inheritance changes — maps entity→benefactor and benefactor→children relationships. Batch ACL operations use `asyncio.as_completed()` for concurrency with tqdm progress bars.

### storable_container.py
Queue-based concurrent download/upload via `_worker()` coroutine processing `asyncio.Queue`. `FailureStrategy` enum (LOG_EXCEPTION vs RAISE_EXCEPTION) controls child entity error handling. Uses `wrap_async_generator_to_sync_generator()` for `get_children`. Child entity type dispatch via concrete type → model class mapping.

### asynchronous_job.py
`ASYNC_JOB_URIS` dict maps concrete types to REST endpoints — when adding a new async job type, register here AND in `core/constants/concrete_types.py`. Subclasses must implement `to_synapse_request()` and `fill_from_dict()`.

Resource-scoped endpoints use `{placeholder}` segments (e.g. `/curation/task/{taskId}/execute/async`). `_resolve_async_job_uri()` fills them from the request body, matching each placeholder to the identically-named camelCase key emitted by `to_synapse_request()` — so a `{taskId}` URI requires the request to carry `taskId`.

### table_components.py
Column type mapping between Python types and Synapse column types. Multiple TODOs for incomplete features (SYNPY-1651).

### json_schema.py
Schema validation and creation via async jobs. Used by entities that support schema binding (Folder, Project).

## Constraints

- When adding a new async job type, register in BOTH `ASYNC_JOB_URIS` (here) and `concrete_types.py` — missing either causes runtime errors.
- Child collections on `StorableContainer` models must use `compare=False` in field definition to avoid breaking `has_changed` comparison.
