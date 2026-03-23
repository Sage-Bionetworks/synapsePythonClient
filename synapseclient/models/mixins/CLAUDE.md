<!-- Last reviewed: 2026-03 -->

## Project

Composable behavior mixins for model classes — ACL management, container operations, async job orchestration, table CRUD, form submissions, and JSON schema validation.

## Conventions

### access_control.py (2527 lines)
`AccessControllable` mixin provides `get_permissions()`, `set_permissions()`, `delete_acl()`. Uses `BenefactorTracker` dataclass to track ACL cascade when inheritance changes — maps entity→benefactor and benefactor→children relationships. Batch ACL operations use `asyncio.as_completed()` for concurrency with tqdm progress bars.

### storable_container.py (1530 lines)
`StorableContainer` mixin for entities containing files/folders/tables. Queue-based concurrent download/upload via `_worker()` coroutine processing `asyncio.Queue`. `sync_from_synapse_async()` recursively downloads child entities. `FailureStrategy` enum (LOG_EXCEPTION vs RAISE_EXCEPTION) controls child entity error handling. Uses `wrap_async_generator_to_sync_generator()` for `get_children`. Child entity type dispatch via concrete type → model class mapping.

### asynchronous_job.py (516 lines)
`AsynchronousCommunicator` abstract mixin for long-running Synapse jobs. `ASYNC_JOB_URIS` dict maps concrete types to REST endpoints — when adding a new async job type, register here AND in `core/constants/concrete_types.py`. `send_job_and_wait_async()` polls job status with tqdm progress. Subclasses must implement `to_synapse_request()` and `fill_from_dict()`.

### table_components.py (4634 lines)
Massive table CRUD mixin: schema management, query execution, row operations, CSV upload/download, snapshot creation. Uses `multipart_upload_dataframe_async` for CSV data. Pandas integration for to_csv/read_csv. Column type mapping between Python types and Synapse column types. Multiple TODOs for incomplete features (SYNPY-1651).

### form.py (178 lines)
`FormData` submission mixin with `FormGroup`, `FormChangeRequest`, `FormSubmissionStatus` dataclasses. `StateEnum`: WAITING_FOR_SUBMISSION, SUBMITTED_WAITING_FOR_REVIEW, ACCEPTED, REJECTED.

### json_schema.py (1267 lines)
Schema validation and creation via async jobs. `JSONSchemaVersionInfo`, `JSONSchemaBinding`, `JSONSchemaValidation` dataclasses. Used by entities that support schema binding (Folder, Project).

## Constraints

- When adding a new async job type, register in BOTH `ASYNC_JOB_URIS` (here) and `concrete_types.py` — missing either causes runtime errors.
- Child collections on `StorableContainer` models must use `compare=False` in field definition to avoid breaking `has_changed` comparison.
