<!-- Last reviewed: 2026-03 -->

## Project

Infrastructure layer — authentication, file transfer, retry logic, caching, OpenTelemetry tracing, and the `async_to_sync` decorator that powers the dual sync/async API.

## Conventions

### async_to_sync decorator (`async_utils.py`)
- Scans class for `*_async` methods and creates sync wrappers stripping the suffix
- Uses `ClassOrInstance` descriptor — methods work on both class and instance
- Detects running event loop: uses `nest_asyncio.apply()` for nested loops (Python <3.14), raises `RuntimeError` on Python 3.14+ instructing users to call async directly
- `wrap_async_to_sync()` for standalone functions (not class methods) — used in `operations/` layer
- `wrap_async_generator_to_sync_generator()` for async generators — must call `aclose()` in finally block
- `@skip_async_to_sync` decorator excludes specific methods from sync wrapper generation (sets `_skip_conversion = True`)
- `@otel_trace_method()` wraps async methods with OpenTelemetry spans. Format: `f"{ClassName}_{Operation}: ID: {self.id}, Name: {self.name}"`

### Retry patterns (`retry.py`)
- `with_retry()` — count-based exponential backoff (default 3 retries), jitter 0.5-1.5x multiplier
- `with_retry_time_based_async()` — time-bounded (default 20 min), exponential backoff with 0.01-0.1 random jitter
- Default retryable status codes: `[429, 500, 502, 503, 504]`
- `NON_RETRYABLE_ERRORS` list overrides status code retry (currently: `["is not a table or view"]`)
- 429 throttling: wait bumps to 16 seconds minimum
- Sets OTel span attribute `synapse.retries` on retry

### Credentials chain (`credentials/`)
Provider chain tries in order: login args → config file → env var (`SYNAPSE_AUTH_TOKEN`) → AWS SSM. Credentials implement `requests.auth.AuthBase`, adding `Authorization: Bearer` header. Profile selection via `SYNAPSE_PROFILE` env var or `--profile` arg.

### Upload/download
- Both use 60-retry params spanning ~30 minutes for resilience
- Upload determines storage location from project settings, supports S3/SFTP/GCP
- Download validates MD5 post-transfer, raises `SynapseMd5MismatchError` on mismatch
- Progress via `tqdm`; multi-threaded uploads suppress per-file messages via `cumulative_transfer_progress`

### concrete_types.py
Maps Java class names from Synapse REST API for polymorphic deserialization. When adding a new entity type, add its concrete type string here AND in `api/entity_factory.py` type map AND in `models/mixins/asynchronous_job.py` ASYNC_JOB_URIS if it's an async job type.

### Key reusable utilities (`utils.py`)
- `delete_none_keys(d)` — removes None-valued keys from dict. MUST call before all API requests — Synapse rejects null values.
- `id_of(obj)` — extracts Synapse ID from entity, dict, or string
- `concrete_type_of(entity)` — gets the concrete type string from an entity
- `get_synid_and_version(id_str)` — parses "synXXX.N" strings into (id, version) tuples
- `merge_dataclass_entities(source, dest, ...)` — merges fields from one dataclass into another
- `log_dataclass_diff(obj1, obj2)` — logs field-by-field differences between two dataclass instances
- `snake_case(name)` — converts camelCase to snake_case
- `normalize_whitespace(s)` — collapses whitespace
- `MB`, `KB`, `GB` — byte size constants
- `make_bogus_data_file()`, `make_bogus_binary_file(n)`, `make_bogus_uuid_file()` — test file generators (in production code, used by tests)

### Exception hierarchy (`exceptions.py`)
`SynapseError` base with 14+ subclasses: `SynapseHTTPError`, `SynapseMd5MismatchError`, `SynapseFileNotFoundError`, `SynapseNotFoundError`, `SynapseAuthenticationError`, etc. `_raise_for_status()` and `_raise_for_status_httpx()` handle HTTP error responses with Bearer token redaction via `BEARER_TOKEN_PATTERN` regex.

### Rolled-up subdirectories

**`core/models/`** — Internal dataclasses for ACL, Permission, DictObject (dict-like base class), and custom JSON serialization utilities. `DictObject` (`dict_object.py`) provides dot-notation access to dict entries.

**`core/multithread_download/`** — Threaded download manager with `shared_executor()` context manager for external thread pool configuration. Uses `DownloadRequest` dataclass. Default part size: `SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE`.

## Constraints

- Bearer tokens must never appear in logs — use `BEARER_TOKEN_PATTERN` regex for redaction.
- `delete_none_keys()` must be called on all dicts before sending to the API — Synapse rejects null values.
