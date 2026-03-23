<!-- Last reviewed: 2026-03 -->

## Project

Infrastructure layer — authentication, file transfer, retry logic, caching, OpenTelemetry tracing, and the `async_to_sync` decorator that powers the dual sync/async API.

## Conventions

### async_to_sync decorator (`async_utils.py`)
- Scans class for `*_async` methods and creates sync wrappers stripping the suffix
- Uses `ClassOrInstance` descriptor — methods work on both class and instance
- Detects running event loop: uses `nest_asyncio.apply()` for nested loops, raises on Python 3.14+
- `wrap_async_to_sync()` for standalone functions (not class methods)
- `wrap_async_generator_to_sync_generator()` for async generators — must `aclose()` in finally block

### Retry patterns (`retry.py`)
- `with_retry()` — simple exponential backoff, fixed retry count (default 3)
- `with_retry_time_based_async()` — time-bounded (default 20 min), exponential backoff with jitter
- Default retryable status codes: `[429, 500, 502, 503, 504]`
- `NON_RETRYABLE_ERRORS` list overrides status code retry (e.g., "is not a table or view")
- 429 throttling: wait bumps to 16 seconds minimum

### Credentials chain (`credentials/`)
Provider chain tries in order: login args → config file → env var (`SYNAPSE_AUTH_TOKEN`) → AWS SSM. Credentials implement `requests.auth.AuthBase`, adding `Authorization: Bearer` header. Profile selection via `SYNAPSE_PROFILE` env var or `--profile` arg.

### Upload/download
- Both use 60-retry params spanning ~30 minutes for resilience
- Upload determines storage location from project settings, supports S3/SFTP/GCP
- Download validates MD5 post-transfer, raises `SynapseMd5MismatchError` on mismatch
- Progress via `tqdm`; multi-threaded uploads suppress per-file messages via `cumulative_transfer_progress`

### concrete_types.py
Maps Java class names from Synapse REST API for polymorphic deserialization. When adding a new entity type, add its concrete type string here AND in `api/entity_factory.py` type map.

## Constraints

- Bearer tokens must never appear in logs — use `BEARER_TOKEN_PATTERN` regex for redaction.
- `delete_none_keys()` must be called on all dicts before sending to the API — Synapse rejects null values.
