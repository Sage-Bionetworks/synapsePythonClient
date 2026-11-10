<!-- Last reviewed: 2026-11 -->

## Project

Multipart file upload to Synapse storage (S3, GCP, SFTP). Dual implementation: sync (requests) and async (httpx).

## Conventions

### Constants
- `MAX_NUMBER_OF_PARTS = 10000`
- `MIN_PART_SIZE = 5 MB`
- `DEFAULT_PART_SIZE = 8 MB`
- `MAX_RETRIES = 7`
- Upload retry: 60 retries spanning ~30 minutes for resilience

### Sync vs async duality
`multipart_upload.py` (sync/requests) and `multipart_upload_async.py` (async/httpx) must be kept in feature parity. Both implement `UploadAttempt` / `UploadAttemptAsync` classes orchestrating multi-part uploads with presigned URL batching. Exception: the lost-upload restart and `RESUME_NOTE` (see Upload flow) are async-only, because the sync module serves only deprecated callers.

### Async-specific patterns
- `HandlePartResult` dataclass tracks individual part uploads
- `shared_progress_bar()` context manager for tqdm integration across concurrent tasks
- Explicit `gc.collect()` calls and psutil memory monitoring during large uploads — prevents memory pressure
- Uses `asyncio.Lock` for thread-safe state management

### Sync-specific patterns
- Thread-local `requests.Session` storage for persistent HTTP connections per thread
- `shared_executor()` context manager allows callers to provide their own thread pool

### Upload flow
1. Pre-upload: MD5 calculation, MIME type detection, storage location determination from project settings
2. Presigned URL batch fetching with expiry detection and refresh
3. Multi-part upload with retry per part
4. Post-upload: complete upload API call, retrieve file handle

Resume: `POST /file/multipart` is idempotent per (request JSON MD5, user), so a second run of the same upload resumes from `partsState`. In `_multipart_upload_async`, an S3 `NoSuchUpload` error (`_is_lost_upload`; arrives as add-part `ADD_FAILED` (HTTP 200) or complete HTTP 400 — part PUTs go to temp objects, not UploadPart, so they never see it) causes one restart with `force_restart=True` that does not use a retry, and every error that leaves the loop gets `RESUME_NOTE` via `add_note`. User docs: `docs/explanations/upload_resume.md`.

### upload_utils.py
- `get_partial_file_chunk()` — binary file chunk reader with offset tracking
- `get_partial_dataframe_chunk()` — DataFrame chunk reader (iterates in 100-row increments)
- MD5 calculation, MIME type guessing, part size computation
