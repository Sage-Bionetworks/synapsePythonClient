<!-- Last reviewed: 2026-03 -->

## Project

File download from Synapse storage with MD5 validation, collision handling, and progress tracking.

## Conventions

### Primary download path
`download_async.py` is the primary async download implementation. `download_functions.py` contains shared helpers and the sync download wrapper.

### MD5 validation
Post-transfer MD5 validation is mandatory. Raises `SynapseMd5MismatchError` on mismatch — the download is retried automatically (60 retries spanning ~30 minutes).

### Collision handling
Controlled by `if_collision` parameter, using constants from `core/constants/method_flags.py`:
- `overwrite.local` — replace existing local file
- `keep.local` — skip download if local file exists
- `keep.both` — rename downloaded file to avoid collision

### Progress tracking
Uses `shared_download_progress_bar` from `core/transfer_bar.py` for tqdm-based progress. Multi-file downloads track cumulative progress via `cumulative_transfer_progress`.

### Key helpers
- `ensure_download_location_is_directory()` — validates/creates download directory
- `download_by_file_handle()` — downloads a file given its handle metadata
