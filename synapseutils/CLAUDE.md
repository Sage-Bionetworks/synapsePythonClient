<!-- Last reviewed: 2026-03 -->

## Project

Legacy bulk utility functions for copy, sync, migrate, walk, describe, and monitor operations. Pre-OOP code using legacy `requests` HTTP and old-style Entity classes (not modern dataclass models).

## Conventions

### Naming convention
Functions use camelCase (legacy convention) — e.g., `syncFromSynapse()`, `copyFileHandles()`, `notifyMe()`. Do NOT convert to snake_case — this is the public API.

### migrate_functions.py (1429 lines)
Uses SQLite database for migration state persistence. `MigrationResult` proxy object iterates results without loading all into memory — avoids memory issues for repos with millions of files. Two-phase pattern: `index_files_for_migration()` then `migrate_indexed_files()`. Uses concurrent.futures thread pool with configurable part size (default 100 MB).

### sync.py (1528 lines)
`syncFromSynapse()` / `syncToSynapse()` for bulk folder transfer. Generates manifest files for tracking. Known issue: TODO at line 967 notes "absence of a raise here appears to be a bug and yet tests fail if this is raised" — `SynapseFileNotFoundError` handling may be incorrect.

### copy_functions.py (965 lines)
`copyFileHandles()` batches by `MAX_FILE_HANDLE_PER_COPY_REQUEST`. Returns list with potential `failureCodes` (UNAUTHORIZED, NOT_FOUND). `copyWiki()` and `changeFileMetaData()` for metadata operations.

### monitor.py (192 lines)
`notifyMe()` — decorator for sync functions that sends email notification on completion/failure. `notify_me_async()` — async variant. Both retry on failure with configurable retry count. Uses `syn.sendMessage()` with user's owner ID.

### walk_functions.py
`walk()` — recursive entity tree traversal similar to `os.walk()`. Returns generator of (dirpath, dirnames, filenames) tuples.

### describe_functions.py
Opens CSV/TSV entities as pandas DataFrames. Calculates per-column stats: mode, min/max (numeric), mean, dtype.

## Constraints

- ALL functions use legacy sync `requests` library, NOT httpx. Do NOT add async methods here — new async equivalents go in `synapseclient/models/` or `synapseclient/operations/`.
- Uses legacy Entity classes (`from synapseclient import Entity, File, Folder`) — NOT modern dataclass models.
- Do not refactor to modern patterns without a migration plan — these are public APIs with external consumers.
