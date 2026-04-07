<!-- Last reviewed: 2026-03 -->

## Project

Centralized constants used across the codebase — concrete type mappings, API limits, collision modes, and config file keys.

## Conventions

### concrete_types.py — 3-way registration required
Maps Java class name strings (e.g., `org.sagebionetworks.repo.model.FileEntity`) for polymorphic entity deserialization. When adding a new entity or job type, register in THREE places:
1. `concrete_types.py` — add the constant string
2. `api/entity_factory.py` — add to the type dispatch map
3. `models/mixins/asynchronous_job.py` `ASYNC_JOB_URIS` — add if it's an async job type

### limits.py
`MAX_FILE_HANDLE_PER_COPY_REQUEST = 100` and other API batch size limits.

### method_flags.py
Collision handling modes for file downloads: `COLLISION_OVERWRITE_LOCAL`, `COLLISION_KEEP_LOCAL`, `COLLISION_KEEP_BOTH`.

### config_file_constants.py
Section and key names for the `~/.synapseConfig` file. `AUTHENTICATION_SECTION_NAME` identifies the auth section.
