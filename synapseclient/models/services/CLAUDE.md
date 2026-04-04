<!-- Last reviewed: 2026-03 -->

## Project

Business logic extracted from model classes to keep models thin. Internal-only — not part of the public API.

## Conventions

### storable_entity.py
`store_entity()` async function orchestrates entity POST/PUT to Synapse. Handles version numbering: if `version_label` changed or `force_version=True`, increments version. Note: this function has an explicit TODO marking it as incomplete/WIP.

### storable_entity_components.py
`store_entity_components()` orchestrates storing annotations, activity, and ACL as separate API calls after the entity itself is stored. `FailureStrategy` enum (LOG_EXCEPTION, RAISE_EXCEPTION) controls error handling. `wrap_coroutine()` helper wraps individual component store operations.

### search.py
`get_id()` utility resolves an entity by name+parent or by Synapse ID. Has a TODO for deprecated code replacement (SYNPY-1623) — uses `asyncio.get_event_loop().run_in_executor()` as a legacy pattern for blocking operations.

## Constraints

- These are internal service functions — do not expose in `models/__init__.py` or import from user-facing code.
