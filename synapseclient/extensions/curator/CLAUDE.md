<!-- Last reviewed: 2026-03 -->

## Project

Schema curation tools for data modeling — JSON Schema generation from CSV/JSONLD data models, schema registration/binding to Synapse entities, and metadata task creation for file-based and record-based curation workflows.

## Stack

Optional dependencies (gated by `[curator]` extras): pandas, pandarallel, networkx, rdflib, inflection, dataclasses-json.

## Conventions

### schema_generation.py
Largest file in the codebase. Uses networkx (DiGraph, MultiDiGraph) for node/edge relationships and cycle detection (via multiprocessing). Many deprecated validation rule enums marked for removal (SYNPY-1724, SYNPY-1692). Active development area.

### schema_management.py
Uses `wrap_async_to_sync()` for sync versions (not class decorator). `fix_schema_name()` replaces dashes/underscores with periods for Synapse compliance.

### utils.py
`project_id_from_entity_id()` — traverses folder hierarchy up to project (max 1000 iterations). Uses `operations.get` in a loop — known tech debt.

`validate_column_order_list()` / `resolve_column_order_list()` — shared column ordering used by both metadata task creators for their `column_order` parameter. `validate_column_order_list()` is the cheap shape check (list, non-empty strings, no duplicates) called at the top of the public functions so bad input fails before any entity is created; `resolve_column_order_list()` produces the final order (pinned + requested + remaining) and is where the unknown-column check happens. File-based ordering must run after `EntityView.store()` because Synapse appends its default columns (`name`, `id`, `createdBy`, ...) during that store. That means an unknown column name cannot be detected until the view already exists, so `_create_json_schema_entity_view()` wraps the ordering in a `try`/`except ValueError` that deletes the view before re-raising (a failed delete is logged, not raised, so the ValueError is what reaches the caller) — do not remove this rollback, it is what keeps a typo from leaving an orphaned EntityView in the user's folder. Keep the second `view.store()` outside that `try` and keep the `except` narrowed to `ValueError`: a transient failure while persisting the order must leave the view in place to be retried, not delete it. Only `name` and `id` are pinned for file-based tasks — `createdBy` is deliberately no longer pinned (SYNPY-1840).

## Constraints

- This area is under active development with frequent PRs. Be cautious about large refactors — coordinate with the curator team.
- `schema_generation.py` contains deprecated patterns (SYNPY-1724) that are still in use — do not remove without verifying the deprecation timeline.
- Uses `urllib.request` in one place instead of httpx (has TODO to replace) — do not propagate this pattern elsewhere.
