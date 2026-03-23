<!-- Last reviewed: 2026-03 -->

## Project

Schema curation tools for data modeling — JSON Schema generation from CSV/JSONLD data models, schema registration/binding to Synapse entities, and metadata task creation for file-based and record-based curation workflows.

## Stack

Optional dependencies (gated by `[curator]` extras): pandas, pandarallel, networkx, rdflib, inflection, dataclasses-json.

## Conventions

### schema_generation.py (5984 lines)
Largest file in the codebase. Contains `DataModelParser`, `DataModelComponent`, `DataModelRelationships` classes. Uses networkx (DiGraph, MultiDiGraph) for node/edge relationships and cycle detection (via multiprocessing). Many deprecated validation rule enums marked for removal (SYNPY-1724, SYNPY-1692). Active development area — multiple recent PRs modifying conditionals, display names, and grouping.

### schema_registry.py
Query engine for the schema registry table. Default table ID: `syn69735275` (configurable via parameter). Builds SQL WHERE clauses from filter kwargs — supports exact match and LIKE pattern match. `return_latest_only=True` returns newest version URI only.

### schema_management.py
Thin wrappers around `JSONSchema` OOP model:
- `register_jsonschema()` / `register_jsonschema_async()` — loads schema from file, calls `.store_async()`
- `bind_jsonschema()` / `bind_jsonschema_async()` — binds schema to entity
- `fix_schema_name()` — replaces dashes/underscores with periods for Synapse compliance

Uses `wrap_async_to_sync()` for sync versions (not class decorator).

### file_based_metadata_task.py
Creates EntityView from JSON Schema bound to folder/project. `create_json_schema_entity_view()` auto-reorders columns (createdBy→name→id to front). `create_or_update_wiki_with_entity_view()` embeds EntityView query in Wiki page.

### record_based_metadata_task.py
Extracts schema properties → DataFrame → RecordSet → CurationTask + Grid. Supports URI-based schemas via `JSONSchema.from_uri()`.

### utils.py
`project_id_from_entity_id()` — traverses folder hierarchy up to project (max 1000 iterations). Uses legacy sync `get()` API in a loop — known tech debt.

## Constraints

- This area is under active development with frequent PRs. Be cautious about large refactors — coordinate with the curator team.
- `schema_generation.py` contains deprecated patterns (SYNPY-1724) that are still in use — do not remove without verifying the deprecation timeline.
- Uses `urllib.request` in one place instead of httpx (has TODO to replace) — do not propagate this pattern elsewhere.
