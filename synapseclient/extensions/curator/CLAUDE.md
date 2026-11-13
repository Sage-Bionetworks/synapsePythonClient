<!-- Last reviewed: 2026-11 -->

## Project

Schema curation tools for data modeling — JSON Schema generation from CSV/JSONLD data models, schema registration/binding to Synapse entities, and metadata task creation for file-based and record-based curation workflows.

## Stack

Optional dependencies (gated by `[curator]` extras): pandas, networkx, rdflib, inflection, dataclasses-json.

## Conventions

### schema_generation.py
Largest file in the codebase. Uses networkx (DiGraph, MultiDiGraph) for node/edge relationships and cycle detection (via multiprocessing). Many deprecated validation rule enums marked for removal (SYNPY-1724, SYNPY-1692). Active development area.

### schema_management.py
Uses `wrap_async_to_sync()` for sync versions (not class decorator). `fix_schema_name()` replaces dashes/underscores with periods for Synapse compliance.

### utils.py
`project_id_from_entity_id()` — traverses folder hierarchy up to project (max 1000 iterations). Uses `operations.get` in a loop — known tech debt.

### curation_task_types.py and curation_tasks.py
Bulk record-based tasks behind `synapse curate plan|apply`. `curation_task_types.py` holds the declarations: the manifest dataclasses (`from_csv`, `check`, `select`), `CurationManifestError`, the plan and outcome dataclasses and enums, and the CSV reader; it never imports `curation_tasks.py`. `curation_tasks.py` holds the logic: planning, filling through a temporary grid session never attached to the task, applying, archiving, and the summary formatter and report writer that only the CLI calls. Public functions are async with `wrap_async_to_sync()` twins. Never deletes tasks, tables or rows.

## Constraints

- This area is under active development with frequent PRs. Be cautious about large refactors — coordinate with the curator team.
- `schema_generation.py` contains deprecated patterns (SYNPY-1724) that are still in use — do not remove without verifying the deprecation timeline.
- Uses `urllib.request` in one place instead of httpx (has TODO to replace) — do not propagate this pattern elsewhere.
