<!-- Last reviewed: 2026-03 -->

## Project

Protocol classes providing sync method type hints for IDE autocompletion. One protocol file per model class (18 files).

## Conventions

### Naming convention
- File: `{entity}_protocol.py` (e.g., `file_protocol.py`, `project_protocol.py`)
- Class: `{Entity}SynchronousProtocol` (e.g., `FileSynchronousProtocol`)

### Signature matching
Every async method on a model must have a corresponding sync signature here — method name without `_async` suffix, same parameters (including `synapse_client: Optional["Synapse"] = None`). Body is always `...` (no implementation).

### Purpose
The `@async_to_sync` decorator generates the actual sync implementation at class definition time. These protocol files exist solely so IDEs can provide type hints, autocomplete, and documentation for the generated sync methods.

### Adding a new method
1. Add async method to model class (e.g., `store_async()`)
2. Add sync signature to the corresponding protocol (e.g., `store()` with `...` body)
3. The decorator auto-generates the working sync implementation

## Constraints

- Protocol signatures must exactly match async signatures minus the `_async` suffix — mismatches cause IDE type hint errors.
- Do not add implementation logic to protocols — they are type stubs only.
