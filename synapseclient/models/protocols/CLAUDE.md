<!-- Last reviewed: 2026-03 -->

## Project

Protocol classes providing sync method type hints for IDE autocompletion. Contains 18 protocol modules covering both individual model classes and shared behaviors/mixins; there is not always a strict 1:1 mapping between models and protocol files.

## Conventions

### Naming convention
- File: `{entity}_protocol.py` (e.g., `file_protocol.py`, `project_protocol.py`)
- Class: `{Entity}SynchronousProtocol` (e.g., `FileSynchronousProtocol`)

### Signature matching
Every async method on a model must have a corresponding sync signature here — method name without `_async` suffix, same parameters (including `synapse_client: Optional["Synapse"] = None`). Method bodies use minimal placeholder implementations (e.g., `return self`, returning an empty list, or `...`), matching the existing pattern in each protocol file. Use placeholder return values that satisfy static type checkers — `...` alone will cause type errors for methods with non-None return types. Docstrings should match the async counterpart with updated examples showing sync usage.

### Purpose
The `@async_to_sync` decorator generates the actual sync implementation at class definition time. These protocol files exist solely so IDEs can provide type hints, autocomplete, and documentation for the generated sync methods.

### Adding a new method
1. Add async method to model class (e.g., `store_async()`)
2. Add sync signature to the corresponding protocol (e.g., `store()` with a placeholder body consistent with that file, such as `return self`, an empty collection, or `...`)
3. The decorator auto-generates the working sync implementation

## Constraints

- Protocol signatures must exactly match async signatures minus the `_async` suffix — mismatches cause IDE type hint errors.
- Do not add implementation logic to protocols — they are type stubs only.
