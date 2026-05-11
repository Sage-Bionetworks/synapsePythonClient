<!-- Last reviewed: 2026-03 -->

## Project

High-level CRUD factory methods (`get`, `store`, `delete`) that dispatch to the correct entity-type-specific handler. Entry point for users who want a simpler interface than calling model methods directly.

## Conventions

### Sync wrapper pattern
Uses `wrap_async_to_sync()` on standalone async functions — NOT the `@async_to_sync` class decorator (which only works on classes). Every public async function has a sync counterpart generated this way.

### Factory dispatch via isinstance()
`store_async()` routes to entity-specific handlers via `isinstance()` checks:
- File/RecordSet → `_handle_store_file_entity()`
- Project/Folder → `_handle_store_container_entity()`
- Table-like entities → `_handle_store_table_entity()`
- Link → `_handle_store_link_entity()`
- Team → if has `id`: `.store_async()`, else: `.create_async()`
- AgentSession → `.update_async()` (not `.store_async()`)

### Options dataclasses
Type-specific configuration bundled in dataclass objects:
- **Store**: `StoreFileOptions`, `StoreContainerOptions`, `StoreTableOptions`, `StoreGridOptions`, `StoreJSONSchemaOptions`
- **Get**: `FileOptions`, `ActivityOptions`, `TableOptions`, `LinkOptions`

`LinkOptions.follow_link=True` returns the target entity, not the Link itself.

### Delete version precedence
Version resolution order: explicit `version` parameter > entity's `version_number` attribute > version parsed from ID string (e.g., "syn123.4"). Only warns on conflict if both explicit param and attribute are set and differ.

### FailureStrategy
`FailureStrategy` enum controls child entity error handling in container store operations:
- `LOG_EXCEPTION` — log error, continue with remaining children
- `RAISE_EXCEPTION` — raise immediately on first child failure

### Adding new operations
1. Add async function in the appropriate file
2. Create sync wrapper with `wrap_async_to_sync()`
3. Export both in `operations/__init__.py` and `__all__`
