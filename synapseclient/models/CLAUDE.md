<!-- Last reviewed: 2026-03 -->

## Project

Dataclass-based entity models for the Synapse REST API. Each model represents a Synapse resource (Project, File, Folder, Table, etc.) with async-first methods and auto-generated sync wrappers.

## Conventions

### New model checklist
1. Decorate with `@dataclass()` above `@async_to_sync` (follow existing models — `@dataclass` is the outer decorator)
2. Inherit from the model-specific `*SynchronousProtocol` base (e.g., `FileSynchronousProtocol`, `ProjectSynchronousProtocol`), then mixins (`AccessControllable`, `StorableContainer`, etc.) when appropriate
3. Create a matching `*SynchronousProtocol` file in `protocols/` with sync method signatures
4. Register concrete type in `core/constants/concrete_types.py`
5. Add to `models/__init__.py` exports and `__all__`
6. Add to entity factory type map in `api/entity_factory.py` if it's an entity type
7. Add to `ASYNC_JOB_URIS` in `models/mixins/asynchronous_job.py` if it uses async jobs
8. Summarize the model and its core functions in class-level docstrings with examples of distinct API use-cases. Add method-level docstrings with at least one example. Refer to existing models (`evaluation.py`, `materializedview.py`) for guidance on syntax, tone, and scope.
9. Methods are typically scoped to a single API communication (e.g., "download file", "list evaluations") and should wrap functionality that already exists in `api/`. Refer to existing models as guides.
10. Abstract any reusable logic to a new or existing `.py` in `api/` and, if appropriate, in `mixins/`

### Standard fields every mutable entity model must have
```python
id: Optional[str] = None
name: Optional[str] = None
etag: Optional[str] = None
created_on: Optional[str] = field(default=None, compare=False)
modified_on: Optional[str] = field(default=None, compare=False)
created_by: Optional[str] = field(default=None, compare=False)
modified_by: Optional[str] = field(default=None, compare=False)
create_or_update: bool = field(default=True, repr=False)
_last_persistent_instance: Optional["Self"] = field(default=None, repr=False, compare=False)
```

Use `compare=False` for read-only timestamps, child collections, annotations, and internal state — this makes `has_changed` compare only user-modifiable fields.

### fill_from_dict() pattern
Maps camelCase REST keys to snake_case fields via `.get("camelCaseKey", None)`. Must return `self`. Handle annotations separately with `set_annotations` parameter. Reference: `folder.py`, `file.py`.

### Annotations handling
Annotations are deserialized separately from `fill_from_dict()` — they use a `set_annotations` flag parameter. The `Annotations` model wraps key-value metadata. When storing, annotations are sent via a separate API call in `models/services/storable_entity_components.py`.

### Activity/provenance pattern
`Activity` model tracks provenance (what data/code produced an entity). Contains `used` and `executed` lists of `UsedEntity`/`UsedURL` references. Activity is stored as a separate component — the `associate_activity_to_new_version` flag on File controls whether activity transfers to new versions.

### _last_persistent_instance lifecycle
- Set via `_set_last_persistent_instance()` after every successful `store_async()` and `get_async()`
- Uses `dataclasses.replace(self)` with `deepcopy` for annotations
- Enables `has_changed` property — skips redundant API calls when nothing changed
- Drives `create_or_update` logic: if no `_last_persistent_instance`, attempts merge with existing Synapse entity via `merge_dataclass_entities()` from `core/utils.py`. When calling this, set `fields_to_preserve_from_source` to attributes only mutable by the API (e.g., `id`, `etag`, `content_source`, `owner_id`, `created_on`). See `evaluation.py` for an example.

### @otel_trace_method on every async method
Apply to all async methods that call Synapse. Format: `f"{ClassName}_{Operation}: ID: {self.id}, Name: {self.name}"`.

### delete_none_keys() before API calls
Always call `delete_none_keys()` on request dicts before passing to `store_entity()` — the Synapse API rejects `None` values.

### EnumCoercionMixin for enum fields
If a model has enum-typed fields, inherit from `EnumCoercionMixin` and declare `_ENUM_FIELDS: ClassVar[Dict[str, type]]` mapping field names to enum classes. Auto-coerces strings to enums on assignment via `__setattr__`.

### OOP models vs legacy synapseclient/ classes
Modern dataclass models live in `models/`. Legacy classes in the package root (`synapseclient/entity.py`, `synapseclient/table.py`, etc.) are kept for backwards compatibility. New features go in `models/`. When introducing a new OOP model that replaces a legacy class, add a `DeprecationWarning` to the legacy counterpart and update its docstring to point users to the new model.

### Business logic in services/
Complex orchestration logic lives in `models/services/` (storable_entity, storable_entity_components, search) — not directly on model classes. This keeps models thin.

## Constraints

- Never manually write sync methods on models — `@async_to_sync` generates them. Use `@skip_async_to_sync` to exclude specific methods.
- All direct API communications must go in `api/`. Model methods wrap `api/` functions — only skip the `api/` layer if the API call is truly one-off and won't be reused.
- Protocol files must exactly match the async method signatures (minus `_async` suffix) — they exist for IDE type hints, not runtime dispatch.
- Child collections (files, folders, tables) must use `compare=False` to avoid breaking `has_changed`.
