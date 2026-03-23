<!-- Last reviewed: 2026-03 -->

## Project

REST API service layer — thin async functions that map to Synapse REST endpoints. One file per resource type. Called by model layer, never by end users directly.

## Conventions

### Function signature pattern
```python
async def verb_resource(
    required_param: str,
    optional_param: str = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
```
- All functions are `async def`
- `synapse_client` is always the last parameter, keyword-only (after `*`)
- Use `Synapse.get_client(synapse_client=synapse_client)` to get the client instance
- Use `TYPE_CHECKING` guard for `Synapse` import — avoids circular dependencies between `api/` and `client.py`

### REST call pattern
```python
client = Synapse.get_client(synapse_client=synapse_client)
return await client.rest_post_async(uri="/endpoint", body=json.dumps(request))
```
Available methods: `rest_get_async`, `rest_post_async`, `rest_put_async`, `rest_delete_async`. Pass `endpoint=client.fileHandleEndpoint` for file handle operations; omit for the default repository endpoint. Use `json.dumps()` for request bodies — not raw dicts.

### Return values
- Most functions return raw `Dict[str, Any]` — transformation happens in the model layer via `fill_from_dict()`
- Some return typed dataclass instances (e.g., `EntityHeader` from `entity_services.py`) when the data is only used internally
- Delete operations return `None`

### Pagination
Use helpers from `api_client.py`:
- `rest_get_paginated_async()` — for GET endpoints with limit/offset. Expects `results` or `children` key in response.
- `rest_post_paginated_async()` — for POST endpoints with `nextPageToken`. Expects `page` array in response.
Both are async generators yielding individual items.

### Entity factory (`entity_factory.py`)
Polymorphic entity deserialization via concrete type dispatch. Maps Java class names from `core/constants/concrete_types.py` to model classes. When adding a new entity type, register the type mapping here.

### Adding a new service file
1. Create `synapseclient/api/new_service.py`
2. Add all public functions to `api/__init__.py` imports and `__all__` — every public function must be re-exported
3. Use `json.dumps()` for request bodies (not dict)
4. Reference `entity_services.py` for CRUD pattern, `table_services.py` for pagination pattern
