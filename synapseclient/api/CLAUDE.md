<!-- Last reviewed: 2026-03 -->

## Project

REST API service layer — thin async functions that map to Synapse REST endpoints. One file per resource type. Called by model layer, never by end users directly.

## Reference

- [Synapse REST API docs](https://rest-docs.synapse.org/rest/)

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
- `synapse_client` is **always** `Optional["Synapse"] = None` — never make it required. Callers omit it to use the cached singleton returned by `Synapse.get_client()`.
- `synapse_client` is always the last parameter, keyword-only (after `*`)
- Use `Synapse.get_client(synapse_client=synapse_client)` to get the client instance
- Use `TYPE_CHECKING` guard for `Synapse` import — avoids circular dependencies between `api/` and `client.py`
- Construct a `query_params` dictionary for non-null optional args, and pass it to the `params` arg of the REST call. See `entity_services.py` for the pattern.

### Docstring conventions
Module-level — every file opens with boilerplate linking to the Synapse REST controller:
```python
"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.XController>
"""
```
Function-level (Google style):
```python
"""
One-line summary.

<https://rest-docs.synapse.org/rest/POST/endpoint.html>

Arguments:
    param: Description.
    synapse_client: If not passed in and caching was not disabled by
        `Synapse.allow_client_caching(False)` this will use the last created
        instance from the Synapse class constructor.

Returns:
    Description of return value.
"""
```
- The `synapse_client` argument description is boilerplate — always copy it verbatim, not paraphrased.
- The REST endpoint URL uses `<link>` format (angled brackets), not markdown `[text](url)`.
- Parameter descriptions in `Arguments:` must be copied verbatim from the Synapse REST API docs for that endpoint — do not paraphrase or infer.

### REST call pattern
```python
client = Synapse.get_client(synapse_client=synapse_client)
return await client.rest_post_async(uri="/endpoint", body=json.dumps(request))
```
Available methods: `rest_get_async`, `rest_post_async`, `rest_put_async`, `rest_delete_async`. Pass `endpoint=client.fileHandleEndpoint` for file handle operations; omit for the default repository endpoint. Use `json.dumps()` for request bodies — not raw dicts. Always assign the response to a named `response` variable before returning or extracting attributes from it.

### Return values
- Most functions return raw `Dict[str, Any]` — transformation happens in the model layer via `fill_from_dict()`
- Some return typed dataclass instances (e.g., `EntityHeader` from `entity_services.py`) when the data is only used internally
- Delete operations return `None`

### Pagination
Use async pagination helpers when the API endpoint returns a list of results. For single-object responses, a simple `return` is sufficient.

Helpers from `api_client.py`:
- `rest_get_paginated_async()` — for GET endpoints with limit/offset. Expects `results` or `children` key in response.
- `rest_post_paginated_async()` — for POST endpoints with `nextPageToken`. Expects `page` array in response.
Both are async generators yielding individual items. Reference `entity_services.py`, `table_services.py`, or `evaluation_services.py` for pagination patterns.

### Entity factory (`entity_factory.py`)
Polymorphic entity deserialization via concrete type dispatch. Maps Java class names from `core/constants/concrete_types.py` to model classes. When adding a new entity type, register the type mapping here.

### When to add a new service file vs. update an existing one
Add a new file when the Synapse REST controller is different (each file maps to one controller). Update an existing file when adding endpoints under the same controller.

### Adding a new service file
1. Create `synapseclient/api/new_service.py`
2. Add all public functions to `api/__init__.py` imports and `__all__` — every public function must be re-exported
3. Use `json.dumps()` for request bodies (not dict)
4. Reference `entity_services.py` for CRUD pattern, `table_services.py` or `evaluation_services.py` for pagination pattern
