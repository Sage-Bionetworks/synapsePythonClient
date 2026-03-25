<!-- Last reviewed: 2026-03 -->

## Project

Test suite for the Synapse Python Client. Unit tests run without network access; integration tests hit the live Synapse API.

## Conventions

### Write async tests only
Do not create synchronous test files. The `@async_to_sync` decorator is validated by a dedicated smoke test (`tests/integration/synapseclient/models/synchronous/test_sync_wrapper_smoke.py`). Duplicate sync tests were removed to cut CI cost and maintenance burden.

### Unit tests (`tests/unit/`)
- `pytest-socket` blocks all network calls (unix sockets allowed on non-Windows for async event loop). On Windows, socket disabling is skipped entirely — tests still run but are not network-isolated.
- Session-scoped `syn` fixture: `Synapse(skip_checks=True, cache_client=False)` with silent logger
- Autouse `set_timezone` fixture forces `TZ=UTC` for deterministic timestamps
- Client caching disabled via `Synapse.allow_client_caching(False)`
- Use `AsyncMock` for async method mocking, `create_autospec` for type-safe mocks
- Class-based test organization with `@pytest.fixture(scope="function", autouse=True)` for setup
- Test file naming: `unit_test_*.py` (legacy) or `test_*.py` (newer) — both patterns are discovered by pytest

### Integration tests (`tests/integration/`)
- All async tests share one event loop: `asyncio_default_fixture_loop_scope = session`
- `schedule_for_cleanup(item)` — defer entity/file cleanup to session teardown. Always use this instead of inline deletion. Cleanup list is reversed before execution for dependency ordering (children deleted before parents).
- Per-worker project fixtures (`project_model`, `project`) created during session setup
- `--reruns 3` for flaky retry, `-n 8 --dist loadscope` for parallelism
- OpenTelemetry tracing opt-in via `SYNAPSE_INTEGRATION_TEST_OTEL_ENABLED` env var
- Two client fixtures: `syn` (silent logger) and `syn_with_logger` (verbose)
- conftest.py locations: `tests/unit/conftest.py` (session client, socket blocking, UTC timezone), `tests/integration/conftest.py` (logged-in client, per-worker projects, cleanup fixture)

### Test utilities
- `tests/test_utils.py`: `spy_for_async_function(original_func)` — wraps async function for pytest-mock spying while preserving async behavior. `spy_for_function(original_func)` — sync variant.
- `tests/integration/helpers.py`: `wait_for_condition(condition_fn, timeout_seconds=60)` — async polling helper with exponential backoff. Accepts sync or async condition functions.
- `tests/integration/__init__.py`: `QUERY_TIMEOUT_SEC = 600`, `ASYNC_JOB_TIMEOUT_SEC = 600`
- Test data generators in production code: `core/utils.py` has `make_bogus_data_file()`, `make_bogus_binary_file(n)`, `make_bogus_uuid_file()`

### No `@pytest.mark.asyncio` needed
`asyncio_mode = auto` in pytest.ini — all async test functions are auto-detected.

### Python 3.14+ limitation
Sync wrapper smoke tests are skipped on Python 3.14+ — `@async_to_sync` raises `RuntimeError` when an event loop is already active (pytest-asyncio runs one). Users on 3.14+ must call async methods directly.

## Constraints

- Unit tests must never make network calls — `pytest-socket` will fail them. Mock all HTTP interactions.
- Integration test cleanup is mandatory — use `schedule_for_cleanup()` for every created resource to avoid orphaned Synapse entities.
