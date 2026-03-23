<!-- Last reviewed: 2026-03 -->

## Project

Test suite for the Synapse Python Client. Unit tests run without network access; integration tests hit the live Synapse API.

## Conventions

### Write async tests only
Do not create synchronous test files. The `@async_to_sync` decorator is validated by a dedicated smoke test. Duplicate sync tests were removed to cut CI cost and maintenance burden.

### Unit tests (`tests/unit/`)
- `pytest-socket` blocks all network calls (unix sockets allowed on non-Windows for async event loop)
- Session-scoped `syn` fixture: `Synapse(skip_checks=True, cache_client=False)` with silent logger
- Autouse `set_timezone` fixture forces `TZ=UTC` for deterministic timestamps
- Client caching disabled via `Synapse.allow_client_caching(False)`
- Use `AsyncMock` for async method mocking, `create_autospec` for type-safe mocks
- Class-based test organization with `@pytest.fixture(scope="function", autouse=True)` for setup

### Integration tests (`tests/integration/`)
- All async tests share one event loop: `asyncio_default_fixture_loop_scope = session`
- `schedule_for_cleanup(item)` — defer entity/file cleanup to session teardown. Always use this instead of inline deletion.
- Per-worker project fixtures (`project_model`, `project`) created during session setup
- `--reruns 3` for flaky retry, `-n 8 --dist loadscope` for parallelism
- OpenTelemetry tracing opt-in via `SYNAPSE_INTEGRATION_TEST_OTEL_ENABLED` env var

### No `@pytest.mark.asyncio` needed
`asyncio_mode = auto` in pytest.ini — all async test functions are auto-detected.

## Constraints

- Unit tests must never make network calls — `pytest-socket` will fail them. Mock all HTTP interactions.
- Integration test cleanup is mandatory — use `schedule_for_cleanup()` for every created resource to avoid orphaned Synapse entities.
