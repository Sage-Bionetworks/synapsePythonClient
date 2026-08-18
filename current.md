# SYNPY-1912 — upsert_rows misreports Table responses

https://sagebionetworks.jira.com/browse/SYNPY-1912

Reported by a user on 4.12. Still present on `develop` at 4.13.0.

## Status

| Part | State |
| --- | --- |
| 1. Model every `TableUpdateResponse` type and parse them | Done |
| 2. Keep the failure code and message | Done |
| 3. Accumulate row update results across query chunks | Done |
| 4. Only claim a failure when the server reported one | Done |
| Unit tests for the new dataclasses | Done |
| 5. Unit tests for the message block | Done |
| 6. Replace the raw `results` attribute and document the fields | Done |
| 7. Derive `entities_with_changes_applied` from the modelled responses | Done |
| 8. Make both aggregates properties and rename the row count | Done |
| 9. Model every `TableUpdateRequest` type on the request side | Done |
| 10. Move the failure walk onto `TableUpdateTransaction` | Done |
| 11. Extract the message block into `_log_upsert_summary` | Done |

All parts are in, with unit coverage. The message block now reports the count
Synapse confirmed and raises a failure clause only from a failure Synapse reported, so a
successful upsert no longer logs the false claim. It lives in `_log_upsert_summary` as of
part 11 and is covered directly. An integration run, which needs Synapse credentials, is the
only remaining work.

One breaking change ships with this: `TableUpdateTransaction.results` keeps its name but
now holds `TableUpdateResponse` dataclasses instead of the raw response dicts. Part 6 has
the detail.

All work is uncommitted on branch `SYNPY-1912`. The last commit is `3833ee8d`. Modified:
`core/constants/concrete_types.py`, `models/__init__.py`, `models/table_components.py`,
`models/mixins/table_components.py`, `models/table.py`,
`docs/reference/experimental/sync/table.md`,
`docs/reference/experimental/async/table.md`,
`tests/unit/synapseclient/mixins/unit_test_table_components.py`, and three integration
modules under `tests/integration/synapseclient/models/async/`: `test_table_async.py`,
`test_entityview_async.py`, and `test_submissionview_async.py`.

## Problem

Every successful `Table.upsert_rows()` call logs a false failure claim:

```
[syn76890550:demo-table]: Found 5 rows to update and 2 rows to insert. 5 rows could not be updated.
```

All 5 updates and both inserts are applied. The two halves of the message contradict
each other by construction.

### Root cause

`TableUpdateTransaction.fill_from_dict()` in `synapseclient/models/table_components.py`
(lines 383-394 originally) recognised only one response shape. It looked for an
`updateResults` key and collected `entityId` values that carry no `failureCode` or
`failureMessage`. That hand-rolled walk is gone as of part 7.

`updateResults` is a view-only concept. A Table upsert sends an
`AppendableRowSetRequest` for the update half and an `UploadToTableRequest` for the
insert half. The server answers with `RowReferenceSetResults` and
`UploadToTableResult` respectively. Neither carries `updateResults`, and neither
carries `entityId`, because table rows are not entities. So
`entities_with_changes_applied` stays `None` for every table upsert.

The message logic in `synapseclient/models/mixins/table_components.py` (lines
2369-2385, which were 2367-2383 before the part 3 change shifted them) then draws the
wrong conclusion:

- Line 2372 is false, so `total_row_count_actually_updated` stays 0.
- Line 2382 prints `total_row_count_actually_updated or total_row_count_to_update`.
  0 is falsy, so the correct count (5) is printed.
- Line 2378 compares 0 to 5, finds a shortfall, and line 2379 appends
  "5 rows could not be updated."

The failure count therefore always equals the full number of updated rows. It is
never a partial count.

### Secondary defects in the same block

1. `row_update_results` was assigned rather than accumulated inside the per-chunk loop
   (`mixins/table_components.py` line 2344). With `rows_per_query` defaulting to
   50000, any upsert over 50k rows discarded all but the last chunk's results, so the
   count was wrong even on views, where the parsing does work. Fixed by part 3.
2. `failureCode` and `failureMessage` were read and then discarded
   (`table_components.py` lines 847-848). A genuine failure gave the user a bare
   count and no diagnostic detail. Fixed by part 2; nothing logs the retained detail
   until part 4.

### Verified against production Synapse

The raw response the client received for the update half (syn76890550):

```json
{
  "concreteType": "org.sagebionetworks.repo.model.table.RowReferenceSetResults",
  "rowReferenceSet": {
    "tableId": "syn76890550",
    "etag": "5aac0c05-c0dc-4119-b284-4c394a6044aa",
    "rows": [
      {"rowId": 1, "versionNumber": 2},
      {"rowId": 2, "versionNumber": 2},
      {"rowId": 3, "versionNumber": 2},
      {"rowId": 4, "versionNumber": 2},
      {"rowId": 5, "versionNumber": 2}
    ]
  }
}
```

Five row references, all at `versionNumber` 2, no `failureCode` and no
`failureMessage` anywhere. The insert half returned `UploadToTableResult` with
`rowsProcessed` 2. A follow-up query confirmed all five rows held the new value and
both new rows were present, while the client had already logged "5 rows could not be
updated."

## Solution

Nothing about how the upsert stores data changes. Only how the client accounts for
what the server reported.

All parts are done.

### 1. Teach the parser the other response shapes — DONE

Rather than adding shape-sniffing branches inline, every response type is now modeled
as a dataclass, so the count comes off a typed attribute instead of a dict key.

`synapseclient/models/table_components.py`, all new code sitting between
`SnapshotRequest` and `TableUpdateTransaction`:

- `TableUpdateResponse` (line 516) — an abstract base class. It holds the
  `concrete_type` attribute, declares `fill_from_dict` as an abstract classmethod, and
  provides `rows_changed`, a property returning `None` by default.
- The five response types Synapse can return, each a subclass with its own
  `fill_from_dict` and its own `concrete_type` default:
  - `EntityUpdateResults` (line 555) — `update_results: list[EntityUpdateResult]`.
    `rows_changed` is the count of entities with no reported failure.
  - `RowReferenceSetResults` (line 611) — `row_reference_set: RowReferenceSet`.
    `rows_changed` is `len(row_reference_set.rows)`. This is the table update half.
  - `UploadToTableResult` (line 648) — `rows_processed`, `etag`. `rows_changed` is
    `rows_processed`. This is the table insert half.
  - `TableSchemaChangeResponse` (line 683) — `schema: list[Column]`. `rows_changed`
    stays `None`, since a schema change applies no rows.
  - `TableSearchChangeResponse` (line 712) — `search_enabled`. `rows_changed` stays
    `None`.
- `UnknownTableUpdateResponse` (line 736) — a sixth subclass holding the raw response
  in a `data: dict` attribute. Returned when a response cannot be identified, so that
  a response type added to Synapse after this release neither raises nor is
  miscounted. Its `rows_changed` is `None`.
- Supporting types: `RowReference` (line 409), `RowReferenceSet` (line 433),
  `EntityUpdateResult` (line 476) with a `succeeded` property, and the
  `EntityUpdateFailureCode` enum (line 385). An unrecognised failure code string
  coerces to `UNKNOWN` rather than raising.
- `table_update_response_from_dict()` (line 775) — dispatches on `concreteType`, falls
  back to identifying the response by a distinguishing key (`rowReferenceSet`,
  `rowsProcessed`, `updateResults`, `schema`, `searchEnabled`) when the concrete type
  is absent or unrecognised, and falls back to `UnknownTableUpdateResponse` after that.
- `TableUpdateTransaction.results` (line 835) — `list[TableUpdateResponse] | None`,
  populated in `fill_from_dict` (line 916) from the raw results array Synapse returned.
  This is the field parts 3 and 4 consume. It carried the provisional name
  `parsed_results` while parts 1 to 6 were written, then took over the `results` name in
  part 6.
- One aggregate on `TableUpdateTransaction`, a read-only property derived from `results`
  on each access, so a caller does not have to walk `results` itself. Part 10 added a
  second one, `failed_entity_updates`:
  - `total_rows_changed` (line 883) — `int | None`. The sum of `rows_changed` over every
    response that reports one, so table row updates, table inserts, and view entity
    updates all contribute. Responses carrying no row count contribute nothing. `None`
    before the transaction is sent, `0` when nothing changed. This is the count part 4
    should print. It was a field filled in `fill_from_dict` and named
    `table_rows_changed` until part 8.

  A third aggregate, `entities_with_changes_applied2`, was added here and then removed
  again. It duplicated `entities_with_changes_applied` while nothing read it, so it was
  dead weight. Per-entity successes are already reachable through
  `EntityUpdateResults.successful_entity_ids` on the objects in `results`, and through
  `entities_with_changes_applied` at the transaction level. Add the aggregate back only
  when a call site needs it. That test is what part 10 applied to the failure half, which
  a call site does read.

Deliberately left alone:

- `entities_with_changes_applied` (line 845) keeps its meaning. It is consumed at
  `mixins/table_components.py:2186-2193`, where each element is used as a dictionary
  key into `original_synids_and_etags_to_track` to collect etags for the view wait.
  Row IDs there would break view upserts silently. Part 7 changed how it is computed and
  part 8 turned it into a property, but neither changed what it holds.
- The plan's `rows_with_changes_applied` and `failed_changes` fields on
  `TableUpdateTransaction` are no longer needed. The equivalent information now lives
  on the response objects in `results`.

`synapseclient/core/constants/concrete_types.py` — added
`TABLE_SEARCH_CHANGE_RESPONSE` and `TABLE_SEARCH_CHANGE_REQUEST`, which were missing.

`synapseclient/models/__init__.py` — all new names exported and added to `__all__`.

### 2. Keep the failure detail — DONE

- `EntityUpdateResult` retains `entity_id`, `failure_code`, and `failure_message`
  instead of using them as a filter and discarding them. `EntityUpdateResults` exposes
  `successful_entity_ids` and `failed_entity_updates`, so part 4 can build the failure
  clause from real codes and messages.
- `RowReferenceSetResults` carries no per-row failure field, so for tables there is
  nothing to collect. A rejected table update fails the async job and raises instead.
  For tables the honest report is a confirmed count, never a silent partial.

### 3. Accumulate results across query chunks — DONE

`synapseclient/models/mixins/table_components.py`

- Line 2382 — `row_update_results = None` became
  `row_update_results: list[TableUpdateTransaction] = []`.
- Line 2422 — `row_update_results = await _push_row_updates_to_synapse(...)` became
  `row_update_results.extend(await _push_row_updates_to_synapse(...))`. `extend`, not
  `append`, because that function returns a list of transactions, one per size-based
  chunk it sends.
- Line 2460 passes `row_update_results` to `_wait_for_eventually_consistent_changes`,
  which iterates it at line 2186. An empty list behaves there as `None` did, so no
  change was needed at the call site. This also removes a latent `TypeError`: with
  `wait_for_eventually_consistent_view` on, a non-empty
  `original_synids_and_etags_to_track`, and a final chunk that pushed nothing, line 2186
  used to iterate `None`.
- The guard at line 2421 (`if not dry_run and rows_to_update`) is unchanged, so chunks
  with no updates contribute nothing to the accumulated list.
- `rows_to_update` is reset per query chunk at line 2436, so the accumulation cannot
  double count.

Verified: `pre-commit run --files synapseclient/models/mixins/table_components.py`
passes every hook, and the 207 unit tests in
`tests/unit/synapseclient/mixins/unit_test_table_components.py` and
`tests/unit/synapseclient/models/unit_test_table.py` pass. The integration test still
fails on its message assertion, as expected, since that depends on part 4.

### 4. Only claim a failure when the server reported one — DONE

`synapseclient/models/mixins/table_components.py`, which replaced the 17-line block that was
at 2371-2387. Part 11 moved this block out of `_upsert_rows_async` and into
`_log_upsert_summary`, so the line numbers below are the ones inside that helper.

The three defects there masked each other, which is why the message was
self-contradictory rather than simply wrong: the count came from the fallback and was
right, while the failure clause came from the broken count and was wrong. All three had
to change together, or the message would print 0.

- Lines 2252-2256 — `total_rows_updated`, named `total_row_count_actually_updated` until
  part 11, is now the sum of
  `result.total_rows_changed` over the accumulated transactions, skipping `None`, so
  row-based responses contribute. `entities_with_changes_applied` is no longer the source
  of the count. The per-response walk is already done inside the `total_rows_changed`
  property at line 883 of `models/table_components.py`, so no `rows_changed` iteration
  happens here. The
  `is not None` guard matters: `total_rows_changed` stays `None` on a transaction whose
  response carried no `results`, and `sum` over `None` raises.
- Lines 2261-2265 — `failed_row_updates` replaces the shortfall inference, which treated
  unparsed as failed. It flattens `failed_entity_updates` over the accumulated
  transactions. That list is always empty for a Table, which is correct rather than a gap:
  a rejected table update fails the async job and raises out of
  `send_job_and_wait_async` before this block runs. Part 4 walked each transaction's
  `results` here by hand; part 10 moved that walk onto the transaction, so this is now a
  flatten of one property.
- Lines 2267-2284 — the failure clause is built from the retained `entity_id`,
  `failure_code`, and `failure_message`, formatted as
  `. {n} rows could not be updated: syn123 (NOT_FOUND); syn124 (UNKNOWN: detail)`. A
  count alone was not actionable. An update with no failure message prints the code
  alone, and one with neither an ID nor a code reads `unknown row (UNKNOWN)`.
- Lines 2286-2293 — the `total_row_count_actually_updated or total_row_count_to_update`
  fallback is gone, replaced by `reported_row_count_to_update`, which branches on whether
  anything was pushed. Dropping the `or` exposed a case it was also covering: with
  `dry_run=True` the guard at line 2421 never fires, so the confirmed count is 0, but the
  user asked what would happen and the planned count is the only meaningful answer. The
  same holds for a live run where no existing row matched. This is a branch on intent, not
  a resurrection of the `or` — the difference is that the confirmed count is now printed
  even when it is 0 and a push did happen, which is exactly the case the `or` suppressed.
- The wording `Found {n} rows to update and {m} rows to insert` is byte-for-byte
  unchanged. The integration test at `test_table_async.py:1661` asserts on that substring
  and is the only test that does.
- Lines 2295-2307 — a shortfall with no reported failure logs at debug level. It is a
  client accounting gap, most likely an unmodelled response shape reaching
  `UnknownTableUpdateResponse`, not a user-facing failure. Promoting it to the info
  message would reintroduce the original defect for the next response type Synapse adds.
- The unused `EntityUpdateResult` import, singular, was removed from
  `mixins/table_components.py`. `EntityUpdateResults`, plural, was already imported there,
  so the plan's note about adding it was stale. Part 10 removed the plural one as well,
  so the mixin now imports neither.

No double counting from the insert half. `total_rows_changed` does sum a
`RowReferenceSetResults` and an `UploadToTableResult` in the same transaction, but
`_push_row_updates_to_synapse` sends one `AppendableRowSetRequest` per transaction and
nothing else, so a transaction in `row_update_results` never carries an insert response.
The insert goes through `store_rows_async` further down.

Verified: `pre-commit run --files synapseclient/models/mixins/table_components.py` passes
every hook, and the same 207 unit tests still pass. The aggregates were checked by
hand against the recorded production response for the table case, giving 5, and against a
synthetic `EntityUpdateResults` with one success and two failures, giving a count of 1 and
both failures with their codes, including the coercion of an unrecognised code to
`UNKNOWN`.

Out of scope: the insert count stays `len(rows_to_insert_df)`, planned rather than
confirmed. `store_rows_async` runs after this log and returns nothing to the caller.
Making that count confirmed means moving the log below the insert and threading a result
back out of `store_rows_async`, which changes the order of output users see. The false
claim lived in the update half, which this fixes.

### 5. Unit tests — DONE

`tests/unit/synapseclient/mixins/unit_test_table_components.py`, four new classes
appended, 65 tests. Two module-level helpers, `_row_reference_set_results()` and
`_entity_update_results()`, build the payloads. The first is the response recorded from
production above.

- `TestTableUpdateResponseFromDict` — dispatch. One case per known concrete type, one
  per distinguishing-key fallback, an unrecognised concrete type that still carries a
  known key, an unidentifiable response, an empty dict, and that
  `TableUpdateResponse()` cannot be instantiated.
- `TestTableUpdateResponseRowsChanged` — a 12-case parametrized table over
  `rows_changed`, plus the non-count fields of each subclass. The cases pin `0` against
  `None`: `rows: []` and `rowsProcessed: 0` give `0`, while an absent
  `rowReferenceSet` and an absent `rowsProcessed` give `None`.
- `TestEntityUpdateResult` — `succeeded` across all four code/message combinations,
  coercion of every documented failure code and of an unrecognised one, retention of the
  message, the split into `successful_entity_ids` and `failed_entity_updates`, a success
  reported with no `entityId`, and `update_results` of `None`.
- `TestTableUpdateTransactionFillFromDict` — `total_rows_changed` for a table response and
  for a view response, the sum across three response types in one transaction, an
  unmodelled response contributing nothing, that `entities_with_changes_applied` keeps
  its original meaning including staying `None` when nothing succeeded, that
  `total_rows_changed` stays `None` rather than `0` when no result was returned, and that
  `snapshot_version_number` is filled alongside the modelled responses. That last test
  began as a check that the raw dicts were still reachable, became a
  `pytest.deprecated_call()` read of the deprecated property in part 6, and is now
  `test_snapshot_version_number_is_filled`, since the raw dicts are no longer kept.
- `TestUpsertRowsResultReporting` — 10 tests that call `_upsert_rows_async` directly with
  a minimal `TableForTest` and `ViewForTest` entity, patching
  `_push_row_updates_to_synapse` and asserting on `client.logger`. This is the first
  credential-free coverage of the reported defect. It covers the byte-for-byte success
  message with no failure clause, accumulation across three query chunks, `dry_run`
  reporting the planned count, a confirmed count of `0` being printed as `0` with the
  gap logged at debug level, and a 5-case parametrized table over the failure-clause
  format.

Verified: all 65 pass, the 3177 unit tests in `tests/unit` still pass, and pre-commit
passes every hook. The accumulation test was checked against a deliberately reverted
`extend`, where it fails with a count of 2 instead of 6.

One fix came out of writing these. `TableUpdateTransaction.fill_from_dict` guarded the
old loop with `if "results" in synapse_response`, so a response carrying
`"results": null` raised `TypeError: 'NoneType' object is not iterable`. Changed to
`if synapse_response.get("results", None)`, which matches the guard the part 1 block
already used. Pre-existing defect, not introduced by this branch. For `results: []` the
outcome is unchanged, since `successful_entities` stayed empty and the field stayed
`None`.

Also found while writing the failure-format cases: the `unknown row (UNKNOWN)` string in
part 4 is unreachable. `failed_entity_updates` only yields an update that reported a code
or a message, so an update with no code always has a message, which makes the reason read
`UNKNOWN: {message}`. The reachable variant, `unknown row (UNKNOWN: something broke)`, is
what the test asserts. Not worth changing the code for; noted so nobody hunts for it.

### 6. Replace the raw results attribute and document the fields — DONE

The modelled responses made the raw dicts redundant. Nothing inside the client read the raw
array: it was written in `fill_from_dict` and read only by tests.

This landed in two passes. The first kept the raw dicts behind a `_raw_results` field and a
`@deprecated` read-only `results` property, with the modelled list under the provisional
name `parsed_results`. That deprecation path was then dropped: `parsed_results` was renamed
to `results` and `_raw_results`, the property, and the `deprecated` import were all removed.
So `results` keeps its name but changes type, from `list[dict]` to
`list[TableUpdateResponse]`, with no deprecation period. That is the one breaking change on
this branch and it needs a release-note line.

`synapseclient/models/table_components.py`

- `results` (line 835) — `list[TableUpdateResponse] | None`, the modelled responses. The
  raw dicts are no longer retained anywhere on the transaction. A caller that needs the
  server payload verbatim no longer has it; the modelled subclasses expose every field it
  carried, including the unrecognised case through `UnknownTableUpdateResponse.data`.
- `fill_from_dict` (line 916) no longer assigns a raw-results attribute. The stale
  `self._raw_results = ...` line survived the rename for a moment and would have created a
  stray attribute on every filled transaction, since the field was already gone.
- `from deprecated import deprecated` was removed from the imports, as nothing in the module
  uses it now.
- Docstrings added to the three fields that had none: `snapshot_options` (line 831),
  `snapshot_version_number` (line 840), and `entities_with_changes_applied` (line 845). The
  orphaned docstring that sat after `entities_with_changes_applied`, separated by a blank
  line so it documented nothing, was the leftover text for the old raw `results` field and
  has been removed.

Tests

- `tests/unit/synapseclient/mixins/unit_test_table_components.py` — every
  `transaction.parsed_results` assertion reads `transaction.results`.
  `test_raw_results_are_still_available` was replaced by
  `test_snapshot_version_number_is_filled`, which keeps the `snapshotVersionNumber`
  coverage and asserts the modelled response types instead of the raw dicts.
- `tests/integration/.../test_entityview_async.py:606` and
  `test_submissionview_async.py:470` — the snapshot assertions read `snapshot.results is
  not None`, which is where they started, but now against the modelled list.

Verified: 257 tests in the table components unit module and 3177 tests in `tests/unit`
pass, and pre-commit passes every hook. Three unit modules error on collection in this
environment, `unit_test_cred_provider.py`, `unit_test_remote_storage_file_wrappers.py`, and
`unit_test_sts_transfer.py`, because `boto3` and `pysftp` are absent. That is unrelated to
this branch. The count moved from 3178 to 3177 because the deprecation test was replaced
one-for-one and the earlier run counted a subtest separately.

### 7. Derive entities_with_changes_applied from the modelled responses — DONE

`fill_from_dict` walked the raw results array a second time to fill
`entities_with_changes_applied`, re-implementing by hand the failure check that
`EntityUpdateResult.succeeded` and `EntityUpdateResults.successful_entity_ids` already
perform. Once the raw array stopped being retained, that second walk had no source to read
from other than the response Synapse sent, so it was replaced by a walk over `results`.
Part 8 then moved that walk out of `fill_from_dict` and into the property body, which is
where it lives now (lines 845-862):

```python
successful_entities = [
    entity_id
    for response in self.results
    if isinstance(response, EntityUpdateResults)
    for entity_id in response.successful_entity_ids
]
return successful_entities or None
```

- Behaviour is unchanged. `successful_entity_ids` keeps only entries that have an
  `entity_id` and report neither a failure code nor a failure message, which is exactly
  what the hand-rolled loop tested.
- The `isinstance(response, EntityUpdateResults)` guard replaces the `"updateResults" in
  result` key check. `EntityUpdateResults` is the only subclass that carries per-entity
  results, and the dispatch in `table_update_response_from_dict` already keys off that
  same `updateResults` key when the concrete type is missing.
- The field is still left as `None` rather than set to `[]` when nothing succeeded, because
  the call site distinguishes the two. See part 1.
- The block was folded into the existing `if synapse_response.get("results", None)` guard,
  so the raw response is walked once instead of twice.

Verified: the 257 tests in the table components unit module pass unchanged, which is the
point — `TestTableUpdateTransactionFillFromDict` already pinned the original meaning of
this field, including the success with no `entityId` and the all-failures case.

### 8. Make both aggregates properties and rename the row count — DONE

`entities_with_changes_applied` and `total_rows_changed` were dataclass fields that
`fill_from_dict` computed and stored. Both are pure derivations of `results`, so the
transaction carried three copies of the same information, the two aggregates went stale
without warning if a caller replaced `results`, and both appeared in `__init__` as
arguments no caller could meaningfully supply.

Both are now read-only `@property` methods on `TableUpdateTransaction`, computed from
`results` on each access, and `fill_from_dict` assigns only `snapshot_version_number` and
`results`.

- The `None` versus `0` and `None` versus `[]` distinctions are preserved, because both are
  load-bearing at the call sites. `total_rows_changed` returns `None` when `results` is
  `None` and `0` when results exist but no change reports a row count, which is what the
  `is not None` guard at `mixins/table_components.py:2255` reads.
  `entities_with_changes_applied` returns `None` rather than `[]` when no
  `EntityUpdateResults` contributed an ID, which is what the truthiness check at
  `mixins/table_components.py:2187` reads.
- A plain `@property` fails if anything assigns to the attribute. Removing the two
  assignments in `fill_from_dict` was enough; nothing outside `models/table_components.py`
  ever assigned either one.
- Both values now drop out of `__eq__` and `dataclasses.asdict()` for this dataclass, since
  they are no longer fields. Nothing in the codebase compares two transactions or serialises
  one, so this is a deliberate consequence rather than a regression.
- `table_rows_changed` was renamed to `total_rows_changed` at the same time. The `table_`
  prefix was inaccurate, because the sum includes `EntityUpdateResults.rows_changed`, which
  counts the entities behind a view rather than the rows of a table, and the docstring had
  to walk the name back in its second sentence. The prefix was also redundant on a
  `TableUpdateTransaction`. `total_` instead marks the relationship the old name hid: this
  is the sum over all changes in the transaction, while each `TableUpdateResponse` exposes
  its own `rows_changed`. Renamed at eight call sites: the property, the two reads in
  `mixins/table_components.py`, and six assertions plus one docstring in the unit module.
  The property is new on this branch, so no released name changed.

Verified: 1485 tests across the table components unit module and `tests/unit/.../models/`
pass, and 3177 in `tests/unit` before the rename, with the same three `boto3` and `pysftp`
collection errors as under part 6. No test needed changing beyond the rename, because every
existing assertion reads these attributes rather than setting them.

### 9. Model every TableUpdateRequest type on the request side — DONE

Parts 1 to 8 completed the response side. The request side was still short one of the four
changes Synapse accepts inside a transaction, and the three that existed shared no base
class, so `TableUpdateTransaction.changes` had to spell them out as a `Union` that was
copied into five signatures. The gap was visible from the response side: part 1 modelled
`TableSearchChangeResponse`, and part 1 added the `TABLE_SEARCH_CHANGE_REQUEST` concrete
type, but no request class ever produced that response.

`synapseclient/models/table_components.py`

- `TableUpdateRequest` (line 198) — a new abstract base for a single change within a
  transaction. It declares the `concrete_type` and `entity_id` contract that the REST
  interface defines for every change, plus `to_synapse_request` as an abstract method. It is
  a plain `ABC`, deliberately **not** a dataclass: a dataclass base contributes its fields
  ahead of the subclass fields, which would have reordered the positional arguments of
  `AppendableRowSetRequest`, `UploadToTableRequest`, and `TableSchemaChangeRequest` and
  broken every existing caller. `dataclasses` ignores annotations on a non-dataclass base,
  so field order is untouched. Verified by hand against
  `dataclasses.fields()` for all four subclasses.
  Modeled from
  <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/TableUpdateRequest.html>.
- `AppendableRowSetRequest` (line 226), `UploadToTableRequest` (line 247), and
  `TableSchemaChangeRequest` (line 308) now inherit it. No field or payload changed.
- `TableSearchChangeRequest` (line 331) — the missing fourth change. Carries `entity_id`
  and `search_enabled`, and uses the `TABLE_SEARCH_CHANGE_REQUEST` concrete type that part 1
  had already added. Nothing in the client sends one yet: search is set through the
  `is_search_enabled` field on the entity itself, on `Table`, `Dataset`, `EntityView`,
  `MaterializedView`, and `VirtualTable`. This makes it reachable for a caller that needs
  the search change to land in the same transaction as a schema or row change, which the
  entity field cannot do.
- `UploadToTableRequest.entity_id` (line 261) — a read-only property returning `table_id`.
  The REST model documents both `entityId` and `tableId` on that request, and this one class
  named it `table_id` while the other three named it `entity_id`. The property gives every
  change one way to report the entity it applies to, which is what the base class promises.
  The payload is unchanged; `tableId` is still what is sent.
- `TableUpdateTransaction.changes` (line 828) — `list[TableUpdateRequest] | None`, replacing
  the three-way `Union`. Docstring added naming the four subclasses.
- Docstrings added to the three `TableUpdateTransaction` fields that still had none:
  `entity_id`, `concrete_type`, and `create_snapshot`. The `create_snapshot` text points at
  `snapshot_options` for labelling the version and at `snapshot_version_number` for the
  number Synapse assigns, since the three are only useful together.

`synapseclient/models/mixins/table_components.py` — all five copies of the
`List[Union[...]]` annotation now read `List["TableUpdateRequest"]`: `store_rows_async`,
`_send_update`, `_upload_df_chunk`, `_chunk_and_upload_csv`, and `_chunk_and_upload_df`. The
public `store_rows_async` docstring for `additional_changes` names all four accepted types;
the four private helpers say "Each change is a TableUpdateRequest."

`synapseclient/models/table.py` — `TableSynchronousProtocol.store_rows` mirrors the async
signature, so it took the same annotation and the same docstring change. Its three
now-unreferenced request imports were replaced by `TableUpdateRequest`. A caller importing
`UploadToTableRequest` from `synapseclient.models.table` rather than from
`synapseclient.models` loses that path; both classes are still exported from
`synapseclient.models`.

`synapseclient/models/__init__.py` — `TableUpdateRequest` and `TableSearchChangeRequest`
exported and added to `__all__`, and the request names grouped under a comment to match the
response block.

`docs/reference/experimental/sync/table.md` and `.../async/table.md` — entries added for
both new classes, alongside the three request classes that were already documented. The
response classes from parts 1 to 8 are still undocumented in those files.

Tests

`tests/unit/synapseclient/mixins/unit_test_table_components.py` — new
`TestTableUpdateRequest` class, 8 tests:

- a parametrized check that all four request classes are a `TableUpdateRequest`, which is
  the guard against a fifth type being added without the base;
- that `TableUpdateRequest()` cannot be instantiated;
- the search change payload, and separately that `search_enabled=False` is sent as `False`
  rather than dropped, since a request that only ever turns search on would be useless;
- that `UploadToTableRequest.entity_id` reports `table_id`;
- that one transaction carries all four changes and sends them in the order given, each
  converted by the class that models it.

Verified: 266 tests in the table components unit module and 3186 in `tests/unit` pass, and
pre-commit passes every hook on the four changed Python files. The same three `boto3` and
`pysftp` collection errors as under part 6 remain, unrelated to this branch.

### 10. Move the failure walk onto TableUpdateTransaction — DONE

Part 4 built `failed_row_updates` in `mixins/table_components.py` by walking one
transaction's `results`, guarding the `None` case, filtering on
`isinstance(response, EntityUpdateResults)`, and flattening `failed_entity_updates`. Strip
the outer loop over transactions and that is the same body as the
`entities_with_changes_applied` property. So the transaction exposed the success half of
the per-entity outcome as a property and left the caller to hand-roll the failure half,
which is the asymmetry part 7 removed on the success side.

`synapseclient/models/table_components.py`

- `TableUpdateTransaction.failed_entity_updates` (line 864) — a new read-only
  `@property` returning `list[EntityUpdateResult]`, the failures of every
  `EntityUpdateResults` in `results`, in the order Synapse returned the responses. It sits
  between the two aggregates part 8 turned into properties, so the transaction now derives
  three values from `results` and stores none of them.
- It returns `[]` rather than `None` when `results` is `None`. The other two aggregates
  return `None` there because their call sites tell an absent value from an empty one.
  Nothing distinguishes the two for this list: a transaction that reported nothing reported
  no failure, and the call site tests it for truthiness only.

`synapseclient/models/mixins/table_components.py`

- The six-line walk that part 4 wrote is now the flatten at lines 2261-2265, over
  `result.failed_entity_updates`. The `result.results or []` guard went with it, which also
  removes an inconsistency: that guard treated a `None` `results` differently from the
  `if self.results is None` guard the properties use, though both reached the same outcome.
- The `EntityUpdateResults` import is now unused there and has been removed. The comment
  explaining that only a view reports a per-row outcome moved onto the property docstring,
  which is where a caller reads it.

Tests

`tests/unit/synapseclient/mixins/unit_test_table_components.py` — three new tests in
`TestTableUpdateTransactionFillFromDict` plus two assertions added to existing tests:

- the failure detail of a view response, asserted as `entity_id`, `failure_code`, and
  `failure_message` per failure, so the retention from part 2 is pinned at the transaction
  level;
- failures flattened across three responses, with a `RowReferenceSetResults` in the middle
  contributing nothing;
- a table response reporting no failure;
- the all-failures case reports both, alongside the existing `None` and `0` assertions;
- the nothing-returned case reports `[]`, which pins the `None` versus `[]` decision above.

The 10 tests of `TestUpsertRowsResultReporting` cover the call site and needed no change,
which is the point: the message is byte-for-byte the same.

Verified: 268 tests in the table components unit module and 1496 across that module and
`tests/unit/.../models/` pass, and pre-commit passes every hook on the three changed files.

### 11. Extract the message block into _log_upsert_summary — DONE

The reporting block parts 4 and 10 left at the end of `_upsert_rows_async` was 58 lines of
counting and string building inside a function that already ran from the query through the
update to the insert. It read nothing but its four inputs, wrote nothing but log lines, and
none of the locals it created were read after it, so it came out whole.

`synapseclient/models/mixins/table_components.py`

- `_log_upsert_summary` (line 2233) — a new module-level private function taking `entity`,
  `row_update_results`, `total_row_count_to_update`, `row_count_to_insert`, and `client`,
  returning `None`. It sits directly above `_upsert_rows_async`, matching the placement of
  the other private helpers that function calls. The logic is byte-for-byte the same
  message; only the surrounding function changed.
- The call site (line 2447) is now a five-argument call between the insert-candidate
  selection at line 2441 and the eventually-consistent-view wait. `_upsert_rows_async` reads
  as query, update, report, insert.
- `len(rows_to_insert_df)` is evaluated at the call site and passed as `row_count_to_insert`,
  rather than passing the DataFrame. The helper therefore needs nothing from pandas, which
  keeps it clear of the lazy-import rule for optional dependencies.
- The local formerly named `total_row_count_actually_updated` is now `total_rows_updated`,
  and the signature uses built-in generics and a PEP 604 union, matching the style the rest
  of the `TableUpdateTransaction` work on this branch uses.

Tests

`tests/unit/synapseclient/mixins/unit_test_table_components.py` — new
`TestLogUpsertSummary` class at line 2537, placed before `TestQuery` so it sits with the
other upsert suites, 11 tests. Two static helpers build real results rather than mocks:
`_table_transaction()` wraps a `RowReferenceSetResults` and `_view_transaction()` wraps an
`EntityUpdateResults`, so the tests exercise the actual `total_rows_changed` and
`failed_entity_updates` properties from parts 8 and 10 instead of stubbed attributes.

- no results, the dry-run path: the client-side count is reported and no gap is logged;
- results from two transactions: the confirmed counts are summed and reported;
- a 2-case parametrized table over results carrying no row count, an unsent transaction and
  a schema-change-only response, both contributing 0;
- a 4-case parametrized table over the failure-clause format: code alone, code with message,
  message with no code reading `UNKNOWN: ...`, and a failure with no `entity_id` reading
  `unknown row`;
- failures flattened across two transactions, with the successful update counted alone and
  the debug gap suppressed because the failures explain the shortfall;
- a shortfall with no failure logging the debug gap, asserted on both counts and on the
  "not a failed update" wording;
- more rows confirmed than sent logging no gap, which pins the `<` boundary.

This is the first direct coverage of the failure-clause formatting. Before the extraction it
was reachable only by driving a whole upsert, and only for a view.
`TestUpsertRowsResultReporting` from part 5 still covers the call site through
`_upsert_rows_async` and needed no change.

Verified: 279 tests in the table components unit module pass, 11 of them new. Pre-commit
passes every hook, after black reformatted two wrapped lines in the extracted helper.

## Test coverage added

`tests/integration/synapseclient/models/async/test_table_async.py`

- `capture_client_logs()` — module-level helper that attaches a handler directly to
  `syn.logger`. Needed because the integration `syn` fixture uses
  `SILENT_LOGGER_NAME`, which has `propagate: False` in `core/logging_setup.py:97`,
  so `caplog` sees nothing.
- `TestUpsertRows.test_upsert_reports_accurate_row_counts` — stores 5 rows, upserts
  those 5 keys with new values plus 2 new keys, asserts the table holds all 7 correct
  rows, then asserts the logged message contains
  `Found 5 rows to update and 2 rows to insert` and does not contain
  `could not be updated`.
- Parametrized on `rows_per_query`: `50000` (`single_query_chunk`) and `2`
  (`multiple_query_chunks`). Parts 1 and 4 fix the first case. Part 3 is what fixes
  the second, which forces four query chunks.

Both parametrizations fail on current `develop`, on the message assertion, after the
data assertions have already passed.

Not re-run since the part 1 and 2 work, as the integration tests need Synapse
credentials. Both parametrizations are now expected to pass, since parts 3 and 4 are in,
but that is unverified until the tests are run against Synapse.

Unit coverage is described under parts 5, 9, 10, and 11 above.

## Acceptance criteria

- [x] A successful `Table.upsert_rows()` call logs no "rows could not be updated"
      clause. Covered by unit test; integration run still pending.
- [x] The reported update and insert counts match what was applied, for Tables and
      Views. Covered by unit test for both entity kinds.
- [x] An upsert of more than `rows_per_query` rows reports correct totals across all
      chunks. Covered by unit test over three query chunks.
- [x] When the server does report a failure, the log includes the `failureCode` and
      `failureMessage` for the affected rows. Covered by unit test. Only a View can
      produce this case, so an integration run cannot cover it for a Table.
- [x] Unit tests cover all six response shapes: `RowReferenceSetResults`,
      `UploadToTableResult`, and `EntityUpdateResults` in success and failure form, plus
      `TableSchemaChangeResponse`, `TableSearchChangeResponse`, and
      `UnknownTableUpdateResponse`.

## Notes

- The ticket summary has a typo: "repsonses".
- New code in this branch uses built-in generics and PEP 604 unions (`dict`, `list`,
  `X | None`) rather than `Dict`, `List`, and `Optional`. The whole
  `TableUpdateTransaction` block has since been converted to that style, but the rest of
  `table_components.py` is still on the old style, so the file mixes both.
- The modelled response list went through three names: `results2`, then `parsed_results`,
  then `results`. It ended as an in-place replacement of the raw dict array rather than a
  second field, so `TableUpdateTransaction.results` changes type without a deprecation
  period. See parts 6 and 7.
- `entities_with_changes_applied2` was dropped rather than renamed. It only ever carried a
  provisional name because `entities_with_changes_applied` is taken and must keep its
  current behaviour, for the reason given under part 1, and no call site read the new
  field. That removes the naming question from the PR. The row count aggregate was named
  `table_rows_changed` at first and renamed to `total_rows_changed` in part 8.
- The standalone reproduction script that lived at the repo root has been removed. The
  integration test now covers the same case.
- Part 9 widens the branch past the reported defect. The request-side work fixes nothing on
  its own; it closes the gap that parts 1 to 8 exposed, in that the client modelled a
  response type it could never ask for. Split it out of this PR if the review prefers the
  fix alone.
- `TableUpdateRequest` is an `ABC` and not a dataclass on purpose. See part 9 before
  converting it, or the positional arguments of the three existing request classes move.
- `TableUpdateTransaction` now derives every aggregate from `results` on each access:
  `entities_with_changes_applied`, `failed_entity_updates`, and `total_rows_changed`. Read
  one of them rather than walking `results` and testing `isinstance` at a call site. That
  walk was hand-rolled three times on this branch before parts 7 and 10 removed it.
- Three of the four cleanups considered alongside part 10 were rejected. The two aggregations
  across a list of transactions stay in the mixin, because they span transactions and so are
  not transaction methods. Part 11 moved them from `_upsert_rows_async` into
  `_log_upsert_summary`, which is still in the mixin. The per-failure text formatting could
  become a `failure_description` property on `EntityUpdateResult`, which is presentation
  logic in a model and therefore a judgment call. The duplicated
  `AppendableRowSetRequest` block in `_push_row_updates_to_synapse` could become a
  construction-only classmethod, but the chunking, progress bar, and job timeout around it
  do not belong on a dataclass.
- The line numbers cited in this file were refreshed after part 11. Those in the Problem
  section describe the original code on `develop` and are deliberately not updated.
