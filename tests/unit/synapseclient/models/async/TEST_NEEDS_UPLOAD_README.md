# Unit Tests for `_needs_upload` Cache Bugs

## Overview

This test suite verifies three cache-related bugs that prevent files from being uploaded to Synapse:

1. **Possibility 1**: Timestamp-only cache check (Bug #1)
2. **Possibility 2**: MD5 false positive (Edge case)
3. **Possibility 3**: Premature cache population + upload failure (Bug #2)

## Test File

`unit_test_needs_upload.py`

## Running the Tests

### Run all tests:
```bash
pytest tests/unit/synapseclient/models/async/unit_test_needs_upload.py -v
```

### Run specific test class:
```bash
pytest tests/unit/synapseclient/models/async/unit_test_needs_upload.py::TestNeedsUpload -v
```

### Run specific test:
```bash
pytest tests/unit/synapseclient/models/async/unit_test_needs_upload.py::TestNeedsUpload::test_possibility_1_timestamp_unchanged_cache_hit_skips_upload -v
```

### Run with output:
```bash
pytest tests/unit/synapseclient/models/async/unit_test_needs_upload.py -v -s
```

## Test Structure

### Possibility 1: Timestamp-Only Cache Check

**Tests:**
- `test_possibility_1_timestamp_unchanged_cache_hit_skips_upload`
- `test_possibility_1_rapid_edits_within_same_second`

**Scenario:**
1. File is in cache with timestamp T1
2. User modifies file content (MD5 changes)
3. Timestamp remains T1 (via `cp -p`, `touch`, or rapid edits)
4. `cache.contains()` only checks timestamp → returns True (cache hit)
5. Upload is **incorrectly skipped** ❌

**Expected After Fix:**
- `cache.contains()` should check BOTH timestamp AND MD5
- Should return False (cache miss) when MD5 differs
- Upload should proceed ✅

### Possibility 2: MD5 False Positive

**Tests:**
- `test_possibility_2_md5_comparison_edge_case`

**Scenario:**
1. File content changes
2. MD5 calculation returns stale/cached value
3. MD5 comparison incorrectly thinks files are same
4. Upload is skipped ❌

**Note:** This is unlikely but included for completeness. Most likely cause would be a bug in MD5 caching logic.

### Possibility 3: Premature Cache Population

**Tests:**
- `test_possibility_3_premature_cache_add_then_cache_hit`
- `test_possibility_3_same_path_upload_failure_scenario`

**Scenario:**
1. **First upload attempt:**
   - Cache miss (timestamp changed)
   - MD5s don't match → `needs_upload = True`
   - **BUG**: `cache.add()` called BEFORE upload succeeds
   - Upload fails (409 error, network issue, etc.)
2. **Second upload attempt:**
   - Cache hit (file was prematurely added to cache)
   - Upload is **incorrectly skipped** ❌
3. **Result:**
   - Cache has new MD5 ✅
   - Synapse has old MD5 ❌
   - Cache and Synapse are **desynchronized**

**Expected After Fix:**
- `cache.add()` should only be called when MD5s match
- Should be moved inside the `if md5_stored_in_synapse == local_file_md5_hex:` block
- Upload should proceed when MD5s differ ✅

## Correct Behavior Tests

These tests verify expected correct behavior:

- `test_correct_behavior_timestamp_changed_triggers_md5_check`
- `test_correct_behavior_no_cache_entry_triggers_upload`
- `test_correct_behavior_md5_match_skips_upload_and_repairs_cache`
- `test_new_file_needs_upload`
- `test_file_with_data_file_handle_id_skips_upload`

## Integration Tests with Mocks

**Tests:**
- `test_cache_contains_only_checks_timestamp_not_md5`
- `test_cache_add_called_before_md5_match_check`

These tests use mocks to specifically verify:
1. `cache.contains()` behavior (timestamp-only check)
2. `cache.add()` being called prematurely

## Expected Test Results

### Before Fixes Applied:

Some tests are **expected to fail** or document bugs:

```
test_possibility_1_timestamp_unchanged_cache_hit_skips_upload - PASS (documents bug)
test_possibility_1_rapid_edits_within_same_second - PASS (documents bug)
test_possibility_3_premature_cache_add_then_cache_hit - PASS (documents bug)
test_possibility_3_same_path_upload_failure_scenario - PASS (documents bug)
test_cache_contains_only_checks_timestamp_not_md5 - PASS (documents bug)
test_cache_add_called_before_md5_match_check - PASS (documents bug)
```

These tests contain assertions like:
```python
assert needs_upload is False, "BUG: Upload skipped due to... After fix, this should be True!"
```

### After Fixes Applied:

These test assertions should be **updated** to expect correct behavior:

```python
# Before fix:
assert needs_upload is False, "BUG: Documents the bug"

# After fix:
assert needs_upload is True, "Upload correctly triggered when file changed"
```

## Fixes Required

### Fix #1: Make `cache.contains()` check MD5

**File**: `synapseclient/core/cache.py`
**Method**: `Cache.contains()`

**Change:**
```python
def contains(self, file_handle_id, path: str) -> bool:
    # ... setup code ...
    cache_map_entry = cache_map.get(path, None)
    if cache_map_entry:
        # Use existing method that checks BOTH timestamp AND MD5
        return self._cache_item_unmodified(cache_map_entry, path)
    return False
```

### Fix #2: Move `cache.add()` inside MD5 match condition

**File**: `synapseclient/models/file.py`
**Function**: `_needs_upload()`
**Lines**: 1369-1382

**Change:**
```python
if md5_stored_in_synapse == local_file_md5_hex:
    needs_upload = False

    # Only add to cache when we know file is already uploaded
    if (
        not exists_in_cache
        and entity_to_upload.file_handle
        and entity_to_upload.file_handle.id
        and local_file_md5_hex
    ):
        syn.cache.add(
            file_handle_id=entity_to_upload.file_handle.id,
            path=entity_to_upload.path,
            md5=local_file_md5_hex,
        )
```

## Updating Tests After Fixes

After applying the fixes, update the test assertions:

1. Find all tests with "BUG:" in assertion messages
2. Change expected values to correct behavior
3. Update assertion messages to remove "BUG:" prefix
4. Verify all tests pass

Example:
```python
# Before fix:
assert needs_upload is False, "BUG: Upload skipped..."

# After fix:
assert needs_upload is True, "Upload correctly triggered when content changed"
```

## Additional Verification

After fixes are applied, run:

1. **Unit tests**: Verify logic changes
2. **Integration tests**: Verify end-to-end behavior
3. **Manual testing**: Test the user's exact scenario:
   - Modify files in same folder
   - Upload with network interruption (simulate 409 error)
   - Retry upload
   - Verify files upload correctly

## User Scenarios to Test

1. **Timestamp preservation**: `cp -p oldfile newfile`
2. **Rapid edits**: Modify file multiple times within 1 second
3. **Upload failure**: Simulate 409 error during upload
4. **Retry after failure**: Verify upload proceeds correctly
5. **Cache clearing**: Verify `syn.cache.purge()` resolves issues

## References

- Issue: User files not uploading despite changes
- Root cause: Two cache bugs working together
- Files:
  - `synapseclient/core/cache.py` (Bug #1)
  - `synapseclient/models/file.py` (Bug #2)
