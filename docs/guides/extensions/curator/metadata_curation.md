# How to Curate Metadata with the Synapse Python Client

This guide walks you through programmatic metadata curation in Synapse, from setting up curation tasks to validating data and managing grid sessions.

## What you'll learn

- How to find schemas and create curation tasks
- How to manage grid sessions: import CSV data, download data, and synchronize changes
- How to check validation results **before** committing (pre-commit validation via WebSocket)
- How to check validation results **after** committing (export-based validation)
- How to manage curation task lifecycle (list, update, delete with cleanup)

## Prerequisites

- Python environment with `pip install --upgrade "synapseclient[curator]"`
- A Synapse account with project creation permissions
- A JSON Schema registered in Synapse (see [JSON Schema tutorial](../../../tutorials/python/json_schema.md) or [Schema Operations guide](schema_operations.md))

---

## 1. Authentication and setup

```python
{!docs/guides/extensions/curator/scripts/setup_and_create_tasks.py!lines=7-15}
```

## 2. Find a schema for your data

The schema registry contains validated JSON schemas organized by data coordination center (DCC) and data type.

```python
{!docs/guides/extensions/curator/scripts/setup_and_create_tasks.py!lines=17-24}
```

To browse all available versions of a schema:

```python
{!docs/guides/extensions/curator/scripts/setup_and_create_tasks.py!lines=26-31}
```

## 3. Create a curation task

A curation task guides collaborators through metadata entry. There are two types:

### Record-based metadata (structured records in a RecordSet)

Use this when metadata is stored as tabular records, like a spreadsheet of sample annotations.

```python
{!docs/guides/extensions/curator/scripts/setup_and_create_tasks.py!lines=33-51}
```

This creates a RecordSet, a CurationTask, and an initial Grid session for collaborative editing.

### File-based metadata (annotations on individual files)

Use this when metadata describes individual files in a folder.

```python
{!docs/guides/extensions/curator/scripts/setup_and_create_tasks.py!lines=53-63}
```

---

## 4. Work with Grid sessions

Grid sessions are the core editing interface for curation. You can create them, import CSV data, download data, check validation, and synchronize changes.

### Create a Grid session

```python
{!docs/guides/extensions/curator/scripts/grid_session_operations.py!lines=7-20}
```

### Import CSV data into a Grid

Upload CSV data into an active grid session. You can provide a local file path, a pandas DataFrame, or an existing file handle ID. The CSV must match the grid's column schema.

```python
{!docs/guides/extensions/curator/scripts/grid_session_operations.py!lines=22-53}
```

### Download Grid data as CSV

Export the current grid state to a local CSV file. The downloaded CSV does **not** include validation columns.

```python
{!docs/guides/extensions/curator/scripts/grid_session_operations.py!lines=55-57}
```

### Synchronize Grid with data source

Apply grid session changes back to the source entity (table, view, or RecordSet).

```python
{!docs/guides/extensions/curator/scripts/grid_session_operations.py!lines=59-67}
```

### List and delete Grid sessions

```python
{!docs/guides/extensions/curator/scripts/grid_session_operations.py!lines=69-78}
```

---

## 5. Check validation results

There are two ways to check whether metadata passes schema validation:

### Option A: Pre-commit validation (WebSocket snapshot)

Get per-row validation results from an active grid session **without committing changes**. This connects via WebSocket, reads the current grid state, and returns validation data.

```python
{!docs/guides/extensions/curator/scripts/precommit_validation.py!lines=7-29}
```

**When to use:** You want to check validation before committing changes. This is useful for automated pipelines that import data, validate, and only commit if validation passes.

**Note:** If you call `get_snapshot()` immediately after importing CSV data, some rows may show `validation_status = "pending"` while the backend processes validation. Wait briefly and retry if needed.

### Option B: Post-commit validation (export to RecordSet)

Export the grid session back to the RecordSet. This commits changes and generates detailed validation results.

```python
{!docs/guides/extensions/curator/scripts/postcommit_validation.py!lines=7-34}
```

**When to use:** You want committed validation results with full detail. The RecordSet's `get_detailed_validation_results()` returns a pandas DataFrame with row-level error messages.

---

## 6. Manage curation tasks

### List tasks in a project

```python
{!docs/guides/extensions/curator/scripts/manage_tasks.py!lines=7-18}
```

### Update a task

```python
{!docs/guides/extensions/curator/scripts/manage_tasks.py!lines=20-22}
```

### Delete a task

```python
{!docs/guides/extensions/curator/scripts/manage_tasks.py!lines=24-29}
```

When `delete_file_view=True`, the task's associated EntityView is also deleted. This only applies to file-based metadata tasks. Record-based tasks do not have an EntityView.

---

## 7. Validate folder annotations

For file-based workflows, you can validate annotations on files within a folder:

```python
{!docs/guides/extensions/curator/scripts/validate_folder.py!lines=7-22}
```

---

## Complete example: Programmatic CSV upload and validation

This example demonstrates the full workflow for power users who work entirely through the Python client without the grid UI:

```python
{!docs/guides/extensions/curator/scripts/full_csv_workflow.py!lines=7-62}
```

---

## API reference

### Curation task creation

- [create_record_based_metadata_task][synapseclient.extensions.curator.create_record_based_metadata_task]
- [create_file_based_metadata_task][synapseclient.extensions.curator.create_file_based_metadata_task]
- [query_schema_registry][synapseclient.extensions.curator.query_schema_registry]

### Grid session management

- [Grid.create][synapseclient.models.grid.Grid.create]
- [Grid.import_csv][synapseclient.models.grid.Grid.import_csv]
- [Grid.download_csv][synapseclient.models.grid.Grid.download_csv]
- [Grid.synchronize][synapseclient.models.grid.Grid.synchronize]
- [Grid.export_to_record_set][synapseclient.models.grid.Grid.export_to_record_set]
- [Grid.get_snapshot][synapseclient.models.grid.Grid.get_snapshot]
- [Grid.get_validation][synapseclient.models.grid.Grid.get_validation]
- [Grid.delete][synapseclient.models.grid.Grid.delete]
- [Grid.list][synapseclient.models.grid.Grid.list]

### Curation task management

- [CurationTask.store][synapseclient.models.CurationTask.store]
- [CurationTask.get][synapseclient.models.CurationTask.get]
- [CurationTask.delete][synapseclient.models.CurationTask.delete]
- [CurationTask.list][synapseclient.models.CurationTask.list]

### Validation

- [RecordSet.get_detailed_validation_results][synapseclient.models.RecordSet.get_detailed_validation_results]
- [Folder.get_schema_validation_statistics][synapseclient.models.Folder.get_schema_validation_statistics]
- [Folder.get_invalid_validation][synapseclient.models.Folder.get_invalid_validation]

### Related guides

- [Schema Operations](schema_operations.md) - Generate and register JSON schemas
- [JSON Schema Tutorial](../../../tutorials/python/json_schema.md) - Learn JSON schema basics
- [Curator Data Model](../../../explanations/curator_data_model.md) - CSV data model format
