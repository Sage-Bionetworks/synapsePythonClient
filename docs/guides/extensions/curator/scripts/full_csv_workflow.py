"""
Script: Complete programmatic CSV upload and validation workflow.
Demonstrates the full end-to-end flow for power users who work
entirely through the Python client without the grid UI.
"""

from synapseclient import Synapse
from synapseclient.extensions.curator import (
    create_record_based_metadata_task,
    query_schema_registry,
)
from synapseclient.models import Grid

syn = Synapse()
syn.login()

# 1. Find schema and create curation task
schema_uri = query_schema_registry(
    synapse_client=syn, dcc="ad", datatype="IndividualAnimalMetadataTemplate"
)

record_set, curation_task, _ = create_record_based_metadata_task(
    synapse_client=syn,
    project_id="syn123456789",
    folder_id="syn987654321",
    record_set_name="StudyMetadata",
    record_set_description="Animal study metadata",
    curation_task_name="StudyMetadata_Curation",
    upsert_keys=["individualID"],
    instructions="Complete all required fields.",
    schema_uri=schema_uri,
    bind_schema_to_record_set=True,
)

# 2. Import CSV data into a grid session
# Column schema is auto-derived from the CSV header and the
# JSON schema bound to the grid.
grid = Grid(record_set_id=record_set.id).create()
grid = grid.import_csv(path="metadata.csv")
print(f"Imported {grid.csv_import_total_count} rows")

# 3. Check validation before committing
snapshot = grid.get_snapshot()
summary = snapshot.validation_summary
print(f"Validation: {summary['valid']}/{summary['total']} valid")

if summary["invalid"] > 0:
    print("Validation errors found:")
    for row in snapshot.rows:
        if row.validation and not row.validation.is_valid:
            print(f"  Row {row.row_id}: " f"{row.validation.validation_error_message}")
    # Fix errors and re-import if needed...

# 4. Commit when ready
grid = grid.export_to_record_set()
print(f"Exported to RecordSet version {grid.record_set_version_number}")

# 5. Clean up
grid.delete()
