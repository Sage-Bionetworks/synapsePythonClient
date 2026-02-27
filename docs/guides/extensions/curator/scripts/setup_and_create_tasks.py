"""
Script: Setting up curation workflows.
Covers authentication, schema lookup, and creating both
record-based and file-based curation tasks.
"""

from synapseclient import Synapse
from synapseclient.extensions.curator import (
    create_file_based_metadata_task,
    create_record_based_metadata_task,
    query_schema_registry,
)

syn = Synapse()
syn.login()

# Find the latest schema for a specific data type
schema_uri = query_schema_registry(
    synapse_client=syn,
    dcc="ad",
    datatype="IndividualAnimalMetadataTemplate",
)
print(f"Schema URI: {schema_uri}")

# Browse all available versions of a schema
all_schemas = query_schema_registry(
    synapse_client=syn,
    dcc="ad",
    datatype="IndividualAnimalMetadataTemplate",
    return_latest_only=False,
)

# Create a record-based curation task
record_set, curation_task, data_grid = create_record_based_metadata_task(
    synapse_client=syn,
    project_id="syn123456789",
    folder_id="syn987654321",
    record_set_name="AnimalStudy_Records",
    record_set_description="Metadata for animal study specimens",
    curation_task_name="AnimalStudy_Curation",
    upsert_keys=["individualID"],
    instructions="Complete all required fields for each animal.",
    schema_uri=schema_uri,
    bind_schema_to_record_set=True,
    assignee_principal_id="123456",  # Optional: assign to user or team
)

print(f"RecordSet: {record_set.id}")
print(f"CurationTask: {curation_task.task_id}")

# Create a file-based curation task
entity_view_id, task_id = create_file_based_metadata_task(
    synapse_client=syn,
    folder_id="syn987654321",
    curation_task_name="FileAnnotations_Curation",
    instructions="Annotate each file according to the schema.",
    entity_view_name="Animal Study Files View",
    schema_uri=schema_uri,
    assignee_principal_id="123456",  # Optional
)

print(f"EntityView: {entity_view_id}")
print(f"CurationTask: {task_id}")
