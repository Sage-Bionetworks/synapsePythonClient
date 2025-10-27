"""
Generate and upload CSV templates as a RecordSet for record-based metadata, create a
CurationTask, and also create a Grid to bootstrap the ValidationStatistics.

This module provides library functions for creating record-based metadata curation tasks
in Synapse, including RecordSet creation, CurationTask setup, and Grid view initialization.
"""
import tempfile
from typing import Any, Dict, List, Optional, Tuple, TypeVar

from synapseclient import Synapse
from synapseclient.models import (
    CurationTask,
    Grid,
    RecordBasedMetadataTaskProperties,
    RecordSet,
)
from synapseclient.services.json_schema import JsonSchemaService

DATA_FRAME_TYPE = TypeVar("pd.DataFrame")


def extract_property_titles(schema_data: Dict[str, Any]) -> List[str]:
    """
    Extract title fields from all properties in a JSON schema.
    Args:
        schema_data: The parsed JSON schema data
    Returns:
        List of title values from the properties
    """
    titles = []

    # Check if 'properties' exists in the schema
    if "properties" not in schema_data:
        return titles

    properties = schema_data["properties"]

    for property_name in properties.keys():
        titles.append(property_name)

    return titles


def create_dataframe_from_titles(titles: List[str]) -> DATA_FRAME_TYPE:
    """
    Create an empty DataFrame with the extracted titles as column names.
    Args:
        titles: List of title strings to use as column names
    Returns:
        Empty DataFrame with titles as columns
    """
    import pandas as pd

    if not titles:
        return pd.DataFrame()

    df = pd.DataFrame(columns=titles)
    return df


def extract_schema_properties_from_dict(schema_data: Dict[str, Any]) -> DATA_FRAME_TYPE:
    """
    Process a JSON schema dictionary and return a DataFrame with property titles as columns.
    Args:
        schema_data: The parsed JSON schema data as a dictionary
    Returns:
        DataFrame with property titles as columns
    """
    titles = extract_property_titles(schema_data)

    df = create_dataframe_from_titles(titles)

    return df


def extract_schema_properties_from_web(
    syn: Synapse, schema_uri: str
) -> DATA_FRAME_TYPE:
    """
    Extract schema properties from a web-based JSON schema URI using Synapse.

    This function retrieves a JSON schema from a web URI through the Synapse platform
    and extracts property titles to create a DataFrame with those titles as columns.

    Args:
        syn: Authenticated Synapse client instance
        schema_uri: URI pointing to the JSON schema resource

    Returns:
        DataFrame with property titles from the schema as column names

    """
    try:
        org_name, schema_name, _ = schema_uri.split("-")
    except ValueError as e:
        raise ValueError(
            f"Invalid schema URI format: {schema_uri}. Expected format 'org-name-schema.name.schema-version'."
        ) from e

    js = JsonSchemaService(synapse=syn)
    schemas_list = js.list_json_schemas(organization_name=org_name)
    if not any(schema_name == s["schemaName"] for s in schemas_list):
        raise ValueError(
            f"Schema URI '{schema_uri}' not found in Synapse JSON schemas."
        )

    schema = js.get_json_schema_body(json_schema_uri=schema_uri)
    return extract_schema_properties_from_dict(schema_data=schema)


def create_record_based_metadata_task(
    project_id: str,
    folder_id: str,
    record_set_name: str,
    record_set_description: str,
    curation_task_name: str,
    upsert_keys: List[str],
    instructions: str,
    schema_uri: str,
    bind_schema_to_record_set: bool = True,
    enable_derived_annotations: bool = False,
    *,
    synapse_client: Optional[Synapse] = None,
) -> Tuple[RecordSet, CurationTask, Grid]:
    """
    Generate and upload CSV templates as a RecordSet for record-based metadata,
    create a CurationTask, and also create a Grid to bootstrap the ValidationStatistics.

    A number of schema URIs that are already registered to Synapse can be found at:

    - <https://www.synapse.org/Synapse:syn69735275/tables/>


    If you have yet to create and register your JSON schema in Synapse, please refer to
    the tutorial at <https://python-docs.synapse.org/en/stable/tutorials/python/json_schema/>.


    Example: Creating a record-based metadata curation task with a schema URI
        In this example, we create a RecordSet and CurationTask for biospecimen metadata
        curation using a schema URI. By default this will also bind the schema to the
        RecordSet, however the `bind_schema_to_record_set` parameter can be set to
        False to skip that step.


        ```python
        import synapseclient
        from synapseclient.extensions.curator import create_record_based_metadata_task

        syn = synapseclient.Synapse()
        syn.login()

        record_set, task, grid = create_record_based_metadata_task(
            synapse_client=syn,
            project_id="syn12345678",
            folder_id="syn87654321",
            record_set_name="BiospecimenMetadata_RecordSet",
            record_set_description="RecordSet for biospecimen metadata curation",
            curation_task_name="BiospecimenMetadataTemplate",
            upsert_keys=["specimenID"],
            instructions="Please curate this metadata according to the schema requirements",
            schema_uri="schema-org-schema.name.schema-v1.0.0"
        )
        ```

    Arguments:
        project_id: The Synapse ID of the project where the folder exists.
        folder_id: The Synapse ID of the folder to upload to.
        record_set_name: Name for the RecordSet.
        record_set_description: Description for the RecordSet.
        curation_task_name: Name for the CurationTask (used as data_type field).
            Must be unique within the project, otherwise if it matches an existing
            CurationTask, that task will be updated with new data.
        upsert_keys: List of column names to use as upsert keys.
        instructions: Instructions for the curation task.
        schema_uri: JSON schema URI for the RecordSet schema.
            (e.g., 'sage.schemas.v2571-amp.Biospecimen.schema-0.0.1', 'sage.schemas.v2571-ad.Analysis.schema-0.0.0')
        bind_schema_to_record_set: Whether to bind the given schema to the RecordSet
            (default: True).
        enable_derived_annotations: If true, enable derived annotations. Defaults to False.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        Tuple containing the created RecordSet, CurationTask, and Grid objects

    Raises:
        ValueError: If required parameters are missing or if schema_uri is not provided.
        SynapseError: If there are issues with Synapse operations.
    """
    # Validate required parameters
    if not project_id:
        raise ValueError("project_id is required")
    if not folder_id:
        raise ValueError("folder_id is required")
    if not record_set_name:
        raise ValueError("record_set_name is required")
    if not record_set_description:
        raise ValueError("record_set_description is required")
    if not curation_task_name:
        raise ValueError("curation_task_name is required")
    if not upsert_keys:
        raise ValueError("upsert_keys is required and must be a non-empty list")
    if not instructions:
        raise ValueError("instructions is required")
    if not schema_uri:
        raise ValueError("schema_uri is required")

    synapse_client = Synapse.get_client(synapse_client=synapse_client)

    template_df = extract_schema_properties_from_web(
        syn=synapse_client, schema_uri=schema_uri
    )
    synapse_client.logger.info(
        f"Extracted schema properties and created template: {template_df.columns.tolist()}"
    )

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    try:
        with open(tmp.name, "w", encoding="utf-8") as f:
            template_df.to_csv(f, index=False)
    except Exception as e:
        synapse_client.logger.exception("Error writing template to temporary CSV file")
        raise e

    try:
        record_set_with_data = RecordSet(
            name=record_set_name,
            parent_id=folder_id,
            description=record_set_description,
            path=tmp.name,
            upsert_keys=upsert_keys,
        ).store(synapse_client=synapse_client)
        record_set_id = record_set_with_data.id
        synapse_client.logger.info(
            f"Created RecordSet with ID: {record_set_id} in folder {folder_id}"
        )

        if bind_schema_to_record_set:
            record_set_with_data.bind_schema(
                json_schema_uri=schema_uri,
                enable_derived_annotations=enable_derived_annotations,
                synapse_client=synapse_client,
            )
            synapse_client.logger.info(
                f"Bound schema {schema_uri} to RecordSet ID: {record_set_id}"
            )
    except Exception as e:
        synapse_client.logger.exception("Error creating RecordSet in Synapse")
        raise e

    try:
        curation_task = CurationTask(
            data_type=curation_task_name,
            project_id=project_id,
            instructions=instructions,
            task_properties=RecordBasedMetadataTaskProperties(
                record_set_id=record_set_id,
            ),
        ).store(synapse_client=synapse_client)
        synapse_client.logger.info(
            f"Created CurationTask ({curation_task.task_id}) with name {curation_task_name}"
        )
    except Exception as e:
        synapse_client.logger.error(f"Error creating CurationTask in Synapse: {e}")
        raise e

    try:
        curation_grid: Grid = Grid(
            record_set_id=record_set_id,
        )
        curation_grid.create(synapse_client=synapse_client)
        curation_grid = curation_grid.export_to_record_set(
            synapse_client=synapse_client
        )
        synapse_client.logger.info(
            f"Created Grid view for RecordSet ID: {record_set_id} for curation task {curation_task_name}"
        )
    except Exception as e:
        synapse_client.logger.exception("Error creating Grid view in Synapse")
        raise e

    return record_set_with_data, curation_task, curation_grid
