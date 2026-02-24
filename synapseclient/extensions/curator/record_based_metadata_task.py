"""
Generate and upload CSV templates as a RecordSet for record-based metadata, create a
CurationTask, and also create a Grid to bootstrap the ValidationStatistics.

This module provides library functions for creating record-based metadata curation tasks
in Synapse, including RecordSet creation, CurationTask setup, and Grid view initialization.
"""
import tempfile
from typing import Any, Dict, List, Optional, Tuple, Union

from synapseclient import Synapse
from synapseclient.core.typing_utils import DataFrame as DATA_FRAME_TYPE
from synapseclient.core.utils import test_import_pandas
from synapseclient.models import (
    CurationTask,
    Grid,
    JSONSchema,
    Project,
    RecordBasedMetadataTaskProperties,
    RecordSet,
)
from synapseclient.operations import get


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
    test_import_pandas()
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
    schema = JSONSchema.from_uri(schema_uri)
    schema.get(synapse_client=syn)
    body = schema.get_body(synapse_client=syn)
    return extract_schema_properties_from_dict(schema_data=body)


def create_record_based_metadata_task(
    folder_id: str,
    record_set_name: str,
    record_set_description: str,
    curation_task_name: str,
    upsert_keys: List[str],
    instructions: str,
    schema_uri: str,
    bind_schema_to_record_set: bool = True,
    enable_derived_annotations: bool = False,
    assignee_principal_id: Optional[Union[str, int]] = None,
    *,
    synapse_client: Optional[Synapse] = None,
    project_id: Optional[str] = None,  # Deprecated, will be removed in v5.0.0
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
            folder_id="syn87654321",
            record_set_name="BiospecimenMetadata_RecordSet",
            record_set_description="RecordSet for biospecimen metadata curation",
            curation_task_name="BiospecimenMetadataTemplate",
            upsert_keys=["specimenID"],
            instructions="Please curate this metadata according to the schema requirements",
            schema_uri="schema-org-schema.name.schema-v1.0.0",
            assignee_principal_id=123456  # Optional: Assign to a user or team (can be str or int)
        )
        ```

    Arguments:
        folder_id: The Synapse ID of the folder to upload RecordSet to.
        record_set_name: Name for the RecordSet entity that will be created.
            A RecordSet entity captures record-based metadata as a special type of CSV and stores contributor
              provided metadata collected via Curator enabling sharing and download of validated metadata in Synapse.
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
        assignee_principal_id: The principal ID of the user or team to assign to this
            curation task. Can be provided as either a string or an integer. If None
            (default), the task will be unassigned. For metadata tasks, this determines
            the owner of the grid session. Team members can all join grid sessions owned
            by their team, while user-owned grid sessions are restricted to that user only.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
        project_id: Deprecated, will be removed in  v5.0.0

    Returns:
        Tuple containing the created RecordSet, CurationTask, and Grid objects

    Raises:
        ValueError: If required parameters are missing or if schema_uri is not provided.
        SynapseError: If there are issues with Synapse operations.
    """
    # Validate required parameters
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

    if project_id:
        synapse_client.logger.warning(
            "The 'project_id' parameter is deprecated and will be removed in v5.0.0. "
            "The project ID will be inferred from the folder ID provided."
        )

    synapse_client = Synapse.get_client(synapse_client=synapse_client)

    project_id = project_id_from_entity_id(
        entity_id=folder_id, synapse_client=synapse_client
    )

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
            assignee_principal_id=(
                str(assignee_principal_id)
                if assignee_principal_id is not None
                else None
            ),
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


def project_id_from_entity_id(entity_id: str, synapse_client: Synapse) -> str:
    """
    Retrieves the project ID from a given entity ID by traversing up the the folder hierarchy

    Args:
        entity_id: The Synapse ID of the entity (e.g., folder, file) to start from.
        synapse_client: Authenticated Synapse client instance

    Returns:
        The Synapse ID of the project that the entity belongs to.

    Raises:
        ValueError: If the project ID cannot be found within 1000 iterations.
    """

    # Get the project ID from the folder ID
    current_obj = get(entity_id, synapse_client=synapse_client)
    iter = 0
    while not isinstance(current_obj, Project):
        current_obj = get(current_obj.parent_id, synapse_client=synapse_client)
        iter += 1
        if iter > 1000:
            raise ValueError("Could not find project ID in folder hierarchy")
    return current_obj.id
