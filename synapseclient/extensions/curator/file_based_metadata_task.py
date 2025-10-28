"""
Create a file view and CurationTask for schema-bound folders following the file-based metadata workflow.

This module provides library functions for creating file-based metadata curation tasks
in Synapse, including EntityView creation, CurationTask setup, and Wiki attachment.
"""

from typing import Any, Optional, Tuple

from synapseclient import Synapse  # type: ignore
from synapseclient import Wiki  # type: ignore
from synapseclient.core.exceptions import SynapseHTTPError  # type: ignore
from synapseclient.models import (  # type: ignore
    Column,
    ColumnType,
    EntityView,
    Folder,
    JSONSchema,
    Project,
    ViewTypeMask,
)
from synapseclient.models.curation import CurationTask, FileBasedMetadataTaskProperties
from synapseclient.operations import FileOptions, get

TYPE_DICT = {
    "string": ColumnType.STRING,
    "number": ColumnType.DOUBLE,
    "integer": ColumnType.INTEGER,
    "boolean": ColumnType.BOOLEAN,
}

LIST_TYPE_DICT = {
    "string": ColumnType.STRING_LIST,
    "integer": ColumnType.INTEGER_LIST,
    "boolean": ColumnType.BOOLEAN_LIST,
}


def create_json_schema_entity_view(
    syn: Synapse,
    synapse_entity_id: str,
    entity_view_name: str = "JSON Schema view",
) -> str:
    """
    Creates a Synapse entity view based on a JSON Schema that is bound to a Synapse entity
    This functionality is needed only temporarily. See note at top of module.

    Args:
        syn: A Synapse object thats been logged in
        synapse_entity_id: The ID of the entity in Synapse to bind the JSON Schema to
        entity_view_name: The name the crated entity view will have

    Returns:
        The Synapse id of the crated entity view
    """
    entity = get(
        file_options=FileOptions(download_file=False),
        synapse_id=synapse_entity_id,
        synapse_client=syn,
    )
    assert isinstance(entity, (Folder, Project))
    jsb = entity.get_schema()
    version_info = jsb.json_schema_version_info
    schema = JSONSchema(version_info.schema_name, version_info.organization_name)
    body = schema.get_body(version=version_info.semantic_version, synapse_client=syn)
    columns = _create_columns_from_json_schema(body)
    view = EntityView(
        name=entity_view_name,
        parent_id=synapse_entity_id,
        scope_ids=[synapse_entity_id],
        view_type_mask=ViewTypeMask.FILE,
        columns=columns,
    ).store(synapse_client=syn)
    # This reorder is so that these show up in the front of the EntityView in Synapse
    view.reorder_column(name="createdBy", index=0)
    view.reorder_column(name="name", index=0)
    view.reorder_column(name="id", index=0)
    view.store(synapse_client=syn)
    return view.id


def create_or_update_wiki_with_entity_view(
    syn: Synapse,
    entity_view_id: str,
    owner_id: str,
    title: Optional[str] = None,
) -> Wiki:
    """
    Creates or updates a Wiki for an entity if the wiki exists or not.
    An EntityView query is added to the wiki markdown

    This functionality is needed only temporarily. See note at top of module.

    Args:
        syn: A Synapse object thats been logged in
        entity_view_id: The Synapse id of the EntityView for the query
        owner_id: The ID of the entity in Synapse that the wiki will be created/updated
        title: The (new) title of the wiki to be created/updated

    Returns:
        The created Wiki object
    """
    entity = syn.get(owner_id)

    try:
        wiki = syn.getWiki(entity)
    except SynapseHTTPError:
        wiki = None
    if wiki:
        return update_wiki_with_entity_view(syn, entity_view_id, owner_id, title)
    return create_entity_view_wiki(syn, entity_view_id, owner_id, title)


def create_entity_view_wiki(
    syn: Synapse,
    entity_view_id: str,
    owner_id: str,
    title: Optional[str] = None,
) -> Wiki:
    """
    Creates a wiki with a query of an entity view
    This functionality is needed only temporarily. See note at top of module.

    Args:
        syn: A Synapse object thats been logged in
        entity_view_id: The Synapse id of the entity view to make the wiki for
        owner_id: The ID of the entity in Synapse to put as owner of the wiki
        title: The title of the wiki to be created

    Returns:
        The created wiki object
    """
    content = (
        "${synapsetable?query=select %2A from "
        f"{entity_view_id}"
        "&showquery=false&tableonly=false}"
    )
    if title is None:
        title = "Entity View"
    wiki = Wiki(title=title, owner=owner_id, markdown=content)
    wiki = syn.store(wiki)
    return wiki


def update_wiki_with_entity_view(
    syn: Synapse, entity_view_id: str, owner_id: str, title: Optional[str] = None
) -> Wiki:
    """
    Updates a wiki to include a query of an entity view
    This functionality is needed only temporarily. See note at top of module.

    Args:
        syn: A Synapse object thats been logged in
        entity_view_id: The Synapse id of the entity view to make the query for
        owner_id: The ID of the entity in Synapse to put as owner of the wiki
        title: The title of the wiki to be updated

    Returns:
        The created wiki object
    """
    entity = syn.get(owner_id)
    wiki = syn.getWiki(entity)

    new_content = (
        "${synapsetable?query=select %2A from "
        f"{entity_view_id}"
        "&showquery=false&tableonly=false}"
    )

    if new_content in wiki.markdown:
        return wiki

    wiki.markdown = wiki.markdown + f"\n{new_content}"
    if title:
        wiki.title = title

    syn.store(wiki)
    return wiki


def _create_columns_from_json_schema(json_schema: dict[str, Any]) -> list[Column]:
    """Creates a list of Synapse Columns based on the JSON Schema type

    Arguments:
        json_schema: The JSON Schema in dict form

    Raises:
        ValueError: If the JSON Schema has no properties
        ValueError: If the JSON Schema properties is not a dict

    Returns:
        A list of Synapse columns based on the JSON Schema
    """
    properties = json_schema.get("properties")
    if properties is None:
        raise ValueError("The JSON Schema is missing a 'properties' field.")
    if not isinstance(properties, dict):
        raise ValueError(
            "The 'properties' field in the JSON Schema must be a dictionary."
        )
    columns = []
    for name, prop_schema in properties.items():
        column_type = _get_column_type_from_js_property(prop_schema)
        maximum_size = None
        if column_type == "STRING":
            maximum_size = 100
        if column_type in LIST_TYPE_DICT.values():
            maximum_size = 5

        column = Column(
            name=name,
            column_type=column_type,
            maximum_size=maximum_size,
            default_value=None,
        )
        columns.append(column)
    return columns


def _get_column_type_from_js_property(js_property: dict[str, Any]) -> ColumnType:
    """
    Gets the Synapse column type from a JSON Schema property.
    The JSON Schema should be valid but that should not be assumed.
    If the type can not be determined ColumnType.STRING will be returned.

    Args:
        js_property: A JSON Schema property in dict form.

    Returns:
        A Synapse ColumnType based on the JSON Schema type
    """
    # Enums are always strings in Synapse tables
    if "enum" in js_property:
        return ColumnType.STRING
    if "type" in js_property:
        if js_property["type"] == "array":
            return _get_list_column_type_from_js_property(js_property)
        return TYPE_DICT.get(js_property["type"], ColumnType.STRING)
    # A oneOf list usually indicates that the type could be one or more different things
    if "oneOf" in js_property and isinstance(js_property["oneOf"], list):
        return _get_column_type_from_js_one_of_list(js_property["oneOf"])
    return ColumnType.STRING


def _get_column_type_from_js_one_of_list(js_one_of_list: list[Any]) -> ColumnType:
    """
    Gets the Synapse column type from a JSON Schema oneOf list.
    Items in the oneOf list should be dicts, but that should not be assumed.

    Args:
        js_one_of_list: A list of items to check for type

    Returns:
        A Synapse ColumnType based on the JSON Schema type
    """
    # items in a oneOf list should be dicts
    items = [item for item in js_one_of_list if isinstance(item, dict)]
    # Enums are always strings in Synapse tables
    if [item for item in items if "enum" in item]:
        return ColumnType.STRING
    # For Synapse ColumnType we can ignore null types in JSON Schemas
    type_items = [item for item in items if "type" in item if item["type"] != "null"]
    if len(type_items) == 1:
        type_item = type_items[0]
        if type_item["type"] == "array":
            return _get_list_column_type_from_js_property(type_item)
        return TYPE_DICT.get(type_item["type"], ColumnType.STRING)
    return ColumnType.STRING


def _get_list_column_type_from_js_property(js_property: dict[str, Any]) -> ColumnType:
    """
    Gets the Synapse column type from a JSON Schema array property

    Args:
        js_property: A JSON Schema property in dict form.

    Returns:
        A Synapse ColumnType based on the JSON Schema type
    """
    if "items" in js_property and isinstance(js_property["items"], dict):
        # Enums are always strings in Synapse tables
        if "enum" in js_property["items"]:
            return ColumnType.STRING_LIST
        if "type" in js_property["items"]:
            return LIST_TYPE_DICT.get(
                js_property["items"]["type"], ColumnType.STRING_LIST
            )

    return ColumnType.STRING_LIST


def create_file_based_metadata_task(
    folder_id: str,
    curation_task_name: str,
    instructions: str,
    attach_wiki: bool = True,
    entity_view_name: str = "JSON Schema view",
    schema_uri: Optional[str] = None,
    enable_derived_annotations: bool = False,
    *,
    synapse_client: Optional[Synapse] = None,
) -> Tuple[str, str]:
    """
    Create a file view for a schema-bound folder using schematic.

    Example: Creating a file-based metadata curation task with schema binding
        In this example, we create an EntityView and CurationTask for file-based
        metadata curation. If a schema_uri is provided, it will be bound to the folder.

        ```python
        import synapseclient
        from synapseclient.extensions.curator import create_file_based_metadata_task

        syn = synapseclient.Synapse()
        syn.login()

        entity_view_id, task_id = create_file_based_metadata_task(
            synapse_client=syn,
            folder_id="syn12345678",
            curation_task_name="BiospecimenMetadataTemplate",
            instructions="Please curate this metadata according to the schema requirements",
            attach_wiki=True,
            entity_view_name="Biospecimen Metadata View",
            schema_uri="sage.schemas.v2571-amp.Biospecimen.schema-0.0.1"
        )
        ```

    Arguments:
        folder_id: The Synapse Folder ID to create the file view for.
        curation_task_name: Name for the CurationTask (used as data_type field).
            Must be unique within the project, otherwise if it matches an existing
            CurationTask, that task will be updated with new data.
        instructions: Instructions for the curation task.
        attach_wiki: Whether or not to attach a Synapse Wiki (default: True).
        entity_view_name: Name for the created entity view (default: "JSON Schema view").
        schema_uri: Optional JSON schema URI to bind to the folder. If provided,
            the schema will be bound to the folder before creating the entity view.
            (e.g., 'sage.schemas.v2571-amp.Biospecimen.schema-0.0.1')
        enable_derived_annotations: If true, enable derived annotations. Defaults to False.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        A tuple containing:
          - The Synapse ID of the entity view created
          - The task ID of the curation task created

    Raises:
        ValueError: If required parameters are missing.
        SynapseError: If there are issues with Synapse operations.
    """
    # Validate required parameters
    if not folder_id:
        raise ValueError("folder_id is required")
    if not curation_task_name:
        raise ValueError("curation_task_name is required")
    if not instructions:
        raise ValueError("instructions is required")

    synapse_client = Synapse.get_client(synapse_client=synapse_client)

    # Bind schema to folder if schema_uri is provided
    if schema_uri:
        synapse_client.logger.info(
            f"Attempting to bind schema {schema_uri} to folder {folder_id}."
        )
        try:
            folder = Folder(folder_id).get(synapse_client=synapse_client)
            folder.bind_schema(
                json_schema_uri=schema_uri,
                enable_derived_annotations=enable_derived_annotations,
                synapse_client=synapse_client,
            )
            synapse_client.logger.info(
                f"Successfully bound schema {schema_uri} to folder {folder_id}."
            )
        except Exception as e:
            synapse_client.logger.exception(
                f"Error binding schema {schema_uri} to folder {folder_id}"
            )
            raise e

    synapse_client.logger.info("Attempting to create entity view.")
    try:
        entity_view_id = create_json_schema_entity_view(
            syn=synapse_client,
            synapse_entity_id=folder_id,
            entity_view_name=entity_view_name,
        )
    except Exception as e:
        synapse_client.logger.exception("Error creating entity view")
        raise e
    synapse_client.logger.info("Created entity view.")

    if attach_wiki:
        synapse_client.logger.info("Attempting to attach wiki.")
        try:
            create_or_update_wiki_with_entity_view(
                syn=synapse_client, entity_view_id=entity_view_id, owner_id=folder_id
            )
        except Exception as e:
            synapse_client.logger.exception("Error creating wiki")
            raise e
        synapse_client.logger.info("Wiki attached.")

    # Validate that the folder has an attached JSON schema
    # The curation_task_name parameter is now required and used directly for the CurationTask.

    synapse_client.logger.info("Attempting to get the attached schema.")
    print("$$$$$$$$$$$$$$$$$")
    try:
        print("########")
        entity = get(folder_id, synapse_client=synapse_client)
        print(entity)
        entity.get_schema(synapse_client=synapse_client)
    except Exception as e:
        synapse_client.logger.exception("Error getting the attached schema.")
        raise e
    synapse_client.logger.info("Schema retrieval successful")

    # Use the provided curation_task_name (required parameter)
    task_datatype = curation_task_name

    synapse_client.logger.info(
        "Attempting to get the Synapse ID of the provided folders project."
    )
    try:
        entity = Folder(folder_id).get(synapse_client=synapse_client)
        parent = synapse_client.get(entity.parent_id)
        project = None
        while not project:
            if parent.concreteType == "org.sagebionetworks.repo.model.Project":
                project = parent
                break
            parent = synapse_client.get(parent.parentId)
    except Exception as e:
        synapse_client.logger.exception(
            "Error getting the Synapse ID of the provided folders project"
        )
        raise e
    synapse_client.logger.info("Got the Synapse ID of the provided folders project.")

    synapse_client.logger.info("Attempting to create the CurationTask.")
    try:
        task = CurationTask(
            data_type=task_datatype,
            project_id=project.id,
            instructions=instructions,
            task_properties=FileBasedMetadataTaskProperties(
                upload_folder_id=folder_id,
                file_view_id=entity_view_id,
            ),
        ).store(synapse_client=synapse_client)
    except Exception as e:
        synapse_client.logger.exception("Error creating the CurationTask.")
        raise e
    synapse_client.logger.info("Created the CurationTask.")

    return (entity_view_id, task.task_id)
