"""
Create a file view and CurationTask for schema-bound folders following the file-based metadata workflow.

This module provides library functions for creating file-based metadata curation tasks
in Synapse, including EntityView creation, CurationTask setup, and Wiki attachment.
"""

from collections import OrderedDict
from typing import Any, Optional, Tuple, Union

from synapseclient import Synapse  # type: ignore
from synapseclient import Wiki  # type: ignore
from synapseclient.core.exceptions import SynapseHTTPError  # type: ignore
from synapseclient.extensions.curator.utils import (
    project_id_from_entity_id,
    resolve_column_order_list,
    validate_column_order_list,
)
from synapseclient.models import (  # type: ignore
    AuthorizationMode,
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
    "string": ColumnType.MEDIUMTEXT,
    "number": ColumnType.DOUBLE,
    "integer": ColumnType.INTEGER,
    "boolean": ColumnType.BOOLEAN,
}

LIST_TYPE_DICT = {
    "string": ColumnType.STRING_LIST,
    "integer": ColumnType.INTEGER_LIST,
    "boolean": ColumnType.BOOLEAN_LIST,
}


def _create_json_schema_entity_view(
    synapse_entity_id: str,
    entity_view_name: str = "JSON Schema view",
    view_type_mask: Union[int, ViewTypeMask] = ViewTypeMask.FILE,
    column_order: list[str] | None = None,
    syn: Optional[Synapse] = None,
) -> EntityView:
    """
    Creates a Synapse entity view based on a JSON Schema that is bound to a Synapse
    entity and returns the stored EntityView object.

    Arguments:
        synapse_entity_id: The ID of the entity in Synapse to bind the JSON Schema to
        entity_view_name: The name the crated entity view will have
        view_type_mask: The view type mask for the EntityView. Defaults to
            ViewTypeMask.FILE. Additional types can be added using bitwise OR
            (e.g., ViewTypeMask.FILE | ViewTypeMask.DOCKER). Accepts either a
            ViewTypeMask enum member or its raw integer value.
        column_order: Optional list of column names to place immediately after the
            pinned name and id columns, in the order given. Remaining columns keep
            their existing relative order.
        syn: A Synapse object thats been logged in

    Returns:
        The created EntityView object

    Raises:
        ValueError: If synapse_entity_id is not a Folder or a Project, or if
            column_order is malformed or names a column that is not present on the
            created EntityView.
    """
    entity = get(
        file_options=FileOptions(download_file=False),
        synapse_id=synapse_entity_id,
        synapse_client=syn,
    )
    if not isinstance(entity, (Folder, Project)):
        raise ValueError(
            f"A JSON Schema can only be read from a Folder or a Project, but "
            f"{synapse_entity_id} is a {type(entity).__name__}."
        )
    jsb = entity.get_schema(synapse_client=syn)
    version_info = jsb.json_schema_version_info
    schema = JSONSchema(version_info.schema_name, version_info.organization_name)
    body = schema.get_body(version=version_info.semantic_version, synapse_client=syn)
    columns = _create_columns_from_json_schema(body)
    view = EntityView(
        name=entity_view_name,
        parent_id=synapse_entity_id,
        scope_ids=[synapse_entity_id],
        view_type_mask=view_type_mask,
        columns=columns,
    ).store(synapse_client=syn)

    try:
        available_columns = list(view.columns.keys())
        ordered_columns = resolve_column_order_list(
            available_columns=available_columns,
            pinned_columns=["name", "id"],
            requested_columns=column_order,
        )
        view.columns = OrderedDict(
            (column, view.columns[column]) for column in ordered_columns
        )
    except ValueError:
        try:
            view.delete(synapse_client=syn)
        except Exception:
            Synapse.get_client(synapse_client=syn).logger.exception(
                f"Could not delete the created EntityView {view.id}. Delete it "
                "yourself, either from the Synapse web UI, or with the Python "
                f"client: EntityView(id='{view.id}').delete()"
            )
        raise
    view.store(synapse_client=syn)
    return view


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
    columns = [
        _create_synapse_column_from_js_property(prop_schema, name)
        for name, prop_schema in properties.items()
    ]
    return columns


def _create_synapse_column_from_js_property(
    js_property: dict[str, Any], name: str
) -> Column:
    """
    Creates a Synapse Column based on a JSON Schema property.

    Args:
        js_property: A JSON Schema property in dict form.
        name: The name of the column.

    Returns:
        A Synapse Column based on the JSON Schema property.
    """
    column_type = _get_column_type_from_js_property(js_property)
    return Column(name=name, column_type=column_type)


def _get_column_type_from_js_property(js_property: dict[str, Any]) -> ColumnType:
    """
    Gets the Synapse column type from a JSON Schema property.
    The JSON Schema should be valid but that should not be assumed.
    If the type can not be determined ColumnType.MEDIUMTEXT will be returned.

    Args:
        js_property: A JSON Schema property in dict form.

    Returns:
        A Synapse ColumnType based on the JSON Schema type
    """
    # Enums are set as MediumText columns
    if "enum" in js_property:
        return ColumnType.MEDIUMTEXT
    if "type" in js_property:
        js_type = js_property["type"]
        # Synapse columns cannot be more than one type
        # If the JSONSchema type is a list of types, check if it's a nullable single type
        if isinstance(js_type, list):
            types = [t for t in js_type if t != "null"]
            if len(types) == 1:
                js_type = types[0]
            # If there are multiple non-null types, we cannot determine a single column type, so default to MediumText
            else:
                return ColumnType.MEDIUMTEXT
        if js_type == "array":
            return _get_list_column_type_from_js_property(js_property)
        # If there is only one JSONSChema type, return the corresponding Synapse column type,
        #  defaulting to MediumText if there is no match
        return TYPE_DICT.get(js_type, ColumnType.MEDIUMTEXT)
    # A oneOf list usually indicates that the type could be one or more different things
    # Curator extension does not create the types of JSON Schemas where this is the case
    #  but if it is present we will attempt to determine the type based on the items in the oneOf list.
    if "oneOf" in js_property and isinstance(js_property["oneOf"], list):
        return _get_column_type_from_js_one_of_list(js_property["oneOf"])
    return ColumnType.MEDIUMTEXT


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
        return ColumnType.MEDIUMTEXT
    # For Synapse ColumnType we can ignore null types in JSON Schemas
    type_items = [item for item in items if "type" in item if item["type"] != "null"]
    if len(type_items) == 1:
        type_item = type_items[0]
        if type_item["type"] == "array":
            return _get_list_column_type_from_js_property(type_item)
        return TYPE_DICT.get(type_item["type"], ColumnType.MEDIUMTEXT)
    return ColumnType.MEDIUMTEXT


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
    attach_wiki: bool = False,
    entity_view_name: str = "JSON Schema view",
    schema_uri: Optional[str] = None,
    enable_derived_annotations: bool = False,
    assignee_principal_id: Optional[Union[str, int]] = None,
    view_type_mask: Union[int, ViewTypeMask] = ViewTypeMask.FILE,
    authorization_mode: Optional[Union[AuthorizationMode, str]] = None,
    # TODO: https://sagebionetworks.jira.com/browse/SYNPY-1865
    # In v5.0.0 make entity-returning the default: remove the return_entities
    # parameter and change the return type to Tuple[EntityView, CurationTask].
    return_entities: bool = False,
    *,
    column_order: list[str] | None = None,
    synapse_client: Optional[Synapse] = None,
) -> Union[Tuple[str, int], Tuple[EntityView, CurationTask]]:
    """
    Create a file view for a schema-bound folder using schematic.

    Example: Creating a file-based metadata curation task with schema binding
        In this example, we create an EntityView and CurationTask for file-based
        metadata curation. If a schema_uri is provided, it will be bound to the folder.

        ```python
        import synapseclient
        from synapseclient.extensions.curator import create_file_based_metadata_task
        from synapseclient.models import AuthorizationMode, ViewTypeMask

        syn = synapseclient.Synapse()
        syn.login()

        entity_view_id, task_id = create_file_based_metadata_task(
            folder_id="syn12345678",
            curation_task_name="BiospecimenMetadataTemplate",
            instructions="Please curate this metadata according to the schema requirements",
            attach_wiki=False, # Optional: whether to attach a Synapse Wiki
            entity_view_name="Biospecimen Metadata View", # Optional: name for the created entity view
            schema_uri="sage.schemas.v2571-amp.Biospecimen.schema-0.0.1", # Optional: JSON schema URI to bind to the folder
            assignee_principal_id=123456, # Optional: Assign to a user or team (can be str or int)
            view_type_mask=ViewTypeMask.FILE | ViewTypeMask.DOCKER, # Optional: include additional entity types in the view
            authorization_mode=AuthorizationMode.SOURCE_BENEFACTOR, # Optional: recommended access mode for the grid session
            synapse_client=syn, # Optional: defaults to the last created Synapse client
        )
        ```

    Example: Returning the created EntityView and CurationTask objects
        Pass return_entities=True to receive the full EntityView and CurationTask
        objects instead of their ID strings. This avoids a second round-trip to
        Synapse when you need to read or modify the created entities, and matches the
        return shape of create_record_based_metadata_task.

        ```python
        import synapseclient
        from synapseclient.extensions.curator import create_file_based_metadata_task

        syn = synapseclient.Synapse()
        syn.login()

        entity_view, curation_task = create_file_based_metadata_task(
            synapse_client=syn,
            folder_id="syn12345678",
            curation_task_name="BiospecimenMetadataTemplate",
            instructions="Please curate this metadata according to the schema requirements",
            return_entities=True,
        )
        ```

    Example: Controlling the column order of the EntityView
        Pass column_order to place specific columns immediately after the pinned name
        and id columns. You only need to name the columns you care about; every other
        column, including Synapse managed columns such as createdBy, is appended
        afterwards in its existing order.

        ```python
        import synapseclient
        from synapseclient.extensions.curator import create_file_based_metadata_task

        syn = synapseclient.Synapse()
        syn.login()

        entity_view, curation_task = create_file_based_metadata_task(
            synapse_client=syn,
            folder_id="syn12345678",
            curation_task_name="BiospecimenMetadataTemplate",
            instructions="Please curate this metadata according to the schema requirements",
            column_order=["patientId", "sampleId", "assay", "fileFormat"],
            return_entities=True,
        )

        # Resulting column order:
        # name, id, patientId, sampleId, assay, fileFormat, <remaining columns>
        ```

    Arguments:
        folder_id: The Synapse Folder ID to create the file view for.
        curation_task_name: Name for the CurationTask (used as data_type field).
            Must be unique within the project, otherwise if it matches an existing
            CurationTask, that task will be updated with new data.
        instructions: Instructions for the curation task.
        attach_wiki: Whether or not to attach a Synapse Wiki (default: False).
        entity_view_name: Name for the created entity view (default: "JSON Schema view").
        schema_uri: Optional JSON schema URI to bind to the folder. If provided,
            the schema will be bound to the folder before creating the entity view.
            (e.g., 'sage.schemas.v2571-amp.Biospecimen.schema-0.0.1')
        enable_derived_annotations: If true, enable derived annotations. Defaults to False.
        assignee_principal_id: The principal ID of the user or team to assign to this
            curation task. Can be provided as either a string or an integer. If None
            (default), the task will be unassigned. For metadata tasks, this determines
            the owner of the grid session. Team members can all join grid sessions owned
            by their team, while user-owned grid sessions are restricted to that user only.
        view_type_mask: The view type mask for the EntityView. Defaults to
            ViewTypeMask.FILE. Additional types can be added using bitwise OR
            (e.g., ViewTypeMask.FILE | ViewTypeMask.DOCKER). Accepts either a
            ViewTypeMask enum member or its raw integer value.
        authorization_mode: Recommends who is allowed to access the curation
            grid session that a client opens for this task. The value is stored on the
            task as a suggestion; the client applies it when it creates a new session.
            Choose from:
            - SESSION_OWNER: only the person or team who owns the session can access it.
            - SOURCE_BENEFACTOR: anyone with EDIT permission on the
              data being curated can access the session. This lets editors collaborate
              in the same session without being added to a shared ownership team.
            When omitted (None, the default), no recommendation is stored and clients
            fall back to their usual behavior of finding or creating a private session
            for the current user. Changing this value after the task already exists
            resets the task's active session, so a new grid session must be opened
            before curation can continue.
        return_entities: If True, return the created EntityView and CurationTask
            objects instead of their ID strings. Defaults to False for backwards
            compatibility. The entity-returning shape will become the default in
            v5.0.0, at which point this parameter will be removed.
        column_order: Optional list of column names placed immediately after the
            pinned name and id columns, in the order given. Columns that are not
            named keep their existing relative order and are appended afterwards, so
            you only need to list the columns that need intentional placement. The
            name and id columns always remain the two leftmost columns, so naming
            either of them here has no effect. Every name must match a column on the
            created EntityView, which includes the JSON Schema properties as well as
            the Synapse managed columns such as createdBy and modifiedOn.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        If return_entities is False (default): a tuple containing
          - The Synapse ID of the entity view created
          - The task ID of the curation task created

        If return_entities is True: a tuple containing
          - The created EntityView object
          - The created CurationTask object

    Raises:
        ValueError: If required parameters are missing, or if column_order is
            malformed or names a column that is not on the created EntityView.
        SynapseError: If there are issues with Synapse operations.
    """
    # Validate required parameters
    if not folder_id:
        raise ValueError("folder_id is required")
    if not curation_task_name:
        raise ValueError("curation_task_name is required")
    if not instructions:
        raise ValueError("instructions is required")
    validate_column_order_list(column_order)

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
        entity_view = _create_json_schema_entity_view(
            syn=synapse_client,
            synapse_entity_id=folder_id,
            entity_view_name=entity_view_name,
            view_type_mask=view_type_mask,
            column_order=column_order,
        )
        entity_view_id = entity_view.id
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
    try:
        entity = get(folder_id, synapse_client=synapse_client)
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

    project_id = project_id_from_entity_id(folder_id, synapse_client=synapse_client)
    synapse_client.logger.info("Got the Synapse ID of the provided folders project.")

    synapse_client.logger.info("Attempting to create the CurationTask.")
    try:
        task = CurationTask(
            data_type=task_datatype,
            project_id=project_id,
            instructions=instructions,
            assignee_principal_id=(
                str(assignee_principal_id)
                if assignee_principal_id is not None
                else None
            ),
            task_properties=FileBasedMetadataTaskProperties(
                upload_folder_id=folder_id,
                file_view_id=entity_view_id,
                suggested_authorization_mode=authorization_mode,
            ),
        ).store(synapse_client=synapse_client)
    except Exception as e:
        synapse_client.logger.exception("Error creating the CurationTask.")
        raise e
    synapse_client.logger.info("Created the CurationTask.")

    # TODO: https://sagebionetworks.jira.com/browse/SYNPY-1865
    # In v5.0.0 remove this warning and the ID-tuple return below; return
    # (entity_view, task) unconditionally.

    if return_entities:
        return (entity_view, task)

    synapse_client.logger.warning(
        "create_file_based_metadata_task will return the created EntityView and "
        "CurationTask objects instead of their ID strings starting in v5.0.0. Pass "
        "return_entities=True to opt in to the new return type early and silence this "
        "warning."
    )
    return (entity_view_id, task.task_id)
