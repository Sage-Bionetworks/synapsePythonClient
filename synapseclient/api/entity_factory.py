"""Factory type functions to create and retrieve entities from Synapse"""

import os
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from opentelemetry import trace

from synapseclient.api.entity_bundle_services_v2 import (
    get_entity_id_bundle2,
    get_entity_id_version_bundle2,
)
from synapseclient.api.entity_services import get_entities_by_md5, get_entity_path
from synapseclient.core import utils
from synapseclient.core.constants import concrete_types
from synapseclient.core.download import download_file_entity_model
from synapseclient.core.exceptions import (
    SynapseFileNotFoundError,
    SynapseUnmetAccessRestrictions,
)

if TYPE_CHECKING:
    from models.entityview import EntityView

    from synapseclient import Synapse
    from synapseclient.models import Dataset, File, Folder, Project, Table


async def get_from_entity_factory(
    synapse_id_or_path: str,
    version: int = None,
    if_collision: str = "keep.both",
    limit_search: str = None,
    md5: str = None,
    download_file: bool = True,
    download_location: str = None,
    follow_link: bool = False,
    entity_to_update: Union[
        "Project", "File", "Folder", "Table", "Dataset", "EntityView"
    ] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Union["Project", "File", "Folder"]:
    """
    Factory type function to retrieve an entity from Synapse. Optionally you may also
    pass in `entity_to_update` if you want to update the fields on the existing entity
    instead of creating a new instance.

    Arguments:
        synapse_id_or_path: The Synapse ID or file path of the entity to retrieve.
        version:            The version number of the entity to retrieve.
        if_collision: Determines how to handle file collisions. May be:

                - `overwrite.local`
                - `keep.local`
                - `keep.both`
        limit_search:       Limit the search to a specific project or folder. Only used
            if `synapse_id_or_path` is a path.
        md5: The MD5 of the file to retrieve. If not passed in, the MD5 will be
            calculated. Only used if `synapse_id_or_path` is a path.
        download_file: Whether associated files should be downloaded.
        download_location: The directory to download the file to.
        follow_link: Whether to follow a link to its target. This will only do a single
            hop to the target of the link.
        entity_to_update: An existing entity class instance to update with data from
            Synapse.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Example: Using this function
            Download file into cache

                from synapseclient import Synapse
                from synapseclient.api import get_from_entity_factory

                syn = Synapse()
                syn.login()

                entity = await get_from_entity_factory(synapse_id_or_path='syn1906479')
                print(entity.name)
                print(entity.path)

            Download file into current working directory

                from synapseclient import Synapse
                from synapseclient.api import get_from_entity_factory

                syn = Synapse()
                syn.login()

                entity = await get_from_entity_factory(synapse_id_or_path='syn1906479', download_location='.')
                print(entity.name)
                print(entity.path)

    Raises:
        SynapseFileNotFoundError: If the id is not a synapse ID and it is not a valid
            file path.
    """

    # If entity is a local file determine the corresponding synapse entity
    if isinstance(synapse_id_or_path, str) and os.path.isfile(synapse_id_or_path):
        bundle = await _search_for_file_by_md5(
            filepath=synapse_id_or_path,
            limit_search=limit_search,
            md5=md5,
            synapse_client=synapse_client,
        )
        download_file = False

    elif isinstance(synapse_id_or_path, str) and not utils.is_synapse_id_str(
        obj=synapse_id_or_path
    ):
        raise SynapseFileNotFoundError(
            (
                f"The parameter {synapse_id_or_path} is neither a local file path "
                " or a valid entity id"
            )
        )
    else:
        synid_and_version = utils.get_synid_and_version(obj=synapse_id_or_path)
        version = version if version is not None else synid_and_version[1]
        if version:
            bundle = await get_entity_id_version_bundle2(
                entity_id=synid_and_version[0],
                version=version,
                synapse_client=synapse_client,
            )
        else:
            bundle = await get_entity_id_bundle2(
                entity_id=synid_and_version[0], synapse_client=synapse_client
            )

    # Check and warn for unmet access requirements
    _check_entity_restrictions(
        bundle=bundle,
        synapse_id=bundle["entity"]["id"],
        download_file=download_file,
        synapse_client=synapse_client,
    )

    return_data = await _cast_into_class_type(
        entity_to_update=entity_to_update,
        entity_bundle=bundle,
        download_file=download_file,
        download_location=download_location,
        if_collision=if_collision,
        follow_link=follow_link,
        synapse_client=synapse_client,
    )
    trace.get_current_span().set_attributes(
        {
            "synapse.id": return_data.id,
            "synapse.type": return_data.__class__.__name__,
        }
    )
    return return_data


async def _search_for_file_by_md5(
    filepath: str,
    md5: str,
    limit_search: str = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Handle using md5 for a local file to search through Synapse to find a match. By
    default this will search through every entity that you as a user have access to.
    However, you can limit the search to a specific project or folder by passing in the
    Synapse ID of the project or folder into the `limit_search` field.

    Arguments:
        filepath: The path to the file to search for.
        md5: The MD5 of the file to retrieve. If not passed in, the MD5 will be
            calculated.
        limit_search: Limit the search to a specific project or folder.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        A dictionary containing the entity bundle of the file found.

    Raises:
        SynapseFileNotFoundError: If the file is not found in Synapse.
    """
    from synapseclient import Synapse

    syn = Synapse.get_client(synapse_client=synapse_client)
    md5 = md5 or utils.md5_for_file_hex(filename=filepath)
    results = (
        await get_entities_by_md5(
            md5=md5,
            synapse_client=synapse_client,
        )
    )["results"]
    if limit_search is not None:
        # Go through and find the path of every entity found
        paths = [
            await get_entity_path(entity_id=ent["id"], synapse_client=synapse_client)
            for ent in results
        ]
        # Filter out all entities whose path does not contain limitSearch
        results = [
            ent
            for ent, path in zip(results, paths)
            if utils.is_in_path(id=limit_search, path=path)
        ]
    if len(results) == 0:  # None found
        raise SynapseFileNotFoundError(f"File {filepath} not found in Synapse")
    elif len(results) > 1:
        id_txts = "\n".join([f"{r['id']}.{r['versionNumber']}" for r in results])
        syn.logger.warning(
            f"\nThe file {filepath} is associated with many files in Synapse:\n{id_txts}\n"
            "You can limit to files in specific project or folder by setting the limitSearch to the"
            " synapse Id of the project or folder.\n"
            "Will use the first one returned: \n"
            f"{results[0]['id']} version {results[0]['versionNumber']}\n"
        )
    entity_id = results[0]["id"]
    entity_version = results[0]["versionNumber"]

    bundle = await get_entity_id_version_bundle2(
        entity_id=entity_id,
        version=entity_version,
        synapse_client=synapse_client,
    )
    syn.cache.add(
        file_handle_id=bundle["entity"]["dataFileHandleId"], path=filepath, md5=md5
    )

    return bundle


async def _handle_file_entity(
    entity_instance: "File",
    entity_bundle: Dict[str, Any],
    download_file: bool,
    download_location: str,
    if_collision: str,
    submission: str,
    synapse_client: "Synapse",
) -> "File":
    """Helper function to handle File entity specific logic."""
    from synapseclient.models import FileHandle

    entity_instance.fill_from_dict(
        synapse_file=entity_bundle["entity"], set_annotations=False
    )

    # Update entity with FileHandle metadata
    file_handle = next(
        (
            handle
            for handle in entity_bundle.get("fileHandles", [])
            if handle and handle["id"] == entity_instance.data_file_handle_id
        ),
        {},
    )

    entity_instance.file_handle = FileHandle().fill_from_dict(
        synapse_instance=file_handle
    )
    entity_instance._fill_from_file_handle()

    if download_file:
        if file_handle:
            await download_file_entity_model(
                download_location=download_location,
                file=entity_instance,
                if_collision=if_collision,
                submission=submission,
                synapse_client=synapse_client,
            )
        else:
            warning_message = (
                "WARNING: You have READ permission on this file entity but not DOWNLOAD "
                "permission. The file has NOT been downloaded."
            )
            synapse_client.logger.warning(
                "\n"
                + "!" * len(warning_message)
                + "\n"
                + warning_message
                + "\n"
                + "!" * len(warning_message)
                + "\n"
            )

    return entity_instance


async def _cast_into_class_type(
    entity_bundle: Dict[str, Any],
    download_file: bool = True,
    download_location: str = None,
    if_collision: str = None,
    submission: str = None,
    follow_link: bool = False,
    entity_to_update: Union["Project", "File", "Folder"] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Union["Project", "File", "Folder", "Table", "Dataset", "EntityView"]:
    """
    Take an entity_bundle returned from the Synapse API and cast it into the appropriate
    class type. This will also download the file if `download_file` is set to True.
    Additionally, if `entity_to_update` is passed in, the fields of the existing entity
    will be updated instead of creating a new instance. If the entity is a link and
    `follow_link` is set to True, the target entity will be retrieved.

    Arguments:
        entity_bundle: The entity bundle to cast into a class type.
        download_file: Whether associated files should be downloaded.
        download_location: The directory to download the file to.
        if_collision: Determines how to handle file collisions. May be:

                    - `overwrite.local`
                    - `keep.local`
                    - `keep.both`
        submission: The ID of the submission to which the entity belongs.
        follow_link: Whether to follow a link to its target. This will only do a single
            hop to the target of the link.
        entity_to_update: An existing entity class instance to update with data from
            Synapse.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        A Synapse entity object.

    Raises:
        ValueError: If the entity type is not supported.
    """
    from synapseclient import Synapse
    from synapseclient.models import (
        Annotations,
        Dataset,
        DatasetCollection,
        EntityView,
        File,
        Folder,
        MaterializedView,
        Project,
        SubmissionView,
        Table,
        VirtualTable,
    )

    syn = Synapse.get_client(synapse_client=synapse_client)

    # If Link, get target ID entity bundle
    if (
        entity_bundle["entity"]["concreteType"] == concrete_types.LINK_ENTITY
        and follow_link
    ):
        target_id = entity_bundle["entity"]["linksTo"]["targetId"]
        target_version = entity_bundle["entity"]["linksTo"].get("targetVersionNumber")
        entity_bundle = await get_entity_id_version_bundle2(
            entity_id=target_id, version=target_version, synapse_client=synapse_client
        )

    entity = entity_bundle["entity"]
    annotations = Annotations.from_dict(
        synapse_annotations=entity_bundle.get("annotations", None)
    )

    # Map concrete types to their corresponding classes
    ENTITY_TYPE_MAP = {
        concrete_types.PROJECT_ENTITY: Project,
        concrete_types.FOLDER_ENTITY: Folder,
        concrete_types.FILE_ENTITY: File,
        concrete_types.TABLE_ENTITY: Table,
        concrete_types.DATASET_ENTITY: Dataset,
        concrete_types.DATASET_COLLECTION_ENTITY: DatasetCollection,
        concrete_types.ENTITY_VIEW: EntityView,
        concrete_types.MATERIALIZED_VIEW: MaterializedView,
        concrete_types.SUBMISSION_VIEW: SubmissionView,
        concrete_types.VIRTUAL_TABLE: VirtualTable,
    }

    entity_class = ENTITY_TYPE_MAP.get(entity["concreteType"], None)
    if not entity_class:
        raise ValueError(
            f"Attempting to retrieve an unsupported entity type of {entity['concreteType']}."
        )

    # Create or use existing entity instance
    entity_instance = entity_to_update or entity_class()

    # Handle special case for File entities
    if entity["concreteType"] == concrete_types.FILE_ENTITY:
        entity_instance = await _handle_file_entity(
            entity_instance=entity_instance,
            entity_bundle=entity_bundle,
            download_file=download_file,
            download_location=download_location,
            if_collision=if_collision,
            submission=submission,
            synapse_client=syn,
        )
    else:
        # Handle all other entity types
        entity_instance.fill_from_dict(entity_bundle["entity"], set_annotations=False)

    if annotations:
        entity_instance.annotations = annotations

    return entity_instance


def _check_entity_restrictions(
    bundle: Dict[str, Any],
    synapse_id: str,
    download_file: bool,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Check and warn for unmet access requirements.

    Arguments:
        bundle: A Synapse entityBundle
        entity: A Synapse ID, a Synapse Entity object, a plain dictionary in which 'id' maps to a
                Synapse ID or a local file that is stored in Synapse (found by the file MD5)
        downloadFile: Whether associated files(s) should be downloaded.

    Raises:
        SynapseUnmetAccessRestrictions: Warning for unmet access requirements.
    """
    from synapseclient import Synapse

    syn = Synapse.get_client(synapse_client=synapse_client)

    restriction_information = bundle.get("restrictionInformation", None)
    if restriction_information and restriction_information.get(
        "hasUnmetAccessRequirement", None
    ):
        if not syn.credentials or not syn.credentials._token:
            warning_message = (
                "You have not provided valid credentials for authentication with Synapse."
                " Please provide an authentication token and use `synapseclient.login()` before your next attempt."
                " See https://python-docs.synapse.org/tutorials/authentication/ for more information."
            )
        else:
            warning_message = (
                "\nThis entity has access restrictions. Please visit the web page for this entity "
                f'(syn.onweb("{synapse_id}")). Look for the "Access" label and the lock icon underneath '
                'the file name. Click "Request Access", and then review and fulfill the file '
                "download requirement(s).\n"
            )
        if download_file and bundle.get("entityType") not in ("project", "folder"):
            raise SynapseUnmetAccessRestrictions(warning_message)
        syn.logger.warning(warning_message)
