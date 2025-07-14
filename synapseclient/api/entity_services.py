"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.EntityController>
"""

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from async_lru import alru_cache

from synapseclient.core.utils import get_synid_and_version

if TYPE_CHECKING:
    from synapseclient import Synapse


@dataclass
class EntityHeader:
    """
    JSON schema for EntityHeader POJO. This represents metadata about a Synapse entity.

    Attributes:
        name: The name of the entity
        id: The id of the entity
        type: The type of the entity
        version_number: The version number of the entity
        version_label: The user defined version label of the entity
        is_latest_version: If this version is the latest version of the entity
        benefactor_id: The ID of the entity that this Entity's ACL is inherited from
        created_on: The date this entity was created
        modified_on: The date this entity was last modified
        created_by: The ID of the user that created this entity
        modified_by: The ID of the user that last modified this entity
    """

    name: Optional[str] = None
    """The name of the entity"""

    id: Optional[str] = None
    """The id of the entity"""

    type: Optional[str] = None
    """The type of the entity"""

    version_number: Optional[int] = None
    """The version number of the entity"""

    version_label: Optional[str] = None
    """The user defined version label of the entity"""

    is_latest_version: Optional[bool] = None
    """If this version is the latest version of the entity"""

    benefactor_id: Optional[int] = None
    """The ID of the entity that this Entity's ACL is inherited from"""

    created_on: Optional[str] = None
    """The date this entity was created"""

    modified_on: Optional[str] = None
    """The date this entity was last modified"""

    created_by: Optional[str] = None
    """The ID of the user that created this entity"""

    modified_by: Optional[str] = None
    """The ID of the user that last modified this entity"""

    def fill_from_dict(self, synapse_response: Dict[str, Any]) -> "EntityHeader":
        """Converts a response from the REST API into this dataclass."""
        self.name = synapse_response.get("name", None)
        self.id = synapse_response.get("id", None)
        self.type = synapse_response.get("type", None)
        self.version_number = synapse_response.get("versionNumber", None)
        self.version_label = synapse_response.get("versionLabel", None)
        self.is_latest_version = synapse_response.get("isLatestVersion", None)
        self.benefactor_id = synapse_response.get("benefactorId", None)
        self.created_on = synapse_response.get("createdOn", None)
        self.modified_on = synapse_response.get("modifiedOn", None)
        self.created_by = synapse_response.get("createdBy", None)
        self.modified_by = synapse_response.get("modifiedBy", None)
        return self


async def post_entity(
    request: Dict[str, Any],
    generated_by: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Arguments:
        request: The request for the entity matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
        generated_by: The ID of the activity to associate with the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The requested entity matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    params = {}
    if generated_by:
        params["generatedBy"] = generated_by
    return await client.rest_post_async(
        uri="/entity", body=json.dumps(request), params=params
    )


async def put_entity(
    entity_id: str,
    request: Dict[str, Any],
    new_version: bool = False,
    generated_by: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Arguments:
        entity_id: The ID of the entity to update.
        request: The request for the entity matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
        new_version: If true, a new version of the entity will be created.
        generated_by: The ID of the activity to associate with the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The requested entity bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    params = {}
    if generated_by:
        params["generatedBy"] = generated_by
    if new_version:
        params["newVersion"] = "true"
    return await client.rest_put_async(
        uri=f"/entity/{entity_id}", body=json.dumps(request), params=params
    )


async def get_entity(
    entity_id: str,
    version_number: int = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The requested entity bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    if version_number:
        return await client.rest_get_async(
            uri=f"/entity/{entity_id}/version/{version_number}",
        )
    else:
        return await client.rest_get_async(
            uri=f"/entity/{entity_id}",
        )


@alru_cache(ttl=60)
async def get_upload_destination(
    entity_id: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Union[str, int]]:
    """
    <https://rest-docs.synapse.org/rest/GET/entity/id/uploadDestination.html>

    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The upload destination.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/UploadDestination.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}/uploadDestination",
        endpoint=client.fileHandleEndpoint,
    )


async def get_upload_destination_location(
    entity_id: str, location: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Union[str, int]]:
    """
    <https://rest-docs.synapse.org/rest/GET/entity/id/uploadDestination/storageLocationId.html>

    Arguments:
        entity_id: The ID of the entity.
        location: A storage location ID of the upload destination.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The upload destination.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/UploadDestination.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}/uploadDestination/{location}",
        endpoint=client.fileHandleEndpoint,
    )


async def create_access_requirements_if_none(
    entity_id: str, *, synapse_client: Optional["Synapse"] = None
) -> None:
    """
    Checks to see if the given entity has access requirements. If not, then one is added

    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    existing_restrictions = await client.rest_get_async(
        f"/entity/{entity_id}/accessRequirement?offset=0&limit=1"
    )
    if (
        existing_restrictions is None
        or not hasattr(existing_restrictions, "results")
        or len(existing_restrictions["results"]) == 0
    ):
        access_requirements = await client.rest_post_async(
            f"/entity/{entity_id}/lockAccessRequirement"
        )
        client.logger.info(
            "Created an access requirements request for "
            f"{entity_id}: {access_requirements['jiraKey']}. An email will be sent to "
            "the Synapse access control team to start the process of adding "
            "terms-of-use or review board approval for this entity."
        )


async def delete_entity_generated_by(
    entity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns: None
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_delete_async(
        uri=f"/entity/{entity_id}/generatedBy",
    )


async def delete_entity(
    entity_id: str,
    version_number: int = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Deletes an entity from Synapse.

    Arguments:
        entity_id: The ID of the entity. This may include version `syn123.0` or `syn123`.
            If the version is included in `entity_id` and `version_number` is also
            passed in, then the version in `entity_id` will be used.
        version_number: The version number of the entity to delete.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Example: Delete the entity `syn123`:
        This will delete all versions of the entity.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import delete_entity

        syn = Synapse()
        syn.login()


        async def main():
            await delete_entity(entity_id="syn123")

        asyncio.run(main())
        ```

    Example: Delete a specific version of the entity `syn123`:
        This will delete version `3` of the entity.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import delete_entity

        syn = Synapse()
        syn.login()


        async def main():
            await delete_entity(entity_id="syn123", version_number=3)

        asyncio.run(main())
        ```

    Returns: None
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    syn_id, syn_version = get_synid_and_version(entity_id)
    if not syn_version:
        syn_version = version_number

    if syn_version:
        return await client.rest_delete_async(
            uri=f"/entity/{syn_id}/version/{syn_version}",
        )
    else:
        return await client.rest_delete_async(
            uri=f"/entity/{syn_id}",
        )


async def delete_entity_acl(
    entity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Delete the Access Control List (ACL) for a given Entity.

    By default, Entities such as FileEntity and Folder inherit their permission from
    their containing Project. For such Entities the Project is the Entity's 'benefactor'.
    This permission inheritance can be overridden by creating an ACL for the Entity.
    When this occurs the Entity becomes its own benefactor and all permission are
    determined by its own ACL.

    If the ACL of an Entity is deleted, then its benefactor will automatically be set
    to its parent's benefactor. The ACL for a Project cannot be deleted.

    Note: The caller must be granted ACCESS_TYPE.CHANGE_PERMISSIONS on the Entity to
    call this method.

    Arguments:
        entity_id: The ID of the entity that should have its ACL deleted.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Example: Delete the ACL for entity `syn123`:
        This will delete the ACL for the entity, making it inherit permissions from
        its parent.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import delete_entity_acl

        syn = Synapse()
        syn.login()

        async def main():
            await delete_entity_acl(entity_id="syn123")

        asyncio.run(main())
        ```

    Returns: None
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_delete_async(
        uri=f"/entity/{entity_id}/acl",
    )


async def get_entity_acl(
    entity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, List[Dict[str, Union[int, List[str]]]]]]:
    """
    Get the Access Control List (ACL) for an entity.

    Note: If this method is called on an Entity that is inheriting its permission
    from another Entity a NOT_FOUND (404) response will be generated. The error
    response message will include the Entity's benefactor ID.

    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}/acl",
    )


async def get_entity_benefactor(
    entity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> EntityHeader:
    """
    Get an Entity's benefactor. An Entity gets its ACL from its benefactor.

    Implements:
    <https://rest-docs.synapse.org/rest/GET/entity/id/benefactor.html>

    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The EntityHeader of the entity's benefactor (the entity from which it inherits its ACL).

    Example: Get the benefactor of an entity
        Get the benefactor entity header for entity `syn123`.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_entity_benefactor

        syn = Synapse()
        syn.login()

        async def main():
            benefactor = await get_entity_benefactor(entity_id="syn123")
            print(f"Entity benefactor: {benefactor.name} (ID: {benefactor.id})")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    response = await client.rest_get_async(
        uri=f"/entity/{entity_id}/benefactor",
    )

    entity_header = EntityHeader()
    return entity_header.fill_from_dict(response)


async def get_entity_path(
    entity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, List[Dict[str, Union[str, int, bool]]]]:
    """
    Implements:
    <https://rest-docs.synapse.org/rest/GET/entity/id/path.html>

    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        Entity paths matching:
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityPath.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}/path",
    )


async def get_entities_by_md5(
    md5: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[int, List[Dict[str, Any]]]]:
    """
    Implements:
    <https://rest-docs.synapse.org/rest/GET/entity/md5/md5.html>

    Arguments:
        md5: The MD5 of the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        Paginated results of:
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/reflection/model/PaginatedResults.html>
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityHeader.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/md5/{md5}",
    )
