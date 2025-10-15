"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.EntityController>
"""

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, List, Optional, Union

from async_lru import alru_cache

from synapseclient.api.api_client import rest_post_paginated_async
from synapseclient.core.exceptions import SynapseHTTPError
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

    Returns:
        A dictionary of the Entity's ACL.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/AccessControlList.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}/acl",
    )


async def get_entity_acl_with_benefactor(
    entity_id: str,
    check_benefactor: bool = True,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, List[Dict[str, Union[int, List[str]]]]]]:
    """
    Get the effective Access Control List (ACL) for a Synapse Entity.

    Arguments:
        entity_id: The ID of the entity.
        check_benefactor: If True (default), check the benefactor for the entity
                         to get the ACL. If False, only check the entity itself.
                         This is useful for checking the ACL of an entity that has local sharing
                         settings, but you want to check the ACL of the entity itself and not
                         the benefactor it may inherit from.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        A dictionary of the Entity's ACL.
        https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/AccessControlList.html
        If the entity does not have its own ACL and check_benefactor is False,
        returns {"resourceAccess": []}.

    Example: Get ACL with benefactor checking
        Get the effective ACL for entity `syn123`.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_entity_acl_with_benefactor

        syn = Synapse()
        syn.login()

        async def main():
            # Get ACL from benefactor (default behavior)
            acl = await get_entity_acl_with_benefactor(entity_id="syn123")
            print(f"ACL from benefactor: {acl}")

            # Get ACL from entity only
            acl = await get_entity_acl_with_benefactor(
                entity_id="syn123",
                check_benefactor=False
            )
            print(f"ACL from entity only: {acl}")

        asyncio.run(main())
        ```
    """
    if check_benefactor:
        # Get the ACL from the benefactor (which may be the entity itself)
        benefactor = await get_entity_benefactor(
            entity_id=entity_id, synapse_client=synapse_client
        )
        return await get_entity_acl(
            entity_id=benefactor.id, synapse_client=synapse_client
        )
    else:
        try:
            return await get_entity_acl(
                entity_id=entity_id, synapse_client=synapse_client
            )
        except SynapseHTTPError as e:
            # If entity doesn't have its own ACL and check_benefactor is False,
            # return empty ACL structure indicating no local permissions
            if (
                "The requested ACL does not exist. This entity inherits its permissions from:"
                in str(e)
            ):
                return {"resourceAccess": []}
            raise e


async def put_entity_acl(
    entity_id: str,
    acl: Dict[str, Union[str, List[Dict[str, Union[int, List[str]]]]]],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, List[Dict[str, Union[int, List[str]]]]]]:
    """
    Update the Access Control List (ACL) for an entity.

    API Matches <https://rest-docs.synapse.org/rest/PUT/entity/id/acl.html>.

    Note: The caller must be granted `CHANGE_PERMISSIONS` on the Entity to call this method.

    Arguments:
        entity_id: The ID of the entity.
        acl: The ACL to set for the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The updated ACL matching
        https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/AccessControlList.html

    Example: Update ACL for an entity
        Update the ACL for entity `syn123`.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import put_entity_acl

        syn = Synapse()
        syn.login()

        async def main():
            acl = {
                "id": "syn123",
                "etag": "12345",
                "resourceAccess": [
                    {
                        "principalId": 12345,
                        "accessType": ["READ", "DOWNLOAD"]
                    }
                ]
            }
            updated_acl = await put_entity_acl(entity_id="syn123", acl=acl)
            print(f"Updated ACL: {updated_acl}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri=f"/entity/{entity_id}/acl",
        body=json.dumps(acl),
    )


async def post_entity_acl(
    entity_id: str,
    acl: Dict[str, Union[str, List[Dict[str, Union[int, List[str]]]]]],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, List[Dict[str, Union[int, List[str]]]]]]:
    """
    Create a new Access Control List (ACL) for an entity.

    API Matches <https://rest-docs.synapse.org/rest/POST/entity/id/acl.html>.

    Note: The caller must be granted `CHANGE_PERMISSIONS` on the Entity to call this method.

    Arguments:
        entity_id: The ID of the entity.
        acl: The ACL to create for the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The created ACL matching
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/AccessControlList.html>.

    Example: Create ACL for an entity
        Create a new ACL for entity `syn123`.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import post_entity_acl

        syn = Synapse()
        syn.login()

        async def main():
            acl = {
                "id": "syn123",
                "etag": "12345",
                "resourceAccess": [
                    {
                        "principalId": 12345,
                        "accessType": ["READ", "DOWNLOAD"]
                    }
                ]
            }
            created_acl = await post_entity_acl(entity_id="syn123", acl=acl)
            print(f"Created ACL: {created_acl}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri=f"/entity/{entity_id}/acl",
        body=json.dumps(acl),
    )


async def get_entity_permissions(
    entity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, List[str], bool]]:
    """
    Get the permissions that the caller has on an Entity.

    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        A dictionary containing the permissions that the caller has on the entity.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/auth/UserEntityPermissions.html>

    Example: Get permissions for an entity
        Get the permissions that the caller has on entity `syn123`.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_entity_permissions

        syn = Synapse()
        syn.login()

        async def main():
            permissions = await get_entity_permissions(entity_id="syn123")
            print(f"Permissions: {permissions}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}/permissions",
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


async def get_entity_type(
    entity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> EntityHeader:
    """
    Get the EntityHeader of an Entity given its ID. The EntityHeader is a light weight
    object with basic information about an Entity includes its type.

    Implements:
    <https://rest-docs.synapse.org/rest/GET/entity/id/type.html>

    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        EntityHeader object containing basic information about the entity.

    Example: Get entity type information
        Get the EntityHeader for entity `syn123`.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_entity_type

        syn = Synapse()
        syn.login()

        async def main():
            entity_header = await get_entity_type(entity_id="syn123")
            print(f"Entity type: {entity_header.type}")
            print(f"Entity name: {entity_header.name}")
            print(f"Entity ID: {entity_header.id}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    response = await client.rest_get_async(
        uri=f"/entity/{entity_id}/type",
    )

    entity_header = EntityHeader()
    return entity_header.fill_from_dict(response)


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


async def get_entity_provenance(
    entity_id: str,
    version_number: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Retrieve provenance information for a Synapse Entity.

    Arguments:
        entity_id: The ID of the entity. This may include version `syn123.0` or `syn123`.
            If the version is included in `entity_id` and `version_number` is also
            passed in, then the version in `entity_id` will be used.
        version_number: The version of the Entity to retrieve. Gets the most recent version if omitted.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        Activity object as a dictionary or raises exception if no provenance record exists.

    Example: Get provenance for an entity
        Get the provenance information for entity `syn123`.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_entity_provenance

        syn = Synapse()
        syn.login()

        async def main():
            activity = await get_entity_provenance(entity_id="syn123")
            print(f"Activity: {activity}")

        asyncio.run(main())
        ```

    Example: Get provenance for a specific version
        Get the provenance information for version 3 of entity `syn123`.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_entity_provenance

        syn = Synapse()
        syn.login()

        async def main():
            activity = await get_entity_provenance(entity_id="syn123", version_number=3)
            print(f"Activity: {activity}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    syn_id, syn_version = get_synid_and_version(entity_id)
    if not syn_version:
        syn_version = version_number

    if syn_version:
        uri = f"/entity/{syn_id}/version/{syn_version}/generatedBy"
    else:
        uri = f"/entity/{syn_id}/generatedBy"

    return await client.rest_get_async(uri=uri)


async def set_entity_provenance(
    entity_id: str,
    activity: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Stores a record of the code and data used to derive a Synapse entity.

    Arguments:
        entity_id: The ID of the entity.
        activity: A dictionary representing an Activity object.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        An updated Activity object as a dictionary.

    Example: Set provenance for an entity
        Set the provenance for entity `syn123` with an activity.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import set_entity_provenance, create_activity

        syn = Synapse()
        syn.login()

        async def main():
            # First create or get an activity
            activity = await create_activity({
                "name": "Analysis Step",
                "description": "Data processing step"
            })

            # Set the provenance
            updated_activity = await set_entity_provenance(
                entity_id="syn123",
                activity=activity
            )
            print(f"Updated activity: {updated_activity}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    if "id" in activity:
        saved_activity = await update_activity(activity, synapse_client=synapse_client)
    else:
        saved_activity = await create_activity(activity, synapse_client=synapse_client)

    uri = f"/entity/{entity_id}/generatedBy?generatedBy={saved_activity['id']}"
    return await client.rest_put_async(uri=uri)


async def delete_entity_provenance(
    entity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Removes provenance information from an Entity and deletes the associated Activity.

    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Example: Delete provenance for an entity
        Delete the provenance for entity `syn123`.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import delete_entity_provenance

        syn = Synapse()
        syn.login()

        async def main():
            await delete_entity_provenance(entity_id="syn123")

        asyncio.run(main())
        ```

    Returns: None
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    try:
        activity = await get_entity_provenance(entity_id, synapse_client=synapse_client)
    except SynapseHTTPError:
        # If no provenance exists, nothing to delete
        return

    if not activity:
        return

    await client.rest_delete_async(uri=f"/entity/{entity_id}/generatedBy")

    # If the activity is shared by more than one entity you recieve an HTTP 400 error:
    # "If you wish to delete this activity, please first delete all Entities generated by this Activity.""
    await client.rest_delete_async(uri=f"/activity/{activity['id']}")


async def create_activity(
    activity: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Create a new Activity in Synapse.

    Arguments:
        activity: A dictionary representing an Activity object.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The created Activity object as a dictionary.

    Example: Create a new activity
        Create a new activity in Synapse.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import create_activity

        syn = Synapse()
        syn.login()

        async def main():
            activity = await create_activity({
                "name": "Data Analysis",
                "description": "Processing raw data"
            })
            print(f"Created activity: {activity}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(uri="/activity", body=json.dumps(activity))


async def update_activity(
    activity: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Modifies an existing Activity.

    Arguments:
        activity: The Activity to be updated. Must contain an 'id' field.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        An updated Activity object as a dictionary.

    Raises:
        ValueError: If the activity does not contain an 'id' field.

    Example: Update an existing activity
        Update an existing activity in Synapse.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import update_activity

        syn = Synapse()
        syn.login()

        async def main():
            activity = {
                "id": "12345",
                "name": "Updated Analysis",
                "description": "Updated processing step"
            }
            updated_activity = await update_activity(activity)
            print(f"Updated activity: {updated_activity}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    if "id" not in activity:
        raise ValueError("The activity you want to update must exist on Synapse")

    client = Synapse.get_client(synapse_client=synapse_client)
    uri = f"/activity/{activity['id']}"
    return await client.rest_put_async(uri=uri, body=json.dumps(activity))


async def get_activity(
    activity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Retrieve an Activity by its ID.

    Arguments:
        activity_id: The ID of the activity to retrieve.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        Activity object as a dictionary.

    Example: Get activity by ID
        Retrieve an activity using its ID.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_activity

        syn = Synapse()
        syn.login()

        async def main():
            activity = await get_activity(activity_id="12345")
            print(f"Activity: {activity}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/activity/{activity_id}")


async def get_children(
    parent: Optional[str] = None,
    include_types: List[str] = None,
    sort_by: str = "NAME",
    sort_direction: str = "ASC",
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Retrieve all entities stored within a parent such as folder or project.

    Arguments:
        parent: The ID of a Synapse container (folder or project) or None to retrieve all projects
        include_types: List of entity types to include (e.g., ["folder", "file"]).
                      Available types can be found at:
                      https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityType.html
        sort_by: How results should be sorted. Can be "NAME" or "CREATED_ON"
        sort_direction: The direction of the result sort. Can be "ASC" or "DESC"
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor.

    Yields:
        An async generator that yields entity children dictionaries.

    Example: Getting children of a folder
        Retrieve all children of a folder:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_children

        syn = Synapse()
        syn.login()

        async def main():
            async for child in get_children(parent="syn123456"):
                print(f"Child: {child['name']} (ID: {child['id']})")

        asyncio.run(main())
        ```

    Example: Getting children with specific types
        Retrieve only files and folders:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_children

        syn = Synapse()
        syn.login()

        async def main():
            async for child in get_children(
                parent="syn123456",
                include_types=["file", "folder"],
                sort_by="NAME",
                sort_direction="ASC"
            ):
                print(f"Child: {child['name']} (Type: {child['type']})")

        asyncio.run(main())
        ```
    """
    if include_types is None:
        include_types = [
            "folder",
            "file",
            "table",
            "link",
            "entityview",
            "dockerrepo",
            "submissionview",
            "dataset",
            "materializedview",
        ]

    request_body = {
        "parentId": parent,
        "includeTypes": include_types,
        "sortBy": sort_by,
        "sortDirection": sort_direction,
    }

    response = rest_post_paginated_async(
        uri="/entity/children",
        body=request_body,
        synapse_client=synapse_client,
    )

    async for child in response:
        yield child


async def get_child(
    entity_name: str,
    parent_id: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Optional[str]:
    """
    Retrieve an entityId for a given parent ID and entity name.

    This service can also be used to lookup projectId by setting the parentId to None.

    This calls to the REST API found here: <https://rest-docs.synapse.org/rest/POST/entity/child.html>

    Arguments:
        entity_name: The name of the entity to find
        parent_id: The parent ID. Set to None when looking up a project by name.
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor.

    Returns:
        The entity ID if found, None if not found.

    Raises:
        SynapseHTTPError: If there's an error other than "not found" (404).

    Example: Getting a child entity ID
        Find a file by name within a folder:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_child

        syn = Synapse()
        syn.login()

        async def main():
            entity_id = await get_child(
                entity_name="my_file.txt",
                parent_id="syn123456"
            )
            if entity_id:
                print(f"Found entity: {entity_id}")
            else:
                print("Entity not found")

        asyncio.run(main())
        ```

    Example: Getting a project by name
        Find a project by name:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_child

        syn = Synapse()
        syn.login()

        async def main():
            project_id = await get_child(
                entity_name="My Project",
                parent_id=None  # None for projects
            )
            if project_id:
                print(f"Found project: {project_id}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    entity_lookup_request = {
        "parentId": parent_id,
        "entityName": entity_name,
    }

    try:
        response = await client.rest_post_async(
            uri="/entity/child", body=json.dumps(entity_lookup_request)
        )
        return response.get("id")
    except SynapseHTTPError as e:
        if e.response.status_code == 404:
            # Entity not found
            return None
        raise


async def set_entity_permissions(
    entity_id: str,
    principal_id: Optional[str] = None,
    access_type: Optional[List[str]] = None,
    modify_benefactor: bool = False,
    warn_if_inherits: bool = True,
    overwrite: bool = True,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, List[Dict[str, Union[int, List[str]]]]]]:
    """
    Set permissions for a user or group on an entity.

    Arguments:
        entity_id: The ID of the entity.
        principal_id: Identifier of a user or group. '273948' is for all registered Synapse users
                     and '273949' is for public access. None implies public access.
        access_type: Type of permission to be granted. One or more of CREATE, READ, DOWNLOAD, UPDATE,
                    DELETE, CHANGE_PERMISSIONS. If None or empty list, removes permissions.
        modify_benefactor: Set as True when modifying a benefactor's ACL.
        warn_if_inherits: When modify_benefactor is False, and warn_if_inherits is True,
                         a warning log message is produced if the benefactor for the entity
                         you passed into the function is not itself.
        overwrite: By default this function overwrites existing permissions for the specified user.
                  Set this flag to False to add new permissions non-destructively.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The updated ACL matching
        https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/AccessControlList.html

    Example: Set permissions for an entity
        Grant all registered users download access.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import set_entity_permissions

        syn = Synapse()
        syn.login()

        async def main():
            # Grant all registered users download access
            acl = await set_entity_permissions(
                entity_id="syn123",
                principal_id="273948",
                access_type=["READ", "DOWNLOAD"]
            )
            print(f"Updated ACL: {acl}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Get the benefactor for the entity
    benefactor = await get_entity_benefactor(entity_id, synapse_client=synapse_client)

    if benefactor.id != entity_id:
        if modify_benefactor:
            entity_id = benefactor.id
        elif warn_if_inherits:
            client.logger.warning(
                f"Creating an ACL for entity {entity_id}, which formerly inherited access control from a"
                f' benefactor entity, "{benefactor.name}" ({benefactor.id}).'
            )

    try:
        acl = await get_entity_acl_with_benefactor(
            entity_id=entity_id, synapse_client=synapse_client
        )
    except SynapseHTTPError as e:
        if (
            "The requested ACL does not exist. This entity inherits its permissions from:"
            in str(e)
        ):
            acl = {"resourceAccess": []}
        else:
            raise e

    # Get the principal ID as an integer
    from synapseclient.api.user_services import get_user_by_principal_id_or_name

    principal_id_int = await get_user_by_principal_id_or_name(
        principal_id=principal_id, synapse_client=synapse_client
    )

    # Find existing permissions for this principal
    permissions_to_update = None
    for permissions in acl["resourceAccess"]:
        if (
            "principalId" in permissions
            and permissions["principalId"] == principal_id_int
        ):
            permissions_to_update = permissions
            break

    if access_type is None or access_type == []:
        # Remove permissions
        if permissions_to_update and overwrite:
            acl["resourceAccess"].remove(permissions_to_update)
    else:
        # Add or update permissions
        if not permissions_to_update:
            permissions_to_update = {"accessType": [], "principalId": principal_id_int}
            acl["resourceAccess"].append(permissions_to_update)

        if overwrite:
            permissions_to_update["accessType"] = access_type
        else:
            permissions_to_update["accessType"] = list(
                set(permissions_to_update["accessType"]) | set(access_type)
            )

    benefactor_for_store = await get_entity_benefactor(
        entity_id, synapse_client=synapse_client
    )

    if benefactor_for_store.id == entity_id:
        # Entity is its own benefactor, use PUT
        return await put_entity_acl(
            entity_id=entity_id, acl=acl, synapse_client=synapse_client
        )
    else:
        # Entity inherits ACL, use POST to create new ACL
        return await post_entity_acl(
            entity_id=entity_id, acl=acl, synapse_client=synapse_client
        )


async def get_entity_acl_list(
    entity_id: str,
    principal_id: Optional[str] = None,
    check_benefactor: bool = True,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[str]:
    """
    Get a list of permissions for a user or group on an entity.

    Arguments:
        entity_id: The ID of the entity.
        principal_id: Identifier of a user or group to check permissions for.
                     If None, returns permissions for the current user.
        check_benefactor: If True (default), check the benefactor for the entity
                         to get the ACL. If False, only check the entity itself.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        A list of access types that the specified principal has on the entity.

    Example: Get ACL list for a user
        Get the permissions that a user has on an entity.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import get_entity_acl_list

        syn = Synapse()
        syn.login()

        async def main():
            # Get permissions for current user
            permissions = await get_entity_acl_list(entity_id="syn123")
            print(f"Current user permissions: {permissions}")

            # Get permissions for specific user
            permissions = await get_entity_acl_list(
                entity_id="syn123",
                principal_id="12345"
            )
            print(f"User 12345 permissions: {permissions}")

        asyncio.run(main())
        ```
    """
    from synapseclient import AUTHENTICATED_USERS, PUBLIC
    from synapseclient.api.team_services import get_teams_for_user
    from synapseclient.api.user_services import (
        get_user_bundle,
        get_user_by_principal_id_or_name,
    )

    # Get the ACL for the entity
    acl = await get_entity_acl_with_benefactor(
        entity_id=entity_id,
        check_benefactor=check_benefactor,
        synapse_client=synapse_client,
    )

    # Get the principal ID as an integer (None defaults to PUBLIC)
    principal_id_int = await get_user_by_principal_id_or_name(
        principal_id=principal_id, synapse_client=synapse_client
    )

    # Get teams that the user belongs to
    team_ids = []
    async for team in get_teams_for_user(
        user_id=str(principal_id_int), synapse_client=synapse_client
    ):
        team_ids.append(int(team["id"]))

    user_profile_bundle = await get_user_bundle(
        user_id=principal_id_int, mask=1, synapse_client=synapse_client
    )

    effective_permission_set = set()

    # Loop over all permissions in the returned ACL and add it to the effective_permission_set
    # if the principalId in the ACL matches
    # 1) the one we are looking for,
    # 2) a team the user is a member of,
    # 3) PUBLIC
    # 4) AUTHENTICATED_USERS (if user_profile_bundle exists for the principal_id)
    for permissions in acl["resourceAccess"]:
        if "principalId" in permissions and (
            permissions["principalId"] == principal_id_int
            or permissions["principalId"] in team_ids
            or permissions["principalId"] == PUBLIC
            or (
                permissions["principalId"] == AUTHENTICATED_USERS
                and user_profile_bundle is not None
            )
        ):
            effective_permission_set = effective_permission_set.union(
                permissions["accessType"]
            )
    return list(effective_permission_set)


async def update_entity_acl(
    entity_id: str,
    acl: Dict[str, Union[str, List[Dict[str, Union[int, List[str]]]]]],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, List[Dict[str, Union[int, List[str]]]]]]:
    """
    Create or update the Access Control List(ACL) for an entity.

    Arguments:
        entity_id: The ID of the entity.
        acl: The ACL to be applied to the entity. Should match the format:
            {'resourceAccess': [
                {'accessType': ['READ', 'DOWNLOAD'],
                 'principalId': 222222}
            ]}
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The created or updated ACL matching
        https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/AccessControlList.html

    Example: Update entity ACL
        Update the ACL for entity `syn123`.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import update_entity_acl

        syn = Synapse()
        syn.login()

        async def main():
            acl = {
                'resourceAccess': [
                    {'accessType': ['READ', 'DOWNLOAD'],
                     'principalId': 273948}
                ]
            }
            updated_acl = await update_entity_acl(entity_id="syn123", acl=acl)
            print(f"Updated ACL: {updated_acl}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Get the benefactor to determine whether to use PUT or POST
    benefactor = await get_entity_benefactor(
        entity_id=entity_id, synapse_client=synapse_client
    )

    uri = f"/entity/{entity_id}/acl"
    if benefactor.id == entity_id:
        # Entity is its own benefactor, use PUT to update existing ACL
        return await client.rest_put_async(uri=uri, body=json.dumps(acl))
    else:
        # Entity inherits from a benefactor, use POST to create new ACL
        return await client.rest_post_async(uri=uri, body=json.dumps(acl))
