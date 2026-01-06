"""Factory methods for Synapse utility operations."""

import json
import logging
from typing import TYPE_CHECKING, List, Optional, Union

from synapseclient.core.async_utils import wrap_async_to_sync

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models import (
        Dataset,
        DatasetCollection,
        EntityView,
        File,
        Folder,
        Link,
        MaterializedView,
        Project,
        RecordSet,
        SubmissionView,
        Table,
        VirtualTable,
    )


def find_entity_id(
    name: str,
    parent: Optional[
        Union[
            str,
            "Dataset",
            "DatasetCollection",
            "EntityView",
            "File",
            "Folder",
            "Link",
            "MaterializedView",
            "Project",
            "RecordSet",
            "SubmissionView",
            "Table",
            "VirtualTable",
        ]
    ] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Optional[str]:
    """
    Find an Entity given its name and parent.

    Arguments:
        name: Name of the entity to find.
        parent: An Entity object (Dataset, DatasetCollection, EntityView, File,
            Folder, Link, MaterializedView, Project, RecordSet, SubmissionView,
            Table, VirtualTable) or the Id of an entity as a string. Omit if
            searching for a Project by name.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The Entity ID or None if not found.

    Example: Finding a project by name
        Find a project using only its name:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import find_entity_id

        syn = Synapse()
        syn.login()

        project_id = find_entity_id(name="My Project")
        if project_id:
            print(f"Found project: {project_id}")
        else:
            print("Project not found")
        ```

    Example: Finding an entity within a parent
        Find a file within a specific folder:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import find_entity_id

        syn = Synapse()
        syn.login()

        file_id = find_entity_id(
            name="my_data.csv",
            parent="syn123456"  # Parent folder ID
        )
        if file_id:
            print(f"Found file: {file_id}")
        else:
            print("File not found in folder")
        ```

    Example: Using with entity objects
        Find an entity using an entity object as the parent:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Folder
        from synapseclient.operations import find_entity_id

        syn = Synapse()
        syn.login()

        parent_folder = Folder(id="syn123456")
        entity_id = find_entity_id(
            name="analysis_results.txt",
            parent=parent_folder
        )
        print(f"Entity ID: {entity_id}")
        ```
    """
    return wrap_async_to_sync(
        coroutine=find_entity_id_async(
            name=name,
            parent=parent,
            synapse_client=synapse_client,
        )
    )


async def find_entity_id_async(
    name: str,
    parent: Optional[
        Union[
            str,
            "Dataset",
            "DatasetCollection",
            "EntityView",
            "File",
            "Folder",
            "Link",
            "MaterializedView",
            "Project",
            "RecordSet",
            "SubmissionView",
            "Table",
            "VirtualTable",
        ]
    ] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Optional[str]:
    """
    Find an Entity given its name and parent asynchronously.

    Arguments:
        name: Name of the entity to find.
        parent: An Entity object (Dataset, DatasetCollection, EntityView, File,
            Folder, Link, MaterializedView, Project, RecordSet, SubmissionView,
            Table, VirtualTable) or the Id of an entity as a string. Omit if
            searching for a Project by name.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The Entity ID or None if not found.

    Example: Finding a project by name
        Find a project using only its name:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import find_entity_id_async

        async def main():
            syn = Synapse()
            syn.login()

            project_id = await find_entity_id_async(name="My Project")
            if project_id:
                print(f"Found project: {project_id}")
            else:
                print("Project not found")

        asyncio.run(main())
        ```

    Example: Finding an entity within a parent
        Find a file within a specific folder:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import find_entity_id_async

        async def main():
            syn = Synapse()
            syn.login()

            file_id = await find_entity_id_async(
                name="my_data.csv",
                parent="syn123456"  # Parent folder ID
            )
            if file_id:
                print(f"Found file: {file_id}")
            else:
                print("File not found in folder")

        asyncio.run(main())
        ```
    """
    from synapseclient.api import get_child
    from synapseclient.core.utils import id_of

    parent_id = id_of(parent) if parent else None
    return await get_child(
        entity_name=name,
        parent_id=parent_id,
        synapse_client=synapse_client,
    )


def is_synapse_id(
    syn_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> bool:
    """
    Check if given synID is valid (attached to actual entity).

    Arguments:
        syn_id: A Synapse ID to validate.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        True if the Synapse ID is valid.

    Example: Validating a Synapse ID
        Check if a Synapse ID exists:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import is_synapse_id

        syn = Synapse()
        syn.login()

        if is_synapse_id("syn123456"):
            print("Valid Synapse ID")
        else:
            print("Invalid or non-existent Synapse ID")
        ```

    Example: Validating multiple IDs
        Check multiple Synapse IDs in a loop:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import is_synapse_id

        syn = Synapse()
        syn.login()

        ids_to_check = ["syn123456", "syn999999", "syn789012"]
        for synapse_id in ids_to_check:
            if is_synapse_id(synapse_id):
                print(f"{synapse_id} is valid")
            else:
                print(f"{synapse_id} is invalid or does not exist")
        ```

    Example: Using in conditional logic
        Use ID validation before attempting operations:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import is_synapse_id, get

        syn = Synapse()
        syn.login()

        user_input = "syn123456"
        if is_synapse_id(user_input):
            entity = get(synapse_id=user_input)
            print(f"Retrieved entity: {entity.name}")
        else:
            print("Please provide a valid Synapse ID")
        ```
    """
    return wrap_async_to_sync(
        coroutine=is_synapse_id_async(
            syn_id=syn_id,
            synapse_client=synapse_client,
        )
    )


async def is_synapse_id_async(
    syn_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> bool:
    """
    Check if given synID is valid (attached to actual entity) asynchronously.

    Arguments:
        syn_id: A Synapse ID to validate.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        True if the Synapse ID is valid.

    Example: Validating a Synapse ID
        Check if a Synapse ID exists:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import is_synapse_id_async

        async def main():
            syn = Synapse()
            syn.login()

            if await is_synapse_id_async("syn123456"):
                print("Valid Synapse ID")
            else:
                print("Invalid or non-existent Synapse ID")

        asyncio.run(main())
        ```

    Example: Validating multiple IDs concurrently
        Check multiple Synapse IDs concurrently:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import is_synapse_id_async

        async def main():
            syn = Synapse()
            syn.login()

            ids_to_check = ["syn123456", "syn999999", "syn789012"]
            results = await asyncio.gather(
                *[is_synapse_id_async(synapse_id) for synapse_id in ids_to_check]
            )
            for synapse_id, is_valid in zip(ids_to_check, results):
                if is_valid:
                    print(f"{synapse_id} is valid")
                else:
                    print(f"{synapse_id} is invalid or does not exist")

        asyncio.run(main())
        ```
    """
    from synapseclient.api.entity_services import is_synapse_id as api_is_synapse_id

    return await api_is_synapse_id(
        syn_id=syn_id,
        synapse_client=synapse_client,
    )


def onweb(
    entity: Union[
        str,
        "Dataset",
        "DatasetCollection",
        "EntityView",
        "File",
        "Folder",
        "Link",
        "MaterializedView",
        "Project",
        "RecordSet",
        "SubmissionView",
        "Table",
        "VirtualTable",
    ],
    subpage_id: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> str:
    """
    Open up a browser window to the entity page or wiki-subpage.

    Arguments:
        entity: Either an Entity object (Dataset, DatasetCollection, EntityView,
            File, Folder, Link, MaterializedView, Project, RecordSet,
            SubmissionView, Table, VirtualTable) or a Synapse ID string.
        subpage_id: (Optional) ID of one of the wiki's sub-pages.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The URL that was opened in the browser.

    Example: Opening an entity in browser
        Open an entity's page in the default web browser:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import onweb

        syn = Synapse()
        syn.login()

        # Open by Synapse ID
        url = onweb("syn123456")
        print(f"Opened: {url}")
        ```

    Example: Opening with an entity object
        Open an entity using an entity object:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import File
        from synapseclient.operations import onweb, get

        syn = Synapse()
        syn.login()

        file = get(synapse_id="syn123456")
        url = onweb(file)
        print(f"Opened file: {url}")
        ```

    Example: Opening a wiki subpage
        Open a specific wiki subpage:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import onweb

        syn = Synapse()
        syn.login()

        # Open a specific wiki subpage
        url = onweb("syn123456", subpage_id="12345")
        print(f"Opened wiki subpage: {url}")
        ```
    """
    return wrap_async_to_sync(
        coroutine=onweb_async(
            entity=entity,
            subpage_id=subpage_id,
            synapse_client=synapse_client,
        )
    )


async def onweb_async(
    entity: Union[
        str,
        "Dataset",
        "DatasetCollection",
        "EntityView",
        "File",
        "Folder",
        "Link",
        "MaterializedView",
        "Project",
        "RecordSet",
        "SubmissionView",
        "Table",
        "VirtualTable",
    ],
    subpage_id: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> str:
    """
    Open up a browser window to the entity page or wiki-subpage asynchronously.

    Arguments:
        entity: Either an Entity object (Dataset, DatasetCollection, EntityView,
            File, Folder, Link, MaterializedView, Project, RecordSet,
            SubmissionView, Table, VirtualTable) or a Synapse ID string.
        subpage_id: (Optional) ID of one of the wiki's sub-pages.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The URL that was opened in the browser.

    Example: Opening an entity in browser
        Open an entity's page in the default web browser:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import onweb_async

        async def main():
            syn = Synapse()
            syn.login()

            # Open by Synapse ID
            url = await onweb_async("syn123456")
            print(f"Opened: {url}")

        asyncio.run(main())
        ```

    Example: Opening with an entity object
        Open an entity using an entity object:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import File
        from synapseclient.operations import onweb_async, get_async

        async def main():
            syn = Synapse()
            syn.login()

            file = await get_async(synapse_id="syn123456")
            url = await onweb_async(file)
            print(f"Opened file: {url}")

        asyncio.run(main())
        ```
    """
    from synapseclient.api.web_services import open_entity_in_browser

    return await open_entity_in_browser(
        entity=entity,
        subpage_id=subpage_id,
        synapse_client=synapse_client,
    )


def md5_query(
    md5: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[dict]:
    """
    Find the Entities which have attached file(s) which have the given MD5 hash.

    Arguments:
        md5: The MD5 hash to query for (hexadecimal string).
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A list of Entity headers matching the MD5 hash.

    Example: Finding entities by MD5 hash
        Search for entities with a specific MD5 hash:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import md5_query

        syn = Synapse()
        syn.login()

        md5_hash = "1234567890abcdef1234567890abcdef"
        results = md5_query(md5_hash)

        print(f"Found {len(results)} entities with MD5: {md5_hash}")
        for entity in results:
            print(f"- {entity['id']}: {entity['name']}")
        ```

    Example: Checking for duplicate files
        Use MD5 query to find duplicate files before uploading:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import md5_query
        import hashlib

        syn = Synapse()
        syn.login()

        # Calculate MD5 of a local file
        def calculate_md5(file_path):
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()

        file_path = "/path/to/file.txt"
        file_md5 = calculate_md5(file_path)

        # Check if file already exists in Synapse
        existing_entities = md5_query(file_md5)
        if existing_entities:
            print(f"File already exists in Synapse:")
            for entity in existing_entities:
                print(f"- {entity['id']}: {entity['name']}")
        else:
            print("File is unique, safe to upload")
        ```

    Example: Finding all versions of a file
        Find all entities that share the same content (MD5):

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import md5_query, get

        syn = Synapse()
        syn.login()

        # Get MD5 from a known file
        file = get(synapse_id="syn123456")
        if hasattr(file, 'file_handle') and file.file_handle:
            file_md5 = file.file_handle.content_md5

            # Find all entities with the same MD5
            matching_entities = md5_query(file_md5)
            print(f"Found {len(matching_entities)} entities with same content:")
            for entity in matching_entities:
                print(f"- {entity['id']}: {entity['name']} in {entity.get('parentId', 'unknown')}")
        ```
    """
    return wrap_async_to_sync(
        coroutine=md5_query_async(
            md5=md5,
            synapse_client=synapse_client,
        )
    )


async def md5_query_async(
    md5: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[dict]:
    """
    Find the Entities which have attached file(s) which have the given MD5 hash asynchronously.

    Arguments:
        md5: The MD5 hash to query for (hexadecimal string).
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A list of Entity headers matching the MD5 hash.

    Example: Finding entities by MD5 hash
        Search for entities with a specific MD5 hash:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import md5_query_async

        async def main():
            syn = Synapse()
            syn.login()

            md5_hash = "1234567890abcdef1234567890abcdef"
            results = await md5_query_async(md5_hash)

            print(f"Found {len(results)} entities with MD5: {md5_hash}")
            for entity in results:
                print(f"- {entity['id']}: {entity['name']}")

        asyncio.run(main())
        ```

    Example: Checking multiple MD5 hashes concurrently
        Query multiple MD5 hashes at the same time:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import md5_query_async

        async def main():
            syn = Synapse()
            syn.login()

            md5_hashes = [
                "1234567890abcdef1234567890abcdef",
                "abcdef1234567890abcdef1234567890",
                "567890abcdef1234567890abcdef1234"
            ]

            # Query all MD5 hashes concurrently
            results = await asyncio.gather(
                *[md5_query_async(md5) for md5 in md5_hashes]
            )

            for md5, entities in zip(md5_hashes, results):
                print(f"MD5 {md5}: {len(entities)} matching entities")

        asyncio.run(main())
        ```
    """
    from synapseclient.api import get_entities_by_md5

    response = await get_entities_by_md5(
        md5=md5,
        synapse_client=synapse_client,
    )
    return response.get("results", [])


def print_entity(
    entity: Union[
        str,
        dict,
        "Dataset",
        "DatasetCollection",
        "EntityView",
        "File",
        "Folder",
        "Link",
        "MaterializedView",
        "Project",
        "RecordSet",
        "SubmissionView",
        "Table",
        "VirtualTable",
    ],
    ensure_ascii: bool = True,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Pretty print an Entity as JSON.

    Arguments:
        entity: Either an Entity object (Dataset, DatasetCollection, EntityView,
            File, Folder, Link, MaterializedView, Project, RecordSet,
            SubmissionView, Table, VirtualTable), a Synapse ID string, or a
            dictionary representation of an entity.
        ensure_ascii: If True, escapes all non-ASCII characters in the output.
            Defaults to True.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        None. Prints the entity to the logger.

    Example: Printing an entity by ID
        Print an entity using its Synapse ID:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import print_entity

        syn = Synapse()
        syn.login()

        # Print entity by Synapse ID
        print_entity("syn123456")
        ```

    Example: Printing an entity object
        Print an entity object:

        ```python
        from synapseclient import Synapse
        from synapseclient.models import File
        from synapseclient.operations import print_entity, get

        syn = Synapse()
        syn.login()

        # Get and print a file entity
        file = get(synapse_id="syn123456")
        print_entity(file)
        ```

    Example: Controlling ASCII output
        Print an entity with non-ASCII characters:

        ```python
        from synapseclient import Synapse
        from synapseclient.operations import print_entity

        syn = Synapse()
        syn.login()

        # Print with unicode characters preserved
        print_entity("syn123456", ensure_ascii=False)
        ```
    """
    return wrap_async_to_sync(
        coroutine=print_entity_async(
            entity=entity,
            ensure_ascii=ensure_ascii,
            synapse_client=synapse_client,
        )
    )


async def print_entity_async(
    entity: Union[
        str,
        dict,
        "Dataset",
        "DatasetCollection",
        "EntityView",
        "File",
        "Folder",
        "Link",
        "MaterializedView",
        "Project",
        "RecordSet",
        "SubmissionView",
        "Table",
        "VirtualTable",
    ],
    ensure_ascii: bool = True,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Pretty print an Entity as JSON asynchronously.

    Arguments:
        entity: Either an Entity object (Dataset, DatasetCollection, EntityView,
            File, Folder, Link, MaterializedView, Project, RecordSet,
            SubmissionView, Table, VirtualTable), a Synapse ID string, or a
            dictionary representation of an entity.
        ensure_ascii: If True, escapes all non-ASCII characters in the output.
            Defaults to True.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        None. Prints the entity to the logger.

    Example: Printing an entity by ID
        Print an entity using its Synapse ID:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import print_entity_async

        async def main():
            syn = Synapse()
            syn.login()

            # Print entity by Synapse ID
            await print_entity_async("syn123456")

        asyncio.run(main())
        ```

    Example: Printing multiple entities concurrently
        Print multiple entities at the same time:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.operations import print_entity_async

        async def main():
            syn = Synapse()
            syn.login()

            entity_ids = ["syn123456", "syn789012", "syn345678"]

            # Print all entities concurrently
            await asyncio.gather(
                *[print_entity_async(entity_id) for entity_id in entity_ids]
            )

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse
    from synapseclient.core import utils
    from synapseclient.operations.factory_operations import FileOptions, get_async

    syn = Synapse.get_client(synapse_client=synapse_client)
    logger = logging.getLogger(syn.logger.name)

    # If entity is a Synapse ID string, fetch the entity using get_async
    if isinstance(entity, str) and utils.is_synapse_id_str(entity):
        entity = await get_async(
            synapse_id=entity,
            file_options=FileOptions(download_file=False),
            synapse_client=synapse_client,
        )

    # Convert entity to dict if it's a dataclass (model object)
    if hasattr(entity, "__dataclass_fields__"):
        import dataclasses

        def filter_repr_fields(obj):
            """Recursively convert dataclass to dict, respecting repr=True fields only."""
            if hasattr(obj, "__dataclass_fields__"):
                # Filter fields to only those with repr=True
                filtered_fields = [
                    (field_obj.name, getattr(obj, field_obj.name))
                    for field_obj in dataclasses.fields(obj)
                    if field_obj.repr
                ]
                # Recursively process nested values
                return {
                    name: filter_repr_fields(value) for name, value in filtered_fields
                }
            elif isinstance(obj, list):
                return [filter_repr_fields(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: filter_repr_fields(v) for k, v in obj.items()}
            else:
                return obj

        entity = filter_repr_fields(entity)

    # Try to print as JSON, fall back to string representation
    try:
        logger.info(
            json.dumps(entity, sort_keys=True, indent=2, ensure_ascii=ensure_ascii)
        )
    except TypeError:
        logger.info(str(entity))
