"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.WikiController2>
"""

import json
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, List, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse


async def post_wiki_page(
    owner_id: str,
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a new wiki page.
    <https://rest-docs.synapse.org/rest/POST/entity/ownerId/wiki2.html>

    Arguments:
        owner_id: The ID of the owner entity.
        request: The wiki page to create.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The created wiki page.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri=f"/entity/{owner_id}/wiki2",
        body=json.dumps(request),
    )


async def get_wiki_page(
    owner_id: str,
    wiki_id: Optional[str] = None,
    wiki_version: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Get a wiki page.
    <https://rest-docs.synapse.org/rest/GET/entity/ownerId/wiki2.html>
    <https://rest-docs.synapse.org/rest/GET/entity/ownerId/wiki2/wikiId.html>

    Arguments:
        owner_id: The ID of the owner entity.
        wiki_id: Optional ID of the wiki. If not provided, returns the root wiki page.
        wiki_version: Optional version of the wiki page.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The requested wiki page.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Construct the URI based on whether wiki_id is provided
    uri = f"/entity/{owner_id}/wiki2"
    if wiki_id is not None:
        uri = f"{uri}/{wiki_id}"

    # Add version as a query parameter if provided
    params = {}
    if wiki_version is not None:
        params["wikiVersion"] = wiki_version

    return await client.rest_get_async(
        uri=uri,
        params=params,
    )


async def put_wiki_page(
    owner_id: str,
    wiki_id: str,
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Update a wiki page.
    <https://rest-docs.synapse.org/rest/PUT/entity/ownerId/wiki2/wikiId.html>

    Arguments:
        owner_id: The ID of the owner entity.
        wiki_id: The ID of the wiki.
        request: The updated wiki page.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The updated wiki page.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri=f"/entity/{owner_id}/wiki2/{wiki_id}",
        body=json.dumps(request),
    )


async def put_wiki_version(
    owner_id: str,
    wiki_id: str,
    wiki_version: int,
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Update a specific version of a wiki page.
    <https://rest-docs.synapse.org/rest/PUT/entity/ownerId/wiki2/wikiId/version.html>

    Arguments:
        owner_id: The ID of the owner entity.
        wiki_id: The ID of the wiki.
        wiki_version: The version number to update.
        request: The updated wiki page.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The updated wiki page version.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri=f"/entity/{owner_id}/wiki2/{wiki_id}/{wiki_version}",
        body=json.dumps(request),
    )


async def delete_wiki_page(
    owner_id: str,
    wiki_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """Delete a wiki page.
    <https://rest-docs.synapse.org/rest/DELETE/entity/ownerId/wiki2/wikiId.html>

    Arguments:
        owner_id: The ID of the owner entity.
        wiki_id: The ID of the wiki.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    await client.rest_delete_async(
        uri=f"/entity/{owner_id}/wiki2/{wiki_id}",
    )


async def get_wiki_header_tree(
    owner_id: str,
    offset: Optional[int] = 0,
    limit: Optional[int] = 20,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """Get the header tree (hierarchy) of wiki pages for an entity.
    <https://rest-docs.synapse.org/rest/GET/entity/ownerId/wikiheadertree2.html>

    Arguments:
        owner_id: The ID of the owner entity.
        offset: The index of the pagination offset. For a page size of 10, the first page would be at offset = 0,
               and the second page would be at offset = 10. Default is 0.
        limit: Limits the size of the page returned. For example, a page size of 10 requires limit = 10.
               Limit must be 50 or less. Default is 20.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        A generator over the wiki header tree for the entity. The tree contains the hierarchy of wiki pages
        including their IDs, titles, and parent-child relationships.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    async for item in client.rest_get_paginated_async(
        uri=f"/entity/{owner_id}/wikiheadertree2",
        limit=limit,
        offset=offset,
    ):
        yield item


async def get_wiki_history(
    owner_id: str = None,
    wiki_id: str = None,
    offset: Optional[int] = 0,
    limit: Optional[int] = 20,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """Get the history of a wiki page.
    <https://rest-docs.synapse.org/rest/GET/entity/ownerId/wiki2/wikiId/wikihistory.html>

    Arguments:
        owner_id: The ID of the owner entity.
        wiki_id: The ID of the wiki.
        offset: The index of the pagination offset. For a page size of 10, the first page would be at offset = 0,
               and the second page would be at offset = 10. Default is 0.
        limit: Limits the size of the page returned. For example, a page size of 10 requires limit = 10.
               Limit must be 50 or less. Default is 20.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        A generator over the history of the wiki page. The history contains the history of the wiki page
        including their IDs, titles, and parent-child relationships.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    async for item in client.rest_get_paginated_async(
        uri=f"/entity/{owner_id}/wiki2/{wiki_id}/wikihistory",
        limit=limit,
        offset=offset,
    ):
        yield item


async def get_attachment_handles(
    owner_id: str,
    wiki_id: str,
    wiki_version: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[Dict[str, Any]]:
    """Get the file handles of all attachments on a wiki page.
    <https://rest-docs.synapse.org/rest/GET/entity/ownerId/wiki2/wikiId/attachmenthandles.html>

    Arguments:
        owner_id: The ID of the owner entity.
        wiki_id: The ID of the wiki.
        wiki_version: Optional version of the wiki page.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The the list of FileHandles for all file attachments of a specific WikiPage for a given owning Entity.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Add version as a query parameter if provided
    params = {}
    if wiki_version is not None:
        params["wikiVersion"] = wiki_version

    return await client.rest_get_async(
        uri=f"/entity/{owner_id}/wiki2/{wiki_id}/attachmenthandles",
        params=params,
    )


async def get_attachment_url(
    owner_id: str,
    wiki_id: str,
    file_name: str,
    wiki_version: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Get the URL of a wiki page attachment.
    <https://rest-docs.synapse.org/rest/GET/entity/ownerId/wiki2/wikiId/attachment.html>

    Arguments:
        owner_id: The ID of the owner entity.
        wiki_id: The ID of the wiki.
        file_name: The name of the file to get.
                The file names can be found in the FileHandles from the GET /entity/{ownerId}/wiki/{wikiId}/attachmenthandles method.
        wiki_version: Optional version of the wiki page.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The URL that can be used to download a file for a given WikiPage file attachment.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Add version as a query parameter if provided
    params = {}
    params["fileName"] = file_name
    if wiki_version is not None:
        params["wikiVersion"] = wiki_version

    return await client.rest_get_async(
        uri=f"/entity/{owner_id}/wiki2/{wiki_id}/attachment",
        params=params,
    )


async def get_attachment_preview_url(
    owner_id: str,
    wiki_id: str,
    file_name: str,
    wiki_version: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Get the preview of a wiki page attachment.
    <https://rest-docs.synapse.org/rest/GET/entity/ownerId/wiki2/wikiId/attachmentpreview.html>

    Arguments:
        owner_id: The ID of the owner entity.
        wiki_id: The ID of the wiki.
        file_name: The name of the file to get.
                The file names can be found in the FileHandles from the GET /entity/{ownerId}/wiki/{wikiId}/attachmenthandles method.
        wiki_version: Optional version of the wiki page.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The URL that can be used to download a preview file for a given WikiPage file attachment.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Add version as a query parameter if provided
    params = {}
    params["fileName"] = file_name
    if wiki_version is not None:
        params["wikiVersion"] = wiki_version

    return await client.rest_get_async(
        uri=f"/entity/{owner_id}/wiki2/{wiki_id}/attachmentpreview",
        params=params,
    )


async def get_markdown_url(
    owner_id: str,
    wiki_id: str,
    wiki_version: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Get the markdown of a wiki page.
    <https://rest-docs.synapse.org/rest/GET/entity/ownerId/wiki2/wikiId/markdown.html>

    Arguments:
        owner_id: The ID of the owner entity.
        wiki_id: The ID of the wiki.
        wiki_version: Optional version of the wiki page.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The URL that can be used to download the markdown file for a given WikiPage.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Add version as a query parameter if provided
    params = {}
    if wiki_version is not None:
        params["wikiVersion"] = wiki_version

    return await client.rest_get_async(
        uri=f"/entity/{owner_id}/wiki2/{wiki_id}/markdown",
        params=params,
    )


async def get_wiki_order_hint(
    owner_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Get the order hint of a wiki page tree.
    <https://rest-docs.synapse.org/rest/GET/entity/ownerId/wiki2orderhint.html>

    Arguments:
        owner_id: The ID of the owner entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The order hint that corresponds to the given owner Entity.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{owner_id}/wiki2orderhint",
    )


async def put_wiki_order_hint(
    owner_id: str,
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Update the order hint of a wiki page tree.
    <https://rest-docs.synapse.org/rest/PUT/entity/ownerId/wiki2orderhint.html>

    Arguments:
        owner_id: The ID of the owner entity.
        request: The updated order hint.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The updated order hint that corresponds to the given owner Entity.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri=f"/entity/{owner_id}/wiki2orderhint",
        body=json.dumps(request),
    )
