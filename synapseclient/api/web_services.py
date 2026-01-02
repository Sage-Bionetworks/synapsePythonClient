"""This module provides web browser-related services for Synapse entities."""

import os
import webbrowser
from typing import TYPE_CHECKING, Optional, Union

from synapseclient.core.utils import id_of

if TYPE_CHECKING:
    from synapseclient import Synapse


async def open_entity_in_browser(
    entity: Union[str, object],
    subpage_id: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> str:
    """
    Open up a browser window to the entity page or wiki-subpage.

    Arguments:
        entity: Either an Entity object or a Synapse ID string.
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
        from synapseclient.api import open_entity_in_browser

        syn = Synapse()
        syn.login()

        async def main():
            url = await open_entity_in_browser("syn123456")
            print(f"Opened: {url}")

        asyncio.run(main())
        ```

    Example: Opening a wiki subpage
        Open a specific wiki subpage:

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.api import open_entity_in_browser

        syn = Synapse()
        syn.login()

        async def main():
            url = await open_entity_in_browser("syn123456", subpage_id="12345")
            print(f"Opened wiki subpage: {url}")

        asyncio.run(main())
        ```
    """
    from synapseclient import Synapse
    from synapseclient.api import get_entity

    client = Synapse.get_client(synapse_client=synapse_client)

    # If it's a file path, get the entity first
    if isinstance(entity, str) and os.path.isfile(entity):
        entity_dict = await get_entity(entity_id=entity, synapse_client=synapse_client)
        synapse_id = entity_dict.get("id")
    else:
        synapse_id = id_of(entity)

    # Construct the URL
    portal_endpoint = client.portalEndpoint
    if subpage_id is None:
        url = f"{portal_endpoint}#!Synapse:{synapse_id}"
    else:
        url = f"{portal_endpoint}#!Wiki:{synapse_id}/ENTITY/{subpage_id}"

    # Open in browser
    webbrowser.open(url)

    return url
