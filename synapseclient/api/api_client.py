from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, Optional

import httpx

if TYPE_CHECKING:
    from synapseclient import Synapse


async def rest_post_paginated_async(
    uri: str,
    body: Optional[Dict[str, Any]] = None,
    endpoint: Optional[str] = None,
    headers: Optional[httpx.Headers] = None,
    retry_policy: Optional[Dict[str, Any]] = {},
    requests_session_async_synapse: Optional[httpx.AsyncClient] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
    **kwargs,
) -> AsyncGenerator[Dict[str, str], None]:
    """
    Asynchronously yield items from a paginated POST endpoint.

    Arguments:
        uri: Endpoint URI for the POST request.
        body: Request payload dictionary.
        endpoint: Optional server endpoint override.
        headers: Optional HTTP headers.
        retry_policy: Optional retry settings.
        requests_session_async_synapse: Optional async HTTPX client session.
        kwargs: Additional keyword arguments for the request.
        synapse_client: Optional Synapse client instance for authentication.
    Yields:
        Individual items from each page of the response.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    next_page_token = None
    while True:
        if next_page_token is not None:
            body = body or {}
            body["nextPageToken"] = next_page_token
        response = await client.rest_post_async(
            uri=uri,
            body=body,
            endpoint=endpoint,
            headers=headers,
            retry_policy=retry_policy,
            requests_session_async_synapse=requests_session_async_synapse,
            **kwargs,
        )
        next_page_token = response.get("nextPageToken")
        for item in response.get("page", []):
            yield item
        if next_page_token is None:
            break
