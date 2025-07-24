import json
import sys
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, Optional

import httpx

if TYPE_CHECKING:
    from synapseclient import Synapse


async def rest_post_paginated_async(
    uri: str,
    body: Optional[Dict[str, Any]] = None,
    endpoint: Optional[str] = None,
    headers: Optional[httpx.Headers] = None,
    retry_policy: Optional[Dict[str, Any]] = None,
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

    if not retry_policy:
        retry_policy = {}

    client = Synapse.get_client(synapse_client=synapse_client)
    next_page_token = None
    while True:
        if next_page_token is not None:
            body = body or {}
            body["nextPageToken"] = next_page_token
        response = await client.rest_post_async(
            uri=uri,
            body=json.dumps(body),
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


async def rest_get_paginated_async(
    uri: str,
    limit: int = 20,
    offset: int = 0,
    endpoint: Optional[str] = None,
    headers: Optional[httpx.Headers] = None,
    retry_policy: Optional[Dict[str, Any]] = None,
    requests_session_async_synapse: Optional[httpx.AsyncClient] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
    **kwargs,
) -> AsyncGenerator[Dict[str, str], None]:
    """
    Asynchronously yield items from a paginated GET endpoint.

    Arguments:
        uri: Endpoint URI for the GET request.
        limit: How many records should be returned per request
        offset: At what record offset from the first should iteration start
        endpoint: Optional server endpoint override.
        headers: Optional HTTP headers.
        retry_policy: Optional retry settings.
        requests_session_async_synapse: Optional async HTTPX client session.
        kwargs: Additional keyword arguments for the request.
        synapse_client: Optional Synapse client instance for authentication.
    Yields:
        Individual items from each page of the response.

    The limit parameter is set at 20 by default. Using a larger limit results in fewer calls to the service, but if
    responses are large enough to be a burden on the service they may be truncated.
    """
    from synapseclient import Synapse
    from synapseclient.core import utils

    if not retry_policy:
        retry_policy = {}

    client = Synapse.get_client(synapse_client=synapse_client)
    prev_num_results = sys.maxsize
    while prev_num_results > 0:
        paginated_uri = utils._limit_and_offset(uri, limit=limit, offset=offset)
        response = await client.rest_get_async(
            uri=paginated_uri,
            endpoint=endpoint,
            headers=headers,
            retry_policy=retry_policy,
            requests_session_async_synapse=requests_session_async_synapse,
            **kwargs,
        )
        results = response["results"] if "results" in response else response["children"]
        prev_num_results = len(results)

        for result in results:
            offset += 1
            yield result
