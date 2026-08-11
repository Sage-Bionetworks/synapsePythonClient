"""Unit tests for file_services utility functions."""

import asyncio
import json
from unittest.mock import AsyncMock, patch

import synapseclient.api.file_services as file_services
from synapseclient.core.constants.limits import MAX_FILE_HANDLE_PER_COPY_REQUEST

FILE_HANDLE_ENDPOINT = "https://repo-prod.prod.sagebase.org/file/v1"
MAX_THREADS = 4


def copy_request(index: int) -> dict:
    """Build a file handle copy request for the file handle with the given index."""
    return {
        "originalFile": {
            "fileHandleId": str(index),
            "associateObjectId": "syn123",
            "associateObjectType": "WikiAttachment",
        }
    }


def copy_result(index: int) -> dict:
    """Build the copy result that the API would return for the given index."""
    return {
        "originalFileHandleId": str(index),
        "newFileHandle": {"id": f"new-{index}"},
    }


def mock_client() -> AsyncMock:
    """Build a mock Synapse client for the file handle copy endpoint."""
    client = AsyncMock()
    client.fileHandleEndpoint = FILE_HANDLE_ENDPOINT
    # max_threads sizes the concurrency semaphore, so it must be a real int
    client.max_threads = MAX_THREADS
    return client


def requested_file_handle_ids(mock_rest_post: AsyncMock) -> list[list[str]]:
    """Extract the file handle IDs of each request sent to the copy endpoint."""
    return [
        [
            request["originalFile"]["fileHandleId"]
            for request in json.loads(call.kwargs["body"])["copyRequests"]
        ]
        for call in mock_rest_post.call_args_list
    ]


class TestPostFileHandlesCopy:
    """Tests for post_file_handles_copy function."""

    @patch("synapseclient.Synapse")
    async def test_single_batch(self, mock_synapse):
        """Test that a batch under the limit is sent as one request."""
        # GIVEN a mock client that copies the requested file handles
        client = mock_client()
        mock_synapse.get_client.return_value = client
        client.rest_post_async.return_value = {
            "copyResults": [copy_result(0), copy_result(1)]
        }

        # WHEN I copy two file handles
        results = await file_services.post_file_handles_copy(
            copy_requests=[copy_request(0), copy_request(1)]
        )

        # THEN the copy results are returned
        assert results == [copy_result(0), copy_result(1)]

        # AND a single request was sent to the file handle endpoint
        client.rest_post_async.assert_called_once_with(
            "/filehandles/copy",
            body=json.dumps(
                {"copyRequests": [copy_request(0), copy_request(1)]},
            ),
            endpoint=FILE_HANDLE_ENDPOINT,
        )

    @patch("synapseclient.Synapse")
    async def test_empty_requests(self, mock_synapse):
        """Test that no request is sent when there is nothing to copy."""
        # GIVEN a mock client
        client = mock_client()
        mock_synapse.get_client.return_value = client

        # WHEN I copy an empty list of file handles
        results = await file_services.post_file_handles_copy(copy_requests=[])

        # THEN no results are returned
        assert results == []

        # AND no request was sent
        client.rest_post_async.assert_not_called()

    @patch("synapseclient.Synapse")
    async def test_batches_are_split_and_results_stay_ordered(self, mock_synapse):
        """Test that oversized requests are split and results keep their order."""
        # GIVEN more copy requests than fit in a single request
        total = MAX_FILE_HANDLE_PER_COPY_REQUEST * 2 + 1
        copy_requests = [copy_request(index) for index in range(total)]

        # AND a mock client that responds to later batches first
        client = mock_client()
        mock_synapse.get_client.return_value = client

        async def respond(*_, body: str, **__) -> dict:
            requests = json.loads(body)["copyRequests"]
            first_id = int(requests[0]["originalFile"]["fileHandleId"])
            # Sleep longer for earlier batches so that responses arrive out of order
            await asyncio.sleep((total - first_id) / total / 100)
            return {
                "copyResults": [
                    copy_result(int(request["originalFile"]["fileHandleId"]))
                    for request in requests
                ]
            }

        client.rest_post_async.side_effect = respond

        # WHEN I copy the file handles
        results = await file_services.post_file_handles_copy(
            copy_requests=copy_requests
        )

        # THEN the results are in the order of the requests
        assert results == [copy_result(index) for index in range(total)]

        # AND the requests were split into full batches plus a remainder
        assert requested_file_handle_ids(client.rest_post_async) == [
            [
                str(index)
                for index in range(
                    start, min(start + MAX_FILE_HANDLE_PER_COPY_REQUEST, total)
                )
            ]
            for start in range(0, total, MAX_FILE_HANDLE_PER_COPY_REQUEST)
        ]

    @patch("synapseclient.Synapse")
    async def test_batches_are_sent_concurrently(self, mock_synapse):
        """Test that batches are in flight together, up to max_threads."""
        # GIVEN enough copy requests to fill more batches than max_threads allows
        batches = MAX_THREADS + 2
        copy_requests = [
            copy_request(index)
            for index in range(MAX_FILE_HANDLE_PER_COPY_REQUEST * batches)
        ]

        # AND a mock client that tracks how many requests are in flight at once
        client = mock_client()
        mock_synapse.get_client.return_value = client
        in_flight = 0
        max_in_flight = 0

        async def respond(*_, body: str, **__) -> dict:
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            try:
                await asyncio.sleep(0.01)
                return {
                    "copyResults": [
                        copy_result(int(request["originalFile"]["fileHandleId"]))
                        for request in json.loads(body)["copyRequests"]
                    ]
                }
            finally:
                in_flight -= 1

        client.rest_post_async.side_effect = respond

        # WHEN I copy the file handles
        await file_services.post_file_handles_copy(copy_requests=copy_requests)

        # THEN every batch was sent
        assert client.rest_post_async.call_count == batches

        # AND they were sent concurrently, without exceeding max_threads
        assert max_in_flight == MAX_THREADS
