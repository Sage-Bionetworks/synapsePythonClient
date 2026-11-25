"""Unit tests for the NamedSearchPipeline functions in search_services."""

import json
from unittest.mock import AsyncMock, patch

import synapseclient.api.search_services as search_services

PIPELINE = {
    "organizationName": "biomed",
    "name": "keyword_heavy",
    "settings": {"phase_results_processors": [{"normalization-processor": {}}]},
}


class TestSearchPipelineServices:
    @patch("synapseclient.Synapse")
    async def test_create_search_pipeline(self, mock_synapse):
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        # WHEN I create a search pipeline
        await search_services.create_search_pipeline(PIPELINE)
        # THEN the body is POSTed to the pipeline collection
        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/search/pipeline", body=json.dumps(PIPELINE)
        )

    @patch("synapseclient.Synapse")
    async def test_get_search_pipeline(self, mock_synapse):
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        # WHEN I get a search pipeline by id
        await search_services.get_search_pipeline("7")
        # THEN the id is in the path
        mock_client.rest_get_async.assert_awaited_once_with(uri="/search/pipeline/7")

    @patch("synapseclient.Synapse")
    async def test_update_search_pipeline(self, mock_synapse):
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        # WHEN I update a search pipeline
        await search_services.update_search_pipeline("7", {"id": "7", **PIPELINE})
        # THEN the body is PUT to the id path
        mock_client.rest_put_async.assert_awaited_once_with(
            uri="/search/pipeline/7", body=json.dumps({"id": "7", **PIPELINE})
        )

    @patch("synapseclient.Synapse")
    async def test_list_search_pipelines_drops_none(self, mock_synapse):
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        # WHEN I list pipelines filtered by organization without a page token
        await search_services.list_search_pipelines(organization_name="biomed")
        # THEN only the set keys are sent
        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/search/pipeline/list",
            body=json.dumps({"organizationName": "biomed"}),
        )
