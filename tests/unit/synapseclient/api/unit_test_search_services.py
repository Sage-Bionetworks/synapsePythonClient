"""Unit tests for synapseclient.api.search_services."""

import json
from unittest.mock import AsyncMock, patch

import pytest

import synapseclient.api.search_services as search_services


@pytest.fixture
def mock_client():
    """Patch Synapse.get_client to return an AsyncMock and yield it."""
    with patch("synapseclient.Synapse") as synapse_cls:
        client = AsyncMock()
        synapse_cls.get_client.return_value = client
        yield client


# ---------- Text Analyzer ----------


class TestTextAnalyzer:
    async def test_create_posts_request_body(self, mock_client):
        request = {"organizationName": "org", "name": "n", "settings": {"a": 1}}
        mock_client.rest_post_async.return_value = {"id": "1"}

        result = await search_services.create_text_analyzer(request)

        assert result == {"id": "1"}
        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/search/text/analyzer", body=json.dumps(request)
        )

    async def test_get_uses_path_id(self, mock_client):
        mock_client.rest_get_async.return_value = {"id": "42"}
        await search_services.get_text_analyzer("42")
        mock_client.rest_get_async.assert_awaited_once_with(
            uri="/search/text/analyzer/42"
        )

    async def test_update_puts_body_to_path_id(self, mock_client):
        request = {"id": "42", "name": "n"}
        mock_client.rest_put_async.return_value = request

        await search_services.update_text_analyzer("42", request)

        mock_client.rest_put_async.assert_awaited_once_with(
            uri="/search/text/analyzer/42", body=json.dumps(request)
        )

    async def test_list_drops_none_filters(self, mock_client):
        mock_client.rest_post_async.return_value = {"results": []}

        await search_services.list_text_analyzers()

        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/search/text/analyzer/list", body=json.dumps({})
        )

    async def test_list_includes_org_and_token(self, mock_client):
        mock_client.rest_post_async.return_value = {"results": []}

        await search_services.list_text_analyzers(
            organization_name="org.sagebionetworks", next_page_token="tok"
        )

        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/search/text/analyzer/list",
            body=json.dumps(
                {"organizationName": "org.sagebionetworks", "nextPageToken": "tok"}
            ),
        )


# ---------- Column Analyzer Override ----------


class TestColumnAnalyzerOverride:
    async def test_create(self, mock_client):
        request = {"organizationName": "org", "name": "co", "overrides": []}
        mock_client.rest_post_async.return_value = {"id": "1"}

        await search_services.create_column_analyzer_override(request)

        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/search/column/analyzer/override", body=json.dumps(request)
        )

    async def test_get(self, mock_client):
        mock_client.rest_get_async.return_value = {"id": "1"}
        await search_services.get_column_analyzer_override("1")
        mock_client.rest_get_async.assert_awaited_once_with(
            uri="/search/column/analyzer/override/1"
        )

    async def test_update(self, mock_client):
        request = {"id": "1"}
        mock_client.rest_put_async.return_value = request
        await search_services.update_column_analyzer_override("1", request)
        mock_client.rest_put_async.assert_awaited_once_with(
            uri="/search/column/analyzer/override/1", body=json.dumps(request)
        )

    async def test_list_drops_none(self, mock_client):
        mock_client.rest_post_async.return_value = {"results": []}
        await search_services.list_column_analyzer_overrides()
        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/search/column/analyzer/override/list", body=json.dumps({})
        )


# ---------- Synonym Set ----------


class TestSynonymSet:
    async def test_create(self, mock_client):
        request = {"organizationName": "org", "name": "syn", "definition": {}}
        mock_client.rest_post_async.return_value = {"id": "1"}
        await search_services.create_synonym_set(request)
        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/search/synonym/set", body=json.dumps(request)
        )

    async def test_get(self, mock_client):
        mock_client.rest_get_async.return_value = {"id": "1"}
        await search_services.get_synonym_set("1")
        mock_client.rest_get_async.assert_awaited_once_with(uri="/search/synonym/set/1")

    async def test_update(self, mock_client):
        request = {"id": "1"}
        mock_client.rest_put_async.return_value = request
        await search_services.update_synonym_set("1", request)
        mock_client.rest_put_async.assert_awaited_once_with(
            uri="/search/synonym/set/1", body=json.dumps(request)
        )

    async def test_list(self, mock_client):
        mock_client.rest_post_async.return_value = {"results": []}
        await search_services.list_synonym_sets(organization_name="org")
        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/search/synonym/set/list",
            body=json.dumps({"organizationName": "org"}),
        )


# ---------- Search Configuration ----------


class TestSearchConfiguration:
    async def test_create(self, mock_client):
        request = {"organizationName": "org", "name": "cfg"}
        mock_client.rest_post_async.return_value = {"id": "1"}
        await search_services.create_search_configuration(request)
        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/search/configuration", body=json.dumps(request)
        )

    async def test_get(self, mock_client):
        mock_client.rest_get_async.return_value = {"id": "1"}
        await search_services.get_search_configuration("1")
        mock_client.rest_get_async.assert_awaited_once_with(
            uri="/search/configuration/1"
        )

    async def test_update(self, mock_client):
        request = {"id": "1"}
        mock_client.rest_put_async.return_value = request
        await search_services.update_search_configuration("1", request)
        mock_client.rest_put_async.assert_awaited_once_with(
            uri="/search/configuration/1", body=json.dumps(request)
        )

    async def test_list(self, mock_client):
        mock_client.rest_post_async.return_value = {"results": []}
        await search_services.list_search_configurations()
        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/search/configuration/list", body=json.dumps({})
        )


# ---------- Search Config Bindings ----------


class TestSearchConfigBinding:
    async def test_bind_puts_body_with_entity_and_config_id(self, mock_client):
        mock_client.rest_put_async.return_value = {"bindId": "1"}

        await search_services.bind_search_config_to_entity(
            entity_id="syn1", search_configuration_id="42"
        )

        mock_client.rest_put_async.assert_awaited_once_with(
            uri="/entity/syn1/searchconfig/binding",
            body=json.dumps({"entityId": "syn1", "searchConfigurationId": "42"}),
        )

    async def test_get_binding(self, mock_client):
        mock_client.rest_get_async.return_value = {"bindId": "1"}
        await search_services.get_search_config_binding("syn1")
        mock_client.rest_get_async.assert_awaited_once_with(
            uri="/entity/syn1/searchconfig/binding"
        )

    async def test_clear_returns_none(self, mock_client):
        mock_client.rest_delete_async.return_value = None
        result = await search_services.clear_search_config_binding("syn1")
        assert result is None
        mock_client.rest_delete_async.assert_awaited_once_with(
            uri="/entity/syn1/searchconfig/binding"
        )


# ---------- Autocomplete ----------


class TestAutocomplete:
    async def test_autocomplete_posts_to_search_endpoint(self, mock_client):
        request = {"searchIndexId": "syn1", "searchQuery": {"queryText": "abc"}}
        mock_client.rest_post_async.return_value = {"hits": []}

        await search_services.autocomplete_search(request)

        mock_client.rest_post_async.assert_awaited_once_with(
            uri="/search/autocomplete", body=json.dumps(request)
        )
