"""Unit tests for the SearchIndex entity model."""

from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants import concrete_types
from synapseclient.models.mixins.table_components import (
    DeleteMixin,
    GetMixin,
    TableStoreMixin,
)
from synapseclient.models.search_index import SearchIndex
from synapseclient.models.search_management import SearchHit


class TestSearchIndex:
    synapse_response = {
        "id": "syn1234",
        "name": "test_search_index",
        "description": "test_description",
        "parentId": "syn5678",
        "etag": "etag_value",
        "createdOn": "createdOn_value",
        "createdBy": "createdBy_value",
        "modifiedOn": "modifiedOn_value",
        "modifiedBy": "modifiedBy_value",
        "versionNumber": 1,
        "versionLabel": "versionLabel_value",
        "versionComment": "versionComment_value",
        "isLatestVersion": True,
        "definingSQL": "SELECT * FROM syn9999",
        "searchConfigurationId": "42",
        "annotations": {"key": "value"},
    }

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_fill_from_dict(self):
        # GIVEN an empty SearchIndex
        index = SearchIndex()
        # WHEN I fill it from a Synapse response
        index.fill_from_dict(self.synapse_response)
        # THEN I expect the SearchIndex to be filled with the expected values
        assert index.id == self.synapse_response["id"]
        assert index.name == self.synapse_response["name"]
        assert index.description == self.synapse_response["description"]
        assert index.parent_id == self.synapse_response["parentId"]
        assert index.etag == self.synapse_response["etag"]
        assert index.created_on == self.synapse_response["createdOn"]
        assert index.created_by == self.synapse_response["createdBy"]
        assert index.modified_on == self.synapse_response["modifiedOn"]
        assert index.modified_by == self.synapse_response["modifiedBy"]
        assert index.version_number == self.synapse_response["versionNumber"]
        assert index.version_label == self.synapse_response["versionLabel"]
        assert index.version_comment == self.synapse_response["versionComment"]
        assert index.is_latest_version == self.synapse_response["isLatestVersion"]
        assert index.defining_sql == self.synapse_response["definingSQL"]
        assert (
            index.search_configuration_id
            == self.synapse_response["searchConfigurationId"]
        )
        assert index.annotations == self.synapse_response["annotations"]

    def test_fill_from_dict_without_annotations(self):
        # GIVEN an empty SearchIndex
        index = SearchIndex()
        # WHEN I fill it from a Synapse response with set_annotations=False
        index.fill_from_dict(self.synapse_response, set_annotations=False)
        # THEN I expect annotations to remain untouched
        assert index.annotations == {}

    def test_to_synapse_request(self):
        # GIVEN a SearchIndex
        index = SearchIndex(
            id="syn1234",
            name="test_search_index",
            description="test_description",
            parent_id="syn5678",
            etag="etag_value",
            created_on="createdOn_value",
            created_by="createdBy_value",
            modified_on="modifiedOn_value",
            modified_by="modifiedBy_value",
            version_number=1,
            version_label="versionLabel_value",
            version_comment="versionComment_value",
            is_latest_version=True,
            defining_sql="SELECT * FROM syn9999",
            search_configuration_id="42",
        )
        # WHEN I convert it to a Synapse request
        request = index.to_synapse_request()
        # THEN I expect the entity body to carry the expected values
        entity = request["entity"]
        assert entity["concreteType"] == concrete_types.SEARCH_INDEX_ENTITY
        for key, value in self.synapse_response.items():
            if key != "annotations":
                assert entity[key] == value

    async def test_store_async_requires_defining_sql(self):
        # GIVEN a SearchIndex without defining_sql
        index = SearchIndex(name="test_search_index", parent_id="syn5678")

        with patch.object(TableStoreMixin, "store_async") as mock_super_store_async:
            # WHEN I store it THEN a ValueError is raised
            with pytest.raises(
                ValueError,
                match="The defining_sql attribute must be set for a SearchIndex.",
            ):
                await index.store_async(synapse_client=self.syn)
            # AND the super().store_async method is never called
            mock_super_store_async.assert_not_called()

    async def test_store_async_with_defining_sql_calls_super(self):
        # GIVEN a SearchIndex with defining_sql set
        index = SearchIndex(
            name="test_search_index",
            parent_id="syn5678",
            defining_sql="SELECT * FROM syn9999",
        )

        with patch.object(TableStoreMixin, "store_async") as mock_super_store_async:
            mock_super_store_async.return_value = index
            # WHEN I store it
            result = await index.store_async(
                dry_run=True, job_timeout=100, synapse_client=self.syn
            )
            # THEN the super().store_async method is called with the same arguments
            mock_super_store_async.assert_called_once_with(
                dry_run=True, job_timeout=100, synapse_client=self.syn
            )
            assert result == index

    async def test_get_async_calls_super(self):
        # GIVEN a SearchIndex with an id
        index = SearchIndex(id="syn1234")

        with patch.object(GetMixin, "get_async") as mock_super_get_async:
            mock_super_get_async.return_value = index
            # WHEN I get it
            result = await index.get_async(synapse_client=self.syn)
            # THEN the super().get_async method is called with the default arguments
            mock_super_get_async.assert_called_once_with(
                include_columns=True,
                include_activity=False,
                synapse_client=self.syn,
            )
            assert result == index

    async def test_delete_async_calls_super(self):
        # GIVEN a SearchIndex with an id
        index = SearchIndex(id="syn1234")

        with patch.object(DeleteMixin, "delete_async") as mock_super_delete_async:
            # WHEN I delete it
            await index.delete_async(synapse_client=self.syn)
            # THEN the super().delete_async method is called
            mock_super_delete_async.assert_called_once_with(synapse_client=self.syn)


class TestSearchIndexAutocomplete:
    """Dispatch tests for SearchIndex.autocomplete_async."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_autocomplete_async_dispatch(self):
        # GIVEN a SearchIndex with an id
        index = SearchIndex(id="syn1")
        response = {"hits": [{"rowId": 1, "fields": [{"name": "title", "value": "x"}]}]}
        with patch(
            "synapseclient.api.autocomplete_search",
            new_callable=AsyncMock,
            return_value=response,
        ) as mock_autocomplete:
            # WHEN I run autocomplete
            hits = await index.autocomplete_async(
                query={"prefix": {"title": {"value": "a"}}},
                source={"includes": ["title"]},
                synapse_client=self.syn,
            )
        # THEN the request nests the query/_source under searchQuery for this index
        mock_autocomplete.assert_awaited_once_with(
            {
                "searchIndexId": "syn1",
                "searchQuery": {
                    "query": {"prefix": {"title": {"value": "a"}}},
                    "_source": {"includes": ["title"]},
                },
            },
            synapse_client=self.syn,
        )
        # AND the response hits deserialize to SearchHit
        assert len(hits) == 1
        assert isinstance(hits[0], SearchHit)
        assert hits[0].row_id == 1

    async def test_autocomplete_async_requires_id(self):
        # WHEN autocompleting without an id THEN a ValueError is raised
        with pytest.raises(ValueError):
            await SearchIndex().autocomplete_async(
                query={"prefix": {"title": {"value": "a"}}},
                synapse_client=self.syn,
            )
