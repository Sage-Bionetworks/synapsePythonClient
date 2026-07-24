"""Unit tests for the SearchIndex entity model."""

from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants import concrete_types
from synapseclient.models.mixins.table_components import DeleteMixin, GetMixin
from synapseclient.models.search_index import SearchIndex
from synapseclient.models.search_management import (
    SearchHit,
    SearchQuery,
    SearchQueryPart,
)


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
        entity = index.to_synapse_request()
        # THEN I expect the entity body to carry the expected values
        assert entity["concreteType"] == concrete_types.SEARCH_INDEX_ENTITY
        for key, value in self.synapse_response.items():
            if key != "annotations":
                assert entity[key] == value

    async def test_store_async_requires_defining_sql(self):
        # GIVEN a SearchIndex without defining_sql
        index = SearchIndex(name="test_search_index", parent_id="syn5678")

        with patch(
            "synapseclient.models.search_index.store_entity",
            new_callable=AsyncMock,
        ) as mock_store_entity:
            # WHEN I store it THEN a ValueError is raised
            with pytest.raises(
                ValueError,
                match="The defining_sql attribute must be set for a SearchIndex.",
            ):
                await index.store_async(synapse_client=self.syn)
            # AND no entity is sent to Synapse
            mock_store_entity.assert_not_called()

    async def test_store_async_posts_entity(self):
        # GIVEN a new SearchIndex with defining_sql set
        index = SearchIndex(
            name="test_search_index",
            parent_id="syn5678",
            defining_sql="SELECT * FROM syn9999",
        )

        with (
            patch(
                "synapseclient.models.search_index.get_id",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "synapseclient.models.search_index.store_entity",
                new_callable=AsyncMock,
                return_value=self.synapse_response,
            ) as mock_store_entity,
            patch(
                "synapseclient.models.search_index.store_entity_components",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            # WHEN I store it
            result = await index.store_async(synapse_client=self.syn)

        # THEN the entity body is sent to Synapse
        entity = mock_store_entity.call_args.kwargs["entity"]
        assert entity["definingSQL"] == "SELECT * FROM syn9999"
        assert entity["concreteType"] == concrete_types.SEARCH_INDEX_ENTITY
        # AND the response is filled onto the instance
        assert result.id == self.synapse_response["id"]
        assert result._last_persistent_instance is not None

    async def test_store_async_upserts_onto_existing_index(self):
        # GIVEN a SearchIndex named the same as one that already exists in Synapse,
        # constructed fresh so it carries no `_last_persistent_instance`
        index = SearchIndex(
            name="test_search_index",
            parent_id="syn5678",
            defining_sql="SELECT foo FROM syn9999",
        )
        existing = SearchIndex().fill_from_dict(self.synapse_response)
        existing._set_last_persistent_instance()

        with (
            patch(
                "synapseclient.models.search_index.get_id",
                new_callable=AsyncMock,
                return_value="syn1234",
            ),
            patch.object(
                SearchIndex,
                "get_async",
                new_callable=AsyncMock,
                return_value=existing,
            ),
            patch(
                "synapseclient.models.services.storable_entity.put_entity",
                new_callable=AsyncMock,
                return_value=self.synapse_response,
            ) as mock_put_entity,
            patch(
                "synapseclient.models.search_index.store_entity_components",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            # WHEN I store it
            await index.store_async(synapse_client=self.syn)

        # THEN the existing entity's id and etag are merged in and it is updated
        # in place rather than created
        assert mock_put_entity.await_args.kwargs["entity_id"] == "syn1234"
        request = mock_put_entity.await_args.kwargs["request"]
        assert request["etag"] == self.synapse_response["etag"]
        assert request["definingSQL"] == "SELECT foo FROM syn9999"

    async def test_store_async_dry_run_makes_no_calls(self):
        # GIVEN a new SearchIndex with defining_sql set
        index = SearchIndex(
            name="test_search_index",
            parent_id="syn5678",
            defining_sql="SELECT * FROM syn9999",
        )

        with (
            patch(
                "synapseclient.models.search_index.get_id",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "synapseclient.models.search_index.store_entity",
                new_callable=AsyncMock,
            ) as mock_store_entity,
        ):
            # WHEN I store it as a dry run
            result = await index.store_async(dry_run=True, synapse_client=self.syn)

        # THEN nothing is sent to Synapse
        mock_store_entity.assert_not_called()
        assert result is index

    async def test_get_async_calls_super(self):
        # GIVEN a SearchIndex with an id
        index = SearchIndex(id="syn1234")

        with patch.object(GetMixin, "get_async") as mock_super_get_async:
            mock_super_get_async.return_value = index
            # WHEN I get it
            result = await index.get_async(synapse_client=self.syn)
            # THEN the super().get_async method is called with include_columns
            # forced to False, since SearchIndex has no columns field
            mock_super_get_async.assert_called_once_with(
                include_columns=False,
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


class TestSearchIndexQuery:
    """Dispatch tests for SearchIndex.query_async."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_query_async_dispatch(self):
        # GIVEN a SearchIndex with an id
        index = SearchIndex(id="syn1")
        search_query = SearchQuery(query={"match_all": {}}, size=10)

        with patch(
            "synapseclient.models.search_management.SearchIndexQuery.send_job_and_wait_async",
            autospec=True,
        ) as mock_send_job:
            mock_send_job.return_value = "results"
            # WHEN I query it
            result = await index.query_async(
                search_query=search_query,
                response_parts=[SearchQueryPart.TOTAL_HITS],
                job_timeout=42,
                synapse_client=self.syn,
            )

        # THEN the job is built against this index with the given query
        job = mock_send_job.call_args.args[0]
        assert job.search_index_id == "syn1"
        assert job.search_query is search_query
        assert job.response_parts == [SearchQueryPart.TOTAL_HITS]
        # AND it is sent with the requested timeout
        assert mock_send_job.call_args.kwargs == {
            "timeout": 42,
            "synapse_client": self.syn,
        }
        assert result == "results"

    async def test_query_async_requires_id(self):
        # WHEN querying without an id THEN a ValueError is raised
        with pytest.raises(ValueError):
            await SearchIndex().query_async(
                search_query=SearchQuery(query={"match_all": {}}),
                synapse_client=self.syn,
            )


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
