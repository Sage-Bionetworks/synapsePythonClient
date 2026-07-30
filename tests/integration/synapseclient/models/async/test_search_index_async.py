"""Integration tests for the SearchIndex entity and its query methods.

SearchIndex builds an OpenSearch index from a SQL view of a table-like entity.
Index creation may be restricted to Sage Bionetworks employees on some stacks;
those tests skip gracefully when the server denies the create with a 403.
"""

import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Project,
    SearchIndex,
    SearchQuery,
    SearchQueryPart,
    Table,
)
from synapseclient.models.table_components import SchemaStorageStrategy
from tests.integration import ASYNC_JOB_TIMEOUT_SEC, QUERY_TIMEOUT_SEC
from tests.integration.helpers import wait_for_condition


class TestSearchIndex:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def _create_source_table(self, project_model: Project) -> Table:
        """Create a populated table to use as a SearchIndex source."""
        table = Table(name=str(uuid.uuid4()), parent_id=project_model.id)
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)
        await table.store_rows_async(
            values={
                "title": ["Alzheimer study", "Cancer cohort", "Diabetes trial"],
                "disease_code": ["AD", "CA", "DB"],
            },
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=self.syn,
        )
        return table

    async def _store_search_index(self, **kwargs) -> SearchIndex:
        """Store a SearchIndex, skipping the test if creation is not permitted."""
        try:
            index = await SearchIndex(**kwargs).store_async(synapse_client=self.syn)
        except SynapseHTTPError as e:
            if e.response.status_code == 403:
                pytest.skip(f"SearchIndex creation is restricted on this stack: {e}")
            raise
        self.schedule_for_cleanup(index.id)
        return index

    async def test_empty_defining_sql_validation(self, project_model: Project) -> None:
        # GIVEN a SearchIndex with no defining SQL
        index = SearchIndex(name=str(uuid.uuid4()), parent_id=project_model.id)

        # WHEN storing it
        # THEN a ValueError is raised before any API call
        with pytest.raises(ValueError, match="defining_sql"):
            await index.store_async(synapse_client=self.syn)

    async def test_create_and_retrieve_search_index(
        self, project_model: Project
    ) -> None:
        # GIVEN a populated source table
        table = await self._create_source_table(project_model)

        # WHEN creating a SearchIndex over it
        index_name = str(uuid.uuid4())
        index = await self._store_search_index(
            name=index_name,
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )

        # THEN it is created with an ID
        assert index.id is not None

        # AND when retrieving it, the metadata is present
        retrieved = await SearchIndex(id=index.id).get_async(synapse_client=self.syn)
        assert retrieved.name == index_name
        assert retrieved.defining_sql == f"SELECT * FROM {table.id}"

    async def test_query_search_index_match_all(self, project_model: Project) -> None:
        # GIVEN a SearchIndex over a populated table
        table = await self._create_source_table(project_model)
        index = await self._store_search_index(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )

        # WHEN the OpenSearch index has finished building, a match_all query
        # returns the indexed rows. The build is asynchronous, so poll until the
        # query succeeds and reports the expected total.
        async def _query_total_hits() -> int:
            results = await index.query_async(
                search_query=SearchQuery(query={"match_all": {}}, size=10),
                response_parts=[SearchQueryPart.HITS, SearchQueryPart.TOTAL_HITS],
                job_timeout=QUERY_TIMEOUT_SEC,
                synapse_client=self.syn,
            )
            return results.total_hits

        total_hits = await wait_for_condition(
            _query_total_hits,
            timeout_seconds=ASYNC_JOB_TIMEOUT_SEC,
            description="SearchIndex to build and return all rows",
        )

        # THEN every source row is indexed
        assert total_hits == 3

    # TODO: Re-enable this test once PLFM-9854 is done
    # async def test_autocomplete_search_index(self, project_model: Project) -> None:
    #     # GIVEN a SearchIndex over a populated table
    #     table = await self._create_source_table(project_model)
    #     index = await self._store_search_index(
    #         name=str(uuid.uuid4()),
    #         parent_id=project_model.id,
    #         defining_sql=f"SELECT * FROM {table.id}",
    #     )

    #     # WHEN the index has built, a prefix autocomplete returns the matching row.
    #     # The build is asynchronous, so poll until a hit comes back.
    #     async def _autocomplete_hits():
    #         return await index.autocomplete_async(
    #             query={"match_phrase_prefix": {"title": {"query": "Alz"}}},
    #             synapse_client=self.syn,
    #         )

    #     hits = await wait_for_condition(
    #         _autocomplete_hits,
    #         timeout_seconds=ASYNC_JOB_TIMEOUT_SEC,
    #         description="SearchIndex to build and return an autocomplete hit",
    #     )

    #     # THEN only the Alzheimer row matches the prefix
    #     assert len(hits) == 1

    async def test_delete_search_index(self, project_model: Project) -> None:
        # GIVEN a SearchIndex
        table = await self._create_source_table(project_model)
        index = await self._store_search_index(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )

        # WHEN deleting it
        await SearchIndex(id=index.id).delete_async(synapse_client=self.syn)

        # THEN it can no longer be retrieved
        with pytest.raises(SynapseHTTPError):
            await SearchIndex(id=index.id).get_async(synapse_client=self.syn)
