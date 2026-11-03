"""Integration tests for the SearchIndex entity and its query methods.

SearchIndex builds an OpenSearch index from a SQL view of a table-like entity.
Index creation may be restricted to Sage Bionetworks employees on some stacks;
those tests skip gracefully when the server denies the create with a 403.
"""

import uuid
from typing import Callable, Optional

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    ColumnAnalyzerOverride,
    ColumnAnalyzerOverrideEntry,
    Organization,
    Project,
    SearchConfiguration,
    SearchIndex,
    SearchQuery,
    SearchQueryPart,
    Table,
)
from synapseclient.models.search_dsl import (
    FuzzyFieldOptions,
    HybridClause,
    HybridQuery,
    MatchBoolPrefixFieldOptions,
    MatchFieldOptions,
    NeuralFieldOptions,
    PrefixFieldOptions,
    Query,
    RangeFieldOptions,
    TermFieldOptions,
    WildcardFieldOptions,
)
from synapseclient.models.table_components import SchemaStorageStrategy
from tests.integration import ASYNC_JOB_TIMEOUT_SEC, QUERY_TIMEOUT_SEC
from tests.integration.helpers import wait_for_condition


class TestSearchIndex:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def _create_source_table(
        self, project_model: Project, *, parent_id: Optional[str] = None
    ) -> Table:
        """Create a populated table to use as a SearchIndex source."""
        table = Table(name=str(uuid.uuid4()), parent_id=parent_id or project_model.id)
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

    @pytest.mark.parametrize(
        "query",
        [
            Query(prefix={"disease_code": PrefixFieldOptions(value="AD")}),
            Query(
                match_bool_prefix={"title": MatchBoolPrefixFieldOptions(query="Alz")},
            ),
        ],
        ids=["prefix", "match_bool_prefix"],
    )
    async def test_autocomplete_search_index(
        self, project_model: Project, query: Query
    ) -> None:
        # GIVEN a SearchIndex over a populated table
        table = await self._create_source_table(project_model)
        index = await self._store_search_index(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )

        # WHEN the index has built, a prefix autocomplete returns the matching row.
        # The build is asynchronous, so poll until a hit comes back.
        async def _autocomplete_hits():
            return await index.autocomplete_async(
                query=query,
                synapse_client=self.syn,
            )

        hits = await wait_for_condition(
            _autocomplete_hits,
            timeout_seconds=ASYNC_JOB_TIMEOUT_SEC,
            description="SearchIndex to build and return an autocomplete hit",
        )

        # THEN only the Alzheimer row matches the prefix
        assert len(hits) == 1

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


class TestSearchIndexQuery:
    """Tests for SearchIndex.query_async, using one shared built SearchIndex
    for every test in this class since every query here is read-only against
    the same indexed data -- avoids waiting for a fresh index build per case."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="class")
    async def built_search_index(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> SearchIndex:
        """Create one SearchIndex over a populated table, shared by every test
        method in this class."""
        table = Table(name=str(uuid.uuid4()), parent_id=project_model.id)
        table = await table.store_async(synapse_client=syn)
        schedule_for_cleanup(table.id)
        await table.store_rows_async(
            values={
                "title": ["Alzheimer study", "Cancer cohort", "Diabetes trial"],
                "disease_code": ["AD", "CA", "DB"],
                "priority": [1, 2, 3],
            },
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=syn,
        )

        try:
            index = await SearchIndex(
                name=str(uuid.uuid4()),
                parent_id=project_model.id,
                defining_sql=f"SELECT * FROM {table.id}",
            ).store_async(synapse_client=syn)
        except SynapseHTTPError as e:
            if e.response.status_code == 403:
                pytest.skip(f"SearchIndex creation is restricted on this stack: {e}")
            raise
        schedule_for_cleanup(index.id)
        return index

    @pytest.mark.parametrize(
        "query, expected_total_hits",
        [
            # Baseline: match_all matches every row, confirming all 3 source
            # rows made it into the index.
            (Query(match_all={}), 3),
            (Query(match={"title": MatchFieldOptions(query="Alzheimer")}), 1),
            # test using a match bool prefix clause
            # this should match "Alzheimer study" and "Diabetes trial"
            (
                Query(
                    match_bool_prefix={
                        "title": MatchBoolPrefixFieldOptions(query="Alzheimer Di")
                    }
                ),
                2,
            ),
            # test using a term clause -- exact, non-analyzed match, auto-
            # routed server-side through the disease_code.keyword sub-field.
            # this should match only the row with disease_code "AD"
            (Query(term={"disease_code": TermFieldOptions(value="AD")}), 1),
            # test using a terms clause -- matches any of several exact
            # values. this should match disease_code "AD" and "CA"
            (Query(terms={"disease_code": ["AD", "CA"]}), 2),
            # test using a prefix clause -- exact, non-analyzed prefix match.
            # this should match only the row with disease_code "AD"
            (Query(prefix={"disease_code": PrefixFieldOptions(value="AD")}), 1),
            # test using a wildcard clause -- "*A*" matches any value
            # containing an uppercase "A" anywhere. this should match
            # disease_code "AD" and "CA" (not "DB")
            (Query(wildcard={"disease_code": WildcardFieldOptions(value="A*")}), 1),
            # test using a fuzzy clause -- "AE" is a single-character edit
            # (substitute E for D) away from "AD". this should match only the
            # row with disease_code "AD"
            (
                Query(
                    fuzzy={"disease_code": FuzzyFieldOptions(value="AE", fuzziness=1)}
                ),
                1,
            ),
            # test using a range clause on the numeric "priority" column --
            # this should match the rows with priority 2 and 3
            (Query(range={"priority": RangeFieldOptions(gte=2)}), 2),
        ],
        ids=[
            "match_all",
            "match",
            "match_bool_prefix",
            "term",
            "terms",
            "prefix",
            "wildcard",
            "fuzzy",
            "range",
        ],
    )
    async def test_query(
        self,
        built_search_index: SearchIndex,
        query: Query,
        expected_total_hits: int,
    ) -> None:
        # GIVEN a SearchIndex that has already finished building

        # WHEN running the parametrized query. built_search_index is shared
        # across every case in this class, so it may still be indexing rows
        # the first time any case runs -- poll until this query reports the
        # expected hit count.
        async def _query_total_hits() -> int:
            results = await built_search_index.query_async(
                search_query=SearchQuery(query=query, size=10),
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

        # THEN it returns the expected number of hits
        assert total_hits == expected_total_hits


class TestSemanticSearchIndexQuery:
    """Tests for SearchIndex.query_async with a `neural` clause on
    SearchQuery.hybrid.

    Building a semantic index embeds every flagged row, which is slow and
    costly, so this class builds one index and keeps its tests to wiring
    checks; query behavior is covered by the unit tests.
    """

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="class")
    async def built_semantic_search_index(
        self,
        project_model: Project,
        search_organization: Organization,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> SearchIndex:
        """Create one SearchIndex over a populated table whose description
        column is flagged semantic, shared by every test method in this class."""
        override = await ColumnAnalyzerOverride(
            organization_name=search_organization.name,
            name=f"semantic_{uuid.uuid4().hex}",
            overrides=[
                ColumnAnalyzerOverrideEntry(column_name="description", semantic=True)
            ],
        ).store_async(synapse_client=syn)
        config = await SearchConfiguration(
            organization_name=search_organization.name,
            name=f"semantic_{uuid.uuid4().hex}",
            column_analyzer_overrides=[{"$ref": override.qualified_name}],
        ).store_async(synapse_client=syn)

        table = Table(name=str(uuid.uuid4()), parent_id=project_model.id)
        table = await table.store_async(synapse_client=syn)
        schedule_for_cleanup(table.id)
        await table.store_rows_async(
            values={
                "title": ["Alzheimer study", "Cancer cohort", "Diabetes trial"],
                "description": [
                    "Longitudinal study of memory loss and dementia in aging adults",
                    "Clinical trial of a targeted therapy for metastatic tumors",
                    "Insulin resistance and blood sugar management intervention",
                ],
            },
            schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA,
            synapse_client=syn,
        )

        try:
            index = await SearchIndex(
                name=str(uuid.uuid4()),
                parent_id=project_model.id,
                defining_sql=f"SELECT * FROM {table.id}",
                search_configuration_id=config.id,
            ).store_async(synapse_client=syn)
        except SynapseHTTPError as e:
            if e.response.status_code == 403:
                pytest.skip(f"SearchIndex creation is restricted on this stack: {e}")
            raise
        schedule_for_cleanup(index.id)
        return index

    async def _top_hit_title(
        self, index: SearchIndex, search_query: SearchQuery
    ) -> Optional[str]:
        """Run search_query and return the top hit's title, or None when the
        index has not finished building yet."""
        results = await index.query_async(
            search_query=search_query,
            response_parts=[SearchQueryPart.HITS],
            job_timeout=QUERY_TIMEOUT_SEC,
            synapse_client=self.syn,
        )
        if not results.hits:
            return None
        return next(
            (f.value for f in results.hits[0].fields if f.name == "title"), None
        )

    async def test_hybrid_query_blends_keyword_and_neural_clauses(
        self, built_semantic_search_index: SearchIndex
    ) -> None:
        # GIVEN a SearchIndex with a description column embedded via
        # semantic=True

        # WHEN a two-clause hybrid query blends a keyword clause with a neural
        # clause under an inline search pipeline weighting them 30/70. The index
        # build (including embedding) is asynchronous, so poll until the
        # expected top hit comes back.
        keyword_clause = Query(
            match={"description": MatchFieldOptions(query="dementia")}
        )
        search_query = SearchQuery(
            # A plain Query is accepted as a hybrid clause; only the neural
            # clause needs HybridClause
            hybrid=HybridQuery(
                queries=[
                    keyword_clause,
                    HybridClause(
                        neural={
                            "semantic_search": NeuralFieldOptions(
                                query_text="losing memories with age", k=100
                            )
                        }
                    ),
                ]
            ),
            search_pipeline={
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {"technique": "min_max"},
                            "combination": {
                                "technique": "arithmetic_mean",
                                "parameters": {"weights": [0.3, 0.7]},
                            },
                        }
                    }
                ]
            },
            size=1,
        )
        top_title = await wait_for_condition(
            lambda: self._top_hit_title(built_semantic_search_index, search_query),
            timeout_seconds=ASYNC_JOB_TIMEOUT_SEC,
            description="SearchIndex to build and rank the Alzheimer row first",
        )

        # THEN the row both clauses agree on ranks first
        assert top_title == "Alzheimer study"
