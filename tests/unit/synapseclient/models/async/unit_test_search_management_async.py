"""Unit tests for the search-management dataclasses.

Covers fill_from_dict / to_synapse_request round-trips for the raw
OpenSearch-DSL pass-through models and the SearchIndexQuery async flow.
"""

from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants.concrete_types import SEARCH_INDEX_QUERY
from synapseclient.models.search_index import SearchIndex
from synapseclient.models.search_management import (
    ColumnAnalyzerOverride,
    ColumnAnalyzerOverrideEntry,
    SearchAutocompleteRequest,
    SearchConfigBinding,
    SearchConfiguration,
    SearchFieldValue,
    SearchHighlight,
    SearchHit,
    SearchIndexQuery,
    SearchIndexState,
    SearchIndexStatus,
    SearchQuery,
    SearchQueryPart,
    SynonymSet,
    TextAnalyzer,
)


class TestSearchQuery:
    """Round-trip tests for the raw-DSL SearchQuery."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_to_synapse_request_maps_special_keys(self):
        # GIVEN a SearchQuery exercising the keyword-renamed slots
        query = SearchQuery(
            query={"match_all": {}},
            post_filter={"term": {"disease": {"value": "AD"}}},
            aggregations={"by_disease": {"terms": {"field": "disease", "size": 10}}},
            highlight={"fields": {"title": {}}},
            collapse={"field": "study"},
            rescore={"window_size": 50},
            sort=[{"title": "asc"}],
            source={"includes": ["title", "abstract"]},
            from_=5,
            size=25,
            search_after=["abc", 123],
        )
        # WHEN I serialize it
        request = query.to_synapse_request()
        # THEN source/from_/search_after map to the OpenSearch key names
        assert request["_source"] == {"includes": ["title", "abstract"]}
        assert request["from"] == 5
        assert request["search_after"] == ["abc", 123]
        assert "source" not in request
        assert "from_" not in request
        assert request["query"] == {"match_all": {}}
        assert request["aggregations"] == {
            "by_disease": {"terms": {"field": "disease", "size": 10}}
        }

    def test_to_synapse_request_drops_none(self):
        # GIVEN a minimal SearchQuery
        query = SearchQuery(query={"match_all": {}})
        # WHEN I serialize it
        request = query.to_synapse_request()
        # THEN only the populated key is present
        assert request == {"query": {"match_all": {}}}

    def test_fill_from_dict_round_trip(self):
        # GIVEN a SearchQueryResults-style request body
        body = {
            "query": {"match": {"title": {"query": "alz"}}},
            "_source": {"includes": ["title"]},
            "from": 10,
            "size": 50,
            "search_after": [1, 2],
            "sort": [{"_score": "desc"}],
        }
        # WHEN I fill a SearchQuery from it
        query = SearchQuery().fill_from_dict(body)
        # THEN the keyword-renamed slots are populated
        assert query.source == {"includes": ["title"]}
        assert query.from_ == 10
        assert query.size == 50
        assert query.search_after == [1, 2]
        assert query.sort == [{"_score": "desc"}]
        # AND re-serializing reproduces the original body
        assert query.to_synapse_request() == body


class TestSearchHit:
    """SearchHit deserialization including SearchHighlight."""

    synapse_response = {
        "rowId": 42,
        "rowVersion": 3,
        "score": 1.5,
        "fields": [
            {"name": "title", "value": "Alzheimer study"},
            {"name": "disease", "value": "AD"},
        ],
        "highlights": [
            {"name": "title", "snippets": ["<em>Alzheimer</em> study"]},
        ],
    }

    def test_fill_from_dict(self):
        # WHEN I fill a SearchHit from a response
        hit = SearchHit().fill_from_dict(self.synapse_response)
        # THEN scalar fields populate
        assert hit.row_id == 42
        assert hit.row_version == 3
        assert hit.score == 1.5
        # AND fields are SearchFieldValue
        assert all(isinstance(f, SearchFieldValue) for f in hit.fields)
        assert hit.fields[0].name == "title"
        assert hit.fields[0].value == "Alzheimer study"
        # AND highlights are SearchHighlight with snippets
        assert all(isinstance(h, SearchHighlight) for h in hit.highlights)
        assert hit.highlights[0].name == "title"
        assert hit.highlights[0].snippets == ["<em>Alzheimer</em> study"]

    def test_fill_from_dict_empty(self):
        # WHEN I fill from an empty dict
        hit = SearchHit().fill_from_dict({})
        # THEN collections default to empty lists
        assert hit.fields == []
        assert hit.highlights == []


class TestTextAnalyzer:
    """TextAnalyzer carries raw settings."""

    def test_round_trip(self):
        # GIVEN a TextAnalyzer with a raw OpenSearch analysis block
        settings = {
            "tokenizer": {"std": {"type": "standard"}},
            "filter": {"med_syn": {"$ref": "biomed-medical_terms"}},
            "analyzer": {
                "default": {
                    "type": "custom",
                    "tokenizer": "std",
                    "filter": ["lowercase", "med_syn"],
                }
            },
        }
        analyzer = TextAnalyzer(
            organization_name="biomed",
            name="publications",
            settings=settings,
        )
        # WHEN I serialize then deserialize
        request = analyzer.to_synapse_request()
        # THEN settings pass through unchanged
        assert request["settings"] == settings
        assert request["organizationName"] == "biomed"
        # AND qualified_name composes org + name
        assert analyzer.qualified_name == "biomed-publications"
        # AND fill_from_dict is the inverse
        round_tripped = TextAnalyzer().fill_from_dict(request)
        assert round_tripped.settings == settings


class TestSynonymSet:
    """SynonymSet carries a raw definition object."""

    def test_round_trip(self):
        # GIVEN a SynonymSet with a raw synonym_graph definition
        definition = {
            "type": "synonym_graph",
            "synonyms": ["tumor, neoplasm, cancer", "AD => Alzheimer's disease"],
        }
        synonym_set = SynonymSet(
            organization_name="biomed",
            name="medical_terms",
            definition=definition,
        )
        # WHEN I serialize it
        request = synonym_set.to_synapse_request()
        # THEN definition passes through unchanged
        assert request["definition"] == definition
        # AND fill_from_dict is the inverse
        assert SynonymSet().fill_from_dict(request).definition == definition


class TestSearchConfiguration:
    """SearchConfiguration with $ref and inline analyzer slots."""

    def test_round_trip_with_refs(self):
        # GIVEN a SearchConfiguration referencing saved resources
        config = SearchConfiguration(
            organization_name="biomed",
            name="publications_v1",
            default_analyzer={"$ref": "org.sagebionetworks-SCIENTIFIC"},
            column_analyzer_overrides=[{"$ref": "biomed-publications_overrides"}],
        )
        # WHEN I serialize it
        request = config.to_synapse_request()
        # THEN the analyzer slots pass through as raw objects
        assert request["defaultAnalyzer"] == {"$ref": "org.sagebionetworks-SCIENTIFIC"}
        assert request["columnAnalyzerOverrides"] == [
            {"$ref": "biomed-publications_overrides"}
        ]
        # AND fill_from_dict is the inverse
        round_tripped = SearchConfiguration().fill_from_dict(request)
        assert round_tripped.default_analyzer == {
            "$ref": "org.sagebionetworks-SCIENTIFIC"
        }
        assert round_tripped.column_analyzer_overrides == [
            {"$ref": "biomed-publications_overrides"}
        ]


class TestColumnAnalyzerOverride:
    """ColumnAnalyzerOverride with single analyzer per entry."""

    def test_round_trip(self):
        # GIVEN an override with a single analyzer per column
        override = ColumnAnalyzerOverride(
            organization_name="biomed",
            name="publications_overrides",
            overrides=[
                ColumnAnalyzerOverrideEntry(
                    column_name="disease_code",
                    analyzer={"$ref": "biomed-acronym_exact"},
                ),
            ],
        )
        # WHEN I serialize it
        request = override.to_synapse_request()
        # THEN each entry carries a single analyzer slot
        assert request["overrides"] == [
            {"columnName": "disease_code", "analyzer": {"$ref": "biomed-acronym_exact"}}
        ]
        # AND fill_from_dict is the inverse
        round_tripped = ColumnAnalyzerOverride().fill_from_dict(request)
        assert round_tripped.overrides[0].column_name == "disease_code"
        assert round_tripped.overrides[0].analyzer == {"$ref": "biomed-acronym_exact"}


class TestSearchConfigBinding:
    def test_fill_from_dict(self):
        # WHEN I fill a binding from a response
        binding = SearchConfigBinding().fill_from_dict(
            {
                "bindId": "1",
                "searchConfigurationId": "2",
                "objectId": "syn3",
                "objectType": "entity",
                "createdBy": "9",
                "createdOn": "2024-01-01T00:00:00.000Z",
            }
        )
        # THEN all fields populate
        assert binding.bind_id == "1"
        assert binding.search_configuration_id == "2"
        assert binding.object_id == "syn3"
        assert binding.object_type == "entity"

    async def test_store_async_binds_and_fills(self):
        # GIVEN a binding with an object and configuration id
        binding = SearchConfigBinding(object_id="syn3", search_configuration_id="2")
        with patch(
            "synapseclient.models.search_management.bind_search_config_to_entity",
            new_callable=AsyncMock,
            return_value={"bindId": "9", "searchConfigurationId": "2"},
        ) as mock_bind:
            # WHEN I store it
            result = await binding.store_async(synapse_client=self.syn)
        # THEN the entity and configuration ids are forwarded and the response fills
        mock_bind.assert_awaited_once_with("syn3", "2", synapse_client=self.syn)
        assert result.bind_id == "9"

    @pytest.mark.parametrize(
        "binding",
        [
            SearchConfigBinding(search_configuration_id="2"),
            SearchConfigBinding(object_id="syn3"),
        ],
        ids=["missing_object_id", "missing_configuration_id"],
    )
    async def test_store_async_requires_ids(self, binding):
        # WHEN storing a binding missing a required id THEN a ValueError is raised
        with pytest.raises(ValueError):
            await binding.store_async(synapse_client=self.syn)

    async def test_get_async_resolves_binding(self):
        # GIVEN a binding for an entity
        binding = SearchConfigBinding(object_id="syn3")
        with patch(
            "synapseclient.models.search_management.get_search_config_binding",
            new_callable=AsyncMock,
            return_value={"bindId": "9", "objectId": "syn3"},
        ) as mock_get:
            # WHEN I get it
            result = await binding.get_async(synapse_client=self.syn)
        # THEN the entity id is forwarded and the response fills
        mock_get.assert_awaited_once_with("syn3", synapse_client=self.syn)
        assert result.bind_id == "9"

    async def test_delete_async_clears_binding(self):
        # GIVEN a binding for an entity
        binding = SearchConfigBinding(object_id="syn3")
        with patch(
            "synapseclient.models.search_management.clear_search_config_binding",
            new_callable=AsyncMock,
        ) as mock_clear:
            # WHEN I delete it
            await binding.delete_async(synapse_client=self.syn)
        # THEN the clear endpoint is called for that entity
        mock_clear.assert_awaited_once_with("syn3", synapse_client=self.syn)

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn


class TestOrgScopedResource:
    """Dispatch tests for the create/get/update/list lifecycle shared by the
    org-scoped search-management resources."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    # Each org-scoped resource wires its own api functions; validate every one.
    RESOURCE_CLASSES = [
        TextAnalyzer,
        ColumnAnalyzerOverride,
        SynonymSet,
        SearchConfiguration,
    ]

    @pytest.mark.parametrize(
        "cls", RESOURCE_CLASSES, ids=[c.__name__ for c in RESOURCE_CLASSES]
    )
    async def test_lifecycle_dispatch(self, cls):
        # GIVEN a new resource (no id) THEN store dispatches to the create fn
        mock_create = AsyncMock(
            return_value={"id": "5", "organizationName": "biomed", "name": "n"}
        )
        with patch.object(cls, "_CREATE_FN", new=staticmethod(mock_create)):
            new_resource = cls(organization_name="biomed", name="n")
            expected_create_body = new_resource.to_synapse_request()
            stored = await new_resource.store_async(synapse_client=self.syn)
        mock_create.assert_awaited_once_with(
            expected_create_body, synapse_client=self.syn
        )
        assert stored.id == "5"

        # GIVEN a resource with an id THEN store dispatches to update by id
        mock_update = AsyncMock(return_value={"id": "5", "name": "n2"})
        with patch.object(cls, "_UPDATE_FN", new=staticmethod(mock_update)):
            existing = cls(id="5", organization_name="biomed", name="n2")
            expected_update_body = existing.to_synapse_request()
            await existing.store_async(synapse_client=self.syn)
        assert mock_update.await_args.args[0] == "5"
        assert mock_update.await_args.args[1] == expected_update_body

        # WHEN getting by id THEN get dispatches by id
        mock_get = AsyncMock(return_value={"id": "5", "name": "n"})
        with patch.object(cls, "_GET_FN", new=staticmethod(mock_get)):
            await cls(id="5").get_async(synapse_client=self.syn)
        mock_get.assert_awaited_once_with("5", synapse_client=self.syn)

        # WHEN listing THEN all pages are followed via nextPageToken
        mock_list = AsyncMock(
            side_effect=[
                {"results": [{"id": "1"}], "nextPageToken": "tok"},
                {"results": [{"id": "2"}]},
            ]
        )
        with patch.object(cls, "_LIST_FN", new=staticmethod(mock_list)):
            listed = await cls.list_async(
                organization_name="biomed", synapse_client=self.syn
            )
        assert mock_list.await_count == 2
        assert [item.id for item in listed] == ["1", "2"]
        # AND the second page request forwards the token from the first page
        assert mock_list.await_args_list[1].kwargs["next_page_token"] == "tok"

    async def test_get_async_requires_id(self):
        # WHEN getting a resource without an id THEN a ValueError is raised
        with pytest.raises(ValueError):
            await TextAnalyzer().get_async(synapse_client=self.syn)


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


class TestSearchIndexStatus:
    def test_fill_from_dict(self):
        # WHEN I fill a status from a response
        status = SearchIndexStatus().fill_from_dict(
            {
                "searchIndexId": "syn1",
                "state": "ACTIVE",
                "changedOn": "2024-01-01T00:00:00.000Z",
                "errorMessage": None,
            }
        )
        # THEN the state coerces to the enum
        assert status.search_index_id == "syn1"
        assert status.state is SearchIndexState.ACTIVE


class TestSearchAutocompleteRequest:
    def test_to_synapse_request(self):
        # GIVEN an autocomplete request with a prefix query
        request = SearchAutocompleteRequest(
            search_index_id="syn22806626",
            query={"match_phrase_prefix": {"title": {"query": "alz"}}},
            source={"includes": ["title"]},
        )
        # WHEN I serialize it
        body = request.to_synapse_request()
        # THEN the body nests query and _source under searchQuery
        assert body == {
            "searchIndexId": "syn22806626",
            "searchQuery": {
                "query": {"match_phrase_prefix": {"title": {"query": "alz"}}},
                "_source": {"includes": ["title"]},
            },
        }

    def test_to_synapse_request_minimal(self):
        # GIVEN an autocomplete request with only a query
        request = SearchAutocompleteRequest(
            search_index_id="syn1",
            query={"prefix": {"title": {"value": "a"}}},
        )
        # WHEN I serialize it
        body = request.to_synapse_request()
        # THEN _source is omitted
        assert body == {
            "searchIndexId": "syn1",
            "searchQuery": {"query": {"prefix": {"title": {"value": "a"}}}},
        }


class TestSearchIndexQuery:
    """Request serialization, response deserialization, and the async flow."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def _build_query(self) -> SearchIndexQuery:
        return SearchIndexQuery(
            search_index_id="syn1",
            search_query=SearchQuery(query={"match_all": {}}, size=10),
            response_parts=[SearchQueryPart.HITS, SearchQueryPart.TOTAL_HITS],
        )

    def test_to_synapse_request(self):
        # GIVEN a SearchIndexQuery
        query = self._build_query()
        # WHEN I serialize it
        request = query.to_synapse_request()
        # THEN it carries the concrete type, index id, query body, and parts
        assert request == {
            "concreteType": SEARCH_INDEX_QUERY,
            "searchIndexId": "syn1",
            "searchQuery": {"query": {"match_all": {}}, "size": 10},
            "responseParts": ["HITS", "TOTAL_HITS"],
        }

    def test_fill_from_dict_response(self):
        # GIVEN a SearchQueryResults response body
        response = {
            "hits": [
                {
                    "rowId": 1,
                    "fields": [{"name": "title", "value": "x"}],
                    "highlights": [{"name": "title", "snippets": ["<em>x</em>"]}],
                }
            ],
            "totalHits": 7,
            "selectColumns": [{"name": "title", "columnType": "STRING"}],
            "aggregationResults": {"by_disease": {"buckets": []}},
            "nextSearchAfter": ["cursor", 99],
            "offset": 0,
        }
        # WHEN I fill the query from it
        query = self._build_query().fill_from_dict(response)
        # THEN the response fields populate
        assert query.total_hits == 7
        assert len(query.hits) == 1
        assert query.hits[0].row_id == 1
        assert query.hits[0].highlights[0].snippets == ["<em>x</em>"]
        assert query.select_columns[0].name == "title"
        assert query.aggregation_results == {"by_disease": {"buckets": []}}
        assert query.next_search_after == ["cursor", 99]
        assert query.offset == 0

    async def test_send_job_and_wait_async(self):
        # GIVEN a SearchIndexQuery
        query = self._build_query()
        response = {"hits": [], "totalHits": 0, "offset": 0}
        with (
            patch(
                "synapseclient.models.mixins.asynchronous_job.send_job_and_wait_async",
                new_callable=AsyncMock,
                return_value=response,
            ) as mock_send_job,
            patch.object(
                query, "fill_from_dict", wraps=query.fill_from_dict
            ) as mock_fill,
        ):
            # WHEN I send the job and wait
            await query.send_job_and_wait_async(synapse_client=self.syn)
            # THEN the async job service is called with the serialized request
            mock_send_job.assert_called_once_with(
                request=query.to_synapse_request(),
                request_type=SEARCH_INDEX_QUERY,
                timeout=120,
                synapse_client=self.syn,
            )
            # AND fill_from_dict is invoked with the response body
            mock_fill.assert_called_once_with(synapse_response=response)
            # AND the response fields land on the instance
            assert query.total_hits == 0
            assert query.hits == []
