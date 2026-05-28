"""Unit tests for the search_management dataclasses."""

from unittest.mock import patch

import pytest

from synapseclient.core.constants import concrete_types
from synapseclient.models.search_management import (
    ColumnAnalyzerOverride,
    ColumnAnalyzerOverrideEntry,
    FacetRequest,
    FacetSortField,
    KeyRange,
    KeyValues,
    SearchConfigBinding,
    SearchConfiguration,
    SearchFieldValue,
    SearchHit,
    SearchIndexQuery,
    SearchIndexState,
    SearchIndexStatus,
    SearchQuery,
    SearchQueryPart,
    SearchQueryType,
    SortDirection,
    SortField,
    SynonymSet,
    TextAnalyzer,
)


class TestTextAnalyzer:
    def test_qualified_name(self):
        ta = TextAnalyzer(organization_name="org", name="n")
        assert ta.qualified_name == "org-n"

    def test_qualified_name_none_when_missing_pieces(self):
        assert TextAnalyzer().qualified_name is None
        assert TextAnalyzer(organization_name="org").qualified_name is None

    def test_ref_static_method(self):
        assert TextAnalyzer.ref("org-n") == {"$ref": "org-n"}

    def test_round_trip(self):
        original = {
            "id": "1",
            "organizationName": "org",
            "name": "n",
            "description": "d",
            "settings": {"analyzer": {"default": {"type": "standard"}}},
            "etag": "e",
            "createdOn": "c",
            "createdBy": "u",
            "modifiedOn": "m",
            "modifiedBy": "u2",
        }
        ta = TextAnalyzer().fill_from_dict(original)
        assert ta.id == "1"
        assert ta.organization_name == "org"
        assert ta.name == "n"
        assert ta.settings == {"analyzer": {"default": {"type": "standard"}}}
        # Server-managed timestamps are NOT echoed back in to_synapse_request.
        body = ta.to_synapse_request()
        assert body == {
            "id": "1",
            "organizationName": "org",
            "name": "n",
            "description": "d",
            "settings": {"analyzer": {"default": {"type": "standard"}}},
            "etag": "e",
        }


class TestColumnAnalyzerOverrideEntry:
    def test_from_ref_builds_ref_dict(self):
        entry = ColumnAnalyzerOverrideEntry.from_ref("col1", "org-n")
        assert entry.column_name == "col1"
        assert entry.analyzer == {"$ref": "org-n"}

    def test_to_synapse_request_normalizes_string_to_ref(self):
        entry = ColumnAnalyzerOverrideEntry(column_name="col", analyzer="org-n")
        body = entry.to_synapse_request()
        assert body == {"columnName": "col", "analyzer": {"$ref": "org-n"}}

    def test_to_synapse_request_preserves_dict_analyzer(self):
        entry = ColumnAnalyzerOverrideEntry(
            column_name="col", analyzer={"$ref": "org-n"}
        )
        assert entry.to_synapse_request() == {
            "columnName": "col",
            "analyzer": {"$ref": "org-n"},
        }

    def test_fill_from_dict(self):
        entry = ColumnAnalyzerOverrideEntry().fill_from_dict(
            {"columnName": "c", "analyzer": {"$ref": "org-n"}}
        )
        assert entry.column_name == "c"
        assert entry.analyzer == {"$ref": "org-n"}


class TestColumnAnalyzerOverride:
    def test_round_trip_with_nested_overrides(self):
        data = {
            "id": "1",
            "organizationName": "org",
            "name": "n",
            "description": "d",
            "overrides": [
                {"columnName": "c1", "analyzer": {"$ref": "org-a1"}},
                {"columnName": "c2", "analyzer": {"$ref": "org-a2"}},
            ],
            "etag": "e",
            "createdOn": "co",
            "createdBy": "cu",
            "modifiedOn": "mo",
            "modifiedBy": "mu",
        }
        cao = ColumnAnalyzerOverride().fill_from_dict(data)
        assert len(cao.overrides) == 2
        assert cao.overrides[0].column_name == "c1"
        # Server-managed timestamps are NOT echoed back in to_synapse_request.
        body = cao.to_synapse_request()
        assert body == {
            "id": "1",
            "organizationName": "org",
            "name": "n",
            "description": "d",
            "overrides": [
                {"columnName": "c1", "analyzer": {"$ref": "org-a1"}},
                {"columnName": "c2", "analyzer": {"$ref": "org-a2"}},
            ],
            "etag": "e",
        }

    def test_qualified_name(self):
        assert (
            ColumnAnalyzerOverride(organization_name="org", name="n").qualified_name
            == "org-n"
        )


class TestSynonymSet:
    def test_round_trip(self):
        data = {
            "id": "1",
            "organizationName": "org",
            "name": "syn",
            "description": "d",
            "definition": {"type": "synonym_graph", "synonyms": ["a, b"]},
            "etag": "e",
            "createdOn": "co",
            "createdBy": "cu",
            "modifiedOn": "mo",
            "modifiedBy": "mu",
        }
        ss = SynonymSet().fill_from_dict(data)
        assert ss.definition["synonyms"] == ["a, b"]
        body = ss.to_synapse_request()
        assert body == {
            "id": "1",
            "organizationName": "org",
            "name": "syn",
            "description": "d",
            "definition": {"type": "synonym_graph", "synonyms": ["a, b"]},
            "etag": "e",
        }


class TestSearchConfiguration:
    def test_string_default_analyzer_serialized_as_ref(self):
        cfg = SearchConfiguration(
            organization_name="org",
            name="cfg",
            default_analyzer="org-an",
        )
        body = cfg.to_synapse_request()
        assert body["defaultAnalyzer"] == {"$ref": "org-an"}

    def test_dict_default_analyzer_preserved(self):
        cfg = SearchConfiguration(
            organization_name="org",
            name="cfg",
            default_analyzer={"$ref": "org-an"},
        )
        body = cfg.to_synapse_request()
        assert body["defaultAnalyzer"] == {"$ref": "org-an"}

    def test_string_overrides_normalized_to_ref(self):
        cfg = SearchConfiguration(
            organization_name="org",
            name="cfg",
            column_analyzer_overrides=["org-co1", {"$ref": "org-co2"}],
        )
        body = cfg.to_synapse_request()
        assert body["columnAnalyzerOverrides"] == [
            {"$ref": "org-co1"},
            {"$ref": "org-co2"},
        ]

    def test_empty_overrides_omitted(self):
        cfg = SearchConfiguration(organization_name="org", name="cfg")
        body = cfg.to_synapse_request()
        assert "columnAnalyzerOverrides" not in body

    def test_fill_from_dict(self):
        cfg = SearchConfiguration().fill_from_dict(
            {
                "id": "1",
                "organizationName": "org",
                "name": "cfg",
                "defaultAnalyzer": {"$ref": "org-an"},
                "columnAnalyzerOverrides": [{"$ref": "org-co"}],
            }
        )
        assert cfg.default_analyzer == {"$ref": "org-an"}
        assert cfg.column_analyzer_overrides == [{"$ref": "org-co"}]


class TestSearchConfigBinding:
    def test_fill_from_dict(self):
        binding = SearchConfigBinding().fill_from_dict(
            {
                "bindId": "b1",
                "searchConfigurationId": "42",
                "objectId": "syn1",
                "objectType": "ENTITY",
                "createdBy": "u",
                "createdOn": "now",
            }
        )
        assert binding.bind_id == "b1"
        assert binding.search_configuration_id == "42"
        assert binding.object_id == "syn1"


class TestSearchIndexStatus:
    def test_state_enum_coerced(self):
        status = SearchIndexStatus().fill_from_dict(
            {"searchIndexId": "syn1", "state": "ACTIVE", "changedOn": "now"}
        )
        assert status.state == SearchIndexState.ACTIVE
        assert status.search_index_id == "syn1"

    def test_state_missing_returns_none(self):
        status = SearchIndexStatus().fill_from_dict({"searchIndexId": "syn1"})
        assert status.state is None


class TestKeyValues:
    def test_python_keyword_not_serialized_as_not(self):
        kv = KeyValues(key="col", values=["a", "b"], not_=True)
        body = kv.to_synapse_request()
        assert body == {"key": "col", "values": ["a", "b"], "not": True}

    def test_fill_from_dict_handles_not_key(self):
        kv = KeyValues().fill_from_dict({"key": "c", "values": ["x"], "not": False})
        assert kv.key == "c"
        assert kv.not_ is False

    def test_to_synapse_request_drops_empty_values(self):
        kv = KeyValues(key="c")
        assert kv.to_synapse_request() == {"key": "c"}


class TestKeyRange:
    def test_min_max_keys(self):
        kr = KeyRange(key="c", min_value="1", max_value="10")
        body = kr.to_synapse_request()
        assert body == {"key": "c", "min": "1", "max": "10"}

    def test_fill_from_dict_min_max(self):
        kr = KeyRange().fill_from_dict({"key": "c", "min": "1", "max": "10"})
        assert kr.min_value == "1"
        assert kr.max_value == "10"


class TestFacetRequest:
    def test_enum_coercion_round_trip(self):
        fr = FacetRequest(
            column_name="c",
            max_value_count=5,
            sort_field=FacetSortField.COUNT,
            sort_direction=SortDirection.DESC,
        )
        body = fr.to_synapse_request()
        assert body == {
            "columnName": "c",
            "maxValueCount": 5,
            "sortField": "COUNT",
            "sortDirection": "DESC",
        }

    def test_fill_from_dict_coerces_enums(self):
        fr = FacetRequest().fill_from_dict(
            {"columnName": "c", "sortField": "KEY", "sortDirection": "ASC"}
        )
        assert fr.sort_field == FacetSortField.KEY
        assert fr.sort_direction == SortDirection.ASC


class TestSortField:
    def test_round_trip(self):
        sf = SortField(column_name="c", direction=SortDirection.DESC)
        assert sf.to_synapse_request() == {"columnName": "c", "direction": "DESC"}
        sf2 = SortField().fill_from_dict({"columnName": "c", "direction": "ASC"})
        assert sf2.direction == SortDirection.ASC


class TestSearchQuery:
    def test_full_round_trip(self):
        query = SearchQuery(
            query_type=SearchQueryType.MATCH,
            query_text="alzheimer",
            query_fields=["name^2"],
            terms_filters=[KeyValues(key="status", values=["active"])],
            range_filters=[KeyRange(key="year", min_value="2020")],
            exists_filters=["a"],
            not_exists_filters=["b"],
            fuzziness="AUTO",
            facet_requests=[FacetRequest(column_name="c")],
            return_fields=["id", "name"],
            sort=[SortField(column_name="rel", direction=SortDirection.ASC)],
            highlight=True,
            offset=0,
            limit=10,
        )
        body = query.to_synapse_request()
        assert body["queryType"] == "MATCH"
        assert body["queryText"] == "alzheimer"
        assert body["queryFields"] == ["name^2"]
        assert body["termsFilters"] == [{"key": "status", "values": ["active"]}]
        assert body["rangeFilters"] == [{"key": "year", "min": "2020"}]
        assert body["existsFilters"] == ["a"]
        assert body["notExistsFilters"] == ["b"]
        assert body["fuzziness"] == "AUTO"
        assert body["facetRequests"] == [{"columnName": "c"}]
        assert body["returnFields"] == ["id", "name"]
        assert body["sort"] == [{"columnName": "rel", "direction": "ASC"}]
        assert body["highlight"] is True
        assert body["offset"] == 0
        assert body["limit"] == 10

    def test_minimal_query_drops_empty_lists(self):
        query = SearchQuery(query_type=SearchQueryType.MATCH_ALL)
        body = query.to_synapse_request()
        assert body == {"queryType": "MATCH_ALL"}

    def test_fill_from_dict(self):
        query = SearchQuery().fill_from_dict(
            {
                "queryType": "MATCH",
                "queryText": "x",
                "termsFilters": [{"key": "k", "values": ["v"], "not": True}],
                "facetRequests": [{"columnName": "c"}],
            }
        )
        assert query.query_type == SearchQueryType.MATCH
        assert len(query.terms_filters) == 1
        assert query.terms_filters[0].not_ is True
        assert len(query.facet_requests) == 1


class TestSearchHit:
    def test_fill_from_dict(self):
        hit = SearchHit().fill_from_dict(
            {
                "rowId": 5,
                "rowVersion": 1,
                "score": 0.75,
                "fields": [{"name": "n", "value": "v"}],
                "highlights": [{"name": "h", "value": "<em>v</em>"}],
            }
        )
        assert hit.row_id == 5
        assert hit.score == 0.75
        assert isinstance(hit.fields[0], SearchFieldValue)
        assert hit.fields[0].name == "n"
        assert hit.highlights[0].value == "<em>v</em>"


class TestSearchIndexQuery:
    def test_concrete_type_default(self):
        q = SearchIndexQuery()
        assert q.concrete_type == concrete_types.SEARCH_INDEX_QUERY

    def test_to_synapse_request_includes_concrete_type_and_response_parts(self):
        q = SearchIndexQuery(
            search_index_id="syn1",
            search_query=SearchQuery(query_type=SearchQueryType.MATCH_ALL),
            response_parts=[SearchQueryPart.HITS, SearchQueryPart.TOTAL_HITS],
        )
        body = q.to_synapse_request()
        assert body["concreteType"] == concrete_types.SEARCH_INDEX_QUERY
        assert body["searchIndexId"] == "syn1"
        assert body["searchQuery"] == {"queryType": "MATCH_ALL"}
        assert body["responseParts"] == ["HITS", "TOTAL_HITS"]

    def test_to_synapse_request_omits_empty_response_parts(self):
        q = SearchIndexQuery(
            search_index_id="syn1",
            search_query=SearchQuery(query_type=SearchQueryType.MATCH_ALL),
        )
        body = q.to_synapse_request()
        assert "responseParts" not in body

    def test_fill_from_dict_populates_response_fields(self):
        q = SearchIndexQuery()
        q.fill_from_dict(
            {
                "hits": [
                    {"rowId": 1, "score": 0.5, "fields": [{"name": "a", "value": "b"}]}
                ],
                "totalHits": 100,
                "selectColumns": [{"name": "a", "columnType": "STRING"}],
                "facets": [{"facetType": "ENUMERATION"}],
                "offset": 0,
            }
        )
        assert len(q.hits) == 1
        assert q.hits[0].row_id == 1
        assert q.total_hits == 100
        assert len(q.select_columns) == 1
        assert q.facets == [{"facetType": "ENUMERATION"}]
        assert q.offset == 0

    async def test_send_job_and_wait_async_invokes_communicator(self):
        q = SearchIndexQuery(
            search_index_id="syn1",
            search_query=SearchQuery(query_type=SearchQueryType.MATCH_ALL),
        )

        sentinel_response = {
            "hits": [],
            "totalHits": 0,
            "selectColumns": [],
            "facets": [],
            "offset": 0,
        }

        with patch(
            "synapseclient.models.mixins.asynchronous_job.send_job_and_wait_async"
        ) as mock_send:

            async def _fake(*args, **kwargs):
                return sentinel_response

            mock_send.side_effect = _fake

            result = await q.send_job_and_wait_async(synapse_client=None)

            assert mock_send.await_count == 1
            sent_kwargs = mock_send.await_args.kwargs
            assert sent_kwargs["request_type"] == concrete_types.SEARCH_INDEX_QUERY
            assert sent_kwargs["request"]["concreteType"] == (
                concrete_types.SEARCH_INDEX_QUERY
            )
            # Response parts populated on the same instance
            assert result is q
            assert q.total_hits == 0
            assert q.hits == []


@pytest.mark.parametrize(
    ("enum_cls", "value"),
    [
        (SearchIndexState, "ACTIVE"),
        (SearchQueryPart, "HITS"),
        (SearchQueryType, "MATCH"),
        (SortDirection, "ASC"),
        (FacetSortField, "COUNT"),
    ],
)
def test_enum_string_values(enum_cls, value):
    """Each enum is a `str, Enum`, so its value equals the string literal."""
    member = enum_cls(value)
    assert member.value == value
    assert str(member.value) == value
