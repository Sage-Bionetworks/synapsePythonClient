"""Unit tests for synapseclient.models.services.search_setup."""

from unittest.mock import AsyncMock, patch

import pytest

from synapseclient.models.services import search_setup
from synapseclient.models.services.search_setup import (
    EQUIVALENT,
    EXPLICIT,
    SYSTEM_ANALYZER_KEYWORD,
    SYSTEM_ANALYZER_SCIENTIFIC,
    _build_search_chain,
    _find_by_name,
    build_column_overrides,
    clone_settings_with_search_synonyms,
    ensure_search_configuration,
    ensure_synonym_aware_analyzer,
    ensure_synonym_set,
    render_synonym_definition,
)
from synapseclient.models.table_components import ColumnType


class TestRenderSynonymDefinition:
    def test_equivalent_rule(self):
        out = render_synonym_definition([(EQUIVALENT, ["a", "b", "c"])])
        assert out == {"type": "synonym_graph", "synonyms": ["a, b, c"]}

    def test_explicit_rule(self):
        out = render_synonym_definition([(EXPLICIT, ["a", "b", "c"])])
        assert out == {"type": "synonym_graph", "synonyms": ["a => a, b, c"]}

    def test_mixed_rules(self):
        out = render_synonym_definition(
            [(EQUIVALENT, ["x", "y"]), (EXPLICIT, ["foo", "bar"])]
        )
        assert out == {
            "type": "synonym_graph",
            "synonyms": ["x, y", "foo => foo, bar"],
        }

    def test_unknown_rule_kind_raises(self):
        with pytest.raises(ValueError, match="Unknown synonym rule kind"):
            render_synonym_definition([("UNKNOWN", ["a"])])


class TestBuildSearchChain:
    def test_lowercase_hoisted_with_synonym_slot_after(self):
        chain = _build_search_chain(["lowercase", "stop"], "syn_slot")
        assert chain == ["lowercase", "syn_slot", "stop"]

    def test_lowercase_added_when_missing_from_default(self):
        chain = _build_search_chain(["stop"], "syn_slot")
        assert chain == ["lowercase", "syn_slot", "stop"]

    def test_empty_default(self):
        chain = _build_search_chain([], "syn_slot")
        assert chain == ["lowercase", "syn_slot"]


class TestCloneSettingsWithSearchSynonyms:
    def test_basic_clone_adds_default_search(self):
        base = {
            "analyzer": {
                "default": {"tokenizer": "standard", "filter": ["lowercase", "stop"]}
            },
            "filter": {},
        }
        out = clone_settings_with_search_synonyms(base, "org-syn")
        assert "default" in out["analyzer"]
        assert "default_search" in out["analyzer"]
        assert out["filter"]["synonyms"] == {"$ref": "org-syn"}
        assert out["analyzer"]["default_search"]["filter"][:2] == [
            "lowercase",
            "synonyms",
        ]

    def test_does_not_mutate_input(self):
        base = {
            "analyzer": {"default": {"tokenizer": "standard", "filter": ["lowercase"]}}
        }
        original_copy = {
            "analyzer": {"default": {"tokenizer": "standard", "filter": ["lowercase"]}}
        }
        clone_settings_with_search_synonyms(base, "org-syn")
        assert base == original_copy

    def test_missing_default_raises(self):
        with pytest.raises(ValueError, match="missing required `analyzer.default`"):
            clone_settings_with_search_synonyms({"analyzer": {}}, "org-syn")

    def test_extra_analyzer_keys_raises(self):
        base = {
            "analyzer": {
                "default": {"tokenizer": "standard"},
                "weird_extra": {"tokenizer": "keyword"},
            }
        }
        with pytest.raises(ValueError, match="must contain only 'default'"):
            clone_settings_with_search_synonyms(base, "org-syn")

    def test_filter_slot_collision_raises(self):
        base = {
            "analyzer": {"default": {"tokenizer": "standard"}},
            "filter": {"synonyms": {"type": "synonym"}},
        }
        with pytest.raises(ValueError, match="already used in base analyzer settings"):
            clone_settings_with_search_synonyms(base, "org-syn")

    def test_existing_default_search_preserved_and_extended(self):
        base = {
            "analyzer": {
                "default": {"tokenizer": "standard", "filter": ["lowercase", "stop"]},
                "default_search": {
                    "tokenizer": "edge_ngram",
                    "filter": ["lowercase", "stem"],
                },
            }
        }
        out = clone_settings_with_search_synonyms(base, "org-syn")
        # default_search starts from existing search analyzer, not default
        assert out["analyzer"]["default_search"]["tokenizer"] == "edge_ngram"
        assert out["analyzer"]["default_search"]["filter"][:2] == [
            "lowercase",
            "synonyms",
        ]


class TestBuildColumnOverrides:
    def test_columns_matching_default_substitution_skipped(self):
        # All STRING columns default to SCIENTIFIC; if default is the synonym-aware
        # clone of SCIENTIFIC, those columns inherit -> no override.
        result = build_column_overrides(
            {"col1": ColumnType.STRING, "col2": ColumnType.STRING},
            default_analyzer_qname="org-clone-of-scientific",
            default_substitutes_system_qname=SYSTEM_ANALYZER_SCIENTIFIC,
            system_analyzer_substitutions={
                SYSTEM_ANALYZER_SCIENTIFIC: "org-clone-of-scientific"
            },
        )
        assert result == []

    def test_keyword_columns_pinned_when_default_is_scientific(self):
        # INTEGER → KEYWORD by default, but the default analyzer is SCIENTIFIC clone.
        # That mismatch produces an override pinned to KEYWORD.
        result = build_column_overrides(
            {"col1": ColumnType.STRING, "id_col": ColumnType.INTEGER},
            default_analyzer_qname="org-clone-of-scientific",
            default_substitutes_system_qname=SYSTEM_ANALYZER_SCIENTIFIC,
            system_analyzer_substitutions={
                SYSTEM_ANALYZER_SCIENTIFIC: "org-clone-of-scientific"
            },
        )
        assert result == [("id_col", SYSTEM_ANALYZER_KEYWORD)]

    def test_substituted_keyword_used_when_provided(self):
        result = build_column_overrides(
            {"id_col": ColumnType.INTEGER},
            default_analyzer_qname="org-clone-of-scientific",
            default_substitutes_system_qname=SYSTEM_ANALYZER_SCIENTIFIC,
            system_analyzer_substitutions={
                SYSTEM_ANALYZER_SCIENTIFIC: "org-clone-of-scientific",
                SYSTEM_ANALYZER_KEYWORD: "org-clone-of-keyword",
            },
        )
        assert result == [("id_col", "org-clone-of-keyword")]


class TestFindByName:
    async def test_returns_match_in_first_page(self):
        list_fn = AsyncMock(
            return_value={
                "results": [{"name": "alpha"}, {"name": "beta"}],
                "nextPageToken": None,
            }
        )

        out = await _find_by_name(list_fn, "org", "beta", synapse_client=None)

        assert out == {"name": "beta"}
        assert list_fn.await_count == 1

    async def test_paginates_through_pages(self):
        responses = [
            {"results": [{"name": "alpha"}], "nextPageToken": "tok1"},
            {"results": [{"name": "beta"}], "nextPageToken": None},
        ]
        list_fn = AsyncMock(side_effect=responses)

        out = await _find_by_name(list_fn, "org", "beta", synapse_client=None)

        assert out == {"name": "beta"}
        assert list_fn.await_count == 2

    async def test_returns_none_when_not_found(self):
        list_fn = AsyncMock(
            return_value={"results": [{"name": "alpha"}], "nextPageToken": None}
        )

        out = await _find_by_name(list_fn, "org", "missing", synapse_client=None)

        assert out is None


class TestEnsureSynonymSet:
    async def test_returns_existing_when_found(self):
        existing = {"id": "1", "organizationName": "org", "name": "syn"}

        with (
            patch.object(
                search_setup, "_find_by_name", AsyncMock(return_value=existing)
            ),
            patch.object(search_setup, "create_synonym_set") as mock_create,
        ):

            ss = await ensure_synonym_set(
                "org", "syn", rules=[(EQUIVALENT, ["a", "b"])], synapse_client=None
            )

            assert ss.id == "1"
            mock_create.assert_not_called()

    async def test_creates_when_missing(self):
        created = {"id": "2", "organizationName": "org", "name": "syn"}

        with (
            patch.object(search_setup, "_find_by_name", AsyncMock(return_value=None)),
            patch.object(
                search_setup, "create_synonym_set", AsyncMock(return_value=created)
            ) as mock_create,
        ):

            ss = await ensure_synonym_set(
                "org", "syn", rules=[(EQUIVALENT, ["a", "b"])], synapse_client=None
            )

            assert ss.id == "2"
            mock_create.assert_awaited_once()
            request_body = mock_create.await_args.kwargs["request"]
            assert request_body["definition"] == {
                "type": "synonym_graph",
                "synonyms": ["a, b"],
            }


class TestEnsureSynonymAwareAnalyzer:
    async def test_returns_existing_when_found(self):
        existing = {"id": "1", "organizationName": "org", "name": "ta"}

        with (
            patch.object(
                search_setup, "_find_by_name", AsyncMock(return_value=existing)
            ),
            patch.object(search_setup, "create_text_analyzer") as mock_create,
        ):

            ta = await ensure_synonym_aware_analyzer(
                "org",
                "ta",
                base_system_analyzer_name="SCIENTIFIC",
                synonym_set_qname="org-syn",
                synapse_client=None,
            )

            assert ta.id == "1"
            mock_create.assert_not_called()

    async def test_creates_clone_when_missing(self):
        base_settings = {
            "analyzer": {"default": {"tokenizer": "standard", "filter": ["lowercase"]}}
        }

        async def _system_lookup(name, *, synapse_client):
            from synapseclient.models.search_management import TextAnalyzer

            return TextAnalyzer(
                organization_name="org.sagebionetworks",
                name=name,
                settings=base_settings,
            )

        created = {"id": "99", "organizationName": "org", "name": "ta"}

        with (
            patch.object(search_setup, "_find_by_name", AsyncMock(return_value=None)),
            patch.object(
                search_setup, "get_system_analyzer", side_effect=_system_lookup
            ),
            patch.object(
                search_setup, "create_text_analyzer", AsyncMock(return_value=created)
            ) as mock_create,
        ):

            ta = await ensure_synonym_aware_analyzer(
                "org",
                "ta",
                base_system_analyzer_name="SCIENTIFIC",
                synonym_set_qname="org-syn",
                synapse_client=None,
            )

            assert ta.id == "99"
            request = mock_create.await_args.kwargs["request"]
            assert "default_search" in request["settings"]["analyzer"]
            assert request["settings"]["filter"]["synonyms"] == {"$ref": "org-syn"}


class TestEnsureSearchConfiguration:
    async def test_returns_existing(self):
        existing = {
            "id": "5",
            "organizationName": "org",
            "name": "cfg",
            "defaultAnalyzer": {"$ref": "org-an"},
        }

        with (
            patch.object(
                search_setup, "_find_by_name", AsyncMock(return_value=existing)
            ),
            patch.object(search_setup, "create_search_configuration") as mock_create,
        ):

            cfg = await ensure_search_configuration(
                "org",
                "cfg",
                default_analyzer_qname="org-an",
                synapse_client=None,
            )

            assert cfg.id == "5"
            mock_create.assert_not_called()

    async def test_creates_with_inline_overrides(self):
        created = {
            "id": "6",
            "organizationName": "org",
            "name": "cfg",
        }

        with (
            patch.object(search_setup, "_find_by_name", AsyncMock(return_value=None)),
            patch.object(
                search_setup,
                "create_search_configuration",
                AsyncMock(return_value=created),
            ) as mock_create,
        ):

            cfg = await ensure_search_configuration(
                "org",
                "cfg",
                default_analyzer_qname="org-an",
                column_overrides=[("col1", "org-co")],
                synapse_client=None,
            )

            assert cfg.id == "6"
            request = mock_create.await_args.kwargs["request"]
            assert request["defaultAnalyzer"] == {"$ref": "org-an"}
            inline = request["columnAnalyzerOverrides"][0]
            assert inline["overrides"] == [
                {"columnName": "col1", "analyzer": {"$ref": "org-co"}}
            ]
