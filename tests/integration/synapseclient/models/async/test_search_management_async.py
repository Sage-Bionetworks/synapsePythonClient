"""Integration tests for the org-scoped search-management resources: TextAnalyzer,
SynonymSet, ColumnAnalyzerOverride, NamedSearchPipeline, SearchConfiguration, and
SearchConfigBinding.

Each test creates its own uniquely-named resources under the shared
`search_organization`, so the tests do not depend on pre-seeded data or on each
other. TextAnalyzer, SynonymSet, ColumnAnalyzerOverride, NamedSearchPipeline,
and SearchConfiguration have no delete endpoint, so they are left in place after
the run and are never updated -- a test only creates and reads them.
Update dispatch is covered by the unit tests. SearchConfigBinding does support
delete; its test clears its own binding.
"""

import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    ColumnAnalyzerOverride,
    ColumnAnalyzerOverrideEntry,
    Folder,
    NamedSearchPipeline,
    Organization,
    Project,
    SearchConfigBinding,
    SearchConfiguration,
    SynonymSet,
    TextAnalyzer,
)


def _unique_name() -> str:
    """A resource name: starts with a letter, letters/digits/underscores only."""
    return f"synpy_{uuid.uuid4().hex}"


def _text_analyzer(org: str) -> TextAnalyzer:
    return TextAnalyzer(
        organization_name=org,
        name=_unique_name(),
        settings={
            "analyzer": {
                "default": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"],
                }
            }
        },
    )


def _synonym_set(org: str) -> SynonymSet:
    return SynonymSet(
        organization_name=org,
        name=_unique_name(),
        definition={"type": "synonym_graph", "synonyms": ["tumor, neoplasm, cancer"]},
    )


def _column_analyzer_override(org: str) -> ColumnAnalyzerOverride:
    return ColumnAnalyzerOverride(
        organization_name=org,
        name=_unique_name(),
        overrides=[
            ColumnAnalyzerOverrideEntry(
                column_name="disease_code",
                analyzer={"analyzer": {"default": {"type": "keyword"}}},
            ),
            ColumnAnalyzerOverrideEntry(column_name="abstract", semantic=True),
        ],
    )


def _named_search_pipeline(org: str) -> NamedSearchPipeline:
    return NamedSearchPipeline(
        organization_name=org,
        name=_unique_name(),
        settings={
            "phase_results_processors": [
                {
                    "normalization-processor": {
                        "normalization": {
                            "technique": "min_max",
                            "parameters": {
                                "lower_bounds": [
                                    {"mode": "clip", "min_score": 0.0},
                                    {"mode": "apply"},
                                ]
                            },
                        },
                        "combination": {
                            "technique": "arithmetic_mean",
                            "parameters": {"weights": [0.3, 0.7]},
                        },
                    }
                }
            ]
        },
    )


def _search_configuration(org: str) -> SearchConfiguration:
    return SearchConfiguration(organization_name=org, name=_unique_name())


# (factory, the payload attribute expected to survive the round trip)
RESOURCES = [
    (_text_analyzer, "settings"),
    (_synonym_set, "definition"),
    (_column_analyzer_override, "overrides"),
    (_named_search_pipeline, "settings"),
    (_search_configuration, "name"),
]


class TestOrgScopedResourceLifecycle:
    @pytest.mark.parametrize(
        "factory, payload_attribute",
        RESOURCES,
        ids=[factory.__name__.lstrip("_") for factory, _ in RESOURCES],
    )
    async def test_store_get_list(
        self,
        syn: Synapse,
        search_organization: Organization,
        factory: Callable[[str], object],
        payload_attribute: str,
    ) -> None:
        # GIVEN a new, uniquely-named resource in the shared Organization
        resource = factory(search_organization.name)
        expected_payload = getattr(resource, payload_attribute)
        cls = type(resource)

        # WHEN storing it
        created = await resource.store_async(synapse_client=syn)

        # THEN it is created with an ID and its payload intact
        assert created.id is not None
        assert getattr(created, payload_attribute) == expected_payload

        # AND it can be retrieved by ID
        retrieved = await cls(id=created.id).get_async(synapse_client=syn)
        assert retrieved.qualified_name == (
            f"{search_organization.name}-{resource.name}"
        )
        assert getattr(retrieved, payload_attribute) == expected_payload

        # AND it appears when listing the Organization's resources
        listed = await cls.list_async(
            organization_name=search_organization.name, synapse_client=syn
        )
        assert created.id in [item.id for item in listed]

    async def test_search_configuration_resolves_refs(
        self, syn: Synapse, search_organization: Organization
    ) -> None:
        # GIVEN a saved analyzer, override and search pipeline
        org = search_organization.name
        analyzer = await _text_analyzer(org).store_async(synapse_client=syn)
        override = await _column_analyzer_override(org).store_async(synapse_client=syn)
        pipeline = await _named_search_pipeline(org).store_async(synapse_client=syn)

        # WHEN a SearchConfiguration references all three by qualified name
        config = await SearchConfiguration(
            organization_name=org,
            name=_unique_name(),
            default_analyzer={"$ref": analyzer.qualified_name},
            default_search_pipeline={"$ref": pipeline.qualified_name},
            column_analyzer_overrides=[{"$ref": override.qualified_name}],
        ).store_async(synapse_client=syn)

        # THEN the references are saved as written
        retrieved = await SearchConfiguration(id=config.id).get_async(
            synapse_client=syn
        )
        assert retrieved.default_analyzer == {"$ref": analyzer.qualified_name}
        assert retrieved.default_search_pipeline == {"$ref": pipeline.qualified_name}
        assert retrieved.column_analyzer_overrides == [
            {"$ref": override.qualified_name}
        ]

        # AND a reference to a pipeline that does not exist is rejected
        with pytest.raises(SynapseHTTPError, match="does not exist"):
            await SearchConfiguration(
                organization_name=org,
                name=_unique_name(),
                default_search_pipeline={"$ref": f"{org}-missing_pipeline"},
            ).store_async(synapse_client=syn)


class TestSearchConfigBinding:
    async def test_bind_get_and_clear(
        self,
        syn: Synapse,
        search_organization: Organization,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a SearchConfiguration and a fresh Folder to bind it to
        config = await _search_configuration(search_organization.name).store_async(
            synapse_client=syn
        )
        folder = await Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)

        # WHEN binding it to the Folder
        binding = await SearchConfigBinding(
            object_id=folder.id,
            search_configuration_id=config.id,
        ).store_async(synapse_client=syn)

        # THEN the binding is created for that entity
        assert binding.bind_id is not None
        assert binding.object_id == folder.id.removeprefix("syn")
        assert binding.search_configuration_id == config.id

        # AND getting the effective binding on the same entity resolves to it
        effective = await SearchConfigBinding(object_id=folder.id).get_async(
            synapse_client=syn
        )
        assert effective.search_configuration_id == config.id

        # WHEN clearing the binding
        await SearchConfigBinding(object_id=folder.id).delete_async(synapse_client=syn)

        # THEN there is no longer an effective binding on that entity
        with pytest.raises(SynapseHTTPError):
            await SearchConfigBinding(object_id=folder.id).get_async(synapse_client=syn)
