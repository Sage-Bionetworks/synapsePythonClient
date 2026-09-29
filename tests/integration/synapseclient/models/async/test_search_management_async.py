"""Integration tests for the org-scoped search-management resources: TextAnalyzer,
SynonymSet, ColumnAnalyzerOverride, SearchConfiguration, and SearchConfigBinding.

These tests run against the **dev** Synapse environment. All resources are
created in dev. If you run these tests locally, make sure to point your
client to the dev endpoints.

TextAnalyzer, SynonymSet, ColumnAnalyzerOverride, and SearchConfiguration have no
delete endpoint on the Synapse REST API, so these tests do not create or update
them -- they only get and list a fixed set of resources pre-seeded under the
`SEARCH_ORG_NAME` organization (identified by the `*_NAME`/`*_ID` constants
below), which keeps the tests idempotent and safe to run concurrently on CI.
Because an Organization cannot be deleted once one of these resources has been
attached to it, `SEARCH_ORG_NAME` is a permanent, shared test Organization
rather than one created fresh per test run.
Each resource's class docstring shows the `store()` call used to seed it.
SearchConfigBinding does support delete/clear; its test creates and tears down
its own binding on a freshly-created Folder.
"""

import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    ColumnAnalyzerOverride,
    Folder,
    Project,
    SearchConfigBinding,
    SearchConfiguration,
    SynonymSet,
    TextAnalyzer,
)

SEARCH_ORG_NAME = "SYNPY.TEST.SEARCH.MANAGEMENT"
TEXT_ANALYZER_NAME = "test_analyzer"
TEXT_ANALYZER_ID = "1001"
SYNONYM_SET_NAME = "test_synonyms"
SYNONYM_SET_ID = "1"
COLUMN_ANALYZER_OVERRIDE_NAME = "disease_column_overrides"
COLUMN_ANALYZER_OVERRIDE_ID = "1"
TEST_CONFIG_NAME = "test_config"
TEST_CONFIG_ID = "2"


@pytest.fixture(scope="function")
async def folder(
    project_model: Project,
    syn: Synapse,
    schedule_for_cleanup: Callable[..., None],
) -> Folder:
    """A fresh Folder under the shared test Project, used as the bind target for
    SearchConfigBinding tests instead of the shared Project itself."""
    folder = await Folder(
        name=str(uuid.uuid4()),
        parent_id=project_model.id,
    ).store_async(synapse_client=syn)
    schedule_for_cleanup(folder.id)
    return folder


class TestTextAnalyzer:
    async def test_get_and_list(self, syn: Synapse) -> None:
        """
        Test that a TextAnalyzer can be retrieved by ID and listed in the
        organization.

        The TestTextAnalyzer was stored like below:

        from synapseclient import Synapse
        from synapseclient.models import TextAnalyzer


        syn = Synapse()
        syn.login()
        analyzer = TextAnalyzer(
            organization_name="SYNPY.TEST.SEARCH.MANAGEMENT",
            name="test_analyzer",
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
        analyzer = analyzer.store(synapse_client=syn)
        print(f"Created TextAnalyzer: {analyzer.id} ({analyzer.qualified_name})")
        """
        # GIVEN a TextAnalyzer definition
        name = TEXT_ANALYZER_NAME
        settings = {
            "analyzer": {
                "default": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"],
                }
            }
        }

        # AND it can be retrieved by ID with its settings intact
        retrieved = await TextAnalyzer(id=TEXT_ANALYZER_ID).get_async(
            synapse_client=syn
        )
        assert retrieved.settings == settings
        # AND it appears when listing analyzers in the organization
        listed = await TextAnalyzer.list_async(
            organization_name=SEARCH_ORG_NAME, synapse_client=syn
        )
        assert name in [item.name for item in listed]


class TestSynonymSet:
    async def test_get_and_list(self, syn: Synapse) -> None:
        """
        Test that a SynonymSet can be retrieved by ID and listed in the
        organization.

        The TestSynonymSet was stored like below:

        from synapseclient import Synapse
        from synapseclient.models import SynonymSet


        syn = Synapse()
        syn.login()
        synonyms = SynonymSet(
            organization_name="SYNPY.TEST.SEARCH.MANAGEMENT",
            name="test_synonyms",
            definition={
                "type": "synonym_graph",
                "synonyms": ["tumor, neoplasm, cancer"],
            },
        )
        synonyms = synonyms.store(synapse_client=syn)
        print(f"Created SynonymSet: {synonyms.id} ({synonyms.qualified_name})")
        """
        # GIVEN a SynonymSet definition
        name = SYNONYM_SET_NAME
        definition = {
            "type": "synonym_graph",
            "synonyms": ["tumor, neoplasm, cancer"],
        }

        # AND it can be retrieved by ID with its definition intact
        retrieved = await SynonymSet(id=SYNONYM_SET_ID).get_async(synapse_client=syn)
        assert retrieved.definition == definition
        # AND it appears when listing synonym sets in the organization
        listed = await SynonymSet.list_async(
            organization_name=SEARCH_ORG_NAME, synapse_client=syn
        )
        assert name in [item.name for item in listed]


class TestColumnAnalyzerOverride:
    async def test_get_and_list(self, syn: Synapse) -> None:
        """
        Test that a ColumnAnalyzerOverride can be retrieved by ID and listed in the
        organization.

        The TestColumnAnalyzerOverride was stored like below:

        from synapseclient import Synapse
        from synapseclient.models import ColumnAnalyzerOverride, ColumnAnalyzerOverrideEntry


        syn = Synapse()
        syn.login()
        override = ColumnAnalyzerOverride(
            organization_name="SYNPY.TEST.SEARCH.MANAGEMENT",
            name="disease_column_overrides",
            description="Use a keyword analyzer for the disease_code column",
            overrides=[
                ColumnAnalyzerOverrideEntry(
                    column_name="disease_code",
                    analyzer={"analyzer": {"default": {"type": "keyword"}}},
                ),
            ],
        )
        override = override.store(synapse_client=syn)
        print(f"Created ColumnAnalyzerOverride: {override.id} ({override.qualified_name})")
        """
        # GIVEN a ColumnAnalyzerOverride with a single inline analyzer entry
        name = COLUMN_ANALYZER_OVERRIDE_NAME

        # AND it can be retrieved by ID with its entry intact
        retrieved = await ColumnAnalyzerOverride(
            id=COLUMN_ANALYZER_OVERRIDE_ID
        ).get_async(synapse_client=syn)
        assert retrieved.overrides[0].column_name == "disease_code"
        # AND it appears when listing overrides in the organization
        listed = await ColumnAnalyzerOverride.list_async(
            organization_name=SEARCH_ORG_NAME, synapse_client=syn
        )
        assert name in [item.name for item in listed]


class TestSearchConfiguration:
    async def test_get_and_list(self, syn: Synapse) -> None:
        """
        Test that a SearchConfiguration can be retrieved by ID and listed in the
        organization.

        The TestSearchConfiguration was stored like below:

        from synapseclient import Synapse
        from synapseclient.models import ColumnAnalyzerOverride, ColumnAnalyzerOverrideEntry, SearchConfiguration, TextAnalyzer


        syn = Synapse()
        syn.login()
        analyzer = TextAnalyzer(
            organization_name="SYNPY.TEST.SEARCH.MANAGEMENT",
            name="test_analyzer",
            settings={
                "analyzer": {
                    "default": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase"],
                    }
                }
            },
        ).store(synapse_client=syn)
        override = ColumnAnalyzerOverride(
            organization_name="SYNPY.TEST.SEARCH.MANAGEMENT",
            name="disease_column_overrides",
            description="Use a keyword analyzer for the disease_code column",
            overrides=[
                ColumnAnalyzerOverrideEntry(
                    column_name="disease_code",
                    analyzer={"analyzer": {"default": {"type": "keyword"}}},
                ),
            ],
        ).store(synapse_client=syn)
        config = SearchConfiguration(
            organization_name="SYNPY.TEST.SEARCH.MANAGEMENT",
            name="test_config",
            default_analyzer={"$ref": analyzer.qualified_name},
            column_analyzer_overrides=[{"$ref": override.qualified_name}],
        )
        config = config.store(synapse_client=syn)
        print(f"Created SearchConfiguration: {config.id} ({config.qualified_name})")
        """
        # GIVEN a SearchConfiguration referencing a TextAnalyzer and
        # ColumnAnalyzerOverride by qualified name
        name = TEST_CONFIG_NAME
        analyzer_ref = {"$ref": f"{SEARCH_ORG_NAME}-{TEXT_ANALYZER_NAME}"}
        override_ref = {"$ref": f"{SEARCH_ORG_NAME}-{COLUMN_ANALYZER_OVERRIDE_NAME}"}

        # AND it can be retrieved by ID with its analyzer references intact
        retrieved = await SearchConfiguration(id=TEST_CONFIG_ID).get_async(
            synapse_client=syn
        )
        assert retrieved.default_analyzer == analyzer_ref
        assert retrieved.column_analyzer_overrides == [override_ref]
        # AND it appears when listing configurations in the organization
        listed = await SearchConfiguration.list_async(
            organization_name=SEARCH_ORG_NAME, synapse_client=syn
        )
        assert name in [item.name for item in listed]


class TestSearchConfigBinding:
    async def test_bind_get_and_clear(self, syn: Synapse, folder: Folder) -> None:
        """
        Test that a SearchConfiguration can be bound to a Folder, that the
        effective binding resolves to it on a fresh get, and that clearing the
        binding removes it.
        """
        # GIVEN a SearchConfiguration to bind
        config = await SearchConfiguration(id=TEST_CONFIG_ID).get_async(
            synapse_client=syn
        )
        # WHEN binding it to a Folder
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
