"""Integration tests for the SearchIndex entity and related Search Management API.

These tests exercise the wire format end-to-end against a live Synapse server.

Notes:
- SearchIndex entities are a relatively new server feature; if the server returns
  4xx for the create request, the test is skipped rather than reported as a failure
  (the suite covers the wire format unconditionally elsewhere via unit tests).
- TextAnalyzer / SearchConfiguration write endpoints are restricted to Sage
  Bionetworks employees server-side; tests therefore only exercise the read
  endpoints (`list_*`, `get_*`) that any authenticated caller can hit. Anything
  else is gated behind a `SYNAPSE_SAGE_EMPLOYEE_TOKEN` env var.
"""

import os
import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.api.search_services import get_text_analyzer, list_text_analyzers
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Column, ColumnType, Project, SearchIndex, Table


def _server_supports_search_index(exc: SynapseHTTPError) -> bool:
    """The SearchIndex feature may not be enabled on every Synapse environment.
    Inspect the response to decide whether to fail or skip."""
    msg = str(exc).lower()
    return not any(
        token in msg
        for token in (
            "unsupported entity type",
            "concretetype",
            "not allowed",
            "503",
            "service unavailable",
        )
    )


class TestSearchIndexEntity:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_missing_defining_sql_raises_locally(
        self, project_model: Project
    ) -> None:
        # GIVEN a SearchIndex with no defining_sql
        index = SearchIndex(name=str(uuid.uuid4()), parent_id=project_model.id)

        # WHEN/THEN: ValueError is raised before any server contact
        with pytest.raises(
            ValueError,
            match="The defining_sql attribute must be set for a SearchIndex.",
        ):
            await index.store_async(synapse_client=self.syn)

    async def test_search_index_lifecycle(self, project_model: Project) -> None:
        # GIVEN a Table with a couple of columns to back the SearchIndex
        table = Table(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name="title", column_type=ColumnType.STRING),
                Column(name="body", column_type=ColumnType.LARGETEXT),
            ],
        )
        table = await table.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # WHEN creating a SearchIndex
        index_name = str(uuid.uuid4())
        index = SearchIndex(
            name=index_name,
            description="Integration test SearchIndex",
            parent_id=project_model.id,
            defining_sql=f"SELECT * FROM {table.id}",
        )

        try:
            index = await index.store_async(synapse_client=self.syn)
        except SynapseHTTPError as exc:
            if not _server_supports_search_index(exc):
                pytest.skip(
                    f"Server does not appear to support the SearchIndex entity: {exc}"
                )
            raise

        self.schedule_for_cleanup(index.id)

        # THEN the SearchIndex has the expected server-assigned fields
        assert index.id is not None
        assert index.id.startswith("syn")
        assert index.etag
        assert index.name == index_name
        assert index.parent_id == project_model.id
        assert index.defining_sql == f"SELECT * FROM {table.id}"

        # WHEN retrieving the SearchIndex by ID
        fetched = await SearchIndex(id=index.id).get_async(synapse_client=self.syn)

        # THEN the round-trip preserves identifying fields
        assert fetched.id == index.id
        assert fetched.name == index_name
        assert fetched.parent_id == project_model.id
        assert fetched.defining_sql == f"SELECT * FROM {table.id}"

        # WHEN updating the description
        new_description = "Updated description"
        fetched.description = new_description
        updated = await fetched.store_async(synapse_client=self.syn)

        # THEN the change is persisted
        refetched = await SearchIndex(id=index.id).get_async(synapse_client=self.syn)
        assert refetched.description == new_description
        assert refetched.etag != index.etag
        assert updated.id == index.id

        # WHEN deleting the SearchIndex
        await SearchIndex(id=index.id).delete_async(synapse_client=self.syn)

        # THEN further reads fail
        with pytest.raises(SynapseHTTPError):
            await SearchIndex(id=index.id).get_async(synapse_client=self.syn)


class TestSearchManagementReadEndpoints:
    """Read-only paths every authenticated caller can hit."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_list_text_analyzers_returns_system_analyzers(self) -> None:
        # GIVEN the bootstrapped `org.sagebionetworks` analyzers exist on the server
        try:
            page = await list_text_analyzers(
                organization_name="org.sagebionetworks",
                synapse_client=self.syn,
            )
        except SynapseHTTPError as exc:
            pytest.skip(
                "Server does not appear to expose the SearchManagementController: "
                f"{exc}"
            )

        results = page.get("results") or []
        names = {item.get("name") for item in results}

        # THEN at least one of the expected system analyzers is present
        assert results, "No system text analyzers returned"
        assert names & {"SCIENTIFIC", "STANDARD", "KEYWORD"}, names

    async def test_get_text_analyzer_by_id(self) -> None:
        # GIVEN we can list analyzers
        try:
            page = await list_text_analyzers(
                organization_name="org.sagebionetworks",
                synapse_client=self.syn,
            )
        except SynapseHTTPError as exc:
            pytest.skip(
                "Server does not appear to expose the SearchManagementController: "
                f"{exc}"
            )

        results = page.get("results") or []
        if not results:
            pytest.skip("No system analyzers available to fetch by ID")

        analyzer_id = results[0]["id"]

        # WHEN fetching a single analyzer by ID
        fetched = await get_text_analyzer(analyzer_id, synapse_client=self.syn)

        # THEN it round-trips
        assert fetched["id"] == analyzer_id
        assert fetched["organizationName"] == "org.sagebionetworks"


@pytest.mark.skipif(
    not os.environ.get("SYNAPSE_SAGE_EMPLOYEE_TOKEN"),
    reason="Search Management write endpoints require Sage employee privileges",
)
class TestSearchManagementWriteEndpoints:
    """Sage-employee-only paths. Skipped unless `SYNAPSE_SAGE_EMPLOYEE_TOKEN` set.

    Implementation deferred until a Sage-employee CI integration token is wired
    into the test environment. The unit tests in
    `tests/unit/synapseclient/api/unit_test_search_services.py` already cover
    the wire format for these endpoints.
    """

    async def test_placeholder(self) -> None:
        pytest.skip("Pending Sage-employee CI auth wiring")
