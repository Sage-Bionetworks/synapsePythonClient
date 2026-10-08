"""Fixtures shared by the async model integration tests."""

import pytest_asyncio

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Organization

SEARCH_ORG_NAME = "SYNPY.TEST.SEARCH.MANAGEMENT"


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def search_organization(syn: Synapse) -> Organization:
    """The shared Organization that owns the search-management resources
    (TextAnalyzer, SynonymSet, NamedSearchPipeline, ...) created by the tests.

    Those resources have no delete endpoint, and an Organization cannot be
    deleted while it still owns one, so every run reuses this one Organization
    and creates it only if it does not exist yet.
    """
    try:
        return await Organization(name=SEARCH_ORG_NAME).get_async(synapse_client=syn)
    except SynapseHTTPError as e:
        if e.response.status_code != 404:
            raise
    try:
        return await Organization(name=SEARCH_ORG_NAME).store_async(synapse_client=syn)
    except SynapseHTTPError as e:
        # Another xdist worker created it first
        if "already exists" not in str(e):
            raise
        return await Organization(name=SEARCH_ORG_NAME).get_async(synapse_client=syn)
