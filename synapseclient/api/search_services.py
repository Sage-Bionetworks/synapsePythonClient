"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/index.html#org.sagebionetworks.repo.web.controller.SearchManagementController>

It covers TextAnalyzer, ColumnAnalyzerOverride, SynonymSet, SearchConfiguration,
SearchConfigBinding, and the synchronous SearchIndex autocomplete endpoint.
The async SearchIndex query endpoint is exposed via the
`SearchIndexQuery.send_job_and_wait_async()` method on the model class
(`models.search_management.SearchIndexQuery`), which uses the shared
`AsynchronousCommunicator` mixin.
"""

import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse


# ---------- Text Analyzer ----------


async def create_text_analyzer(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a TextAnalyzer.

    <https://rest-docs.synapse.org/rest/POST/search/text/analyzer.html>

    Arguments:
        request: TextAnalyzer body. Must include organizationName, name, settings.
            `settings` is a JSON object (the contents of the OpenSearch
            `settings.analysis` block), not a JSON-encoded string.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The created TextAnalyzer.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri="/search/text/analyzer", body=json.dumps(request)
    )


async def get_text_analyzer(
    analyzer_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Get a TextAnalyzer by ID.

    <https://rest-docs.synapse.org/rest/GET/search/text/analyzer/id.html>

    Arguments:
        analyzer_id: The numeric ID of the text analyzer to retrieve.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The requested TextAnalyzer.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/search/text/analyzer/{analyzer_id}")


async def update_text_analyzer(
    analyzer_id: str,
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Update a TextAnalyzer.

    <https://rest-docs.synapse.org/rest/PUT/search/text/analyzer/id.html>

    Arguments:
        analyzer_id: The path ID (must match the request body's ID).
        request: The updated TextAnalyzer.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The updated TextAnalyzer.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri=f"/search/text/analyzer/{analyzer_id}", body=json.dumps(request)
    )


async def list_text_analyzers(
    organization_name: Optional[str] = None,
    next_page_token: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """List TextAnalyzers, optionally filtered by Organization.

    <https://rest-docs.synapse.org/rest/POST/search/text/analyzer/list.html>

    Arguments:
        organization_name: Optional filter by organization name.
        next_page_token: Optional pagination token.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        Page of TextAnalyzers and a nextPageToken if more results exist.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    body = {"organizationName": organization_name, "nextPageToken": next_page_token}
    body = {k: v for k, v in body.items() if v is not None}
    return await client.rest_post_async(
        uri="/search/text/analyzer/list", body=json.dumps(body)
    )


# ---------- Column Analyzer Override ----------


async def create_column_analyzer_override(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a ColumnAnalyzerOverride.

    <https://rest-docs.synapse.org/rest/POST/search/column/analyzer/override.html>

    Each entry under `request["overrides"]` carries `analyzer` as a JSON
    object — either a `$ref` reference `{"$ref": "{organizationName}-{name}"}`
    to a saved TextAnalyzer or an inline TextAnalyzer literal — not a bare
    qualified-name string.

    Arguments:
        request: ColumnAnalyzerOverride body. Must include organizationName, name,
            and overrides.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The created ColumnAnalyzerOverride.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri="/search/column/analyzer/override", body=json.dumps(request)
    )


async def get_column_analyzer_override(
    column_analyzer_override_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Get a ColumnAnalyzerOverride by ID.

    <https://rest-docs.synapse.org/rest/GET/search/column/analyzer/override/columnAnalyzerOverrideId.html>

    Arguments:
        column_analyzer_override_id: The numeric ID of the column analyzer
            override to retrieve.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The requested ColumnAnalyzerOverride.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/search/column/analyzer/override/{column_analyzer_override_id}"
    )


async def update_column_analyzer_override(
    column_analyzer_override_id: str,
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Update a ColumnAnalyzerOverride.

    <https://rest-docs.synapse.org/rest/PUT/search/column/analyzer/override/columnAnalyzerOverrideId.html>

    Arguments:
        column_analyzer_override_id: The path ID (must match the request body's ID).
        request: The updated ColumnAnalyzerOverride.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The updated ColumnAnalyzerOverride.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri=f"/search/column/analyzer/override/{column_analyzer_override_id}",
        body=json.dumps(request),
    )


async def list_column_analyzer_overrides(
    organization_name: Optional[str] = None,
    next_page_token: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """List ColumnAnalyzerOverrides, optionally filtered by Organization.

    <https://rest-docs.synapse.org/rest/POST/search/column/analyzer/override/list.html>

    Arguments:
        organization_name: Optional filter by organization name.
        next_page_token: Optional pagination token.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        Page of ColumnAnalyzerOverrides and a nextPageToken if more results exist.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    body = {"organizationName": organization_name, "nextPageToken": next_page_token}
    body = {k: v for k, v in body.items() if v is not None}
    return await client.rest_post_async(
        uri="/search/column/analyzer/override/list", body=json.dumps(body)
    )


# ---------- Synonym Set ----------


async def create_synonym_set(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a SynonymSet.

    <https://rest-docs.synapse.org/rest/POST/search/synonym/set.html>

    `request["definition"]` is a JSON object (the OpenSearch token-filter
    definition), not a JSON-encoded string.

    Arguments:
        request: SynonymSet body. Must include organizationName, name, definition.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The created SynonymSet.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri="/search/synonym/set", body=json.dumps(request)
    )


async def get_synonym_set(
    synonym_set_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Get a SynonymSet by ID.

    <https://rest-docs.synapse.org/rest/GET/search/synonym/set/synonymSetId.html>

    Arguments:
        synonym_set_id: The numeric ID of the synonym set to retrieve.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The requested SynonymSet.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/search/synonym/set/{synonym_set_id}")


async def update_synonym_set(
    synonym_set_id: str,
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Update a SynonymSet.

    <https://rest-docs.synapse.org/rest/PUT/search/synonym/set/synonymSetId.html>

    Arguments:
        synonym_set_id: The path ID (must match the request body's ID).
        request: The updated SynonymSet.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The updated SynonymSet.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri=f"/search/synonym/set/{synonym_set_id}", body=json.dumps(request)
    )


async def list_synonym_sets(
    organization_name: Optional[str] = None,
    next_page_token: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """List SynonymSets, optionally filtered by Organization.

    <https://rest-docs.synapse.org/rest/POST/search/synonym/set/list.html>

    Arguments:
        organization_name: Optional filter by organization name.
        next_page_token: Optional pagination token.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        Page of SynonymSets and a nextPageToken if more results exist.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    body = {"organizationName": organization_name, "nextPageToken": next_page_token}
    body = {k: v for k, v in body.items() if v is not None}
    return await client.rest_post_async(
        uri="/search/synonym/set/list", body=json.dumps(body)
    )


# ---------- Search Configuration ----------


async def create_search_configuration(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a SearchConfiguration.

    <https://rest-docs.synapse.org/rest/POST/search/configuration.html>

    `request["defaultAnalyzer"]` is a JSON object — either a `$ref` reference
    `{"$ref": "{organizationName}-{name}"}` to a saved TextAnalyzer or an
    inline TextAnalyzer literal. Each entry of
    `request["columnAnalyzerOverrides"]` is likewise a `$ref` dict or an
    inline ColumnAnalyzerOverride literal — not a bare qualified-name string.

    Arguments:
        request: SearchConfiguration body. Must include organizationName and name.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The created SearchConfiguration.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri="/search/configuration", body=json.dumps(request)
    )


async def get_search_configuration(
    search_configuration_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Get a SearchConfiguration by ID.

    <https://rest-docs.synapse.org/rest/GET/search/configuration/searchConfigurationId.html>

    Arguments:
        search_configuration_id: The numeric ID of the search configuration
            to retrieve.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The requested SearchConfiguration.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/search/configuration/{search_configuration_id}"
    )


async def update_search_configuration(
    search_configuration_id: str,
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Update a SearchConfiguration.

    <https://rest-docs.synapse.org/rest/PUT/search/configuration/searchConfigurationId.html>

    Arguments:
        search_configuration_id: The path ID (must match the request body's ID).
        request: The updated SearchConfiguration.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The updated SearchConfiguration.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri=f"/search/configuration/{search_configuration_id}",
        body=json.dumps(request),
    )


async def list_search_configurations(
    organization_name: Optional[str] = None,
    next_page_token: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """List SearchConfigurations, optionally filtered by Organization.

    <https://rest-docs.synapse.org/rest/POST/search/configuration/list.html>

    Arguments:
        organization_name: Optional filter by organization name.
        next_page_token: Optional pagination token.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        Page of SearchConfigurations and a nextPageToken if more results exist.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    body = {"organizationName": organization_name, "nextPageToken": next_page_token}
    body = {k: v for k, v in body.items() if v is not None}
    return await client.rest_post_async(
        uri="/search/configuration/list", body=json.dumps(body)
    )


# ---------- Search Configuration Bindings ----------


async def bind_search_config_to_entity(
    entity_id: str,
    search_configuration_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Bind a SearchConfiguration to an entity. Replaces any existing binding.

    <https://rest-docs.synapse.org/rest/PUT/entity/entityId/searchconfig/binding.html>

    Arguments:
        entity_id: The ID of the entity to bind to.
        search_configuration_id: The ID of the SearchConfiguration to bind.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The created SearchConfigBinding.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    body = {
        "entityId": entity_id,
        "searchConfigurationId": search_configuration_id,
    }
    return await client.rest_put_async(
        uri=f"/entity/{entity_id}/searchconfig/binding", body=json.dumps(body)
    )


async def get_search_config_binding(
    entity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Get the effective SearchConfigBinding for an entity by walking up the
    hierarchy.

    <https://rest-docs.synapse.org/rest/GET/entity/entityId/searchconfig/binding.html>

    Arguments:
        entity_id: The ID of the entity whose effective binding to resolve.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The effective SearchConfigBinding.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/entity/{entity_id}/searchconfig/binding")


async def clear_search_config_binding(
    entity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """Clear the SearchConfigBinding on a specific entity.

    <https://rest-docs.synapse.org/rest/DELETE/entity/entityId/searchconfig/binding.html>

    Arguments:
        entity_id: The ID of the entity whose binding to clear.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    await client.rest_delete_async(uri=f"/entity/{entity_id}/searchconfig/binding")


# ---------- Search Queries ----------
#
# The async search query endpoint (POST /search/query/async/start +
# GET /search/query/async/get/{token}) is exposed through the shared
# AsynchronousCommunicator mixin. Use:
#
#     query = SearchIndexQuery(search_index_id=..., search_query=SearchQuery(...))
#     await query.send_job_and_wait_async()
#
# The synchronous autocomplete endpoint stays here because it is not an
# async job.


async def autocomplete_search(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Synchronous autocomplete search. Caps results at 8.

    <https://rest-docs.synapse.org/rest/POST/search/autocomplete.html>

    Arguments:
        request: SearchIndexQuery body (must include searchIndexId).
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        SearchQueryResults.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri="/search/autocomplete", body=json.dumps(request)
    )


__all__: List[str] = [
    "create_text_analyzer",
    "get_text_analyzer",
    "update_text_analyzer",
    "list_text_analyzers",
    "create_column_analyzer_override",
    "get_column_analyzer_override",
    "update_column_analyzer_override",
    "list_column_analyzer_overrides",
    "create_synonym_set",
    "get_synonym_set",
    "update_synonym_set",
    "list_synonym_sets",
    "create_search_configuration",
    "get_search_configuration",
    "update_search_configuration",
    "list_search_configurations",
    "bind_search_config_to_entity",
    "get_search_config_binding",
    "clear_search_config_binding",
    "autocomplete_search",
]
