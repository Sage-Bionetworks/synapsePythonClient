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
from typing import TYPE_CHECKING, Any, Dict, Optional

from synapseclient.core.utils import delete_none_keys

if TYPE_CHECKING:
    from synapseclient import Synapse


async def create_text_analyzer(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a new TextAnalyzer within the specified Organization.

    <https://rest-docs.synapse.org/rest/POST/search/text/analyzer.html>

    Arguments:
        request: The TextAnalyzer object body. The analyzer's `settings` JSON is
            parsed and any `$ref` entries inside its `filter` registry are
            checked for qualified-name format and existence.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the created TextAnalyzer.
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
    """Get a TextAnalyzer by its ID.

    <https://rest-docs.synapse.org/rest/GET/search/text/analyzer/id.html>

    Arguments:
        analyzer_id: The numeric ID of the text analyzer to retrieve.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the requested TextAnalyzer.
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
        analyzer_id: The path ID of the text analyzer (must match the request
            body's ID).
        request: The updated TextAnalyzer object body. The `organizationName`
            and `name` fields are immutable and cannot be modified after
            creation.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the updated TextAnalyzer.
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
    """List TextAnalyzer objects, optionally filtered by Organization.

    <https://rest-docs.synapse.org/rest/POST/search/text/analyzer/list.html>

    Arguments:
        organization_name: If `organizationName` is null, all text analyzers
            across all Organizations are returned.
        next_page_token: Results are paginated using a next page token.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the ListTextAnalyzersResponse, containing a
        page of TextAnalyzers and a nextPageToken if more results exist.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    body = {"organizationName": organization_name, "nextPageToken": next_page_token}
    delete_none_keys(body)
    return await client.rest_post_async(
        uri="/search/text/analyzer/list", body=json.dumps(body)
    )


async def create_column_analyzer_override(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a new ColumnAnalyzerOverride within the specified Organization.

    <https://rest-docs.synapse.org/rest/POST/search/column/analyzer/override.html>

    Arguments:
        request: The ColumnAnalyzerOverride object body.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the created ColumnAnalyzerOverride.
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
    """Get a ColumnAnalyzerOverride by its ID.

    <https://rest-docs.synapse.org/rest/GET/search/column/analyzer/override/columnAnalyzerOverrideId.html>

    Arguments:
        column_analyzer_override_id: The ID of the column analyzer override to
            retrieve.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the requested ColumnAnalyzerOverride.
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
        column_analyzer_override_id: The path ID (must match the request
            body's ID).
        request: The updated ColumnAnalyzerOverride object body. Concurrency
            is managed via the `etag` field.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the updated ColumnAnalyzerOverride.
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
    """List ColumnAnalyzerOverride objects, optionally filtered by Organization.

    <https://rest-docs.synapse.org/rest/POST/search/column/analyzer/override/list.html>

    Arguments:
        organization_name: If organizationName is null, all column analyzer
            overrides across all Organizations are returned.
        next_page_token: Results are paginated using a next page token.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the ListColumnAnalyzerOverridesResponse,
        containing a page of ColumnAnalyzerOverrides and a nextPageToken if
        more results exist.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    body = {"organizationName": organization_name, "nextPageToken": next_page_token}
    delete_none_keys(body)
    return await client.rest_post_async(
        uri="/search/column/analyzer/override/list", body=json.dumps(body)
    )


async def create_synonym_set(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a new SynonymSet within the specified Organization.

    <https://rest-docs.synapse.org/rest/POST/search/synonym/set.html>

    Arguments:
        request: The SynonymSet object body. The supplied `definition` is
            parsed to confirm it is valid JSON before being passed to AOSS;
            supply synonym lists inline via the `synonyms` array rather than
            using file-based parameters.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the created SynonymSet.
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
    """Get a SynonymSet by its ID.

    <https://rest-docs.synapse.org/rest/GET/search/synonym/set/synonymSetId.html>

    Arguments:
        synonym_set_id: The ID of the synonym set to retrieve.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the requested SynonymSet.
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
        request: The updated SynonymSet object body. The `definition` field is
            re-parsed to validate JSON formatting. The `organizationName` and
            `name` fields are immutable after creation. Concurrency is managed
            via `etag`; mismatches return a 409 Conflict.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the updated SynonymSet.
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
    """List SynonymSet objects, optionally filtered by Organization.

    <https://rest-docs.synapse.org/rest/POST/search/synonym/set/list.html>

    Arguments:
        organization_name: If organizationName is null, all synonym sets
            across all Organizations are returned.
        next_page_token: Results are paginated using a next page token.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the ListSynonymSetsResponse, containing a
        page of SynonymSets and a nextPageToken if more results exist.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    body = {"organizationName": organization_name, "nextPageToken": next_page_token}
    delete_none_keys(body)
    return await client.rest_post_async(
        uri="/search/synonym/set/list", body=json.dumps(body)
    )


async def create_search_configuration(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a new SearchConfiguration within the specified Organization.

    <https://rest-docs.synapse.org/rest/POST/search/configuration.html>

    Arguments:
        request: The SearchConfiguration object body.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the created SearchConfiguration.
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
    """Get a SearchConfiguration by its ID.

    <https://rest-docs.synapse.org/rest/GET/search/configuration/searchConfigurationId.html>

    Arguments:
        search_configuration_id: The ID of the search configuration to
            retrieve.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the requested SearchConfiguration.
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
        search_configuration_id: The path ID (must match the request body's
            ID).
        request: The updated SearchConfiguration object body.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the updated SearchConfiguration.
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
    """List SearchConfiguration objects, optionally filtered by Organization.

    <https://rest-docs.synapse.org/rest/POST/search/configuration/list.html>

    Arguments:
        organization_name: If organizationName is null, all search
            configurations across all Organizations are returned.
        next_page_token: Results are paginated using a next page token.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the ListSearchConfigurationsResponse,
        containing a page of SearchConfigurations and a nextPageToken if more
        results exist.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    body = {"organizationName": organization_name, "nextPageToken": next_page_token}
    delete_none_keys(body)
    return await client.rest_post_async(
        uri="/search/configuration/list", body=json.dumps(body)
    )


async def bind_search_config_to_entity(
    entity_id: str,
    search_configuration_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Bind a SearchConfiguration to an entity (typically a project). The
    caller must have EDIT permission on the entity. Replaces any existing
    binding on that entity.

    <https://rest-docs.synapse.org/rest/PUT/entity/entityId/searchconfig/binding.html>

    Arguments:
        entity_id: The ID of the entity to bind to.
        search_configuration_id: The ID of the SearchConfiguration to bind.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the created SearchConfigBinding.
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
    """Get the effective SearchConfigBinding for an entity. Walks up the
    entity hierarchy (entity -> folder -> project) and returns the first
    binding found on the entity or any ancestor.

    <https://rest-docs.synapse.org/rest/GET/entity/entityId/searchconfig/binding.html>

    Arguments:
        entity_id: The ID of the entity to look up.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the effective SearchConfigBinding.
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

    Returns:
        None
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    await client.rest_delete_async(uri=f"/entity/{entity_id}/searchconfig/binding")


async def autocomplete_search(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Perform a synchronous autocomplete search query against a SearchIndex.
    Purpose-built for type-ahead input: no pagination, sorting customization,
    or aggregations are available; the server caps every response at 8 hits,
    ordered by relevance. For more complex search needs, use the asynchronous
    search endpoint instead.

    <https://rest-docs.synapse.org/rest/POST/search/autocomplete.html>

    Arguments:
        request: The SearchAutocompleteRequest body. Only two top-level keys
            are permitted in the `searchQuery`: `query` (required, must use
            one of `prefix`, `match_phrase_prefix`, or `match_bool_prefix`) and
            `_source` (optional, a source filter to narrow returned fields).
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary representing the SearchQueryResults, capped at 8 hits.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri="/search/autocomplete", body=json.dumps(request)
    )
