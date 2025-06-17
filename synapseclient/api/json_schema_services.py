import json
from typing import TYPE_CHECKING, AsyncGenerator, Optional

from synapseclient.api.api_client import rest_post_paginated_async

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models.mixins import (
        InvalidJSONSchemaValidation,
        JSONSchemaBinding,
        JSONSchemaDerivedKeys,
        JSONSchemaValidation,
        JSONSchemaValidationStatistics,
    )


async def bind_json_schema_to_entity(
    synapse_id: str,
    json_schema_uri: str,
    *,
    enable_derived_annos: bool = False,
    synapse_client: Optional["Synapse"] = None,
) -> "JSONSchemaBinding":
    """Bind a JSON schema to an entity

    Arguments:
        synapse_id:      Synapse Entity or Synapse Id
        json_schema_uri: JSON schema URI
        enable_derived_annos:  If True, derived annotations will be enabled for this entity
        synapse_client:  If not passed in and caching was not disabled by
                         `Synapse.allow_client_caching(False)` this will use the last created
                         instance from the Synapse class constructor
    Returns:
        A JSONSchemaBindingResponse object containing the details of the binding.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    request_body = {
        "entityId": synapse_id,
        "schema$id": json_schema_uri,
        "enableDerivedAnnotations": enable_derived_annos,
    }
    return await client.rest_put_async(
        uri=f"/entity/{synapse_id}/schema/binding", body=json.dumps(request_body)
    )


async def get_json_schema_from_entity(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> "JSONSchemaBinding":
    """Get bound schema from entity

    Arguments:
        synapse_id:      Synapse Id
        synapse_client:  If not passed in and caching was not disabled by
                         `Synapse.allow_client_caching(False)` this will use the last created
                         instance from the Synapse class constructor

    Returns:
        A JSONSchemaBindingResponse object containing the details of the binding.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/entity/{synapse_id}/schema/binding")


async def delete_json_schema_from_entity(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> None:
    """Delete bound schema from entity
    Arguments:
        synapse_id:      Synapse Id
        synapse_client:  If not passed in and caching was not disabled by
                         `Synapse.allow_client_caching(False)` this will use the last created
                         instance from the Synapse class constructor
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_delete_async(uri=f"/entity/{synapse_id}/schema/binding")


async def validate_entity_with_json_schema(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> "JSONSchemaValidation":
    """Get validation results of an entity against bound JSON schema

    Arguments:
        synapse_id:      Synapse Id
        synapse_client:  If not passed in and caching was not disabled by
                         `Synapse.allow_client_caching(False)` this will use the last created
                         instance from the Synapse class constructor
    Returns:
        A JSONSchemaValidationResponse object containing the validation results.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/entity/{synapse_id}/schema/validation")


async def get_json_schema_validation_statistics(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> "JSONSchemaValidationStatistics":
    """Get the summary statistic of json schema validation results for
        a container entity
    Arguments:
        synapse_id:      Synapse Id
        synapse_client:  If not passed in and caching was not disabled by
                         `Synapse.allow_client_caching(False)` this will use the last created
                         instance from the Synapse class constructor

    Returns: a JSONSchemaValidationStatisticsResponse object
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{synapse_id}/schema/validation/statistics"
    )


async def get_invalid_json_schema_validation(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> AsyncGenerator["InvalidJSONSchemaValidation", None]:
    """Get a single page of invalid JSON schema validation results for a container Entity
    (Project or Folder).

    Arguments:
        synapse_id:      Synapse Id
        synapse_client:  If not passed in and caching was not disabled by
                         `Synapse.allow_client_caching(False)` this will use the last created
                         instance from the Synapse class constructor
    Returns:
        An AsyncGenerator yielding InvalidJSONSchemaValidationResponse objects.
    """

    request_body = {"containerId": synapse_id}
    response = rest_post_paginated_async(
        f"/entity/{synapse_id}/schema/validation/invalid",
        body=json.dumps(request_body),
        synapse_client=synapse_client,
    )
    async for item in response:
        yield item


async def get_json_schema_derived_keys(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> "JSONSchemaDerivedKeys":
    """Retrieve derived JSON schema keys for a given Synapse entity.

    Arguments:
        synapse_id (str): The Synapse ID of the entity for which to retrieve derived keys.

    Returns:
        dict: JSONSchemaDerivedKeysResponse object containing the derived keys for the entity.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/entity/{synapse_id}/derivedKeys")
