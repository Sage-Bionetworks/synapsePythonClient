import json
from typing import TYPE_CHECKING, AsyncGenerator, Dict, List, Optional, Union

from synapseclient.api.api_client import rest_post_paginated_async

if TYPE_CHECKING:
    from synapseclient import Synapse


async def bind_json_schema_to_entity(
    synapse_id: str, json_schema_uri: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Union[str, int, bool]]:
    """Bind a JSON schema to an entity

    Arguments:
        json_schema_uri: JSON schema URI
        entity:          Synapse Entity or Synapse Id
        synapse_client:  If not passed in and caching was not disabled by
                         `Synapse.allow_client_caching(False)` this will use the last created
                         instance from the Synapse class constructor
    Returns:
        A dictionary with the following structure:

        {
            "jsonSchemaVersionInfo": {
                "organizationId": str,
                "organizationName": str,
                "schemaId": str,
                "schemaName": str,
                "versionId": str,
                "$id": str (unique identifier for the schema),
                "semanticVersion": str,
                "jsonSHA256Hex": str,
                "createdOn": str (ISO datetime),
                "createdBy": str (synapse user ID),
            },
            "objectId": int,
            "objectType": str,
            "createdOn": str (ISO datetime),
            "createdBy": str (synapse user ID),
            "enableDerivedAnnotations": bool
        }
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    request_body = {"entityId": synapse_id, "schema$id": json_schema_uri}
    return await client.rest_put_async(
        uri=f"/entity/{synapse_id}/schema/binding", body=json.dumps(request_body)
    )


async def get_json_schema_from_entity(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Union[str, int, bool]]:
    """Get bound schema from entity

    Arguments:
        synapse_id:      Synapse Id
        synapse_client:  If not passed in and caching was not disabled by
                         `Synapse.allow_client_caching(False)` this will use the last created
                         instance from the Synapse class constructor

    Returns:
        A dictionary with the following structure:
        {
            "jsonSchemaVersionInfo": {
                "organizationId": str,
                "organizationName": str,
                "schemaId": str,
                "schemaName": str,
                "versionId": str,
                "$id": str (unique identifier for the schema),
                "semanticVersion": str,
                "jsonSHA256Hex": str,
                "createdOn": str (ISO datetime),
                "createdBy": str (synapse user ID),
            },
            "objectId": int,
            "objectType": str,
            "createdOn": str (ISO datetime),
            "createdBy": str (synapse user ID),
            "enableDerivedAnnotations": bool
        }
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
) -> Dict[str, Union[str, bool]]:
    """Get validation results of an entity against bound JSON schema

    Arguments:
        synapse_id:      Synapse Id
        synapse_client:  If not passed in and caching was not disabled by
                         `Synapse.allow_client_caching(False)` this will use the last created
                         instance from the Synapse class constructor

    Returns:
        {
            "objectId": str,         # Synapse ID of the object (e.g., "syn12345678")
            "objectType": str,       # Type of the object (e.g., "entity")
            "objectEtag": str,       # ETag of the object at the time of validation
            "schema$id": str,        # Full URL of the bound schema version
            "isValid": bool,         # True if the object content conforms to the schema
            "validatedOn": str       # ISO 8601 timestamp of when validation occurred
        }
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/entity/{synapse_id}/schema/validation")


async def get_json_schema_validation_statistics(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Union[int, str]]:
    """Get the summary statistic of json schema validation results for
        a container entity
     Arguments:
        synapse_id:      Synapse Id
        synapse_client:  If not passed in and caching was not disabled by
                         `Synapse.allow_client_caching(False)` this will use the last created
                         instance from the Synapse class constructor

    Returns:
        {
            "containerId": str,              # Synapse ID of the parent container
            "totalNumberOfChildren": int,    # Total number of child entities
            "numberOfValidChildren": int,    # Number of children that passed validation
            "numberOfInvalidChildren": int,  # Number of children that failed validation
            "numberOfUnknownChildren": int,  # Number of children with unknown validation status
            "generatedOn": str               # ISO 8601 timestamp when this summary was generated
        }
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{synapse_id}/schema/validation/statistics"
    )


async def get_invalid_json_schema_validation(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> AsyncGenerator[Dict[str, str], None]:
    """Get a single page of invalid JSON schema validation results for a container Entity
        (Project or Folder).

        Arguments:
            synapse_id:      Synapse Id
            synapse_client:  If not passed in and caching was not disabled by
                             `Synapse.allow_client_caching(False)` this will use the last created
                             instance from the Synapse class constructor

    Example usage:
    ```python
    # for python 3.10+
    async def main():
        gen = get_invalid_json_schema_validation(synapse_client=syn, synapse_id=dataset_folder)
        try:
            while True:
                item = await anext(gen)
                print(item)
        except StopAsyncIteration:
            print("All items processed.")
    asyncio.run(main())
    ```
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
) -> Dict[str, List[str]]:
    """Retrieve derived JSON schema keys for a given Synapse entity.

    Args:
        synapse_id (str): The Synapse ID of the entity for which to retrieve derived keys.

    Returns:
        dict: A dictionary containing the derived keys associated with the entity.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/entity/{synapse_id}/derivedKeys")
