from typing import TYPE_CHECKING, AsyncGenerator, Dict, List, Optional, Union

from synapseclient.api.json_schema_services import (
    bind_json_schema_to_entity,
    delete_json_schema_from_entity,
    get_invalid_json_schema_validation,
    get_json_schema_derived_keys,
    get_json_schema_from_entity,
    get_json_schema_validation_statistics,
    validate_entity_with_json_schema,
)
from synapseclient.core.async_utils import async_to_sync

if TYPE_CHECKING:
    from synapseclient import Synapse


@async_to_sync
class JSONSchema:
    """
    Mixin class to provide JSON schema functionality.
    This class is intended to be used with classes that represent Synapse entities.
    It provides methods to bind, delete, and validate JSON schemas associated with the entity.
    """

    id: Optional[str] = None

    async def bind_json_schema_to_entity_async(
        self,
        json_schema_uri: str,
        *,
        enable_derived_annos: bool = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> Dict[str, Union[str, int, bool]]:
        """Bind a JSON schema to an entity"""
        return await bind_json_schema_to_entity(
            synapse_id=self.id,
            json_schema_uri=json_schema_uri,
            enable_derived_annos=enable_derived_annos,
            synapse_client=synapse_client,
        )

    async def get_json_schema_from_entity_async(
        self, *, synapse_client: Optional["Synapse"] = None
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
        return await get_json_schema_from_entity(
            synapse_id=self.id, synapse_client=synapse_client
        )

    async def delete_json_schema_from_entity_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> None:
        """Delete bound schema from entity
        Arguments:
            synapse_id:      Synapse Id
            synapse_client:  If not passed in and caching was not disabled by
                            `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor
        """
        return await delete_json_schema_from_entity(
            synapse_id=self.id, synapse_client=synapse_client
        )

    async def validate_entity_with_json_schema_async(
        self, *, synapse_client: Optional["Synapse"] = None
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
        return await validate_entity_with_json_schema(
            synapse_id=self.id, synapse_client=synapse_client
        )

    async def get_json_schema_validation_statistics_async(
        self, *, synapse_client: Optional["Synapse"] = None
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
        return await get_json_schema_validation_statistics(
            synapse_id=self.id, synapse_client=synapse_client
        )

    async def get_invalid_json_schema_validation_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> AsyncGenerator[Dict[str, str], None]:
        """Get a single page of invalid JSON schema validation results for a container Entity
        (Project or Folder).

        Arguments:
            synapse_id:      Synapse Id
            synapse_client:  If not passed in and caching was not disabled by
                            `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor
        """
        gen = get_invalid_json_schema_validation(
            synapse_client=synapse_client, synapse_id=self.id
        )
        async for item in gen:
            yield item

    async def get_json_schema_derived_keys_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> Dict[str, List[str]]:
        """Retrieve derived JSON schema keys for a given Synapse entity.

        Args:
            synapse_id (str): The Synapse ID of the entity for which to retrieve derived keys.

        Returns:
            dict: A dictionary containing the derived keys associated with the entity.
        """
        return await get_json_schema_derived_keys(
            synapse_id=self.id, synapse_client=synapse_client
        )
