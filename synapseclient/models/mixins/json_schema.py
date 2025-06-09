from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AsyncGenerator, List, Optional

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


@dataclass
class JsonSchemaVersionInfo:
    organization_id: str
    """The unique identifier for the organization."""
    organization_name: str
    """The name of the organization."""

    schema_id: str
    """The unique identifier of the bound schema. renamed from "$id" to a valid Python identifier"""

    schema_name: str
    """The name of the schema."""

    version_id: str
    """The unique identifier for the schema version."""

    schema_uri: str
    """The URI of the schema (renamed from "$id" to a valid Python identifier)."""

    semantic_version: str
    """The semantic version of the schema."""

    json_sha256_hex: str
    """The SHA-256 hash of the schema in hexadecimal format."""

    created_on: str
    """The ISO 8601 datetime when the schema version was created."""

    created_by: str
    """The Synapse user ID of the creator of the schema version."""


@dataclass
class JSONSchemaBindingResponse:
    """Represents the response for binding a JSON schema to an entity."""

    json_schema_version_info: JsonSchemaVersionInfo
    """Information about the JSON schema version."""

    object_id: int
    """The ID of the object to which the schema is bound."""

    object_type: str
    """The type of the object (e.g., 'entity')."""

    created_on: str
    """The ISO 8601 datetime when the binding was created."""

    created_by: str
    """The Synapse user ID of the creator of the binding."""

    enable_derived_annotations: bool
    """Indicates whether derived annotations are enabled."""


@dataclass
class JSONSchemaValidationResponse:
    """Represents the response for validating an entity against a JSON schema."""

    object_id: str
    """The Synapse ID of the object (e.g., 'syn12345678')."""

    object_type: str
    """The type of the object (e.g., 'entity')."""

    object_etag: str
    """The ETag of the object at the time of validation."""

    schema_id: str
    """The unique identifier of the bound schema. renamed from "$id" to a valid Python identifier"""

    is_valid: bool
    """Indicates whether the object content conforms to the schema."""

    validated_on: str
    """The ISO 8601 timestamp of when validation occurred."""


@dataclass
class JSONSchemaValidationStatisticsResponse:
    """Represents the summary statistics of JSON schema validation results for a container."""

    container_id: str
    """The Synapse ID of the parent container."""

    total_number_of_children: int
    """The total number of child entities."""

    number_of_valid_children: int
    """The number of children that passed validation."""

    number_of_invalid_children: int
    """The number of children that failed validation."""

    number_of_unknown_children: int
    """The number of children with unknown validation status."""


@dataclass
class CausingException:
    """Represents an exception causing a validation failure."""

    keyword: str
    """The JSON schema keyword that caused the exception."""

    pointer_to_violation: str
    """A JSON pointer to the location of the violation."""

    message: str
    """A message describing the exception."""

    schema_location: str
    """The location of the schema that caused the exception."""

    causing_exceptions: List["CausingException"] = field(default_factory=list)
    """A list of nested causing exceptions."""


@dataclass
class ValidationException:
    """Represents a validation exception."""

    pointer_to_violation: str
    """A JSON pointer to the location of the violation."""

    message: str
    """A message describing the exception."""

    schema_location: str
    """The location of the schema that caused the exception."""

    causing_exceptions: List[CausingException]
    """A list of causing exceptions."""


@dataclass
class InvalidJSONSchemaValidationResponse:
    """Represents the response for invalid JSON schema validation results."""

    validation_response: JSONSchemaValidationResponse
    """The validation response object."""

    all_validation_messages: List[str]
    """A list of all validation messages."""

    validation_exception: ValidationException
    """The validation exception details."""


@dataclass
class JSONSchemaDerivedKeysResponse:
    """Represents the response for derived JSON schema keys."""

    keys: List[str]
    """A list of derived keys for the entity."""


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
    ) -> JSONSchemaBindingResponse:
        """Bind a JSON schema to the entity.

        Args:
            json_schema_uri (str): The URI of the JSON schema to bind to the entity.
            enable_derived_annos (bool, optional): If true, enable derived annotations. Defaults to False.
            synapse_client:  If not passed in and caching was not disabled by
                            `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor

        Returns: a JSONSchemaBindingResponse object
        """
        return await bind_json_schema_to_entity(
            synapse_id=self.id,
            json_schema_uri=json_schema_uri,
            enable_derived_annos=enable_derived_annos,
            synapse_client=synapse_client,
        )

    async def get_json_schema_from_entity_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> JSONSchemaBindingResponse:
        """Get bound schema from entity

        Arguments:
            synapse_client:  If not passed in and caching was not disabled by
                            `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor
        Returns:
            A JSONSchemaBindingResponse object
        """
        return await get_json_schema_from_entity(
            synapse_id=self.id, synapse_client=synapse_client
        )

    async def delete_json_schema_from_entity_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> None:
        """Delete bound schema from entity
        Arguments:
            synapse_client:  If not passed in and caching was not disabled by
                            `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor
        """
        return await delete_json_schema_from_entity(
            synapse_id=self.id, synapse_client=synapse_client
        )

    async def validate_entity_with_json_schema_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> JSONSchemaValidationResponse:
        """Get validation results of an entity against bound JSON schema

        Arguments:
            synapse_client:  If not passed in and caching was not disabled by
                            `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor

        Returns: a JSONSchemaValidationResponse object
        """
        return await validate_entity_with_json_schema(
            synapse_id=self.id, synapse_client=synapse_client
        )

    async def get_json_schema_validation_statistics_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> JSONSchemaValidationStatisticsResponse:
        """Get the summary statistic of json schema validation results for
            a container entity
        Arguments:
            synapse_client:  If not passed in and caching was not disabled by
                            `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor

        Returns: a JSONSchemaValidationStatisticsResponse object
        """
        return await get_json_schema_validation_statistics(
            synapse_id=self.id, synapse_client=synapse_client
        )

    async def get_invalid_json_schema_validation_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> AsyncGenerator[InvalidJSONSchemaValidationResponse, None]:
        """Get a single page of invalid JSON schema validation results for a container Entity
        (Project or Folder).

        Arguments:
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
    ) -> JSONSchemaDerivedKeysResponse:
        """Retrieve derived JSON schema keys for a given Synapse entity.

        Args:
            synapse_client:  If not passed in and caching was not disabled by
                            `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor

        Returns:
            JSONSchemaDerivedKeysResponse: An object containing the derived keys for the entity.
        """
        return JSONSchemaDerivedKeysResponse(
            **await get_json_schema_derived_keys(
                synapse_id=self.id, synapse_client=synapse_client
            )
        )
