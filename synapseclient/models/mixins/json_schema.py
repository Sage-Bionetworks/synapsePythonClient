from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AsyncGenerator, List, Optional, Union

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
    """The identifier of the bound schema, represented as a numeric string."""

    id: str
    """this is renamed from "$id" to a valid Python identifier."""

    schema_name: str
    """The name of the schema."""

    version_id: str
    """The unique identifier for the schema version."""

    semantic_version: str
    """The semantic version of the schema."""

    json_sha256_hex: str
    """The SHA-256 hash of the schema in hexadecimal format."""

    created_on: str
    """The ISO 8601 datetime when the schema version was created."""

    created_by: str
    """The Synapse user ID of the creator of the schema version."""


@dataclass
class JSONSchemaBinding:
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
class JSONSchemaValidation:
    """Represents the response for validating an entity against a JSON schema."""

    object_id: str
    """The Synapse ID of the object (e.g., 'syn12345678')."""

    object_type: str
    """The type of the object (e.g., 'entity')."""

    object_etag: str
    """The ETag of the object at the time of validation."""

    id: str
    """Note: this is renamed from "schema$id" to a valid Python identifier."""

    is_valid: bool
    """Indicates whether the object content conforms to the schema."""

    validated_on: str
    """The ISO 8601 timestamp of when validation occurred."""


@dataclass
class JSONSchemaValidationStatistics:
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
class InvalidJSONSchemaValidation:
    """Represents the response for invalid JSON schema validation results."""

    validation_response: JSONSchemaValidation
    """The validation response object."""

    validation_error_message: str
    """A message describing the validation error."""

    all_validation_messages: List[str]
    """A list of all validation messages."""

    validation_exception: ValidationException
    """The validation exception details."""


@dataclass
class JSONSchemaDerivedKeys:
    """Represents the response for derived JSON schema keys."""

    keys: List[str]
    """A list of derived keys for the entity."""


@async_to_sync
class BaseJSONSchema:
    """
    Mixin class to provide JSON schema functionality.
    This class is intended to be used with classes that represent Synapse entities.
    It provides methods to bind, delete, and validate JSON schemas associated with the entity.
    """

    id: Optional[str] = None

    async def bind_schema_async(
        self,
        json_schema_uri: str,
        *,
        enable_derived_annos: bool = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> JSONSchemaBinding:
        """
        Bind a JSON schema to the entity.

        Args:
            json_schema_uri (str): The URI of the JSON schema to bind to the entity.
            enable_derived_annos (bool, optional): If true, enable derived annotations. Defaults to False.
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            JSONSchemaBinding: An object containing details about the JSON schema binding.
        """
        response = await bind_json_schema_to_entity(
            synapse_id=self.id,
            json_schema_uri=json_schema_uri,
            enable_derived_annos=enable_derived_annos,
            synapse_client=synapse_client,
        )
        json_schema_version = response.get("jsonSchemaVersionInfo", {})
        return JSONSchemaBinding(
            json_schema_version_info=JsonSchemaVersionInfo(
                organization_id=json_schema_version.get("organizationId", ""),
                organization_name=json_schema_version.get("organizationName", ""),
                schema_id=json_schema_version.get("schemaId", ""),
                id=json_schema_version.get("$id", ""),
                schema_name=json_schema_version.get("schemaName", ""),
                version_id=json_schema_version.get("versionId", ""),
                semantic_version=json_schema_version.get("semanticVersion", ""),
                json_sha256_hex=json_schema_version.get("jsonSHA256Hex", ""),
                created_on=json_schema_version.get("createdOn", ""),
                created_by=json_schema_version.get("createdBy", ""),
            ),
            object_id=response.get("objectId", ""),
            object_type=response.get("objectType", ""),
            created_on=response.get("createdOn", ""),
            created_by=response.get("createdBy", ""),
            enable_derived_annotations=response.get("enableDerivedAnnotations", False),
        )

    async def get_schema_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> JSONSchemaBinding:
        """
        Get the JSON schema bound to the entity.

        Args:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            JSONSchemaBinding: An object containing details about the bound JSON schema.
        """
        response = await get_json_schema_from_entity(
            synapse_id=self.id, synapse_client=synapse_client
        )
        json_schema_version_info = response.get("jsonSchemaVersionInfo", {})
        return JSONSchemaBinding(
            json_schema_version_info=JsonSchemaVersionInfo(
                organization_id=json_schema_version_info.get("organizationId", ""),
                organization_name=json_schema_version_info.get("organizationName", ""),
                schema_id=json_schema_version_info.get("schemaId", ""),
                id=json_schema_version_info.get("$id", ""),
                schema_name=json_schema_version_info.get("schemaName", ""),
                version_id=json_schema_version_info.get("versionId", ""),
                semantic_version=json_schema_version_info.get("semanticVersion", ""),
                json_sha256_hex=json_schema_version_info.get("jsonSHA256Hex", ""),
                created_on=json_schema_version_info.get("createdOn", ""),
                created_by=json_schema_version_info.get("createdBy", ""),
            ),
            object_id=response.get("objectId", ""),
            object_type=response.get("objectType", ""),
            created_on=response.get("createdOn", ""),
            created_by=response.get("createdBy", ""),
            enable_derived_annotations=response.get("enableDerivedAnnotations", False),
        )

    async def delete_schema_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> None:
        """
        Delete the JSON schema bound to the entity.

        Args:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.
        """
        return await delete_json_schema_from_entity(
            synapse_id=self.id, synapse_client=synapse_client
        )

    async def validate_schema_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> Union[JSONSchemaValidation, InvalidJSONSchemaValidation]:
        """
        Validate the entity against the bound JSON schema.

        Args:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            Union[JSONSchemaValidation, InvalidJSONSchemaValidation]: The validation results.
        """
        response = await validate_entity_with_json_schema(
            synapse_id=self.id, synapse_client=synapse_client
        )
        if "validationException" in response:
            return InvalidJSONSchemaValidation(
                validation_response=JSONSchemaValidation(
                    object_id=response.get("objectId", ""),
                    object_type=response.get("objectType", ""),
                    object_etag=response.get("objectEtag", ""),
                    id=response.get("schema$id", ""),
                    is_valid=response.get("isValid", False),
                    validated_on=response.get("validatedOn", ""),
                ),
                validation_error_message=response.get("validationErrorMessage", ""),
                all_validation_messages=response.get("allValidationMessages", []),
                validation_exception=ValidationException(
                    pointer_to_violation=response.get("validationException", {}).get(
                        "pointerToViolation", ""
                    ),
                    message=response.get("validationException", {}).get("message", ""),
                    schema_location=response.get("validationException", {}).get(
                        "schemaLocation", ""
                    ),
                    causing_exceptions=[
                        CausingException(
                            keyword=ce.get("keyword", ""),
                            pointer_to_violation=ce.get("pointerToViolation", ""),
                            message=ce.get("message", ""),
                            schema_location=ce.get("schemaLocation", ""),
                            causing_exceptions=[
                                CausingException(
                                    keyword=nce.get("keyword", ""),
                                    pointer_to_violation=nce.get(
                                        "pointerToViolation", ""
                                    ),
                                    message=nce.get("message", ""),
                                    schema_location=nce.get("schemaLocation", ""),
                                )
                                for nce in ce.get("causingExceptions", [])
                            ],
                        )
                        for ce in response.get("validationException", {}).get(
                            "causingExceptions", []
                        )
                    ],
                ),
            )
        return JSONSchemaValidation(
            object_id=response.get("objectId", ""),
            object_type=response.get("objectType", ""),
            object_etag=response.get("objectEtag", ""),
            id=response.get("schema$id", ""),
            is_valid=response.get("isValid", ""),
            validated_on=response.get("validatedOn", ""),
        )

    async def get_schema_derived_keys_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> JSONSchemaDerivedKeys:
        """
        Retrieve derived JSON schema keys for the entity.

        Args:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            JSONSchemaDerivedKeys: An object containing the derived keys for the entity.
        """
        response = await get_json_schema_derived_keys(
            synapse_id=self.id, synapse_client=synapse_client
        )
        return JSONSchemaDerivedKeys(keys=response["keys"])


@async_to_sync
class ContainerEntityJSONSchema(BaseJSONSchema):
    """
    Mixin class to provide JSON schema functionality.
    This class is intended to be used with classes that represent Synapse entities.
    It provides methods to bind, delete, and validate JSON schemas associated with the entity.
    """

    async def get_schema_validation_statistics_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> JSONSchemaValidationStatistics:
        """
        Get validation statistics for a container entity.

        Args:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            JSONSchemaValidationStatistics: The validation statistics.
        """
        response = await get_json_schema_validation_statistics(
            synapse_id=self.id, synapse_client=synapse_client
        )
        return JSONSchemaValidationStatistics(
            container_id=response.get("containerId", ""),
            total_number_of_children=response.get("totalNumberOfChildren", None),
            number_of_valid_children=response.get("numberOfValidChildren", None),
            number_of_invalid_children=response.get("numberOfInvalidChildren", None),
            number_of_unknown_children=response.get("numberOfUnknownChildren", None),
        )

    async def get_invalid_validation_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> AsyncGenerator[InvalidJSONSchemaValidation, None]:
        """
        Get invalid JSON schema validation results for a container entity.

        Args:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Yields:
            InvalidJSONSchemaValidation: An object containing the validation response, all validation messages,
                                         and the validation exception details.
        """
        gen = get_invalid_json_schema_validation(
            synapse_client=synapse_client, synapse_id=self.id
        )
        async for item in gen:
            yield InvalidJSONSchemaValidation(
                validation_response=JSONSchemaValidation(
                    object_id=item.get("objectId", ""),
                    object_type=item.get("objectType", ""),
                    object_etag=item.get("objectEtag", ""),
                    id=item.get("schema$id", ""),
                    is_valid=item.get("isValid", ""),
                    validated_on=item.get("validatedOn", ""),
                ),
                validation_error_message=item.get("validationErrorMessage", ""),
                all_validation_messages=item.get("allValidationMessages", []),
                validation_exception=ValidationException(
                    pointer_to_violation=item.get("validationException", {}).get(
                        "pointerToViolation", ""
                    ),
                    message=item.get("validationException", {}).get("message", ""),
                    schema_location=item.get("validationException", {}).get(
                        "schemaLocation", ""
                    ),
                    causing_exceptions=[
                        CausingException(
                            keyword=ce.get("keyword", ""),
                            pointer_to_violation=ce.get("pointerToViolation", ""),
                            message=ce.get("message", ""),
                            schema_location=ce.get("schemaLocation", ""),
                            causing_exceptions=[
                                CausingException(
                                    keyword=nce.get("keyword", ""),
                                    pointer_to_violation=nce.get(
                                        "pointerToViolation", ""
                                    ),
                                    message=nce.get("message", ""),
                                    schema_location=nce.get("schemaLocation", ""),
                                )
                                for nce in ce.get("causingExceptions", [])
                            ],
                        )
                        for ce in item.get("validationException", {}).get(
                            "causingExceptions", []
                        )
                    ],
                ),
            )
