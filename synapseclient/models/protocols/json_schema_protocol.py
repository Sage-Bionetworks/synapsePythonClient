from typing import TYPE_CHECKING, Generator, Optional, Protocol, Union

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models.mixins import (
        InvalidJSONSchemaValidation,
        JSONSchemaBinding,
        JSONSchemaDerivedKeys,
        JSONSchemaValidation,
        JSONSchemaValidationStatistics,
    )


class BaseJSONSchemaProtocol(Protocol):
    """
    Mixin class to provide JSON schema functionality.
    This class is intended to be used with classes that represent Synapse entities.
    It provides methods to bind, delete, and validate JSON schemas associated with the entity.
    """

    id: Optional[str] = None

    def bind_schema(
        self,
        json_schema_uri: str,
        *,
        enable_derived_annotations: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> "JSONSchemaBinding":
        """
        Bind a JSON schema to the entity.

        Arguments:
            json_schema_uri (str): The URI of the JSON schema to bind to the entity.
            enable_derived_annotations (bool, optional): If true, enable derived annotations. Defaults to False.
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            JSONSchemaBinding: An object containing details about the JSON schema binding.
        """
        return JSONSchemaBinding()

    def get_schema(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> "JSONSchemaBinding":
        """
        Get the JSON schema bound to the entity.

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            JSONSchemaBinding: An object containing details about the bound JSON schema.
        """
        return JSONSchemaBinding()

    def unbind_schema(self, *, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Unbind the JSON schema from the entity.

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.
        """

    def validate_schema(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> Union["JSONSchemaValidation", "InvalidJSONSchemaValidation"]:
        """
        Validate the entity against the bound JSON schema.

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            Union[JSONSchemaValidation, InvalidJSONSchemaValidation]: The validation results.
        """
        return InvalidJSONSchemaValidation() or JSONSchemaValidation()

    def get_schema_derived_keys(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> "JSONSchemaDerivedKeys":
        """
        Retrieve derived JSON schema keys for the entity.

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            JSONSchemaDerivedKeys: An object containing the derived keys for the entity.
        """
        return JSONSchemaDerivedKeys()


class ContainerEntityJSONSchemaProtocol(BaseJSONSchemaProtocol):
    """
    Mixin class to provide JSON schema functionality for container entities.
    This class extends BaseJSONSchemaProtocol and is intended for use with Synapse container entities.
    It provides methods to bind, delete, and validate JSON schemas associated with the container entity.
    """

    def get_schema_validation_statistics(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> "JSONSchemaValidationStatistics":
        """
        Get validation statistics for a container entity.

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            JSONSchemaValidationStatistics: The validation statistics.
        """
        return JSONSchemaValidationStatistics()

    def get_invalid_validation(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> Generator["InvalidJSONSchemaValidation", None, None]:
        """
        Get invalid JSON schema validation results for a container entity.

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Yields:
            InvalidJSONSchemaValidation: An object containing the validation response, all validation messages,
                                         and the validation exception details.
        """
        yield InvalidJSONSchemaValidation()
