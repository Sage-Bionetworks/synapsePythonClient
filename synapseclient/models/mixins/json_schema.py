from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AsyncGenerator, Generator, List, Optional, Union

from synapseclient.api.json_schema_services import (
    bind_json_schema_to_entity,
    delete_json_schema_from_entity,
    get_invalid_json_schema_validation,
    get_invalid_json_schema_validation_sync,
    get_json_schema_derived_keys,
    get_json_schema_from_entity,
    get_json_schema_validation_statistics,
    validate_entity_with_json_schema,
)
from synapseclient.core.async_utils import async_to_sync, skip_async_to_sync
from synapseclient.models.protocols.json_schema_protocol import (
    BaseJSONSchemaProtocol,
    ContainerEntityJSONSchemaProtocol,
)

if TYPE_CHECKING:
    from synapseclient import Synapse


@dataclass
class JSONSchemaVersionInfo:
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

    json_schema_version_info: JSONSchemaVersionInfo
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
class BaseJSONSchema(BaseJSONSchemaProtocol):
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

        Arguments:
            json_schema_uri (str): The URI of the JSON schema to bind to the entity.
            enable_derived_annos (bool, optional): If true, enable derived annotations. Defaults to False.
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Example: Using this function
            Binding JSON schema to a folder or a file

            ```python
                import synapseclient
                from synapseclient.models import File, Folder
                import asyncio

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project") #replace with your project name
                ORG_NAME = "UniqueOrg" # replace with your organization name
                SCHEMA_NAME = "myTestSchema" # replace with your schema name
                VERSION = "0.0.1"
                SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}"

                # Create organization (if not already created)
                js = syn.service("json_schema")
                all_orgs = js.list_organizations()
                for org in all_orgs:
                    if org["name"] == ORG_NAME:
                        print(f"Organization {ORG_NAME} already exists.")
                        break
                else:
                    print(f"Creating organization {ORG_NAME}.")
                    js.create_organization(ORG_NAME)

                # Create the schema (if not already created)
                schema_definition = {
                    "$id": "mySchema",
                    "type": "object",
                    "properties": {
                        "foo": {"type": "string"},
                        "bar": {"type": "integer"},
                    },
                    "required": ["foo"]
                }

                my_test_org = js.JsonSchemaOrganization(ORG_NAME)
                test_schema = my_test_org.get_json_schema(SCHEMA_NAME)
                if not test_schema:
                    test_schema = my_test_org.create_json_schema(schema_definition, SCHEMA_NAME, VERSION)

                # Create a test folder
                test_folder = Folder(name="test_script_folder", parent_id=PROJECT_ID)
                test_folder.store()

                # Bind JSON schema to the folder
                async def bind_json_schema():
                    bound_schema = await test_folder.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema
                asyncio.run(bind_json_schema())

                # Optionally, bind the same schema to a file
                example_file = File(
                    path="Sample.txt",  # Replace with your test file path
                    parent_id=test_folder.id,
                ).store()

                async def bind_schema_to_file():
                    bound_schema_file = await example_file.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema_file
                asyncio.run(bind_schema_to_file())
            ```
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
            json_schema_version_info=JSONSchemaVersionInfo(
                organization_id=json_schema_version.get("organizationId", None),
                organization_name=json_schema_version.get("organizationName", None),
                schema_id=json_schema_version.get("schemaId", None),
                id=json_schema_version.get("$id", None),
                schema_name=json_schema_version.get("schemaName", None),
                version_id=json_schema_version.get("versionId", None),
                semantic_version=json_schema_version.get("semanticVersion", None),
                json_sha256_hex=json_schema_version.get("jsonSHA256Hex", None),
                created_on=json_schema_version.get("createdOn", None),
                created_by=json_schema_version.get("createdBy", None),
            ),
            object_id=response.get("objectId", None),
            object_type=response.get("objectType", None),
            created_on=response.get("createdOn", None),
            created_by=response.get("createdBy", None),
            enable_derived_annotations=response.get("enableDerivedAnnotations", None),
        )

    async def get_schema_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> JSONSchemaBinding:
        """
        Get the JSON schema bound to the entity.
        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Example: Using this function
            Retrieving the bound JSON schema from a folder or file

            ```python
                import synapseclient
                from synapseclient.models import File, Folder
                import asyncio

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project") #replace with your project name
                ORG_NAME = "UniqueOrg" # replace with your organization name                SCHEMA_NAME = "myTestSchema" # replace with your schema name
                VERSION = "0.0.1"
                SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}"

                # Create organization (if not already created)
                js = syn.service("json_schema")
                all_orgs = js.list_organizations()
                for org in all_orgs:
                    if org["name"] == ORG_NAME:
                        print(f"Organization {ORG_NAME} already exists.")
                        break
                else:
                    print(f"Creating organization {ORG_NAME}.")
                    js.create_organization(ORG_NAME)

                # Create the schema (if not already created)
                schema_definition = {
                    "$id": "mySchema",
                    "type": "object",
                    "properties": {
                        "foo": {"type": "string"},
                        "bar": {"type": "integer"},
                    },
                    "required": ["foo"]
                }

                my_test_org = js.JsonSchemaOrganization(ORG_NAME)
                test_schema = my_test_org.get_json_schema(SCHEMA_NAME)
                if not test_schema:
                    test_schema = my_test_org.create_json_schema(schema_definition, SCHEMA_NAME, VERSION)

                # Create a test folder
                test_folder = Folder(name="test_script_folder", parent_id=PROJECT_ID)
                test_folder.store()

                # Bind JSON schema to the folder
                async def bind_json_schema():
                    bound_schema = await test_folder.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema
                asyncio.run(bind_json_schema())

                # Optionally, bind the same schema to a file
                example_file = File(
                    path="Sample.txt",  # Replace with your test file path
                    parent_id=test_folder.id,
                ).store()

                async def bind_schema_to_file():
                    bound_schema_file = await example_file.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema_file
                asyncio.run(bind_schema_to_file())

                # Retrieve the bound schema from the folder
                async def get_bound_schema():
                    bound_schema = await test_folder.get_schema_async()
                    return bound_schema
                bound_schema = asyncio.run(get_bound_schema())
                print("Bound schema retrieved:", bound_schema)

                # Retrieve the bound schema from the file
                async def get_bound_schema_from_file():
                    bound_schema_file = await example_file.get_schema_async()
                    return bound_schema_file
                bound_schema_file = asyncio.run(get_bound_schema_from_file())
                print("Bound schema from file retrieved:", bound_schema_file)
            ```
        Returns:
            JSONSchemaBinding: An object containing details about the bound JSON schema.
        """
        response = await get_json_schema_from_entity(
            synapse_id=self.id, synapse_client=synapse_client
        )
        json_schema_version_info = response.get("jsonSchemaVersionInfo", {})
        return JSONSchemaBinding(
            json_schema_version_info=JSONSchemaVersionInfo(
                organization_id=json_schema_version_info.get("organizationId", None),
                organization_name=json_schema_version_info.get(
                    "organizationName", None
                ),
                schema_id=json_schema_version_info.get("schemaId", None),
                id=json_schema_version_info.get("$id", None),
                schema_name=json_schema_version_info.get("schemaName", None),
                version_id=json_schema_version_info.get("versionId", None),
                semantic_version=json_schema_version_info.get("semanticVersion", None),
                json_sha256_hex=json_schema_version_info.get("jsonSHA256Hex", None),
                created_on=json_schema_version_info.get("createdOn", None),
                created_by=json_schema_version_info.get("createdBy", None),
            ),
            object_id=response.get("objectId", None),
            object_type=response.get("objectType", None),
            created_on=response.get("createdOn", None),
            created_by=response.get("createdBy", None),
            enable_derived_annotations=response.get("enableDerivedAnnotations", None),
        )

    async def unbind_schema_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> None:
        """
        Unbind the JSON schema bound to the entity.

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Example: Using this function
            Unbinding a JSON schema from a folder or file

            ```python
                import synapseclient
                from synapseclient.models import File, Folder
                import asyncio

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project") #replace with your project name
                ORG_NAME = "UniqueOrg" # replace with your organization name
                SCHEMA_NAME = "myTestSchema" # replace with your schema name
                VERSION = "0.0.1"
                SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}"

                # Create organization (if not already created)
                js = syn.service("json_schema")
                all_orgs = js.list_organizations()
                for org in all_orgs:
                    if org["name"] == ORG_NAME:
                        print(f"Organization {ORG_NAME} already exists.")
                        break
                else:
                    print(f"Creating organization {ORG_NAME}.")
                    js.create_organization(ORG_NAME)

                # Create the schema (if not already created)
                schema_definition = {
                    "$id": "mySchema",
                    "type": "object",
                    "properties": {
                        "foo": {"type": "string"},
                        "bar": {"type": "integer"},
                    },
                    "required": ["foo"]
                }

                my_test_org = js.JsonSchemaOrganization(ORG_NAME)
                test_schema = my_test_org.get_json_schema(SCHEMA_NAME)
                if not test_schema:
                    test_schema = my_test_org.create_json_schema(schema_definition, SCHEMA_NAME, VERSION)

                # Create a test folder
                test_folder = Folder(name="test_script_folder", parent_id=PROJECT_ID)
                test_folder.store()

                # Bind JSON schema to the folder
                async def bind_json_schema():
                    bound_schema = await test_folder.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema
                asyncio.run(bind_json_schema())

                # Optionally, bind the same schema to a file
                example_file = File(
                    path="Sample.txt",  # Replace with your test file path
                    parent_id=test_folder.id,
                ).store()

                async def bind_schema_to_file():
                    bound_schema_file = await example_file.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema_file
                asyncio.run(bind_schema_to_file())

                # Unbind the schema from the folder
                async def unbind_schema():
                    response = await test_folder.unbind_schema_async()
                    return response
                unbind_response = asyncio.run(unbind_schema())

                # Unbind the schema from the file
                async def unbind_schema_from_file():
                    response = await example_file.unbind_schema_async()
                    return response
                unbind_response_file = asyncio.run(unbind_schema_from_file())
            ```
        """
        return await delete_json_schema_from_entity(
            synapse_id=self.id, synapse_client=synapse_client
        )

    async def validate_schema_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> Union[JSONSchemaValidation, InvalidJSONSchemaValidation]:
        """
        Validate the entity against the bound JSON schema.

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Example: Using this function
            Validating a folder or file against the bound JSON schema

            ```python
                import synapseclient
                from synapseclient.models import File, Folder
                import asyncio

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project") #replace with your project name
                ORG_NAME = "UniqueOrg" # replace with your organization name
                SCHEMA_NAME = "myTestSchema" # replace with your schema name
                VERSION = "0.0.1"
                SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}"

                # Create organization (if not already created)
                js = syn.service("json_schema")
                all_orgs = js.list_organizations()
                for org in all_orgs:
                    if org["name"] == ORG_NAME:
                        print(f"Organization {ORG_NAME} already exists.")
                        break
                else:
                    print(f"Creating organization {ORG_NAME}.")
                    js.create_organization(ORG_NAME)

                # Create the schema (if not already created)
                schema_definition = {
                    "$id": "mySchema",
                    "type": "object",
                    "properties": {
                        "foo": {"type": "string"},
                        "bar": {"type": "integer"},
                    },
                    "required": ["foo"]
                }

                my_test_org = js.JsonSchemaOrganization(ORG_NAME)
                test_schema = my_test_org.get_json_schema(SCHEMA_NAME)
                if not test_schema:
                    test_schema = my_test_org.create_json_schema(schema_definition, SCHEMA_NAME, VERSION)

                # Create a test folder
                test_folder = Folder(name="test_script_folder", parent_id=PROJECT_ID)
                test_folder.store()

                # Bind JSON schema to the folder
                async def bind_json_schema():
                    bound_schema = await test_folder.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema
                asyncio.run(bind_json_schema())

                # Optionally, bind the same schema to a file
                example_file = File(
                    path="/Users/lpeng/Downloads/Sample_E.csv",  # Replace with your test file path
                    parent_id=test_folder.id,
                ).store()

                async def bind_schema_to_file():
                    bound_schema_file = await example_file.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema_file
                asyncio.run(bind_schema_to_file())


                # Validate the folder entity against the bound schema
                test_folder.annotations = {"foo": "test_value", "bar": 42}  # Example annotations
                test_folder.store()
                async def validate_folder_with_json_schema():
                    response = await test_folder.validate_schema_async()
                    return response

                validation_response = asyncio.run(validate_folder_with_json_schema())
                print('validation response:', validation_response)


                # Validate the file entity against the bound schema
                example_file.annotations = {"foo": "test_value", "bar": 43}  # Example annotations
                example_file.store()
                async def validate_file_with_json_schema():
                    response = await example_file.validate_schema_async()
                    return response
                validation_response_file = asyncio.run(validate_file_with_json_schema())
                print('validation response:', validation_response_file)
            ```
        Returns:
            Union[JSONSchemaValidation, InvalidJSONSchemaValidation]: The validation results.
        """
        response = await validate_entity_with_json_schema(
            synapse_id=self.id, synapse_client=synapse_client
        )
        if "validationException" in response:
            return InvalidJSONSchemaValidation(
                validation_response=JSONSchemaValidation(
                    object_id=response.get("objectId", None),
                    object_type=response.get("objectType", None),
                    object_etag=response.get("objectEtag", None),
                    id=response.get("schema$id", None),
                    is_valid=response.get("isValid", None),
                    validated_on=response.get("validatedOn", None),
                ),
                validation_error_message=response.get("validationErrorMessage", None),
                all_validation_messages=response.get("allValidationMessages", []),
                validation_exception=ValidationException(
                    pointer_to_violation=response.get("validationException", {}).get(
                        "pointerToViolation", None
                    ),
                    message=response.get("validationException", {}).get(
                        "message", None
                    ),
                    schema_location=response.get("validationException", {}).get(
                        "schemaLocation", None
                    ),
                    causing_exceptions=[
                        CausingException(
                            keyword=ce.get("keyword", None),
                            pointer_to_violation=ce.get("pointerToViolation", None),
                            message=ce.get("message", None),
                            schema_location=ce.get("schemaLocation", None),
                            causing_exceptions=[
                                CausingException(
                                    keyword=nce.get("keyword", None),
                                    pointer_to_violation=nce.get(
                                        "pointerToViolation", None
                                    ),
                                    message=nce.get("message", None),
                                    schema_location=nce.get("schemaLocation", None),
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
            object_id=response.get("objectId", None),
            object_type=response.get("objectType", None),
            object_etag=response.get("objectEtag", None),
            id=response.get("schema$id", None),
            is_valid=response.get("isValid", None),
            validated_on=response.get("validatedOn", None),
        )

    async def get_schema_derived_keys_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> JSONSchemaDerivedKeys:
        """
        Retrieve derived JSON schema keys for the entity.

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Example: Using this function
            Retrieving derived keys from a folder or file

            ```python
                import synapseclient
                from synapseclient.models import File, Folder
                import asyncio

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project") #replace with your project name
                ORG_NAME = "UniqueOrg" # replace with your organization name
                DERIVED_TEST_SCHEMA_NAME = "myTestDerivedSchema" # replace with your derived schema name
                VERSION = "0.0.1"
                SCHEMA_URI = f"{ORG_NAME}-{DERIVED_TEST_SCHEMA_NAME}-{VERSION}"

                # Create organization (if not already created)
                js = syn.service("json_schema")
                all_orgs = js.list_organizations()
                for org in all_orgs:
                    if org["name"] == ORG_NAME:
                        print(f"Organization {ORG_NAME} already exists.")
                        break
                else:
                    print(f"Creating organization {ORG_NAME}.")
                    js.create_organization(ORG_NAME)

                # Create the schema (if not already created)
                schema_definition = {
                    "$id": "mySchema",
                    "type": "object",
                    "properties": {
                        "foo": {"type": "string"},
                        "baz": {"type": "string", "const": "example_value"},  # Example constant for derived annotation
                        "bar": {"type": "integer"},
                    },
                    "required": ["foo"]
                }

                my_test_org = js.JsonSchemaOrganization(ORG_NAME)
                test_schema = my_test_org.get_json_schema(DERIVED_TEST_SCHEMA_NAME)
                if not test_schema:
                    test_schema = my_test_org.create_json_schema(schema_definition, DERIVED_TEST_SCHEMA_NAME, VERSION)

                # Create a test folder
                test_folder = Folder(name="test_script_folder", parent_id=PROJECT_ID)
                test_folder.store()

                # Bind JSON schema to the folder
                async def bind_json_schema():
                    bound_schema = await test_folder.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema
                asyncio.run(bind_json_schema())

                # Optionally, bind the same schema to a file
                example_file = File(
                    path="Sample.txt",  # Replace with your test file path
                    parent_id=test_folder.id,
                ).store()

                async def bind_schema_to_file():
                    bound_schema_file = await example_file.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema_file
                asyncio.run(bind_schema_to_file())

                # Get the derived keys from the bound schema of the folder
                test_folder.annotations = {"foo": "test_value_new", "bar": 42}  # Example annotations
                test_folder.store()
                async def get_schema_derived_keys():
                    derived_keys = await test_folder.get_schema_derived_keys_async()
                    return derived_keys

                derived_keys = asyncio.run(get_schema_derived_keys())
                print('Derived keys from folder:', derived_keys)

                # Get the derived keys from the bound schema of the file
                example_file.annotations = {"foo": "test_value_new", "bar": 43}  # Example annotations
                example_file.store()
                async def get_schema_derived_keys_from_file():
                    derived_keys_file = await example_file.get_schema_derived_keys_async()
                    return derived_keys_file
                print('Derived keys from file:', asyncio.run(get_schema_derived_keys_from_file()))
            ```
        Returns:
            JSONSchemaDerivedKeys: An object containing the derived keys for the entity.
        """
        response = await get_json_schema_derived_keys(
            synapse_id=self.id, synapse_client=synapse_client
        )
        return JSONSchemaDerivedKeys(keys=response["keys"])


@async_to_sync
class ContainerEntityJSONSchema(BaseJSONSchema, ContainerEntityJSONSchemaProtocol):
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

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Example: Using this function
            Retrieving validation statistics for a folder

            ```python
                import synapseclient
                from synapseclient.models import File, Folder
                import asyncio

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project") #replace with your project name
                ORG_NAME = "UniqueOrg" # replace with your organization name
                SCHEMA_NAME = "myTestSchema" # replace with your schema name
                VERSION = "0.0.1"
                SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}"

                # Create organization (if not already created)
                js = syn.service("json_schema")
                all_orgs = js.list_organizations()
                for org in all_orgs:
                    if org["name"] == ORG_NAME:
                        print(f"Organization {ORG_NAME} already exists.")
                        break
                else:
                    print(f"Creating organization {ORG_NAME}.")
                    js.create_organization(ORG_NAME)

                # Create the schema (if not already created)
                schema_definition = {
                    "$id": "mySchema",
                    "type": "object",
                    "properties": {
                        "foo": {"type": "string"},
                        "baz": {"type": "string", "const": "example_value"},  # Example constant for derived annotation
                        "bar": {"type": "integer"},
                    },
                    "required": ["foo"]
                }

                my_test_org = js.JsonSchemaOrganization(ORG_NAME)
                test_schema = my_test_org.get_json_schema(SCHEMA_NAME)
                if not test_schema:
                    test_schema = my_test_org.create_json_schema(schema_definition, SCHEMA_NAME, VERSION)

                # Create a test folder
                test_folder = Folder(name="test_script_folder", parent_id=PROJECT_ID)
                test_folder.store()

                # Bind JSON schema to the folder
                async def bind_json_schema():
                    bound_schema = await test_folder.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema
                asyncio.run(bind_json_schema())

                # Validate the folder entity against the bound schema
                test_folder.annotations = {"foo": "test_value_new", "bar": 42}  # Example annotations
                test_folder.store()

                async def get_validation_statistics():
                    stats = await test_folder.get_schema_validation_statistics_async()
                    return stats

                stats = asyncio.run(get_validation_statistics())
                print('Validation statistics:', stats)
            ```
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

    @skip_async_to_sync
    async def get_invalid_validation_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> AsyncGenerator[InvalidJSONSchemaValidation, None]:
        """
        Get invalid JSON schema validation results for a container entity.

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Example: Using this function
            Retrieving invalid validation results for a folder

            ```python
                import synapseclient
                from synapseclient.models import File, Folder
                import asyncio

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project") #replace with your project name
                ORG_NAME = "UniqueOrg" # replace with your organization name
                SCHEMA_NAME = "myTestSchema" # replace with your schema name
                VERSION = "0.0.1"
                SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}"

                # Create organization (if not already created)
                js = syn.service("json_schema")
                all_orgs = js.list_organizations()
                for org in all_orgs:
                    if org["name"] == ORG_NAME:
                        print(f"Organization {ORG_NAME} already exists.")
                        break
                else:
                    print(f"Creating organization {ORG_NAME}.")
                    js.create_organization(ORG_NAME)

                # Create the schema (if not already created)
                schema_definition = {
                    "$id": "mySchema",
                    "type": "object",
                    "properties": {
                        "foo": {"type": "string"},
                        "baz": {"type": "string", "const": "example_value"},  # Example constant for derived annotation
                        "bar": {"type": "integer"},
                    },
                    "required": ["foo"]
                }

                my_test_org = js.JsonSchemaOrganization(ORG_NAME)
                test_schema = my_test_org.get_json_schema(SCHEMA_NAME)
                if not test_schema:
                    test_schema = my_test_org.create_json_schema(schema_definition, SCHEMA_NAME, VERSION)

                # Create a test folder
                test_folder = Folder(name="test_script_folder", parent_id=PROJECT_ID)
                test_folder.store()

                # Bind JSON schema to the folder
                async def bind_json_schema():
                    bound_schema = await test_folder.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema
                asyncio.run(bind_json_schema())

                # Validate the folder entity against the bound schema
                test_folder.annotations = {"foo": "test_value_new", "bar": 'invalid_string'}  # Example annotations
                test_folder.store()

                async def get_invalid_validation_async():
                    gen  = test_folder.get_invalid_validation_async(synapse_client=syn)
                    async for child in gen:
                        print(child)
            ```

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
                    object_id=item.get("objectId", None),
                    object_type=item.get("objectType", None),
                    object_etag=item.get("objectEtag", None),
                    id=item.get("schema$id", None),
                    is_valid=item.get("isValid", None),
                    validated_on=item.get("validatedOn", None),
                ),
                validation_error_message=item.get("validationErrorMessage", None),
                all_validation_messages=item.get("allValidationMessages", []),
                validation_exception=ValidationException(
                    pointer_to_violation=item.get("validationException", {}).get(
                        "pointerToViolation", None
                    ),
                    message=item.get("validationException", {}).get("message", None),
                    schema_location=item.get("validationException", {}).get(
                        "schemaLocation", None
                    ),
                    causing_exceptions=[
                        CausingException(
                            keyword=ce.get("keyword", None),
                            pointer_to_violation=ce.get("pointerToViolation", None),
                            message=ce.get("message", None),
                            schema_location=ce.get("schemaLocation", None),
                            causing_exceptions=[
                                CausingException(
                                    keyword=nce.get("keyword", None),
                                    pointer_to_violation=nce.get(
                                        "pointerToViolation", None
                                    ),
                                    message=nce.get("message", None),
                                    schema_location=nce.get("schemaLocation", None),
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

    def get_invalid_validation(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> Generator[InvalidJSONSchemaValidation, None, None]:
        """
        Get invalid JSON schema validation results for a container entity.

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Example: Using this function
            Retrieving invalid validation results for a folder

            ```python
                import synapseclient
                from synapseclient.models import File, Folder
                import asyncio

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project") #replace with your project name
                ORG_NAME = "UniqueOrg" # replace with your organization name
                SCHEMA_NAME = "myTestSchema" # replace with your schema name
                VERSION = "0.0.1"
                SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}"

                # Create organization (if not already created)
                js = syn.service("json_schema")
                all_orgs = js.list_organizations()
                for org in all_orgs:
                    if org["name"] == ORG_NAME:
                        print(f"Organization {ORG_NAME} already exists.")
                        break
                else:
                    print(f"Creating organization {ORG_NAME}.")
                    js.create_organization(ORG_NAME)

                # Create the schema (if not already created)
                schema_definition = {
                    "$id": "mySchema",
                    "type": "object",
                    "properties": {
                        "foo": {"type": "string"},
                        "baz": {"type": "string", "const": "example_value"},  # Example constant for derived annotation
                        "bar": {"type": "integer"},
                    },
                    "required": ["foo"]
                }

                my_test_org = js.JsonSchemaOrganization(ORG_NAME)
                test_schema = my_test_org.get_json_schema(SCHEMA_NAME)
                if not test_schema:
                    test_schema = my_test_org.create_json_schema(schema_definition, SCHEMA_NAME, VERSION)

                # Create a test folder
                test_folder = Folder(name="test_script_folder", parent_id=PROJECT_ID)
                test_folder.store()

                # Bind JSON schema to the folder
                async def bind_json_schema():
                    bound_schema = await test_folder.bind_schema_async(
                        json_schema_uri=SCHEMA_URI,
                        enable_derived_annos=True
                    )
                    return bound_schema
                asyncio.run(bind_json_schema())

                # Validate the folder entity against the bound schema
                test_folder.annotations = {"foo": "test_value_new", "bar": 'invalid_string'}  # Example annotations
                test_folder.store()

                invalid_results = test_folder.get_invalid_validation(synapse_client=syn)
                for child in invalid_results:
                    print(child)
            ```

        Yields:
            InvalidJSONSchemaValidation: An object containing the validation response, all validation messages,
                                         and the validation exception details.
        """
        gen = get_invalid_json_schema_validation_sync(
            synapse_client=synapse_client, synapse_id=self.id
        )

        for item in gen:
            yield InvalidJSONSchemaValidation(
                validation_response=JSONSchemaValidation(
                    object_id=item.get("objectId", None),
                    object_type=item.get("objectType", None),
                    object_etag=item.get("objectEtag", None),
                    id=item.get("schema$id", None),
                    is_valid=item.get("isValid", None),
                    validated_on=item.get("validatedOn", None),
                ),
                validation_error_message=item.get("validationErrorMessage", None),
                all_validation_messages=item.get("allValidationMessages", []),
                validation_exception=ValidationException(
                    pointer_to_violation=item.get("validationException", {}).get(
                        "pointerToViolation", None
                    ),
                    message=item.get("validationException", {}).get("message", None),
                    schema_location=item.get("validationException", {}).get(
                        "schemaLocation", None
                    ),
                    causing_exceptions=[
                        CausingException(
                            keyword=ce.get("keyword", None),
                            pointer_to_violation=ce.get("pointerToViolation", None),
                            message=ce.get("message", None),
                            schema_location=ce.get("schemaLocation", None),
                            causing_exceptions=[
                                CausingException(
                                    keyword=nce.get("keyword", None),
                                    pointer_to_violation=nce.get(
                                        "pointerToViolation", None
                                    ),
                                    message=nce.get("message", None),
                                    schema_location=nce.get("schemaLocation", None),
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
