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

        Example: Using this function
            Binding JSON schema to a folder or a file

            ```python
                import synapseclient
                from synapseclient.models import File, Folder

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project")  # replace with your project name
                ORG_NAME = "UniqueOrg"  # replace with your organization name
                SCHEMA_NAME = "myTestSchema"  # replace with your schema name
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

                # Bind JSON schema to the folder (synchronous version)
                bound_schema = test_folder.bind_schema(
                    json_schema_uri=SCHEMA_URI,
                    enable_derived_annotations=True
                )

                # Optionally, bind the same schema to a file
                example_file = File(
                    path="Sample.txt",  # Replace with your test file path
                    parent_id=test_folder.id,
                ).store()

                bound_schema_file = example_file.bind_schema(
                    json_schema_uri=SCHEMA_URI,
                    enable_derived_annotations=True
                )
            ```
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

        Example: Using this function
            Retrieving the bound JSON schema from a folder or file

            ```python
                import synapseclient
                from synapseclient.models import File, Folder

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project")  # replace with your project name
                ORG_NAME = "UniqueOrg"  # replace with your organization name
                SCHEMA_NAME = "myTestSchema"  # replace with your schema name
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

                # Bind JSON schema to the folder (synchronously)
                bound_schema = test_folder.bind_schema(
                    json_schema_uri=SCHEMA_URI,
                    enable_derived_annotations=True
                )

                # Optionally, bind the same schema to a file
                example_file = File(
                    path="Sample.txt",  # Replace with your test file path
                    parent_id=test_folder.id,
                ).store()

                bound_schema_file = example_file.bind_schema(
                    json_schema_uri=SCHEMA_URI,
                    enable_derived_annotations=True
                )

                # Retrieve the bound schema from the folder
                bound_schema = test_folder.get_schema()
                print("Bound schema retrieved:", bound_schema)

                # Retrieve the bound schema from the file
                bound_schema_file = example_file.get_schema()
                print("Bound schema for file retrieved:", bound_schema_file)
            ```
        """
        return JSONSchemaBinding()

    def unbind_schema(self, *, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Unbind the JSON schema from the entity.

        Arguments:
            synapse_client (Optional[Synapse], optional): The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Example: Using this function
            Unbinding a JSON schema from a folder or file

            ```python
                import synapseclient
                from synapseclient.models import File, Folder

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project")  # replace with your project name
                ORG_NAME = "UniqueOrg"  # replace with your organization name
                SCHEMA_NAME = "myTestSchema"  # replace with your schema name
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
                bound_schema = test_folder.bind_schema(
                    json_schema_uri=SCHEMA_URI,
                    enable_derived_annotations=True
                )

                # Optionally, bind the same schema to a file
                example_file = File(
                    path="Sample.txt",  # Replace with your test file path
                    parent_id=test_folder.id,
                ).store()

                bound_schema_file = example_file.bind_schema(
                    json_schema_uri=SCHEMA_URI,
                    enable_derived_annotations=True
                )

                # Unbind the schema from the folder
                unbind_response = test_folder.unbind_schema()

                # Unbind the schema from the file
                unbind_response_file = example_file.unbind_schema()
            ```
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

        Example: Using this function
            Validating a folder or file against the bound JSON schema

            ```python
                import synapseclient
                from synapseclient.models import File, Folder
                import time

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project")  # replace with your project name
                ORG_NAME = "UniqueOrg"  # replace with your organization name
                SCHEMA_NAME = "myTestSchema"  # replace with your schema name
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
                bound_schema = test_folder.bind_schema(
                    json_schema_uri=SCHEMA_URI,
                    enable_derived_annotations=True
                )

                # Optionally, bind the same schema to a file
                example_file = File(
                    path="Sample.txt",  # Replace with your test file path
                    parent_id=test_folder.id,
                ).store()

                bound_schema_file = example_file.bind_schema(
                    json_schema_uri=SCHEMA_URI,
                    enable_derived_annotations=True
                )

                # Validate the folder entity against the bound schema
                test_folder.annotations = {"foo": "test_value", "bar": 42}  # Example annotations
                test_folder.store()
                time.sleep(2)

                validation_response = test_folder.validate_schema()
                print('validation response:', validation_response)

                # Validate the file entity against the bound schema
                example_file.annotations = {"foo": "test_value", "bar": 43}  # Example annotations
                example_file.store()
                time.sleep(2)

                validation_response_file = example_file.validate_schema()
                print('validation response:', validation_response_file)
            ```
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

        Example: Using this function
            Retrieving derived keys from a folder or file

            ```python
                import synapseclient
                from synapseclient.models import File, Folder

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project")  # Replace with your project name
                ORG_NAME = "UniqueOrg"  # Replace with your organization name
                DERIVED_TEST_SCHEMA_NAME = "myTestDerivedSchema"  # Replace with your derived schema name
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
                bound_schema = test_folder.bind_schema(
                    json_schema_uri=SCHEMA_URI,
                    enable_derived_annotations=True
                )

                # Optionally, bind the same schema to a file
                example_file = File(
                    path="Sample.txt",  # Replace with your test file path
                    parent_id=test_folder.id,
                ).store()

                bound_schema_file = example_file.bind_schema(
                    json_schema_uri=SCHEMA_URI,
                    enable_derived_annotations=True
                )

                # Get the derived keys from the bound schema of the folder
                test_folder.annotations = {"foo": "test_value_new", "bar": 42}  # Example annotations
                test_folder.store()

                derived_keys = test_folder.get_schema_derived_keys()
                print('Derived keys from folder:', derived_keys)

                # Get the derived keys from the bound schema of the file
                example_file.annotations = {"foo": "test_value_new", "bar": 43}  # Example annotations
                example_file.store()

                derived_keys_file = example_file.get_schema_derived_keys()
                print('Derived keys from file:', derived_keys_file)
            ```
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

        Example: Using this function
            Retrieving validation statistics for a folder

            ```python
                import synapseclient
                from synapseclient.models import File, Folder
                import time

                syn = synapseclient.Synapse()
                syn.login()

                # Define Project and JSON schema info
                PROJECT_ID = syn.findEntityId(name="test_json_schema_project") # use your project name
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
                        "baz": {"type": "string", "const": "example_value"},
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
                bound_schema = test_folder.bind_schema(
                    json_schema_uri=SCHEMA_URI,
                    enable_derived_annotations=True
                )

                # Create files within the folder with invalid annotations
                invalid_file1 = File(
                    path="test.txt",  # assumes you have something here or adjust path
                    parent_id=test_folder.id
                )
                invalid_file1.annotations = {"foo": 123, "bar": "not_an_integer"}  # both invalid
                invalid_file1.store()
                time.sleep(2)

                # Get schema validation statistics
                stats = test_folder.get_schema_validation_statistics()
                print('Validation statistics:', stats)
            ```
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

        Example: Using this function
            Retrieving invalid validation results for a folder

        ```python
            import synapseclient
            from synapseclient.models import File, Folder
            import time

            syn = synapseclient.Synapse()
            syn.login()

            # Define Project and JSON schema info
            PROJECT_ID = syn.findEntityId(name="test_json_schema_project") # use your project name
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
                    "baz": {"type": "string", "const": "example_value"},
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
            bound_schema = test_folder.bind_schema(
                json_schema_uri=SCHEMA_URI,
                enable_derived_annotations=True
            )

            # Create files within the folder with invalid annotations
            invalid_file1 = File(
                path="test.txt",  # assumes you have something here or adjust path
                parent_id=test_folder.id
            )
            invalid_file1.annotations = {"foo": 123, "bar": "not_an_integer"}  # both invalid
            invalid_file1.store()
            time.sleep(2)

            # Validate the folder (this will also validate children)
            invalid_items = test_folder.get_invalid_validation()
            print('invalid items:', invalid_items)
            for item in invalid_items:
                print('item:', item)
        ```
        """
        yield InvalidJSONSchemaValidation()
