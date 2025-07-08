from typing import TYPE_CHECKING, Optional, Protocol, Union

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
            json_schema_uri: The URI of the JSON schema to bind to the entity.
            enable_derived_annotations: If true, enable derived annotations. Defaults to False.
            synapse_client: The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            An object containing details about the JSON schema binding.

        Example: Using this function
            Binding JSON schema to a folder or a file. This example expects that you
            have a Synapse project to use, and a file to upload. Set the `PROJECT_NAME`
            and `FILE_PATH` variables to your project name and file path respectively.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import File, Folder

            syn = Synapse()
            syn.login()

            # Define Project and JSON schema info
            PROJECT_NAME = "test_json_schema_project"  # replace with your project name
            FILE_PATH = "~/Sample.txt"  # replace with your test file path

            PROJECT_ID = syn.findEntityId(name=PROJECT_NAME)
            ORG_NAME = "UniqueOrg"  # replace with your organization name
            SCHEMA_NAME = "myTestSchema"  # replace with your schema name
            FOLDER_NAME = "test_script_folder"
            VERSION = "0.0.1"
            SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}"

            # Create organization (if not already created)
            js = syn.service("json_schema")
            all_orgs = js.list_organizations()
            for org in all_orgs:
                if org["name"] == ORG_NAME:
                    print(f"Organization {ORG_NAME} already exists: {org}")
                    break
            else:
                print(f"Creating organization {ORG_NAME}.")
                created_organization = js.create_organization(ORG_NAME)
                print(f"Created organization: {created_organization}")


            my_test_org = js.JsonSchemaOrganization(ORG_NAME)
            test_schema = my_test_org.get_json_schema(SCHEMA_NAME)

            if not test_schema:
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
                test_schema = my_test_org.create_json_schema(schema_definition, SCHEMA_NAME, VERSION)

            # Create a test folder
            test_folder = Folder(name=FOLDER_NAME, parent_id=PROJECT_ID)
            test_folder.store()

            # Bind JSON schema to the folder
            bound_schema = test_folder.bind_schema(
                json_schema_uri=SCHEMA_URI,
                enable_derived_annotations=True
            )
            print(f"Result from binding schema to folder: {bound_schema}")

            # Bind the same schema to a file
            example_file = File(
                path=FILE_PATH,  # Replace with your test file path
                parent_id=test_folder.id,
            ).store()

            bound_schema_file = example_file.bind_schema(
                json_schema_uri=SCHEMA_URI,
                enable_derived_annotations=True
            )
            print(f"Result from binding schema to file: {bound_schema_file}")
            ```
        """
        return JSONSchemaBinding()

    def get_schema(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> "JSONSchemaBinding":
        """
        Get the JSON schema bound to the entity.

        Arguments:
            synapse_client: The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            An object containing details about the bound JSON schema.

        Example: Using this function
            Retrieving the bound JSON schema from a folder or file. This example demonstrates
            how to get existing schema bindings from entities that already have schemas bound.
            Set the `PROJECT_NAME` variable to your project name.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import File, Folder

            syn = Synapse()
            syn.login()

            # Define Project and JSON schema info
            PROJECT_NAME = "test_json_schema_project"  # replace with your project name
            FILE_PATH = "~/Sample.txt"  # replace with your test file path

            PROJECT_ID = syn.findEntityId(name=PROJECT_NAME)
            ORG_NAME = "UniqueOrg"  # replace with your organization name
            SCHEMA_NAME = "myTestSchema"  # replace with your schema name
            FOLDER_NAME = "test_script_folder"
            VERSION = "0.0.1"
            SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}"

            # Create organization (if not already created)
            js = syn.service("json_schema")
            all_orgs = js.list_organizations()
            for org in all_orgs:
                if org["name"] == ORG_NAME:
                    print(f"Organization {ORG_NAME} already exists: {org}")
                    break
            else:
                print(f"Creating organization {ORG_NAME}.")
                created_organization = js.create_organization(ORG_NAME)
                print(f"Created organization: {created_organization}")

            my_test_org = js.JsonSchemaOrganization(ORG_NAME)
            test_schema = my_test_org.get_json_schema(SCHEMA_NAME)

            if not test_schema:
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
                test_schema = my_test_org.create_json_schema(schema_definition, SCHEMA_NAME, VERSION)
                print(f"Created new schema: {SCHEMA_NAME}")

            # Create a test folder
            test_folder = Folder(name=FOLDER_NAME, parent_id=PROJECT_ID)
            test_folder.store()
            print(f"Created test folder: {FOLDER_NAME}")

            # Bind JSON schema to the folder first
            bound_schema = test_folder.bind_schema(
                json_schema_uri=SCHEMA_URI,
                enable_derived_annotations=True
            )
            print(f"Bound schema to folder: {bound_schema}")

            # Create and bind schema to a file
            example_file = File(
                path=FILE_PATH,  # Replace with your test file path
                parent_id=test_folder.id,
            ).store()

            bound_schema_file = example_file.bind_schema(
                json_schema_uri=SCHEMA_URI,
                enable_derived_annotations=True
            )
            print(f"Bound schema to file: {bound_schema_file}")

            # Retrieve the bound schema from the folder
            retrieved_folder_schema = test_folder.get_schema()
            print(f"Retrieved schema from folder: {retrieved_folder_schema}")

            # Retrieve the bound schema from the file
            retrieved_file_schema = example_file.get_schema()
            print(f"Retrieved schema from file: {retrieved_file_schema}")
            ```
        """
        return JSONSchemaBinding()

    def unbind_schema(self, *, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Unbind the JSON schema from the entity.

        Arguments:
            synapse_client: The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Example: Using this function
            Unbinding a JSON schema from a folder or file. This example demonstrates
            how to remove schema bindings from entities. Assumes entities already have
            schemas bound. Set the `PROJECT_NAME` variable to your project name.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import File, Folder

            syn = Synapse()
            syn.login()

            # Define Project and JSON schema info
            PROJECT_NAME = "test_json_schema_project"  # replace with your project name
            FILE_PATH = "~/Sample.txt"  # replace with your test file path

            PROJECT_ID = syn.findEntityId(name=PROJECT_NAME)
            ORG_NAME = "UniqueOrg"  # replace with your organization name
            SCHEMA_NAME = "myTestSchema"  # replace with your schema name
            FOLDER_NAME = "test_script_folder"
            VERSION = "0.0.1"
            SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}"

            # Create organization (if not already created)
            js = syn.service("json_schema")
            all_orgs = js.list_organizations()
            for org in all_orgs:
                if org["name"] == ORG_NAME:
                    print(f"Organization {ORG_NAME} already exists: {org}")
                    break
            else:
                print(f"Creating organization {ORG_NAME}.")
                created_organization = js.create_organization(ORG_NAME)
                print(f"Created organization: {created_organization}")

            my_test_org = js.JsonSchemaOrganization(ORG_NAME)
            test_schema = my_test_org.get_json_schema(SCHEMA_NAME)

            if not test_schema:
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
                test_schema = my_test_org.create_json_schema(schema_definition, SCHEMA_NAME, VERSION)
                print(f"Created new schema: {SCHEMA_NAME}")

            # Create a test folder
            test_folder = Folder(name=FOLDER_NAME, parent_id=PROJECT_ID)
            test_folder.store()
            print(f"Created test folder: {FOLDER_NAME}")

            # Bind JSON schema to the folder first
            bound_schema = test_folder.bind_schema(
                json_schema_uri=SCHEMA_URI,
                enable_derived_annotations=True
            )
            print(f"Bound schema to folder: {bound_schema}")

            # Create and bind schema to a file
            example_file = File(
                path=FILE_PATH,  # Replace with your test file path
                parent_id=test_folder.id,
            ).store()

            bound_schema_file = example_file.bind_schema(
                json_schema_uri=SCHEMA_URI,
                enable_derived_annotations=True
            )
            print(f"Bound schema to file: {bound_schema_file}")

            # Unbind the schema from the folder
            test_folder.unbind_schema()
            print("Successfully unbound schema from folder")

            # Unbind the schema from the file
            example_file.unbind_schema()
            print("Successfully unbound schema from file")
            ```
        """

    def validate_schema(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> Union["JSONSchemaValidation", "InvalidJSONSchemaValidation"]:
        """
        Validate the entity against the bound JSON schema.

        Arguments:
            synapse_client: The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            The validation results.

        Example: Using this function
            Validating a folder or file against the bound JSON schema. This example demonstrates
            how to validate entities with annotations against their bound schemas. Requires entities
            to have schemas already bound. Set the `PROJECT_NAME` variable to your project name.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import File, Folder
            import time

            syn = Synapse()
            syn.login()

            # Define Project and JSON schema info
            PROJECT_NAME = "test_json_schema_project"  # replace with your project name
            FILE_PATH = "~/Sample.txt"  # replace with your test file path

            PROJECT_ID = syn.findEntityId(name=PROJECT_NAME)
            ORG_NAME = "UniqueOrg"  # replace with your organization name
            SCHEMA_NAME = "myTestSchema"  # replace with your schema name
            FOLDER_NAME = "test_script_folder"
            VERSION = "0.0.1"
            SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}"

            # Create organization (if not already created)
            js = syn.service("json_schema")
            all_orgs = js.list_organizations()
            for org in all_orgs:
                if org["name"] == ORG_NAME:
                    print(f"Organization {ORG_NAME} already exists: {org}")
                    break
            else:
                print(f"Creating organization {ORG_NAME}.")
                created_organization = js.create_organization(ORG_NAME)
                print(f"Created organization: {created_organization}")

            my_test_org = js.JsonSchemaOrganization(ORG_NAME)
            test_schema = my_test_org.get_json_schema(SCHEMA_NAME)

            if not test_schema:
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
                test_schema = my_test_org.create_json_schema(schema_definition, SCHEMA_NAME, VERSION)
                print(f"Created new schema: {SCHEMA_NAME}")

            # Create a test folder
            test_folder = Folder(name=FOLDER_NAME, parent_id=PROJECT_ID)
            test_folder.store()
            print(f"Created test folder: {FOLDER_NAME}")

            # Bind JSON schema to the folder
            bound_schema = test_folder.bind_schema(
                json_schema_uri=SCHEMA_URI,
                enable_derived_annotations=True
            )
            print(f"Bound schema to folder: {bound_schema}")

            # Create and bind schema to a file
            example_file = File(
                path=FILE_PATH,  # Replace with your test file path
                parent_id=test_folder.id,
            ).store()

            bound_schema_file = example_file.bind_schema(
                json_schema_uri=SCHEMA_URI,
                enable_derived_annotations=True
            )
            print(f"Bound schema to file: {bound_schema_file}")

            # Validate the folder entity against the bound schema
            test_folder.annotations = {"foo": "test_value", "bar": 42}  # Example annotations
            test_folder.store()
            print("Added annotations to folder and stored")
            time.sleep(2)  # Allow time for processing

            validation_response = test_folder.validate_schema()
            print(f"Folder validation response: {validation_response}")

            # Validate the file entity against the bound schema
            example_file.annotations = {"foo": "test_value", "bar": 43}  # Example annotations
            example_file.store()
            print("Added annotations to file and stored")
            time.sleep(2)  # Allow time for processing

            validation_response_file = example_file.validate_schema()
            print(f"File validation response: {validation_response_file}")
            ```
        """
        return InvalidJSONSchemaValidation() or JSONSchemaValidation()

    def get_schema_derived_keys(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> "JSONSchemaDerivedKeys":
        """
        Retrieve derived JSON schema keys for the entity.

        Arguments:
            synapse_client: The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            An object containing the derived keys for the entity.

        Example: Using this function
            Retrieving derived keys from a folder or file. This example demonstrates
            how to get derived annotation keys from schemas with constant values.
            Set the `PROJECT_NAME` variable to your project name.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import File, Folder

            syn = Synapse()
            syn.login()

            # Define Project and JSON schema info
            PROJECT_NAME = "test_json_schema_project"  # replace with your project name
            FILE_PATH = "~/Sample.txt"  # replace with your test file path

            PROJECT_ID = syn.findEntityId(name=PROJECT_NAME)
            ORG_NAME = "UniqueOrg"  # replace with your organization name
            DERIVED_TEST_SCHEMA_NAME = "myTestDerivedSchema"  # replace with your derived schema name
            FOLDER_NAME = "test_script_folder"
            VERSION = "0.0.1"
            SCHEMA_URI = f"{ORG_NAME}-{DERIVED_TEST_SCHEMA_NAME}-{VERSION}"

            # Create organization (if not already created)
            js = syn.service("json_schema")
            all_orgs = js.list_organizations()
            for org in all_orgs:
                if org["name"] == ORG_NAME:
                    print(f"Organization {ORG_NAME} already exists: {org}")
                    break
            else:
                print(f"Creating organization {ORG_NAME}.")
                created_organization = js.create_organization(ORG_NAME)
                print(f"Created organization: {created_organization}")

            my_test_org = js.JsonSchemaOrganization(ORG_NAME)
            test_schema = my_test_org.get_json_schema(DERIVED_TEST_SCHEMA_NAME)

            if not test_schema:
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
                test_schema = my_test_org.create_json_schema(schema_definition, DERIVED_TEST_SCHEMA_NAME, VERSION)
                print(f"Created new derived schema: {DERIVED_TEST_SCHEMA_NAME}")

            # Create a test folder
            test_folder = Folder(name=FOLDER_NAME, parent_id=PROJECT_ID)
            test_folder.store()
            print(f"Created test folder: {FOLDER_NAME}")

            # Bind JSON schema to the folder
            bound_schema = test_folder.bind_schema(
                json_schema_uri=SCHEMA_URI,
                enable_derived_annotations=True
            )
            print(f"Bound schema to folder with derived annotations: {bound_schema}")

            # Create and bind schema to a file
            example_file = File(
                path=FILE_PATH,  # Replace with your test file path
                parent_id=test_folder.id,
            ).store()

            bound_schema_file = example_file.bind_schema(
                json_schema_uri=SCHEMA_URI,
                enable_derived_annotations=True
            )
            print(f"Bound schema to file with derived annotations: {bound_schema_file}")

            # Get the derived keys from the bound schema of the folder
            test_folder.annotations = {"foo": "test_value_new", "bar": 42}  # Example annotations
            test_folder.store()
            print("Added annotations to folder and stored")

            derived_keys = test_folder.get_schema_derived_keys()
            print(f"Derived keys from folder: {derived_keys}")

            # Get the derived keys from the bound schema of the file
            example_file.annotations = {"foo": "test_value_new", "bar": 43}  # Example annotations
            example_file.store()
            print("Added annotations to file and stored")

            derived_keys_file = example_file.get_schema_derived_keys()
            print(f"Derived keys from file: {derived_keys_file}")
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
            synapse_client: The Synapse client instance. If not provided,
                the last created instance from the Synapse class constructor will be used.

        Returns:
            The validation statistics.

        Example: Using this function
            Retrieving validation statistics for a folder. This example demonstrates
            how to get validation statistics for a container entity after creating
            entities with various validation states. Set the `PROJECT_NAME` variable to your project name.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import File, Folder
            import time

            syn = Synapse()
            syn.login()

            # Define Project and JSON schema info
            PROJECT_NAME = "test_json_schema_project"  # replace with your project name
            FILE_PATH = "~/Sample.txt"  # replace with your test file path

            PROJECT_ID = syn.findEntityId(name=PROJECT_NAME)
            ORG_NAME = "UniqueOrg"  # replace with your organization name
            SCHEMA_NAME = "myTestSchema"  # replace with your schema name
            FOLDER_NAME = "test_script_folder"
            VERSION = "0.0.1"
            SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}"

            # Create organization (if not already created)
            js = syn.service("json_schema")
            all_orgs = js.list_organizations()
            for org in all_orgs:
                if org["name"] == ORG_NAME:
                    print(f"Organization {ORG_NAME} already exists: {org}")
                    break
            else:
                print(f"Creating organization {ORG_NAME}.")
                created_organization = js.create_organization(ORG_NAME)
                print(f"Created organization: {created_organization}")

            my_test_org = js.JsonSchemaOrganization(ORG_NAME)
            test_schema = my_test_org.get_json_schema(SCHEMA_NAME)

            if not test_schema:
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
                test_schema = my_test_org.create_json_schema(schema_definition, SCHEMA_NAME, VERSION)
                print(f"Created new schema: {SCHEMA_NAME}")

            # Create a test folder
            test_folder = Folder(name=FOLDER_NAME, parent_id=PROJECT_ID)
            test_folder.store()
            print(f"Created test folder: {FOLDER_NAME}")

            # Bind JSON schema to the folder
            bound_schema = test_folder.bind_schema(
                json_schema_uri=SCHEMA_URI,
                enable_derived_annotations=True
            )
            print(f"Bound schema to folder: {bound_schema}")

            # Create files within the folder with invalid annotations to generate statistics
            invalid_file1 = File(
                path=FILE_PATH,  # assumes you have something here or adjust path
                parent_id=test_folder.id
            )
            invalid_file1.annotations = {"foo": 123, "bar": "not_an_integer"}  # both invalid
            invalid_file1.store()
            print("Created file with invalid annotations")
            time.sleep(2)  # Allow time for processing

            # Get schema validation statistics
            stats = test_folder.get_schema_validation_statistics()
            print(f"Validation statistics: {stats}")
            ```
        """
        return JSONSchemaValidationStatistics()
