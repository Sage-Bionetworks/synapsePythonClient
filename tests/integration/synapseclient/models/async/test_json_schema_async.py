import asyncio
import random
import uuid
from typing import Callable, Generator, Tuple

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import File, Folder, Project
from synapseclient.services.json_schema import JsonSchemaOrganization

DESCRIPTION_FOLDER = "This is a folder for testing JSON schema functionality."
DESCRIPTION_FILE = "This is an example file."
CONTENT_TYPE_FILE = "text/plain"
VERSION_COMMENT = "My version comment"

TEST_SCHEMA_NAME = "example.dpetest.jsonschema"
SCHEMA_VERSION = "0.0.1"


class TestJSONSchema:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    def create_test_organization_with_schema(
        self, syn: Synapse
    ) -> Generator[Tuple[JsonSchemaOrganization, str], None, None]:
        """Create a test organization for JSON schema functionality."""
        org_name = "dpetest" + str(random.randint(1000, 9999))

        js = syn.service("json_schema")
        created_org = js.create_organization(org_name)

        # Add a JSON schema
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://example.com/schema/productschema.json",
            "title": "Product Schema",
            "type": "object",
            "properties": {
                "productId": {
                    "description": "The unique identifier for a product",
                    "type": "integer",
                    "const": 123,
                },
                "productName": {
                    "description": "Name of the product",
                    "type": "string",
                    "const": "default product name",
                },
                "productDescription": {
                    "description": "description of the product",
                    "type": "string",
                },
                "productQuantity": {
                    "description": "quantity of the product",
                    "type": "integer",
                },
            },
        }
        test_org = js.JsonSchemaOrganization(org_name)
        created_schema = test_org.create_json_schema(schema, TEST_SCHEMA_NAME, "0.0.1")
        yield test_org, created_schema.uri

        js.delete_json_schema(created_schema.uri)
        js.delete_organization(created_org["id"])

    @pytest.fixture(autouse=True, scope="function")
    def folder(self) -> Folder:
        """Create a folder for testing JSON schema functionality."""
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)
        return folder

    async def test_bind_json_schema_to_entity_async(
        self,
        folder: Folder,
        project_model: Project,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test binding a JSON schema to a folder entity."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # Bind the JSON schema to the folder
        test_org, test_product_schema_uri = create_test_organization_with_schema
        try:
            response = await created_folder.bind_json_schema_to_entity_async(
                json_schema_uri=test_product_schema_uri, synapse_client=self.syn
            )
            json_schema_version_info = response.json_schema_version_info
            assert json_schema_version_info.organization_name == test_org.name
            assert json_schema_version_info.id == test_product_schema_uri
        finally:
            # Clean up the JSON schema binding
            await created_folder.delete_json_schema_from_entity_async(
                synapse_client=self.syn
            )

    async def test_get_json_schema_from_entity_async(
        self,
        folder: Folder,
        project_model: Project,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test retrieving a bound JSON schema from an entity."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # Bind the JSON schema to the folder
        try:
            test_org, test_product_schema_uri = create_test_organization_with_schema
            await created_folder.bind_json_schema_to_entity_async(
                json_schema_uri=test_product_schema_uri, synapse_client=self.syn
            )

            # Retrieve the JSON schema from the folder
            response = await created_folder.get_json_schema_from_entity_async(
                synapse_client=self.syn
            )
            assert response.json_schema_version_info.organization_name == test_org.name
            assert response.json_schema_version_info.id == test_product_schema_uri
        finally:
            # Clean up the JSON schema binding
            await created_folder.delete_json_schema_from_entity_async(
                synapse_client=self.syn
            )

    async def test_delete_json_schema_from_entity_async(
        self,
        folder: Folder,
        project_model: Project,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test deleting a bound JSON schema from an entity."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # Bind the JSON schema to the folder
        _, test_product_schema_uri = create_test_organization_with_schema
        await created_folder.bind_json_schema_to_entity_async(
            json_schema_uri=test_product_schema_uri, synapse_client=self.syn
        )

        # Delete the JSON schema from the folder
        await created_folder.delete_json_schema_from_entity_async(
            synapse_client=self.syn
        )

        # Verify that the JSON schema is no longer bound
        with pytest.raises(
            SynapseHTTPError,
            match=f"404 Client Error: No JSON schema found for '{folder.id}'",
        ):
            await created_folder.get_json_schema_from_entity_async(
                synapse_client=self.syn
            )

    async def test_get_json_schema_derived_keys_async(
        self,
        folder: Folder,
        project_model: Project,
        create_test_organization_with_schema: Tuple[
            JsonSchemaOrganization,
            str,
        ],
    ) -> None:
        """Test retrieving derived keys from a bound JSON schema."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)
        _, test_product_schema_uri = create_test_organization_with_schema

        # Bind the JSON schema to the folder
        try:
            await created_folder.bind_json_schema_to_entity_async(
                json_schema_uri=test_product_schema_uri,
                enable_derived_annos=True,
                synapse_client=self.syn,
            )

            # Store annotations
            created_folder.annotations = {
                "productDescription": "test description here",
                "productQuantity": 100,
                "productPrice": 100,
            }

            stored_folder = await folder.store_async(parent=project_model)
            response = await created_folder.get_json_schema_from_entity_async(
                synapse_client=self.syn
            )
            response = await created_folder.get_json_schema_from_entity_async(
                synapse_client=self.syn
            )
            assert response.enable_derived_annotations == True

            await asyncio.sleep(2)

            # Retrieve the derived keys from the folder
            response = await stored_folder.get_json_schema_derived_keys_async(
                synapse_client=self.syn
            )

            assert set(response.keys) == {"productId", "productName"}
        finally:
            # Clean up the JSON schema binding
            await created_folder.delete_json_schema_from_entity_async(
                synapse_client=self.syn
            )

    async def test_validate_entity_with_json_schema_async_invalid_annos(
        self,
        folder: Folder,
        project_model: Project,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test validating invalid annotations against a bound JSON schema."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)
        _, test_product_schema_uri = create_test_organization_with_schema

        # Bind the JSON schema to the folder
        try:
            await created_folder.bind_json_schema_to_entity_async(
                json_schema_uri=test_product_schema_uri,
                synapse_client=self.syn,
            )

            # Store annotations that do not match the schema
            created_folder.annotations = {
                "productDescription": 1000,
                "productQuantity": "invalid string",
            }
            await folder.store_async(parent=project_model)
            # Ensure annotations are stored
            asyncio.sleep(2)

            # Validate the folder against the JSON schema
            response = await created_folder.validate_entity_with_json_schema_async(
                synapse_client=self.syn
            )
            assert response.validation_response.is_valid == False
            assert response.validation_response.id is not None

            all_messages = response.all_validation_messages

            assert (
                "#/productQuantity: expected type: Integer, found: String"
                in all_messages
            )
            assert (
                "#/productDescription: expected type: String, found: Long"
                in all_messages
            )
        finally:
            # Clean up the JSON schema binding
            await created_folder.delete_json_schema_from_entity_async(
                synapse_client=self.syn
            )

    async def test_validate_entity_with_json_schema_async_valid_annos(
        self,
        folder: Folder,
        project_model: Project,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test validating valid annotations against a bound JSON schema."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)
        _, test_product_schema_uri = create_test_organization_with_schema

        # Bind the JSON schema to the folder
        try:
            await created_folder.bind_json_schema_to_entity_async(
                json_schema_uri=test_product_schema_uri,
                synapse_client=self.syn,
            )

            # Store annotations that match the schema
            created_folder.annotations = {
                "productDescription": "This is a test product.",
                "productQuantity": 100,
            }
            await folder.store_async(parent=project_model)
            # Ensure annotations are stored
            asyncio.sleep(2)
            response = await created_folder.validate_entity_with_json_schema_async(
                synapse_client=self.syn
            )
            assert response.is_valid == True
        finally:
            # Clean up the JSON schema binding
            await created_folder.delete_json_schema_from_entity_async(
                synapse_client=self.syn
            )

    async def test_get_json_schema_validation_statistics_async(
        self,
        folder: Folder,
        project_model: Project,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test retrieving JSON schema validation statistics for a folder."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # Create two files under the folder
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        file_1 = await File(
            path=filename,
            name="test_file_1",
            description=DESCRIPTION_FILE,
            content_type=CONTENT_TYPE_FILE,
            version_comment=VERSION_COMMENT,
            version_label=str(uuid.uuid4()),
            parent_id=folder.id,
        ).store_async(synapse_client=self.syn)

        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        file_2 = await File(
            path=filename,
            name="test_file_2",
            description=DESCRIPTION_FILE,
            content_type=CONTENT_TYPE_FILE,
            version_comment=VERSION_COMMENT,
            version_label=str(uuid.uuid4()),
            parent_id=folder.id,
        ).store_async(synapse_client=self.syn)

        _, test_product_schema_uri = create_test_organization_with_schema

        # Bind the JSON SCHEMA to the folder
        try:
            await created_folder.bind_json_schema_to_entity_async(
                json_schema_uri=test_product_schema_uri,
                synapse_client=self.syn,
            )

            # Store annotations in the file, one valid and one invalid
            file_1.annotations = {
                "productDescription": "test description here.",
                "productQuantity": 100,
            }
            file_2.annotations = {
                "productDescription": 200,
                "productQuantity": "invalid string",
            }

            await file_1.store_async(parent=folder)
            await file_2.store_async(parent=folder)
            # Ensure annotations are stored
            asyncio.sleep(2)

            # validate the folder againt the JSON SCHEMA
            await created_folder.validate_entity_with_json_schema_async(
                synapse_client=self.syn
            )

            # Get validation statistics of the folder
            response = await created_folder.get_json_schema_validation_statistics_async(
                synapse_client=self.syn
            )
            assert response.number_of_valid_children == 1
            assert response.number_of_invalid_children == 1
        finally:
            # Clean up the JSON schema binding
            await created_folder.delete_json_schema_from_entity_async(
                synapse_client=self.syn
            )

    async def test_get_invalid_json_schema_validation_async(
        self,
        folder: Folder,
        project_model: Project,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test retrieving invalid JSON schema validation results for a folder."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)
        test_org, test_product_schema_uri = create_test_organization_with_schema

        # Create two files under the folder
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        file_1 = await File(
            path=filename,
            name="test_file_1",
            description=DESCRIPTION_FILE,
            content_type=CONTENT_TYPE_FILE,
            version_comment=VERSION_COMMENT,
            version_label=str(uuid.uuid4()),
            parent_id=folder.id,
        ).store_async(synapse_client=self.syn)

        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        file_2 = await File(
            path=filename,
            name="test_file_2",
            description=DESCRIPTION_FILE,
            content_type=CONTENT_TYPE_FILE,
            version_comment=VERSION_COMMENT,
            version_label=str(uuid.uuid4()),
            parent_id=folder.id,
        ).store_async(synapse_client=self.syn)

        # Bind the JSON SCHEMA to the folder
        try:
            await created_folder.bind_json_schema_to_entity_async(
                json_schema_uri=test_product_schema_uri,
                synapse_client=self.syn,
            )

            # Store annotations in the file, one valid and one invalid
            file_1.annotations = {
                "productDescription": "test description here.",
                "productQuantity": 100,
            }
            file_2.annotations = {
                "productDescription": 200,
                "productQuantity": "invalid string",
            }

            await file_1.store_async(parent=folder)
            await file_2.store_async(parent=folder)
            # Ensure annotations are stored
            asyncio.sleep(2)

            # Get invalid validation results of the folder
            # The generator `gen` yields validation results for entities that failed JSON schema validation.
            # Each item in `gen` is expected to be a dictionary containing details about the validation failure.
            gen = created_folder.get_invalid_json_schema_validation_async(
                synapse_client=self.syn
            )
            async for item in gen:
                validation_response = item.validation_response
                validation_error_message = item.validation_error_message
                validation_exception = item.validation_exception
                causing_exceptions = validation_exception.causing_exceptions

                assert validation_response.object_type == "entity"
                assert validation_response.object_etag is not None
                assert (
                    validation_response.id
                    == f"https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/{test_org.name}-{TEST_SCHEMA_NAME}-{SCHEMA_VERSION}"
                )
                assert validation_response.is_valid == False
                assert validation_exception.message == "2 schema violations found"

                assert validation_error_message == "2 schema violations found"
                # Assert the number of violations
                assert len(causing_exceptions) == 2

                # Assert both expected violations are present
                assert any(
                    exc.pointer_to_violation == "#/productQuantity"
                    and exc.message == "expected type: Integer, found: String"
                    for exc in causing_exceptions
                )

                assert any(
                    exc.pointer_to_violation == "#/productDescription"
                    and exc.message == "expected type: String, found: Long"
                    for exc in causing_exceptions
                )
        finally:
            # Clean up the JSON schema binding
            await created_folder.delete_json_schema_from_entity_async(
                synapse_client=self.syn
            )
