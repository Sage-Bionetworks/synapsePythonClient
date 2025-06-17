import asyncio
import random
import uuid
from typing import Callable, Generator, Optional, Tuple, Type, Union

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Column,
    ColumnType,
    EntityView,
    File,
    Folder,
    Project,
    Table,
    ViewTypeMask,
)
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

    async def create_entity(
        self,
        entity_type: Type[Union[Project, Folder, File, Table, EntityView]],
        project_model: Optional[Project] = None,
        file_fixture: Optional[File] = None,
        table_fixture: Optional[Table] = None,
        entity_view_fixture: Optional[EntityView] = None,
        name_suffix: str = "",
    ) -> Union[Project, Folder, File, Table, EntityView]:
        """Helper to create different entity types with consistent naming"""
        entity_name = str(uuid.uuid4()) + name_suffix

        if entity_type == Project:
            entity = await Project(name=entity_name).store_async()
        elif entity_type == Folder:
            folder = Folder(name=entity_name)
            entity = await folder.store_async(parent=project_model)
        elif entity_type == File:
            file_fixture.name = entity_name
            entity = await file_fixture.store_async(parent=project_model)
        elif entity_type == Table:
            table_fixture.name = entity_name
            entity = await table_fixture.store_async()
        elif entity_type == EntityView:
            entity = await entity_view_fixture.store_async()
        else:
            raise ValueError(f"Unsupported entity type: {entity_type}")

        self.schedule_for_cleanup(entity.id)
        return entity

    @pytest.fixture(scope="function")
    def create_test_organization_with_schema(
        self, syn: Synapse
    ) -> Generator[Tuple[JsonSchemaOrganization, str], None, None]:
        """Create a test organization for JSON schema functionality."""
        org_name = "dpetest" + uuid.uuid4().hex[:6]

        js = syn.service("json_schema")
        created_org = js.create_organization(org_name)

        # Add a JSON schema
        try:
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
            created_schema = test_org.create_json_schema(
                schema, TEST_SCHEMA_NAME, "0.0.1"
            )
            yield test_org, created_schema.uri
        finally:
            js.delete_json_schema(created_schema.uri)
            js.delete_organization(created_org["id"])

    @pytest.fixture(scope="function")
    def file(self) -> File:
        filename = utils.make_bogus_uuid_file()
        return File(path=filename)

    @pytest.fixture(scope="function")
    def table(self, project_model: Project) -> Table:
        columns = [
            Column(id=None, name="my_string_col", column_type=ColumnType.STRING),
        ]
        table = Table(
            columns=columns,
            parent_id=project_model.id,
        )
        return table

    @pytest.fixture(scope="function")
    async def entity_view(self, project_model: Project) -> EntityView:
        entity_view = EntityView(
            parent_id=project_model.id,
            scope_ids=[project_model.id],
            view_type_mask=ViewTypeMask.FILE | ViewTypeMask.FOLDER,
        )
        return entity_view

    @pytest.mark.parametrize("entity_type", [Folder, Project, File, EntityView, Table])
    async def test_bind_schema(
        self,
        entity_type: Type[Union[Folder, Project, File, EntityView, Table]],
        project_model: Project,
        file: File,
        table: Table,
        entity_view: EntityView,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test binding a JSON schema to a folder entity."""
        created_entity = await self.create_entity(
            entity_type,
            project_model,
            file_fixture=file,
            table_fixture=table,
            entity_view_fixture=entity_view,
            name_suffix="_test_json_schema_bind_async",
        )

        # Bind the JSON schema to the entity
        test_org, test_product_schema_uri = create_test_organization_with_schema
        try:
            response = await created_entity.bind_schema_async(
                json_schema_uri=test_product_schema_uri, synapse_client=self.syn
            )
            json_schema_version_info = response.json_schema_version_info
            assert json_schema_version_info.organization_name == test_org.name
            assert json_schema_version_info.id == test_product_schema_uri
        finally:
            # Clean up the JSON schema binding
            await created_entity.delete_schema_async(synapse_client=self.syn)

    @pytest.mark.parametrize("entity_type", [Folder, Project, File, EntityView, Table])
    async def test_get_schema_async(
        self,
        entity_type: Type[Union[Folder, Project, File, EntityView, Table]],
        project_model: Project,
        file: File,
        table: Table,
        entity_view: EntityView,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test retrieving a bound JSON schema from an entity."""
        created_entity = await self.create_entity(
            entity_type,
            project_model,
            file_fixture=file,
            table_fixture=table,
            entity_view_fixture=entity_view,
            name_suffix="_test_json_schema_get_async",
        )

        # Bind the JSON schema to the folder
        try:
            test_org, test_product_schema_uri = create_test_organization_with_schema
            await created_entity.bind_schema_async(
                json_schema_uri=test_product_schema_uri, synapse_client=self.syn
            )

            # Retrieve the JSON schema from the folder
            response = await created_entity.get_schema_async(synapse_client=self.syn)
            assert response.json_schema_version_info.organization_name == test_org.name
            assert response.json_schema_version_info.id == test_product_schema_uri
        finally:
            # Clean up the JSON schema binding
            await created_entity.delete_schema_async(synapse_client=self.syn)

    @pytest.mark.parametrize("entity_type", [Folder, Project, File, EntityView, Table])
    async def test_delete_schema_async(
        self,
        entity_type: Type[Union[Folder, Project, File, EntityView, Table]],
        project_model: Project,
        file: File,
        table: Table,
        entity_view: EntityView,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test deleting a bound JSON schema from an entity."""
        # Create the folder
        created_entity = await self.create_entity(
            entity_type,
            project_model,
            file_fixture=file,
            table_fixture=table,
            entity_view_fixture=entity_view,
            name_suffix="_test_json_schema_delete_async",
        )

        # Bind the JSON schema to the folder
        _, test_product_schema_uri = create_test_organization_with_schema
        await created_entity.bind_schema_async(
            json_schema_uri=test_product_schema_uri, synapse_client=self.syn
        )

        # Delete the JSON schema from the folder
        await created_entity.delete_schema_async(synapse_client=self.syn)

        # Verify that the JSON schema is no longer bound
        with pytest.raises(
            SynapseHTTPError,
            match=f"404 Client Error: No JSON schema found for '{created_entity.id}'",
        ):
            await created_entity.get_schema_async(synapse_client=self.syn)

    @pytest.mark.parametrize("entity_type", [Folder, Project, File, EntityView, Table])
    async def test_get_schema_derived_keys_async(
        self,
        entity_type: Type[Union[Folder, Project, File, EntityView, Table]],
        project_model: Project,
        file: File,
        table: Table,
        entity_view: EntityView,
        create_test_organization_with_schema: Tuple[
            JsonSchemaOrganization,
            str,
        ],
    ) -> None:
        """Test retrieving derived keys from a bound JSON schema."""
        created_entity = await self.create_entity(
            entity_type,
            project_model,
            file_fixture=file,
            table_fixture=table,
            entity_view_fixture=entity_view,
            name_suffix="_test_json_schema_derived_keys_async",
        )
        _, test_product_schema_uri = create_test_organization_with_schema

        # Bind the JSON schema to the entity
        try:
            await created_entity.bind_schema_async(
                json_schema_uri=test_product_schema_uri,
                enable_derived_annos=True,
                synapse_client=self.syn,
            )

            # Store annotations
            created_entity.annotations = {
                "productDescription": "test description here",
                "productQuantity": 100,
                "productPrice": 100,
            }

            await created_entity.store_async()

            response = await created_entity.get_schema_async(synapse_client=self.syn)
            assert response.enable_derived_annotations == True

            await asyncio.sleep(2)

            # Retrieve the derived keys from the folder
            response = await created_entity.get_schema_derived_keys_async(
                synapse_client=self.syn
            )

            assert set(response.keys) == {"productId", "productName"}
        finally:
            # Clean up the JSON schema binding
            await created_entity.delete_schema_async(synapse_client=self.syn)

    @pytest.mark.parametrize("entity_type", [Folder, Project, File, EntityView, Table])
    async def test_validate_schema_async_invalid_annos(
        self,
        entity_type: Type[Union[Folder, Project, File, EntityView, Table]],
        project_model: Project,
        file: File,
        table: Table,
        entity_view: EntityView,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test validating invalid annotations against a bound JSON schema."""
        created_entity = await self.create_entity(
            entity_type,
            project_model,
            file_fixture=file,
            table_fixture=table,
            entity_view_fixture=entity_view,
            name_suffix="_test_json_schema_invalid_annos_async",
        )

        _, test_product_schema_uri = create_test_organization_with_schema

        # Bind the JSON schema to the entity
        try:
            await created_entity.bind_schema_async(
                json_schema_uri=test_product_schema_uri,
                synapse_client=self.syn,
            )

            # Store annotations that do not match the schema
            created_entity.annotations = {
                "productDescription": 1000,
                "productQuantity": "invalid string",
            }
            await created_entity.store_async()
            # Ensure annotations are stored
            await asyncio.sleep(2)

            # Validate the folder against the JSON schema
            response = await created_entity.validate_schema_async(
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
            await created_entity.delete_schema_async(synapse_client=self.syn)

    @pytest.mark.parametrize("entity_type", [Folder, Project, File, EntityView, Table])
    async def test_validate_schema_async_valid_annos(
        self,
        entity_type: Type[Union[Folder, Project, File, EntityView, Table]],
        project_model: Project,
        file: File,
        table: Table,
        entity_view: EntityView,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test validating valid annotations against a bound JSON schema."""
        created_entity = await self.create_entity(
            entity_type,
            project_model,
            file_fixture=file,
            table_fixture=table,
            entity_view_fixture=entity_view,
            name_suffix="_test_json_schema_valid_annos_async",
        )
        _, test_product_schema_uri = create_test_organization_with_schema

        # Bind the JSON schema to the folder
        try:
            await created_entity.bind_schema_async(
                json_schema_uri=test_product_schema_uri,
                synapse_client=self.syn,
            )

            # Store annotations that match the schema
            created_entity.annotations = {
                "productDescription": "This is a test product.",
                "productQuantity": 100,
            }
            await created_entity.store_async()
            # Ensure annotations are stored
            await asyncio.sleep(2)
            response = await created_entity.validate_schema_async(
                synapse_client=self.syn
            )
            assert response.is_valid == True
        finally:
            # Clean up the JSON schema binding
            await created_entity.delete_schema_async(synapse_client=self.syn)

    @pytest.mark.parametrize("entity_type", [Folder, Project])
    async def test_get_validation_statistics_async(
        self,
        entity_type: Type[Union[Folder, Project, File, EntityView, Table]],
        project_model: Project,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test retrieving JSON schema validation statistics for a folder."""
        # Create the folder
        created_entity = await self.create_entity(
            entity_type,
            project_model,
            name_suffix="_test_json_schema_validation_statistics_async",
        )

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
            parent_id=created_entity.id,
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
            parent_id=created_entity.id,
        ).store_async(synapse_client=self.syn)

        _, test_product_schema_uri = create_test_organization_with_schema

        # Bind the JSON SCHEMA to the folder
        try:
            await created_entity.bind_schema_async(
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

            await file_1.store_async(parent=created_entity)
            await file_2.store_async(parent=created_entity)
            # Ensure annotations are stored
            await asyncio.sleep(2)

            # validate the entity against the JSON SCHEMA
            await created_entity.validate_schema_async(synapse_client=self.syn)

            # Get validation statistics of the entity
            response = await created_entity.get_schema_validation_statistics_async(
                synapse_client=self.syn
            )
            assert response.number_of_valid_children == 1
            assert response.number_of_invalid_children == 1
        finally:
            # Clean up the JSON schema binding
            await created_entity.delete_schema_async(synapse_client=self.syn)

    @pytest.mark.parametrize("entity_type", [Folder, Project])
    async def test_get_invalid_validation_async(
        self,
        entity_type: Type[Union[Folder, Project]],
        project_model: Project,
        create_test_organization_with_schema: Tuple[JsonSchemaOrganization, str],
    ) -> None:
        """Test retrieving invalid JSON schema validation results for a folder."""
        created_entity = await self.create_entity(
            entity_type, project_model, name_suffix="_test_invalid_json_schema_async"
        )
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
            parent_id=created_entity.id,
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
            parent_id=created_entity.id,
        ).store_async(synapse_client=self.syn)

        # Bind the JSON SCHEMA to the folder
        try:
            await created_entity.bind_schema_async(
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

            await file_1.store_async(parent=created_entity)
            await file_2.store_async(parent=created_entity)
            # Ensure annotations are stored
            await asyncio.sleep(2)

            # Get invalid validation results of the folder
            # The generator `gen` yields validation results for entities that failed JSON schema validation.
            # Each item in `gen` is expected to be a dictionary containing details about the validation failure.
            gen = created_entity.get_invalid_validation_async(synapse_client=self.syn)
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
            await created_entity.delete_schema_async(synapse_client=self.syn)
