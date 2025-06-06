import uuid
from time import sleep
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Type

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import File, Folder, Project

DESCRIPTION_FOLDER = "This is a folder for testing JSON schema functionality."
DESCRIPTION_FILE = "This is an example file."
CONTENT_TYPE_FILE = "text/plain"
VERSION_COMMENT = "My version comment"


class TestJSONSchema:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup
        self.org_name = "dpetest"
        self.product_schema_name = "example.dpetest.jsonschema"
        self.patient_schema_name = "test.schematic.Patient"
        self.product_schema_version = "0.0.2"
        self.patient_schema_version = "0.0.1"

    @pytest.fixture(autouse=True, scope="function")
    def folder(self) -> Folder:
        folder = Folder(name=str(uuid.uuid4()), description=DESCRIPTION_FOLDER)
        return folder

    @pytest.fixture(autouse=True, scope="function")
    def test_patient_schema_uri(self):
        js = self.syn.service("json_schema")
        org = js.JsonSchemaOrganization(self.org_name)
        test_patient_schema = js.JsonSchema(org, self.patient_schema_name)
        test_patient_schema_uri = (
            self.org_name
            + "-"
            + test_patient_schema.name
            + "-"
            + self.patient_schema_version
        )

        return test_patient_schema_uri

    @pytest.fixture(autouse=True, scope="function")
    def test_product_schema_uri(self):
        js = self.syn.service("json_schema")
        org = js.JsonSchemaOrganization(self.org_name)
        test_product_schema = js.JsonSchema(org, self.product_schema_name)
        test_product_schema_uri = (
            self.org_name
            + "-"
            + test_product_schema.name
            + "-"
            + self.product_schema_version
        )
        return test_product_schema_uri

    async def test_bind_json_schema_to_entity_async(
        self, folder: Folder, test_patient_schema_uri, project_model: Project
    ) -> None:
        """Test binding a JSON schema to a folder entity."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # Bind the JSON schema to the folder
        response = await created_folder.bind_json_schema_to_entity_async(
            json_schema_uri=test_patient_schema_uri, synapse_client=self.syn
        )
        assert response["jsonSchemaVersionInfo"]["organizationName"] == self.org_name
        assert response["jsonSchemaVersionInfo"]["$id"] == test_patient_schema_uri

    async def test_get_json_schema_from_entity_async(
        self, folder: Folder, project_model: Project, test_patient_schema_uri: str
    ) -> None:
        """Test retrieving a bound JSON schema from an entity."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # Bind the JSON schema to the folder
        await created_folder.bind_json_schema_to_entity_async(
            json_schema_uri=test_patient_schema_uri, synapse_client=self.syn
        )

        # Retrieve the JSON schema from the folder
        response = await created_folder.get_json_schema_from_entity_async(
            synapse_client=self.syn
        )
        assert response["jsonSchemaVersionInfo"]["organizationName"] == self.org_name
        assert response["jsonSchemaVersionInfo"]["$id"] == test_patient_schema_uri

    async def test_delete_json_schema_from_entity_async(
        self, folder: Folder, project_model: Project, test_patient_schema_uri: str
    ) -> None:
        """Test deleting a bound JSON schema from an entity."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # Bind the JSON schema to the folder
        await created_folder.bind_json_schema_to_entity_async(
            json_schema_uri=test_patient_schema_uri, synapse_client=self.syn
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
        self, folder: Folder, project_model: Project, test_product_schema_uri: str
    ) -> None:
        """Test retrieving derived keys from a bound JSON schema."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # Bind the JSON schema to the folder
        await created_folder.bind_json_schema_to_entity_async(
            json_schema_uri=test_product_schema_uri,
            enable_derived_annos=True,
            synapse_client=self.syn,
        )

        # Store annotations
        created_folder.annotations = {
            "productPrice": 10,
        }
        stored_folder = await folder.store_async(parent=project_model)
        response = await created_folder.get_json_schema_from_entity_async(
            synapse_client=self.syn
        )
        assert response["enableDerivedAnnotations"] == True

        # Retrieve the derived keys from the folder
        response = await stored_folder.get_json_schema_derived_keys_async(
            synapse_client=self.syn
        )
        assert set(response["keys"]) == {"productId", "productName"}

    async def test_validate_entity_with_json_schema_async_invalid_annos(
        self, folder: Folder, project_model: Project, test_patient_schema_uri: str
    ) -> None:
        """Test validating invalid annotations against a bound JSON schema."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # Bind the JSON schema to the folder
        await created_folder.bind_json_schema_to_entity_async(
            json_schema_uri=test_patient_schema_uri,
            synapse_client=self.syn,
        )

        # Store annotations that do not match the schema
        created_folder.annotations = {
            "Sex": "Male",
            "Diagnosis": "Healthy",
            "Component": "Component",
        }
        await folder.store_async(parent=project_model)
        sleep(2)

        # Validate the folder against the JSON schema
        response = await created_folder.validate_entity_with_json_schema_async(
            synapse_client=self.syn
        )
        assert response["isValid"] == False
        assert (
            "#: required key [Patient ID] not found"
            in response["allValidationMessages"]
        )

    async def test_validate_entity_with_json_schema_async_valid_annos(
        self, folder: Folder, project_model: Project, test_product_schema_uri: str
    ) -> None:
        """Test validating valid annotations against a bound JSON schema."""
        # Create the folder
        created_folder = await folder.store_async(parent=project_model)
        self.schedule_for_cleanup(folder.id)

        # Bind the JSON schema to the folder
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
        sleep(2)
        response = await created_folder.validate_entity_with_json_schema_async(
            synapse_client=self.syn
        )
        assert response["isValid"] == True

    async def test_get_json_schema_validation_statistics_async(
        self, folder: Folder, project_model: Project, test_product_schema_uri: str
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

        # Bind the JSON SCHEMA to the folder
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
        sleep(2)

        # validate the folder againt the JSON SCHEMA
        await created_folder.validate_entity_with_json_schema_async(
            synapse_client=self.syn
        )

        # Get validation statistics of the folder
        response = await created_folder.get_json_schema_validation_statistics_async(
            synapse_client=self.syn
        )
        assert response["numberOfValidChildren"] == 1
        assert response["numberOfInvalidChildren"] == 1

    async def test_get_invalid_json_schema_validation_async(
        self, folder: Folder, project_model: Project, test_product_schema_uri: str
    ) -> None:
        """Test retrieving invalid JSON schema validation results for a folder."""
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

        # Bind the JSON SCHEMA to the folder
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
        sleep(2)

        # Get invalid validation results of the folder
        gen = created_folder.get_invalid_json_schema_validation_async(
            synapse_client=self.syn
        )

        async for item in gen:
            assert item["objectType"] == "entity"
            assert item["objectEtag"] is not None
            assert (
                item["schema$id"]
                == f"https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/{self.org_name}-{self.product_schema_name}-{self.product_schema_version}"
            )
            assert item["isValid"] == False
            assert item["validationException"]["message"] == "2 schema violations found"
            causing_exceptions = item["validationException"]["causingExceptions"]

            # Assert the number of violations
            assert len(causing_exceptions) == 2

            # Assert both expected violations are present
            assert any(
                exc["pointerToViolation"] == "#/productQuantity"
                and exc["message"] == "expected type: Integer, found: String"
                for exc in causing_exceptions
            )

            assert any(
                exc["pointerToViolation"] == "#/productDescription"
                and exc["message"] == "expected type: String, found: Long"
                for exc in causing_exceptions
            )
