import uuid
from unittest.mock import patch
import pytest
from synapseclient.models import File, Project, FailureStrategy
from synapseclient import Project as Synapse_Project, Synapse
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.core.constants.concrete_types import FILE_ENTITY

PROJECT_ID = "syn123"
DERSCRIPTION_PROJECT = "This is an example project."
PARENT_ID = "parent_id_value"
PROJECT_NAME = "example_project"
ETAG = "etag_value"
CREATED_ON = "createdOn_value"
MODIFIED_ON = "modifiedOn_value"
CREATED_BY = "createdBy_value"
MODIFIED_BY = "modifiedBy_value"


class TestProject:
    """Tests for the synapseclient.models.Project class."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def get_example_synapse_project_output(self) -> Synapse_Project:
        return Synapse_Project(
            id=PROJECT_ID,
            name=PROJECT_NAME,
            parentId=PARENT_ID,
            description=DERSCRIPTION_PROJECT,
            etag=ETAG,
            createdOn=CREATED_ON,
            modifiedOn=MODIFIED_ON,
            createdBy=CREATED_BY,
            modifiedBy=MODIFIED_BY,
        )

    def test_fill_from_dict(self) -> None:
        # GIVEN an example Synapse Project `get_example_synapse_project_output`
        # WHEN I call `fill_from_dict` with the example Synapse Project
        project_output = Project().fill_from_dict(
            self.get_example_synapse_project_output()
        )

        # THEN the Project object should be filled with the example Synapse Project
        assert project_output.id == PROJECT_ID
        assert project_output.name == PROJECT_NAME
        assert project_output.parent_id == PARENT_ID
        assert project_output.description == DERSCRIPTION_PROJECT
        assert project_output.etag == ETAG
        assert project_output.created_on == CREATED_ON
        assert project_output.modified_on == MODIFIED_ON
        assert project_output.created_by == CREATED_BY
        assert project_output.modified_by == MODIFIED_BY

    @pytest.mark.asyncio
    async def test_store_with_id(self) -> None:
        # GIVEN a Project object
        project = Project(
            id=PROJECT_ID,
        )

        # AND a random description
        description = str(uuid.uuid4())
        project.description = description

        # WHEN I call `store` with the Project object
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_project_output()),
        ) as mocked_client_call, patch.object(
            self.syn,
            "get",
            return_value=Synapse_Project(
                id=project.id,
            ),
        ) as mocked_get:
            result = await project.store()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_Project(
                    id=project.id,
                    description=description,
                ),
                set_annotations=False,
                createOrUpdate=False,
            )

            # AND we should call the get method
            mocked_get.assert_called_once()

            # AND the project should be stored with the mock return data
            assert result.id == PROJECT_ID
            assert result.name == PROJECT_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DERSCRIPTION_PROJECT
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    @pytest.mark.asyncio
    async def test_store_with_no_changes(self) -> None:
        # GIVEN a Project object
        project = Project(
            id=PROJECT_ID,
        )

        # WHEN I call `store` with the Project object
        with patch.object(
            self.syn,
            "store",
        ) as mocked_store, patch.object(
            self.syn,
            "get",
            return_value=Synapse_Project(
                id=project.id,
            ),
        ) as mocked_get:
            result = await project.store()

            # THEN we should not call store because there are no changes
            mocked_store.assert_not_called()

            # AND we should call the get method
            mocked_get.assert_called_once()

            # AND the project should only contain the ID
            assert result.id == PROJECT_ID

    @pytest.mark.asyncio
    async def test_store_after_get(self) -> None:
        # GIVEN a Project object
        project = Project(
            id=PROJECT_ID,
        )

        # AND I call `get` on the Project object
        with patch.object(
            self.syn,
            "get",
            return_value=Synapse_Project(
                id=project.id,
            ),
        ) as mocked_get:
            await project.get()

            mocked_get.assert_called_once_with(
                entity=project.id,
            )
            assert project.id == PROJECT_ID

        # WHEN I call `store` with the Project object
        with patch.object(
            self.syn,
            "store",
        ) as mocked_store, patch.object(
            self.syn,
            "get",
            return_value=Synapse_Project(
                id=project.id,
            ),
        ) as mocked_get:
            result = await project.store()

            # THEN we should not call store because there are no changes
            mocked_store.assert_not_called()

            # AND we should not call get as we already have
            mocked_get.assert_not_called()

            # AND the project should only contain the ID
            assert result.id == PROJECT_ID

    @pytest.mark.asyncio
    async def test_store_after_get_with_changes(self) -> None:
        # GIVEN a Project object
        project = Project(
            id=PROJECT_ID,
        )

        # AND I call `get` on the Project object
        with patch.object(
            self.syn,
            "get",
            return_value=Synapse_Project(
                id=project.id,
            ),
        ) as mocked_get:
            await project.get()

            mocked_get.assert_called_once_with(
                entity=project.id,
            )
            assert project.id == PROJECT_ID

        # AND I update a field on the project
        description = str(uuid.uuid4())
        project.description = description

        # WHEN I call `store` with the Project object
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_project_output()),
        ) as mocked_store, patch.object(
            self.syn,
            "get",
        ) as mocked_get:
            result = await project.store()

            # THEN we should  call store because there are changes
            mocked_store.assert_called_once_with(
                obj=Synapse_Project(
                    id=project.id,
                    description=description,
                ),
                set_annotations=False,
                createOrUpdate=False,
            )

            # AND we should not call get as we already have
            mocked_get.assert_not_called()

            # AND the project should contained the mocked store return data
            assert result.id == PROJECT_ID
            assert result.name == PROJECT_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DERSCRIPTION_PROJECT
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    @pytest.mark.asyncio
    async def test_store_with_annotations(self) -> None:
        # GIVEN a Project object
        project = Project(
            id=PROJECT_ID,
            annotations={
                "my_single_key_string": ["a"],
                "my_key_string": ["b", "a", "c"],
                "my_key_bool": [False, False, False],
                "my_key_double": [1.2, 3.4, 5.6],
                "my_key_long": [1, 2, 3],
            },
        )

        # AND a random description
        description = str(uuid.uuid4())
        project.description = description

        # WHEN I call `store` with the Project object
        with patch(
            "synapseclient.models.project.store_entity_components",
            return_value=(None),
        ) as mocked_store_entity_components, patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_project_output()),
        ) as mocked_client_call, patch.object(
            self.syn,
            "get",
            return_value=Synapse_Project(
                id=project.id,
            ),
        ) as mocked_get:
            result = await project.store()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_Project(
                    id=project.id,
                    description=description,
                ),
                set_annotations=False,
                createOrUpdate=False,
            )

            # AND we should call the get method
            mocked_get.assert_called_once()

            # AND we should store the annotations component
            mocked_store_entity_components.assert_called_once_with(
                root_resource=project,
                failure_strategy=FailureStrategy.LOG_EXCEPTION,
                synapse_client=None,
            )

            # AND the project should be stored with the mock return data
            assert result.id == PROJECT_ID
            assert result.name == PROJECT_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DERSCRIPTION_PROJECT
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    @pytest.mark.asyncio
    async def test_store_with_name_and_parent_id(self) -> None:
        # GIVEN a Project object
        project = Project(
            name=PROJECT_NAME,
            parent_id=PARENT_ID,
        )

        # AND a random description
        description = str(uuid.uuid4())
        project.description = description

        # WHEN I call `store` with the Project object
        with patch.object(
            self.syn,
            "store",
            return_value=(self.get_example_synapse_project_output()),
        ) as mocked_client_call, patch.object(
            self.syn,
            "findEntityId",
            return_value=PROJECT_ID,
        ) as mocked_get, patch.object(
            self.syn,
            "get",
            return_value=Synapse_Project(
                id=project.id,
            ),
        ) as mocked_get:
            result = await project.store()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=Synapse_Project(
                    id=project.id,
                    name=project.name,
                    parent=project.parent_id,
                    description=description,
                ),
                set_annotations=False,
                createOrUpdate=False,
            )

            # AND we should call the get method
            mocked_get.assert_called_once()

            # AND findEntityId should be called
            mocked_get.assert_called_once()

            # AND the project should be stored
            assert result.id == PROJECT_ID
            assert result.name == PROJECT_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DERSCRIPTION_PROJECT
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    @pytest.mark.asyncio
    async def test_store_no_id_or_name(self) -> None:
        # GIVEN a Project object
        project = Project(parent_id=PARENT_ID)

        # WHEN I call `store` with the Project object
        with pytest.raises(ValueError) as e:
            await project.store()

        # THEN we should get an error
        assert str(e.value) == "Project ID or Name is required"

    @pytest.mark.asyncio
    async def test_get_by_id(self) -> None:
        # GIVEN a Project object
        project = Project(
            id=PROJECT_ID,
        )

        # WHEN I call `get` with the Project object
        with patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_project_output()),
        ) as mocked_client_call:
            result = await project.get()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity=project.id,
            )

            # AND the project should be stored
            assert result.id == PROJECT_ID
            assert result.name == PROJECT_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DERSCRIPTION_PROJECT
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    @pytest.mark.asyncio
    async def test_get_by_name_and_parent(self) -> None:
        # GIVEN a Project object
        project = Project(
            name=PROJECT_NAME,
            parent_id=PARENT_ID,
        )

        # WHEN I call `get` with the Project object
        with patch.object(
            self.syn,
            "findEntityId",
            return_value=(PROJECT_ID),
        ) as mocked_client_search, patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_project_output()),
        ) as mocked_client_call:
            result = await project.get()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                entity=project.id,
            )

            # AND we should search for the entity
            mocked_client_search.assert_called_once_with(
                name=project.name,
                parent=project.parent_id,
            )

            # AND the project should be stored
            assert result.id == PROJECT_ID
            assert result.name == PROJECT_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DERSCRIPTION_PROJECT
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY

    @pytest.mark.asyncio
    async def test_get_by_name_and_parent_not_found(self) -> None:
        # GIVEN a Project object
        project = Project(
            name=PROJECT_NAME,
            parent_id=PARENT_ID,
        )

        # WHEN I call `get` with the Project object
        with patch.object(
            self.syn,
            "findEntityId",
            return_value=(None),
        ) as mocked_client_search:
            with pytest.raises(SynapseNotFoundError) as e:
                await project.get()
            assert (
                str(e.value)
                == "Project [Id: None, Name: example_project, Parent: parent_id_value] not found in Synapse."
            )

            mocked_client_search.assert_called_once_with(
                name=project.name,
                parent=project.parent_id,
            )

    @pytest.mark.asyncio
    async def test_delete_with_id(self) -> None:
        # GIVEN a Project object
        project = Project(
            id=PROJECT_ID,
        )

        # WHEN I call `delete` with the Project object
        with patch.object(
            self.syn,
            "delete",
            return_value=(None),
        ) as mocked_client_call:
            await project.delete()

            # THEN we should call the method with this data
            mocked_client_call.assert_called_once_with(
                obj=project.id,
            )

    @pytest.mark.asyncio
    async def test_delete_missing_id(self) -> None:
        # GIVEN a Project object
        project = Project()

        # WHEN I call `delete` with the Project object
        with pytest.raises(ValueError) as e:
            await project.delete()

        # THEN we should get an error
        assert str(e.value) == "Entity ID or Name/Parent is required"

    @pytest.mark.asyncio
    async def test_copy(self) -> None:
        # GIVEN a Project object
        project = Project(
            id=PROJECT_ID,
        )

        # AND a returned Project object
        returned_project = Project(id="syn456")

        # AND a copy mapping exists
        copy_mapping = {
            PROJECT_ID: "syn456",
        }

        # WHEN I call `copy` with the Project object
        with patch(
            "synapseclient.models.project.copy",
            return_value=(copy_mapping),
        ) as mocked_copy, patch(
            "synapseclient.models.project.Project.get",
            return_value=(returned_project),
        ) as mocked_get, patch(
            "synapseclient.models.project.Project.sync_from_synapse",
            return_value=(returned_project),
        ) as mocked_sync:
            result = await project.copy(destination_id="destination_id")

            # THEN we should call the method with this data
            mocked_copy.assert_called_once_with(
                syn=self.syn,
                entity=project.id,
                destinationId="destination_id",
                excludeTypes=[],
                skipCopyAnnotations=False,
                skipCopyWikiPage=False,
                updateExisting=False,
                setProvenance="traceback",
            )

            # AND we should call the get method
            mocked_get.assert_called_once()

            # AND we should call the sync method
            mocked_sync.assert_called_once_with(
                download_file=False,
                synapse_client=None,
            )

            # AND the file should be stored
            assert result.id == "syn456"

    @pytest.mark.asyncio
    async def test_copy_missing_id(self) -> None:
        # GIVEN a Project object
        project = Project()

        # WHEN I call `copy` with the Project object
        with pytest.raises(ValueError) as e:
            await project.copy(destination_id="destination_id")

        # THEN we should get an error
        assert str(e.value) == "The project must have an ID and destination_id to copy."

    @pytest.mark.asyncio
    async def test_copy_missing_destination(self) -> None:
        # GIVEN a Project object
        project = Project(id=PROJECT_ID)

        # WHEN I call `copy` with the Project object
        with pytest.raises(ValueError) as e:
            await project.copy(destination_id=None)

        # THEN we should get an error
        assert str(e.value) == "The project must have an ID and destination_id to copy."

    @pytest.mark.asyncio
    async def test_sync_from_synapse(self) -> None:
        # GIVEN a Project object
        project = Project(
            id=PROJECT_ID,
        )

        # AND Children that exist on the project in Synapse
        children = [
            {
                "id": "syn456",
                "type": FILE_ENTITY,
                "name": "example_file_1",
            }
        ]

        # WHEN I call `sync_from_synapse` with the Project object
        with patch.object(
            self.syn,
            "getChildren",
            return_value=(children),
        ) as mocked_children_call, patch.object(
            self.syn,
            "get",
            return_value=(self.get_example_synapse_project_output()),
        ) as mocked_project_get, patch(
            "synapseclient.models.file.File.get",
            return_value=(File(id="syn456", name="example_file_1")),
        ):
            result = await project.sync_from_synapse()

            # THEN we should call the method with this data
            mocked_children_call.assert_called_once()

            # AND we should call the get method
            mocked_project_get.assert_called_once()

            # AND the file/project should be retrieved
            assert result.id == PROJECT_ID
            assert result.name == PROJECT_NAME
            assert result.parent_id == PARENT_ID
            assert result.description == DERSCRIPTION_PROJECT
            assert result.etag == ETAG
            assert result.created_on == CREATED_ON
            assert result.modified_on == MODIFIED_ON
            assert result.created_by == CREATED_BY
            assert result.modified_by == MODIFIED_BY
            assert result.files[0].id == "syn456"
            assert result.files[0].name == "example_file_1"
