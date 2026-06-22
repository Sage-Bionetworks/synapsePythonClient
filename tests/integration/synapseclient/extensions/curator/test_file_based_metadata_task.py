"""Integration tests for create_file_based_metadata_task."""

from typing import Any, Callable

import pytest

from synapseclient import Synapse
from synapseclient.extensions.curator.file_based_metadata_task import (
    create_file_based_metadata_task,
)
from synapseclient.models import CurationTask, EntityView, Folder


class TestCreateFileBasedMetadataTask:
    """Integration tests for create_file_based_metadata_task."""

    def test_create_file_based_metadata_task(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[[Any], None],
        request: pytest.FixtureRequest,
        patient_schema_uri: str,
        folder: Folder,
        unique_name: Callable[[], str],
    ) -> None:
        """Test successful creation of CurationTask and EntityView"""
        # GIVEN a folder, a Patient JSON schema, and a unique task name
        test_name = unique_name()

        # WHEN a file-based metadata task is created for the folder
        entity_view_id, task_id = create_file_based_metadata_task(
            folder_id=folder.id,
            curation_task_name=test_name,
            instructions="Contribute Patient data.",
            entity_view_name=test_name,
            schema_uri=patient_schema_uri,
            synapse_client=syn,
        )
        schedule_for_cleanup(entity_view_id)

        # The CurationTask is not a deletable entity via syn.delete, so it keeps a
        # dedicated finalizer. The EntityView it points at is cleaned up via
        # schedule_for_cleanup above, so delete_source is not needed here.
        request.addfinalizer(
            lambda: CurationTask(task_id=task_id).delete(synapse_client=syn)
        )

        # THEN both the entity view and the curation task were created
        assert entity_view_id is not None
        assert task_id is not None

        # AND the entity view exists in Synapse with the expected name
        view = EntityView(id=entity_view_id).get(
            include_columns=True, synapse_client=syn
        )
        assert view.id == entity_view_id
        assert view.name == test_name

        # AND the curation task exists in Synapse pointing at that view
        task = CurationTask(task_id=task_id).get(synapse_client=syn)
        assert task.task_id == task_id
        assert task.data_type == test_name
        assert task.task_properties.file_view_id == entity_view_id

        # AND the leading columns are ordered with "name" first
        column_names = list(view.columns.keys())
        assert column_names[:3] == ["id", "name", "createdBy"]

    def test_create_file_based_metadata_task_return_entities(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[[Any], None],
        request: pytest.FixtureRequest,
        patient_schema_uri: str,
        folder: Folder,
        unique_name: Callable[[], str],
    ) -> None:
        """Test that return_entities=True returns the EntityView and CurationTask objects"""
        # GIVEN a folder, a Patient JSON schema, and a unique task name
        test_name = unique_name()

        # WHEN a file-based metadata task is created with return_entities=True
        entity_view, curation_task = create_file_based_metadata_task(
            folder_id=folder.id,
            curation_task_name=test_name,
            instructions="Contribute Patient data.",
            entity_view_name=test_name,
            schema_uri=patient_schema_uri,
            return_entities=True,
            synapse_client=syn,
        )
        schedule_for_cleanup(entity_view.id)

        # The CurationTask is not a deletable entity via syn.delete, so it keeps a
        # dedicated finalizer. The EntityView it points at is cleaned up via
        # schedule_for_cleanup above, so delete_source is not needed here.
        request.addfinalizer(
            lambda: CurationTask(task_id=curation_task.task_id).delete(
                synapse_client=syn
            )
        )

        # THEN the actual EntityView and CurationTask objects are returned
        assert isinstance(entity_view, EntityView)
        assert isinstance(curation_task, CurationTask)

        # AND the returned EntityView matches what was created in Synapse
        assert entity_view.id is not None
        assert entity_view.name == test_name

        # AND the returned CurationTask points at the returned EntityView
        assert curation_task.task_id is not None
        assert curation_task.data_type == test_name
        assert curation_task.task_properties.file_view_id == entity_view.id

        # AND the leading columns are ordered with "name" first
        column_names = list(entity_view.columns.keys())
        assert column_names[:3] == ["id", "name", "createdBy"]
