"""Integration tests for create_record_based_metadata_task."""

from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.extensions.curator.record_based_metadata_task import (
    create_record_based_metadata_task,
)
from synapseclient.models import Folder, Project


class TestCreateRecordBasedMetadataTask:
    """Integration tests for create_record_based_metadata_task."""

    def test_creates_single_record_set_version(
        self,
        syn: Synapse,
        project: Project,
        request: pytest.FixtureRequest,
        patient_schema_uri: str,
        folder: Folder,
        unique_name: Callable[[], str],
    ):
        """
        The Grid created during bootstrap is initialized from the RecordSet's
        CSV with no edits, so exporting that Grid back to the RecordSet (the
        reported bug) writes the same content as a duplicate v2.
        """
        test_name = unique_name()
        upsert_keys = ["PatientID"]
        instructions = "Contribute Patient data."

        record_set, curation_task, grid = create_record_based_metadata_task(
            folder_id=folder.id,
            record_set_name=test_name,
            record_set_description=test_name,
            curation_task_name=test_name,
            upsert_keys=upsert_keys,
            instructions=instructions,
            schema_uri=patient_schema_uri,
            synapse_client=syn,
        )

        def cleanup():
            curation_task.delete(synapse_client=syn)
            grid.delete(synapse_client=syn)
            record_set.unbind_schema(synapse_client=syn)
            record_set.delete(synapse_client=syn)

        request.addfinalizer(cleanup)

        from synapseclient.models import RecordSet

        record_set = RecordSet(id=record_set.id).get(synapse_client=syn)

        assert grid.record_set_id == record_set.id
        assert grid.grid_json_schema_id == patient_schema_uri

        assert record_set.upsert_keys == upsert_keys
        assert record_set.version_number == 1
        assert record_set.parent_id == folder.id
        assert record_set.name == test_name
        assert record_set.description == test_name

        assert curation_task.data_type == test_name
        assert curation_task.project_id == project.id
        assert curation_task.instructions == instructions
        assert curation_task.task_properties.record_set_id == record_set.id
