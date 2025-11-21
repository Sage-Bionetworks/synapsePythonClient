"""Integration tests for sync functions."""

import datetime
import os
import tempfile
import uuid
from typing import Callable

import pandas as pd
import pytest

import synapseclient.core.utils as utils
import synapseutils
from synapseclient import Activity
from synapseclient import File as SynapseFile
from synapseclient import Folder as SynapseFolder
from synapseclient import Link
from synapseclient import Project as SynapseProject
from synapseclient import Schema, Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import File, Folder, Project

# from unittest import skip


BOGUS_ACTIVITY = "bogus_activity"
BOGUS_DESCRIPTION = "bogus_description"
SYNAPSE_URL = "https://www.synapse.org"
SUB_SYNAPSE_URL = "https://www.asdf.synapse.org"
SEND_MESSAGE = False
MANIFEST_FILE = "SYNAPSE_METADATA_MANIFEST.tsv"

# Manifest columns
PATH_COLUMN = "path"
PARENT_COLUMN = "parent"
PARENT_ATTRIBUTE = "parentId"
USED_COLUMN = "used"
EXECUTED_COLUMN = "executed"
ACTIVITY_NAME_COLUMN = "activityName"
ACTIVITY_DESCRIPTION_COLUMN = "activityDescription"
CONTENT_TYPE_COLUMN = "contentType"
ID_COLUMN = "id"
SYNAPSE_STORE_COLUMN = "synapseStore"
NAME_COLUMN = "name"

# Manifest annotations
STR_ANNO = "strAnno"
INT_ANNO = "intAnno"
BOOL_ANNO = "boolAnno"
FLOAT_ANNO = "floatAnno"
ARRAY_ANNO = "arrayAnno"
DATE_ANNO = "dateAnno"
DATETIME_ANNO = "dateTimeAnno"

# Annotation values to set on file
STR_ANNO_VALUE = "str1"
INT_ANNO_VALUE = 1
BOOL_ANNO_VALUE = [True, False]
FLOAT_ANNO_VALUE = 1.1
ARRAY_ANNO_VALUE = ["aa", "bb"]
DATE_ANNO_VALUE = "2001-01-01"
DATETIME_ANNO_VALUE = [
    "2023-12-05 23:37:02.995+00:00",
    "2001-01-01 23:37:02.995+00:00",
]

# Annotation values in manifest
DATE_ANNO_VALUE_IN_MANIFEST = "2001-01-01T00:00:00Z"
ARRAY_ANNO_VALUE_IN_MANIFEST = "[aa,bb]"
DATETIME_ANNO_VALUE_IN_MANIFEST = (
    "[2023-12-05 23:37:02.995+00:00,2001-01-01 23:37:02.995+00:00]"
)
BOOL_ANNO_VALUE_IN_MANIFEST = "[True,False]"

ACTIVITY_NAME = "activityName"
ACTIVITY_DESCRIPTION = "activityDescription"

ETAG = "etag"
MODIFIED_ON = "modifiedOn"


@pytest.fixture(scope="function", autouse=True)
def test_state(syn: Synapse, schedule_for_cleanup: Callable[..., None]):
    class TestState:
        def __init__(self):
            self.syn = syn
            self.project = syn.store(SynapseProject(name=str(uuid.uuid4())))
            self.folder = syn.store(
                SynapseFolder(name=str(uuid.uuid4()), parent=self.project)
            )
            self.schedule_for_cleanup = schedule_for_cleanup

            # Create testfiles for upload
            self.f1 = utils.make_bogus_data_file(n=10)
            self.f2 = utils.make_bogus_data_file(n=10)
            self.f3 = SYNAPSE_URL

            self.header = "path	parent	used	executed	activityName	synapseStore	foo	date_1	datetime_1	datetime_2	datetime_3	multiple_strings	multiple_dates	multiple_bools	multiple_ints	multiple_floats	annotation_with_escaped_commas\n"
            self.row1 = (
                '%s	%s	%s	"%s;https://www.example.com"	provName		bar	2020-01-01	2023-12-04T07:00:00Z	2023-12-05 23:37:02.995+00:00	2023-12-05 07:00:00+00:00	[a, b,c, d]	[2020-01-01,2023-12-04T07:00:00.111Z, 2023-12-05 23:37:02.333+00:00,2023-12-05 07:00:00+00:00]	[fAlSe,False, tRuE,True        ]	[1,2,3,4]	[1.2,3.4,5.6, 7.8]	["my, string with a comma", "another, string with a comma"       ]\n'
                % (
                    self.f1,
                    self.project.id,
                    self.f2,
                    self.f3,
                )
            )
            self.row2 = (
                '%s	%s	"syn12"	" syn123 ;https://www.example.com"	provName2		bar\n'
                % (self.f2, self.folder.id)
            )
            self.row3 = '%s	%s	"syn12.1"		prov2	False	baz\n' % (self.f3, self.folder.id)
            self.row4 = "%s	%s	%s		act		2\n" % (
                self.f3,
                self.project.id,
                self.f1,
            )  # Circular reference
            self.row5 = "%s	syn12					\n" % (self.f3)  # Wrong parent

    test_state = TestState()
    schedule_for_cleanup(test_state.project)
    schedule_for_cleanup(test_state.f1)
    schedule_for_cleanup(test_state.f2)

    return test_state


def _makeManifest(content, schedule_for_cleanup: Callable[..., None]):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".dat", delete=False) as f:
        f.write(content)
        filepath = utils.normalize_path(f.name)
    schedule_for_cleanup(filepath)
    return filepath


# @skip("Skip integration tests for soon to be removed code")
def test_readManifest(test_state):
    """Creates multiple manifests and verifies that they validate correctly"""
    # Test manifest with missing columns
    manifest = _makeManifest(
        '"path"\t"foo"\n#"result_data.txt"\t"syn123"', test_state.schedule_for_cleanup
    )
    pytest.raises(
        ValueError, synapseutils.sync.readManifestFile, test_state.syn, manifest
    )

    # Test that there are no circular references in file and that Provenance is correct
    manifest = _makeManifest(
        test_state.header + test_state.row1 + test_state.row2 + test_state.row4,
        test_state.schedule_for_cleanup,
    )
    pytest.raises(
        RuntimeError, synapseutils.sync.readManifestFile, test_state.syn, manifest
    )

    # Test non existent parent
    manifest = _makeManifest(
        test_state.header + test_state.row1 + test_state.row5,
        test_state.schedule_for_cleanup,
    )
    pytest.raises(
        SynapseHTTPError, synapseutils.sync.readManifestFile, test_state.syn, manifest
    )

    # Test that all files exist in manifest
    manifest = _makeManifest(
        test_state.header
        + test_state.row1
        + test_state.row2
        + "/bara/basdfasdf/8hiuu.txt	syn123\n",
        test_state.schedule_for_cleanup,
    )
    pytest.raises(IOError, synapseutils.sync.readManifestFile, test_state.syn, manifest)


class TestSyncToSynapse:
    """Testing the .syncToSynapse() function"""

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_file_only(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 2 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(2)]
        for file in temp_files:
            schedule_for_cleanup(file)

        # AND A manifest file with the paths to the temp files exists
        df = pd.DataFrame(
            {
                "path": temp_files,
                "parent": folder.id,
                "used": "",
                "executed": "",
                "activityName": "",
                "activityDescription": "",
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 2

        # AND each of the files are the ones we uploaded
        for file in folder.files:
            assert file.path in temp_files

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_files_with_annotations(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 2 temporary files on disk:
        first_temp_file = utils.make_bogus_uuid_file()
        second_temp_file = utils.make_bogus_uuid_file()
        temp_files = [first_temp_file, second_temp_file]
        for file in temp_files:
            schedule_for_cleanup(file)

        # AND A manifest file with the paths to the temp files exists
        df = pd.DataFrame(
            {
                "path": temp_files,
                "parent": folder.id,
                "used": "",
                "executed": "",
                "activityName": "",
                "activityDescription": "",
                "foo": ["bar", "baz"],
                "date_1": ["2020-01-01", ""],
                "datetime_1": ["2023-12-04T07:00:00Z", ""],
                "datetime_2": ["2023-12-05 23:37:02.995+00:00", ""],
                "datetime_3": ["2023-12-05 07:00:00+00:00", ""],
                "multiple_strings": ["[a, b,c, d]", ""],
                "multiple_dates": [
                    "[2020-01-01,2023-12-04T07:00:00.111Z, 2023-12-05 23:37:02.333+00:00,2023-12-05 07:00:00+00:00]",
                    "",
                ],
                "multiple_bools": ["[False,False, tRuE,True]", ""],
                "multiple_ints": ["[1,2,3,4]", ""],
                "multiple_floats": ["[1.2,3.4,5.6, 7.8]", ""],
                "annotation_with_escaped_commas": [
                    '["my, string with a comma", "another, string with a comma"]',
                    "",
                ],
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 2

        # AND each of the files are the ones we uploaded
        for file in folder.files:
            assert file.path in temp_files
            # AND each file has the correct annotations
            if file.path == first_temp_file:
                assert file.annotations["foo"] == ["bar"]
                assert file.annotations["date_1"] == [
                    datetime.datetime(2020, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
                ]
                assert file.annotations["datetime_1"] == [
                    datetime.datetime(2023, 12, 4, 7, 0, tzinfo=datetime.timezone.utc)
                ]
                assert file.annotations["datetime_2"] == [
                    datetime.datetime(
                        2023, 12, 5, 23, 37, 2, 995000, tzinfo=datetime.timezone.utc
                    )
                ]
                assert file.annotations["datetime_3"] == [
                    datetime.datetime(2023, 12, 5, 7, 0, tzinfo=datetime.timezone.utc)
                ]
                assert file.annotations["multiple_strings"] == ["a", "b", "c", "d"]
                assert file.annotations["multiple_dates"] == [
                    datetime.datetime(2020, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
                    datetime.datetime(
                        2023, 12, 4, 7, 0, 0, 111000, tzinfo=datetime.timezone.utc
                    ),
                    datetime.datetime(
                        2023, 12, 5, 23, 37, 2, 333000, tzinfo=datetime.timezone.utc
                    ),
                    datetime.datetime(2023, 12, 5, 7, 0, tzinfo=datetime.timezone.utc),
                ]
                assert file.annotations["multiple_bools"] == [False, False, True, True]
                assert file.annotations["multiple_ints"] == [1, 2, 3, 4]
                assert file.annotations["multiple_floats"] == [1.2, 3.4, 5.6, 7.8]
                assert file.annotations["annotation_with_escaped_commas"] == [
                    "my, string with a comma",
                    "another, string with a comma",
                ]
                assert len(file.annotations) == 11
            else:
                assert file.annotations["foo"] == ["baz"]
                assert len(file.annotations) == 1

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_with_activities(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 2 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(2)]
        for file in temp_files:
            schedule_for_cleanup(file)

        # AND A manifest file with the paths to the temp files exists
        df = pd.DataFrame(
            {
                "path": temp_files,
                "parent": folder.id,
                "used": SYNAPSE_URL,
                "executed": SUB_SYNAPSE_URL,
                "activityName": BOGUS_ACTIVITY,
                "activityDescription": BOGUS_DESCRIPTION,
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 2

        # AND each of the files are the ones we uploaded
        for file in folder.files:
            assert file.path in temp_files
            assert len(file.activity.used) == 1
            assert file.activity.used[0].url == SYNAPSE_URL
            assert len(file.activity.executed) == 1
            assert file.activity.executed[0].url == SUB_SYNAPSE_URL
            assert file.activity.name == BOGUS_ACTIVITY
            assert file.activity.description == BOGUS_DESCRIPTION

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_activities_pointing_to_files(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """Creates a sequence of files that are used by the next file in the sequence.
        Verifies that the files are uploaded to Synapse and that the used files are
        correctly set in the activity of the files.

        Example chain of files:
        file1 <- file2 <- file3
        """
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 3 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(3)]
        for file in temp_files:
            schedule_for_cleanup(file)

        # AND each file used/executed the previous file in the chain
        used_or_executed_files = [""]
        for file in temp_files[1:]:
            used_or_executed_files.append(temp_files[temp_files.index(file) - 1])

        # AND A manifest file with the paths to the temp files exists
        df = pd.DataFrame(
            {
                "path": temp_files,
                "parent": folder.id,
                "used": used_or_executed_files,
                "executed": used_or_executed_files,
                "activityName": BOGUS_ACTIVITY,
                "activityDescription": BOGUS_DESCRIPTION,
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 3

        # AND each of the files are the ones we uploaded
        file_ids = [file.id for file in folder.files]
        for file in folder.files:
            assert file.path in temp_files
            # AND the first file with no activity doesn't have an activity created
            if file.path == temp_files[0]:
                assert file.activity is None
            else:
                # AND the rest of the files have an activity
                assert file.activity.name == BOGUS_ACTIVITY
                assert file.activity.description == BOGUS_DESCRIPTION
                assert len(file.activity.used) == 1
                assert file.activity.used[0].target_id in file_ids
                assert len(file.activity.executed) == 1
                assert file.activity.executed[0].target_id in file_ids

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_activities_added_then_removed_from_manifest(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND temporary file on disk:
        temp_file = utils.make_bogus_uuid_file()
        schedule_for_cleanup(temp_file)

        # AND A manifest file with the paths to the temp files exists
        df = pd.DataFrame(
            {
                "path": [temp_file],
                "parent": folder.id,
                "used": SYNAPSE_URL,
                "executed": "",
                "activityName": BOGUS_ACTIVITY,
                "activityDescription": BOGUS_DESCRIPTION,
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 1

        # AND the file is the one we uploaded
        assert folder.files[0].version_number == 1
        assert folder.files[0].path == temp_file
        assert folder.files[0].activity.name == BOGUS_ACTIVITY
        assert folder.files[0].activity.description == BOGUS_DESCRIPTION
        assert len(folder.files[0].activity.used) == 1
        assert folder.files[0].activity.used[0].url == SYNAPSE_URL

        # WHEN I update the manifest file to remove the activities
        df = pd.DataFrame(
            {
                "path": [temp_file],
                "parent": folder.id,
                "used": "",
                "executed": "",
                "activityName": "",
                "activityDescription": "",
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # AND I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 1

        # AND the file is the one we uploaded
        assert folder.files[0].version_number == 1
        assert folder.files[0].path == temp_file
        # AND the files has an activity
        assert folder.files[0].activity is not None
        assert folder.files[0].activity.name == BOGUS_ACTIVITY
        assert folder.files[0].activity.description == BOGUS_DESCRIPTION
        assert len(folder.files[0].activity.used) == 1
        assert folder.files[0].activity.used[0].url == SYNAPSE_URL

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_activities_added_then_removed_from_manifest_but_copied_to_new_version(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND temporary file on disk:
        temp_file = utils.make_bogus_uuid_file()
        schedule_for_cleanup(temp_file)

        # AND A manifest file with the paths to the temp files exists
        df = pd.DataFrame(
            {
                "path": [temp_file],
                "parent": folder.id,
                "used": SYNAPSE_URL,
                "executed": "",
                "activityName": BOGUS_ACTIVITY,
                "activityDescription": BOGUS_DESCRIPTION,
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 1

        # AND the file is the one we uploaded
        assert folder.files[0].version_number == 1
        assert folder.files[0].path == temp_file
        assert folder.files[0].activity.name == BOGUS_ACTIVITY
        assert folder.files[0].activity.description == BOGUS_DESCRIPTION
        assert len(folder.files[0].activity.used) == 1
        assert folder.files[0].activity.used[0].url == SYNAPSE_URL

        # WHEN I update the content of the file to be uploaded
        with open(temp_file, "wb") as f:
            f.write(b"0")

        # AND I update the manifest file to remove the activities
        df = pd.DataFrame(
            {
                "path": [temp_file],
                "parent": folder.id,
                "used": "",
                "executed": "",
                "activityName": "",
                "activityDescription": "",
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # AND I sync the manifest to Synapse
        synapseutils.syncToSynapse(
            syn,
            file_name,
            sendMessages=SEND_MESSAGE,
            retries=2,
            associate_activity_to_new_version=True,
        )

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 1

        # AND the file is the one we uploaded
        assert folder.files[0].version_number == 2
        assert folder.files[0].path == temp_file
        # AND the file has an activity
        assert folder.files[0].activity is not None
        assert folder.files[0].activity.name == BOGUS_ACTIVITY
        assert folder.files[0].activity.description == BOGUS_DESCRIPTION
        assert len(folder.files[0].activity.used) == 1
        assert folder.files[0].activity.used[0].url == SYNAPSE_URL

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_field_not_available_in_manifest_persisted(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND temporary file on disk:
        temp_file = utils.make_bogus_uuid_file()
        schedule_for_cleanup(temp_file)

        # AND A manifest file with the paths to the temp files exists
        df = pd.DataFrame(
            {
                "path": [temp_file],
                "parent": folder.id,
                "used": SYNAPSE_URL,
                "executed": "",
                "activityName": BOGUS_ACTIVITY,
                "activityDescription": BOGUS_DESCRIPTION,
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 1

        # AND the file is the one we uploaded
        assert folder.files[0].version_number == 1
        assert folder.files[0].path == temp_file
        assert folder.files[0].activity.name == BOGUS_ACTIVITY
        assert folder.files[0].activity.description == BOGUS_DESCRIPTION
        assert len(folder.files[0].activity.used) == 1
        assert folder.files[0].activity.used[0].url == SYNAPSE_URL

        # WHEN I update a metadata field on the File not available in the manifest
        folder.files[0].description = "new file description"
        folder.files[0].store(synapse_client=syn)
        assert folder.files[0].version_number == 2

        # WHEN I update the manifest file to remove the activities
        df = pd.DataFrame(
            {
                "path": [temp_file],
                "parent": folder.id,
                "used": "",
                "executed": "",
                "activityName": "",
                "activityDescription": "",
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # AND I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 1

        # AND the file is the one we uploaded
        assert folder.files[0].version_number == 2
        assert folder.files[0].path == temp_file
        # AND none of the files have an activity
        assert folder.files[0].activity is None
        # AND The metadata field updated is still present
        assert folder.files[0].description == "new file description"

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_activities_added_then_removed_with_version_updates(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND temporary file on disk:
        temp_file = utils.make_bogus_uuid_file()
        schedule_for_cleanup(temp_file)

        # AND A manifest file with the paths to the temp files exists
        df = pd.DataFrame(
            {
                "path": [temp_file],
                "parent": folder.id,
                "used": SYNAPSE_URL,
                "executed": "",
                "activityName": BOGUS_ACTIVITY,
                "activityDescription": BOGUS_DESCRIPTION,
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 1

        # AND the file is the one we uploaded
        assert folder.files[0].path == temp_file
        assert folder.files[0].activity.name == BOGUS_ACTIVITY
        assert folder.files[0].activity.description == BOGUS_DESCRIPTION
        assert len(folder.files[0].activity.used) == 1
        assert folder.files[0].activity.used[0].url == SYNAPSE_URL

        # WHEN I update the manifest file to remove the activities and update a metadata
        # field
        df = pd.DataFrame(
            {
                "path": [temp_file],
                "parent": folder.id,
                "contentType": "text/html",
                "used": "",
                "executed": "",
                "activityName": "",
                "activityDescription": "",
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # AND I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 1

        # AND the file is the one we uploaded
        assert folder.files[0].version_number == 2
        assert folder.files[0].path == temp_file
        # AND none of the files have an activity
        assert folder.files[0].activity is None

        # AND the first version of the file still has the activity
        first_file_version = File(id=folder.files[0].id, version_number=1).get(
            include_activity=True, synapse_client=syn
        )
        assert first_file_version is not None
        assert first_file_version.activity is not None
        assert first_file_version.activity.name == BOGUS_ACTIVITY
        assert first_file_version.activity.description == BOGUS_DESCRIPTION
        assert len(first_file_version.activity.used) == 1
        assert first_file_version.activity.used[0].url == SYNAPSE_URL

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_annotations_added_then_removed(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Creates a number of files with annotations.
        After the files are uploaded to Synapse, the annotations are removed from the
        manifest file and the files are re-uploaded to Synapse. The annotations should
        be persisted on the files.
        """
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 3 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(3)]
        for file in temp_files:
            schedule_for_cleanup(file)

        # AND A manifest file with the paths to the temp files exists
        annotations = ["foo", "bar", "baz"]
        df = pd.DataFrame(
            {
                "path": temp_files,
                "parent": folder.id,
                "my_file_annotations": annotations,
                "used": "",
                "executed": "",
                "activityName": "",
                "activityDescription": "",
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 3

        # AND each of the files are the ones we uploaded
        for file in folder.files:
            assert file.path in temp_files
            assert file.activity is None
            assert len(file.annotations.keys()) == 1
            assert list(file.annotations.values())[0][0] in annotations

        # WHEN I update the manifest file to remove the annotations
        df = pd.DataFrame(
            {
                "path": temp_files,
                "parent": folder.id,
                "used": "",
                "executed": "",
                "activityName": "",
                "activityDescription": "",
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # AND I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 3

        # AND each of the files are the ones we uploaded
        for file in folder.files:
            assert file.path in temp_files
            assert file.activity is None

            # AND the files have annotations
            assert len(file.annotations.keys()) == 1
            assert list(file.annotations.values())[0][0] in annotations

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_annotations_added_then_removed_with_no_annotation_merge(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Creates a number of files with annotations.
        After the files are uploaded to Synapse, the annotations are removed from the
        manifest file and the files are re-uploaded to Synapse. The annotations should
        be removed from the files.
        """
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 3 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(3)]
        for file in temp_files:
            schedule_for_cleanup(file)

        # AND A manifest file with the paths to the temp files exists
        annotations = ["foo", "bar", "baz"]
        df = pd.DataFrame(
            {
                "path": temp_files,
                "parent": folder.id,
                "my_file_annotations": annotations,
                "used": "",
                "executed": "",
                "activityName": "",
                "activityDescription": "",
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(
            syn,
            file_name,
            sendMessages=SEND_MESSAGE,
            retries=2,
        )

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 3

        # AND each of the files are the ones we uploaded
        for file in folder.files:
            assert file.path in temp_files
            assert file.activity is None
            assert len(file.annotations.keys()) == 1
            assert list(file.annotations.values())[0][0] in annotations

        # WHEN I update the manifest file to remove the annotations
        df = pd.DataFrame(
            {
                "path": temp_files,
                "parent": folder.id,
                "used": "",
                "executed": "",
                "activityName": "",
                "activityDescription": "",
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # AND I sync the manifest to Synapse
        synapseutils.syncToSynapse(
            syn,
            file_name,
            sendMessages=SEND_MESSAGE,
            retries=2,
            merge_existing_annotations=False,
        )

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 3

        # AND each of the files are the ones we uploaded
        for file in folder.files:
            assert file.path in temp_files
            assert file.activity is None

            # AND none of the files have annotations
            assert len(file.annotations.keys()) == 0

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_activities_pointing_to_files_and_urls(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """Creates a sequence of files that are used by the next file in the sequence.
        Verifies that the files are uploaded to Synapse and that the used files are
        correctly set in the activity of the files.

        Example chain of files:
        file1 <- file2 <- file3 <- file4 <- file5
        """
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 3 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(3)]
        for file in temp_files:
            schedule_for_cleanup(file)

        # AND each file used/executed the previous file in the chain
        # AND each file has a URL for the used/executed item
        used_items = [SYNAPSE_URL]
        executed_items = [SUB_SYNAPSE_URL]
        for file in temp_files[1:]:
            used_items.append(
                ";".join([SYNAPSE_URL, temp_files[temp_files.index(file) - 1]])
            )
            executed_items.append(
                ";".join([SUB_SYNAPSE_URL, temp_files[temp_files.index(file) - 1]])
            )

        # AND A manifest file with the paths to the temp files exists
        df = pd.DataFrame(
            {
                "path": temp_files,
                "parent": folder.id,
                "used": used_items,
                "executed": executed_items,
                "activityName": BOGUS_ACTIVITY,
                "activityDescription": BOGUS_DESCRIPTION,
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 3

        # AND each of the files are the ones we uploaded
        file_ids = [file.id for file in folder.files]
        for file in folder.files:
            assert file.path in temp_files
            # AND the first file with no activity doesn't have an activity created
            if file.path == temp_files[0]:
                assert len(file.activity.used) == 1
                assert file.activity.used[0].url == SYNAPSE_URL
                assert len(file.activity.executed) == 1
                assert file.activity.executed[0].url == SUB_SYNAPSE_URL
            else:
                # AND the rest of the files have an activity
                assert file.activity.name == BOGUS_ACTIVITY
                assert file.activity.description == BOGUS_DESCRIPTION
                assert len(file.activity.used) == 2
                assert file.activity.used[0].url == SYNAPSE_URL
                assert file.activity.used[1].target_id in file_ids
                assert len(file.activity.executed) == 2
                assert file.activity.executed[0].url == SUB_SYNAPSE_URL
                assert file.activity.executed[1].target_id in file_ids

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_all_activities_pointing_to_single_file(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Example chain of files:
        file1 <- file2
        file1 <- file3
        """
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 3 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(3)]
        for file in temp_files:
            schedule_for_cleanup(file)

        # AND each file used/executed the first file
        used_or_executed_files = [""]
        for file in temp_files[1:]:
            used_or_executed_files.append(temp_files[0])

        # AND A manifest file with the paths to the temp files exists
        df = pd.DataFrame(
            {
                "path": temp_files,
                "parent": folder.id,
                "used": used_or_executed_files,
                "executed": used_or_executed_files,
                "activityName": BOGUS_ACTIVITY,
                "activityDescription": BOGUS_DESCRIPTION,
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 3

        # AND the root file of the saving process is present
        root_file_id = next(
            (file.id for file in folder.files if file.path == temp_files[0]), None
        )
        assert root_file_id is not None

        # AND each of the files are the ones we uploaded
        file_ids = [file.id for file in folder.files]
        for file in folder.files:
            assert file.path in temp_files
            # AND the first file with no activity doesn't have an activity created
            if file.path == temp_files[0]:
                assert file.activity is None
            else:
                # AND the rest of the files have an activity
                assert file.activity.name == BOGUS_ACTIVITY
                assert file.activity.description == BOGUS_DESCRIPTION
                assert len(file.activity.used) == 1
                assert file.activity.used[0].target_id in file_ids
                assert len(file.activity.executed) == 1
                assert file.activity.executed[0].target_id in file_ids

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_to_synapse_single_file_pointing_to_all_other_files(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Example chain of files:
        file1 -> file2
        file1 -> file3
        """
        # GIVEN a folder to sync to
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 3 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(3)]
        for file in temp_files:
            schedule_for_cleanup(file)

        # AND the first file used/executed all the other files
        used_or_executed_files = [";".join(temp_files[1:]), "", ""]

        # AND A manifest file with the paths to the temp files exists
        df = pd.DataFrame(
            {
                "path": temp_files,
                "parent": folder.id,
                "used": used_or_executed_files,
                "executed": used_or_executed_files,
                "activityName": BOGUS_ACTIVITY,
                "activityDescription": BOGUS_DESCRIPTION,
            }
        )
        # Write the df to the file:
        file_name = write_df_to_tsv(df, schedule_for_cleanup)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        folder.sync_from_synapse(download_file=False, synapse_client=syn)
        assert len(folder.files) == 3

        # AND the root file of the saving process is present
        root_file_id = next(
            (file.id for file in folder.files if file.path == temp_files[0]), None
        )
        assert root_file_id is not None

        # AND each of the files are the ones we uploaded
        file_ids = [file.id for file in folder.files]
        for file in folder.files:
            assert file.path in temp_files
            # AND the first file has an activity created
            if file.path == temp_files[0]:
                assert file.activity.name == BOGUS_ACTIVITY
                assert file.activity.description == BOGUS_DESCRIPTION
                assert len(file.activity.used) == 2
                assert file.activity.used[0].target_id in file_ids
                assert file.activity.used[1].target_id in file_ids
                assert len(file.activity.executed) == 2
                assert file.activity.executed[0].target_id in file_ids
                assert file.activity.executed[1].target_id in file_ids
            else:
                # AND the rest of the files do not have an activity
                assert file.activity is None


# @skip("Skip integration tests for soon to be removed code")
def test_write_manifest_data_unicode_characters_in_rows(test_state):
    # SYNPY-693

    named_temp_file = tempfile.NamedTemporaryFile("w")
    named_temp_file.close()
    test_state.schedule_for_cleanup(named_temp_file.name)

    keys = ["col_A", "col_B"]
    data = [
        {"col_A": "asdf", "col_B": "qwerty"},
        {"col_A": "凵𠘨工匚口刀乇", "col_B": "丅乇丂丅"},
    ]
    synapseutils.sync._write_manifest_data(named_temp_file.name, keys, data)

    df = pd.read_csv(named_temp_file.name, sep="\t", encoding="utf8")

    for dfrow, datarow in zip(df.itertuples(), data):
        assert datarow["col_A"] == dfrow.col_A
        assert datarow["col_B"] == dfrow.col_B


class TestSyncFromSynapse:
    """Testing the .syncFromSynapse() method"""

    # @skip("Skip integration tests for soon to be removed code")
    def test_folder_sync_from_synapse_files_only(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Testing for this case:

        project_model (root)
        └── parent_folder
            ├── file1 (uploaded)
            ├── file2 (uploaded)
        """
        # GIVEN a folder to sync from
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 2 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(2)]

        # AND each file is uploaded to Synapse
        file_entities = []
        for file in temp_files:
            schedule_for_cleanup(file)
            file_entity = syn.store(SynapseFile(path=file, parent=folder.id))
            schedule_for_cleanup(file_entity["id"])
            file_entities.append(file_entity)

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the content from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=folder.id, path=temp_dir
        )

        # THEN I expect that the result has all of the files
        assert len(sync_result) == 2

        # AND each of the files are the ones we uploaded
        for file in sync_result:
            assert file in file_entities

        # AND the manifest that is created matches the expected values
        manifest_df = pd.read_csv(os.path.join(temp_dir, MANIFEST_FILE), sep="\t")
        assert manifest_df.shape[0] == 2
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert manifest_df.shape[1] == 10

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            assert not matching_row.empty
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]
            assert pd.isna(matching_row[USED_COLUMN].values[0])
            assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])

    # @skip("Skip integration tests for soon to be removed code")
    def test_folder_sync_from_synapse_files_with_annotations(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Testing for this case:

        project_model (root)
        └── parent_folder
            ├── file1 (uploaded)
            ├── file2 (uploaded)
        """
        # GIVEN a folder to sync from
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 2 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(2)]

        # AND each file is uploaded to Synapse
        file_entities = []
        for file in temp_files:
            schedule_for_cleanup(file)
            file_entity = syn.store(
                SynapseFile(
                    path=file,
                    parent=folder.id,
                    annotations={
                        STR_ANNO: STR_ANNO_VALUE,
                        INT_ANNO: INT_ANNO_VALUE,
                        FLOAT_ANNO: FLOAT_ANNO_VALUE,
                        ARRAY_ANNO: ARRAY_ANNO_VALUE,
                        DATE_ANNO: DATE_ANNO_VALUE,
                        DATETIME_ANNO: DATETIME_ANNO_VALUE,
                        BOOL_ANNO: BOOL_ANNO_VALUE,
                    },
                )
            )
            schedule_for_cleanup(file_entity["id"])
            file_entities.append(file_entity)

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the content from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=folder.id, path=temp_dir
        )

        # THEN I expect that the result has all of the files
        assert len(sync_result) == 2

        # AND each of the files are the ones we uploaded
        for file in sync_result:
            assert file in file_entities

        # AND the manifest that is created matches the expected values
        manifest_df = pd.read_csv(os.path.join(temp_dir, MANIFEST_FILE), sep="\t")
        assert manifest_df.shape[0] == 2
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert STR_ANNO in manifest_df.columns
        assert INT_ANNO in manifest_df.columns
        assert FLOAT_ANNO in manifest_df.columns
        assert ARRAY_ANNO in manifest_df.columns
        assert DATE_ANNO in manifest_df.columns
        assert DATETIME_ANNO in manifest_df.columns
        assert manifest_df.shape[1] == 17

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            assert not matching_row.empty
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]

            assert pd.isna(matching_row[USED_COLUMN].values[0])
            assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])

            assert matching_row[STR_ANNO].values[0] == STR_ANNO_VALUE
            assert matching_row[INT_ANNO].values[0] == INT_ANNO_VALUE
            assert matching_row[FLOAT_ANNO].values[0] == FLOAT_ANNO_VALUE
            assert matching_row[ARRAY_ANNO].values[0] == ARRAY_ANNO_VALUE_IN_MANIFEST
            assert matching_row[DATE_ANNO].values[0] == DATE_ANNO_VALUE_IN_MANIFEST
            assert (
                matching_row[DATETIME_ANNO].values[0] == DATETIME_ANNO_VALUE_IN_MANIFEST
            )
            assert matching_row[BOOL_ANNO].values[0] == BOOL_ANNO_VALUE_IN_MANIFEST

    # @skip("Skip integration tests for soon to be removed code")
    def test_folder_sync_from_synapse_files_with_activity(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Testing for this case:

        project_model (root)
        └── parent_folder
            ├── file1 (uploaded)
            ├── file2 (uploaded)
        """
        # GIVEN a folder to sync from
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 2 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(2)]

        # AND each file is uploaded to Synapse
        file_entities = []
        for file in temp_files:
            schedule_for_cleanup(file)
            file_entity = syn.store(
                SynapseFile(
                    path=file,
                    parent=folder.id,
                )
            )

            # AND each file has an activity
            syn.setProvenance(
                file_entity,
                activity=Activity(
                    name=ACTIVITY_NAME,
                    description=ACTIVITY_DESCRIPTION,
                    used=[SYNAPSE_URL],
                    executed=[folder.id, project_model.id],
                ),
            )
            schedule_for_cleanup(file_entity["id"])
            # Removed for compare
            del file_entity[ETAG]
            del file_entity[MODIFIED_ON]
            file_entities.append(file_entity)

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the content from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=folder.id, path=temp_dir
        )

        # THEN I expect that the result has all of the files
        assert len(sync_result) == 2

        # AND each of the files are the ones we uploaded
        for file in sync_result:
            # Removed for compare
            del file[ETAG]
            del file[MODIFIED_ON]
            assert file in file_entities

        # AND the manifest that is created matches the expected values
        manifest_df = pd.read_csv(os.path.join(temp_dir, MANIFEST_FILE), sep="\t")
        assert manifest_df.shape[0] == 2
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert manifest_df.shape[1] == 10

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            assert not matching_row.empty
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert matching_row[USED_COLUMN].values[0] == SYNAPSE_URL
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]
            assert (
                matching_row[EXECUTED_COLUMN].values[0]
                == f"{folder.id}.1;{project_model.id}.1"
            )
            assert matching_row[ACTIVITY_NAME_COLUMN].values[0] == ACTIVITY_NAME
            assert (
                matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0]
                == ACTIVITY_DESCRIPTION
            )

    # @skip("Skip integration tests for soon to be removed code")
    def test_folder_sync_from_synapse_mix_of_entities(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Testing for this case:

        project_model (root)
        └── parent_folder
            ├── file1 (uploaded)
            └── table_test_syncFromSynapse (uploaded, not synced)
        """
        # GIVEN a folder to sync from
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 1 temporary file on disk:
        temp_file = utils.make_bogus_uuid_file()

        # AND each file is uploaded to Synapse
        schedule_for_cleanup(temp_file)
        file_entity = syn.store(SynapseFile(path=temp_file, parent=folder.id))
        schedule_for_cleanup(file_entity["id"])

        # AND a table is uploaded to the folder
        schema = syn.store(
            obj=Schema(name="table_test_syncFromSynapse", parent=folder.id)
        )
        assert schema["parentId"] == folder.id

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the content from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=folder.id, path=temp_dir
        )

        # THEN I expect that the result does not contain the table
        assert len(sync_result) == 1

        # AND each of the files are the ones we uploaded
        for file in sync_result:
            assert file == file_entity

        # AND the manifest that is created matches the expected values
        manifest_df = pd.read_csv(os.path.join(temp_dir, MANIFEST_FILE), sep="\t")
        assert manifest_df.shape[0] == 1
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert manifest_df.shape[1] == 10

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            assert not matching_row.empty
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]

            assert pd.isna(matching_row[USED_COLUMN].values[0])
            assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])

    def test_folder_sync_from_synapse_files_contained_within_sub_folder(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Testing for this case:

        project_model (root)
        ├── parent_folder
        │   └── sub_folder
        │       ├── file1 (uploaded)
        │       └── file2 (uploaded)
        """
        # GIVEN a folder
        parent_folder = Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(parent_folder.id)

        # AND a sub folder to sync from
        sub_folder = Folder(name=str(uuid.uuid4()), parent_id=parent_folder.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(sub_folder.id)

        # AND 2 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(2)]

        # AND each file is uploaded to Synapse into the sub folder
        file_entities = []
        for file in temp_files:
            schedule_for_cleanup(file)
            file_entity = syn.store(SynapseFile(path=file, parent=sub_folder.id))
            schedule_for_cleanup(file_entity["id"])
            file_entities.append(file_entity)

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the parent folder from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=parent_folder.id, path=temp_dir
        )

        # THEN I expect that the result has all of the files
        assert len(sync_result) == 2

        # AND each of the files are the ones we uploaded
        for file in sync_result:
            assert file in file_entities

        # AND the manifest that is created matches the expected values
        def verify_manifest(path: str) -> None:
            """Wrapper to verify the manifest file"""

            manifest_df = pd.read_csv(path, sep="\t")
            assert manifest_df.shape[0] == 2
            assert PATH_COLUMN in manifest_df.columns
            assert PARENT_COLUMN in manifest_df.columns
            assert USED_COLUMN in manifest_df.columns
            assert EXECUTED_COLUMN in manifest_df.columns
            assert ACTIVITY_NAME_COLUMN in manifest_df.columns
            assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
            assert CONTENT_TYPE_COLUMN in manifest_df.columns
            assert ID_COLUMN in manifest_df.columns
            assert SYNAPSE_STORE_COLUMN in manifest_df.columns
            assert NAME_COLUMN in manifest_df.columns
            assert manifest_df.shape[1] == 10

            for file in sync_result:
                matching_row = manifest_df[
                    manifest_df[PATH_COLUMN] == file[PATH_COLUMN]
                ]
                assert not matching_row.empty
                assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
                assert (
                    matching_row[CONTENT_TYPE_COLUMN].values[0]
                    == file[CONTENT_TYPE_COLUMN]
                )
                assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
                assert (
                    matching_row[SYNAPSE_STORE_COLUMN].values[0]
                    == file[SYNAPSE_STORE_COLUMN]
                )
                assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]

                assert pd.isna(matching_row[USED_COLUMN].values[0])
                assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
                assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
                assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])

        # AND the default behavior is that a manifest file is created in the directory
        # we supplied as well as sub directories to that.
        verify_manifest(path=os.path.join(temp_dir, MANIFEST_FILE))
        sub_directory = os.path.join(temp_dir, sub_folder.name)
        verify_manifest(path=os.path.join(sub_directory, MANIFEST_FILE))

    # @skip("Skip integration tests for soon to be removed code")
    def test_folder_sync_from_synapse_files_contained_within_sub_folder_root_manifest_only(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Testing for this case:

        project_model (root)
        ├── parent_folder
        │   └── sub_folder
        │       ├── file1 (uploaded)
        │       └── file2 (uploaded)

        Verifies that only the root manifest is created.
        """
        # GIVEN a folder
        parent_folder = Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(parent_folder.id)

        # AND a sub folder to sync from
        sub_folder = Folder(name=str(uuid.uuid4()), parent_id=parent_folder.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(sub_folder.id)

        # AND 2 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(2)]

        # AND each file is uploaded to Synapse into the sub folder
        file_entities = []
        for file in temp_files:
            schedule_for_cleanup(file)
            file_entity = syn.store(SynapseFile(path=file, parent=sub_folder.id))
            schedule_for_cleanup(file_entity["id"])
            file_entities.append(file_entity)

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the parent folder from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=parent_folder.id, path=temp_dir, manifest="root"
        )

        # THEN I expect that the result has all of the files
        assert len(sync_result) == 2

        # AND each of the files are the ones we uploaded
        for file in sync_result:
            assert file in file_entities

        # AND the manifest that is created matches the expected values
        manifest_df = pd.read_csv(os.path.join(temp_dir, MANIFEST_FILE), sep="\t")
        assert manifest_df.shape[0] == 2
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert manifest_df.shape[1] == 10

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            assert not matching_row.empty
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]

            assert pd.isna(matching_row[USED_COLUMN].values[0])
            assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])

        # AND the default behavior is that a manifest file is created in root, but not the sub folder
        sub_directory = os.path.join(temp_dir, sub_folder.name)
        assert not os.path.exists(os.path.join(sub_directory, MANIFEST_FILE))

    # @skip("Skip integration tests for soon to be removed code")
    def test_folder_sync_from_synapse_files_contained_within_sub_folder_suppress_manifest(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Testing for this case:

        project_model (root)
        ├── parent_folder
        │   └── sub_folder
        │       ├── file1 (uploaded)
        │       └── file2 (uploaded)

        Verifies that the manifest is not created at all.
        """
        # GIVEN a folder
        parent_folder = Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(parent_folder.id)

        # AND a sub folder to sync from
        sub_folder = Folder(name=str(uuid.uuid4()), parent_id=parent_folder.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(sub_folder.id)

        # AND 2 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(2)]

        # AND each file is uploaded to Synapse into the sub folder
        file_entities = []
        for file in temp_files:
            schedule_for_cleanup(file)
            file_entity = syn.store(SynapseFile(path=file, parent=sub_folder.id))
            schedule_for_cleanup(file_entity["id"])
            file_entities.append(file_entity)

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the parent folder from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=parent_folder.id, path=temp_dir, manifest="suppress"
        )

        # THEN I expect that the result has all of the files
        assert len(sync_result) == 2

        # AND each of the files are the ones we uploaded
        for file in sync_result:
            assert file in file_entities

        # AND the manifest is not created because it was suppressed
        sub_directory = os.path.join(temp_dir, sub_folder.name)
        assert not os.path.exists(os.path.join(temp_dir, MANIFEST_FILE))
        assert not os.path.exists(os.path.join(sub_directory, MANIFEST_FILE))

    # @skip("Skip integration tests for soon to be removed code")
    def test_folder_sync_from_synapse_files_spread_across_folders(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Testing for this case:

        project_model (root)
        ├── parent_folder
        │   ├── file1
        │   ├── sub_folder_1
        │   │   └── file2
        │   └── sub_folder_2
        │       └── file3
        """
        # GIVEN a folder
        parent_folder = Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(parent_folder.id)

        # AND a sub folder to sync from
        sub_folder_1 = Folder(name=str(uuid.uuid4()), parent_id=parent_folder.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(sub_folder_1.id)

        # AND another sub folder to sync from
        sub_folder_2 = Folder(name=str(uuid.uuid4()), parent_id=parent_folder.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(sub_folder_2.id)

        # AND 3 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(3)]

        # AND each file is uploaded to Synapse into the respective sub folders
        file_entities = []
        for file in temp_files:
            schedule_for_cleanup(file)
            if file == temp_files[0]:
                file_entity = syn.store(SynapseFile(path=file, parent=parent_folder.id))
            elif file == temp_files[1]:
                file_entity = syn.store(SynapseFile(path=file, parent=sub_folder_1.id))
            else:
                file_entity = syn.store(SynapseFile(path=file, parent=sub_folder_2.id))
            schedule_for_cleanup(file_entity["id"])
            file_entities.append(file_entity)

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the parent folder from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=parent_folder.id, path=temp_dir
        )

        # THEN I expect that the result has all of the files
        assert len(sync_result) == 3

        # AND each of the files are the ones we uploaded
        for file in sync_result:
            assert file in file_entities

        # AND the root manifest contain all entries
        manifest_df = pd.read_csv(os.path.join(temp_dir, MANIFEST_FILE), sep="\t")
        assert manifest_df.shape[0] == 3
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert manifest_df.shape[1] == 10
        found_matching_file = False

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            if matching_row.empty:
                continue
            else:
                found_matching_file = True
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]

            assert pd.isna(matching_row[USED_COLUMN].values[0])
            assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])
        assert found_matching_file

        # AND the sub directory manifests contain only a single entry
        sub_directory_1 = os.path.join(temp_dir, sub_folder_1.name)
        sub_directory_2 = os.path.join(temp_dir, sub_folder_2.name)
        manifest_df = pd.read_csv(
            os.path.join(sub_directory_1, MANIFEST_FILE), sep="\t"
        )
        assert manifest_df.shape[0] == 1
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert manifest_df.shape[1] == 10
        found_matching_file = False

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            if matching_row.empty:
                continue
            else:
                found_matching_file = True
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]

            assert pd.isna(matching_row[USED_COLUMN].values[0])
            assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])
        assert found_matching_file

        manifest_df = pd.read_csv(
            os.path.join(sub_directory_2, MANIFEST_FILE), sep="\t"
        )
        assert manifest_df.shape[0] == 1
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert manifest_df.shape[1] == 10
        found_matching_file = False

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            if matching_row.empty:
                continue
            else:
                found_matching_file = True
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]

            assert pd.isna(matching_row[USED_COLUMN].values[0])
            assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])
        assert found_matching_file

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_from_synapse_follow_links_files(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Testing for this case:

        project_model (root)
        ├── folder_with_files
        │   ├── file1 (uploaded)
        │   └── file2 (uploaded)
        └── folder_with_links - This is the folder we are syncing from
            ├── link_to_file1 -> ../folder_with_files/file1
            └── link_to_file2 -> ../folder_with_files/file2
        """
        # GIVEN a folder
        folder_with_files = Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(folder_with_files.id)

        # AND a second folder to sync from
        folder_with_links = Folder(
            name=str(uuid.uuid4()), parent_id=folder_with_files.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(folder_with_links.id)

        # AND 2 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(2)]

        # AND each file is uploaded to Synapse into `folder_with_files`
        file_entities = []
        for file in temp_files:
            schedule_for_cleanup(file)
            file_entity = syn.store(SynapseFile(path=file, parent=folder_with_files.id))
            schedule_for_cleanup(file_entity["id"])
            file_entities.append(file_entity)
            syn.store(obj=Link(targetId=file_entity.id, parent=folder_with_links.id))

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the parent folder from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=folder_with_links.id, path=temp_dir, followLink=True
        )

        # THEN I expect that the result has all of the files
        assert len(sync_result) == 2

        # AND each of the files are the ones we uploaded
        for file in sync_result:
            assert file in file_entities

        # AND the manifest that is created matches the expected values
        manifest_df = pd.read_csv(os.path.join(temp_dir, MANIFEST_FILE), sep="\t")
        assert manifest_df.shape[0] == 2
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert manifest_df.shape[1] == 10

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            assert not matching_row.empty
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]

            assert pd.isna(matching_row[USED_COLUMN].values[0])
            assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_from_synapse_follow_links_folder(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Testing for this case:

        project_model (root)
        ├── folder_with_files
        │   ├── file1 (uploaded)
        │   └── file2 (uploaded)
        └── folder_with_links - This is the folder we are syncing from
            └── link_to_folder_with_files -> ../folder_with_files
        """
        # GIVEN a folder
        folder_with_files = Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(folder_with_files.id)

        # AND two files in the folder
        temp_files = [utils.make_bogus_uuid_file() for _ in range(2)]
        file_entities = []
        for file in temp_files:
            schedule_for_cleanup(file)
            file_entity = syn.store(SynapseFile(path=file, parent=folder_with_files.id))
            schedule_for_cleanup(file_entity["id"])
            file_entities.append(file_entity)

        # AND a second folder to sync from
        folder_with_links = Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(folder_with_links.id)

        # AND a link to folder_with_files in folder_with_links
        syn.store(obj=Link(targetId=folder_with_files.id, parent=folder_with_links.id))

        # AND a temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the parent folder from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=folder_with_links.id, path=temp_dir, followLink=True
        )

        # THEN I expect that the result has all of the files
        assert len(sync_result) == 2

        # AND each of the files are the ones we uploaded
        for file in sync_result:
            assert file in file_entities

        # AND the manifest that is created matches the expected values
        manifest_df = pd.read_csv(os.path.join(temp_dir, MANIFEST_FILE), sep="\t")
        assert manifest_df.shape[0] == 2
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert manifest_df.shape[1] == 10

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            assert not matching_row.empty
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]

            assert pd.isna(matching_row[USED_COLUMN].values[0])
            assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_from_synapse_follow_links_sync_contains_all_folders(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        This is an integration test to note the current behavior of syncFromSynapse.
        This may not be desired behavior, but it is the current behavior.

        The covers this test scenario:

        parent_folder
        ├── folder_with_files
        │   ├── file1
        │   └── file2
        └── folder_with_links
            ├── link_to_file1 -> ../folder_with_files/file1
            └── link_to_file2 -> ../folder_with_files/file2

        In this case a FileEntity is returned for each of the files and links (4) total.
        """
        # GIVEN a parent folder
        parent_folder = Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(parent_folder.id)

        # AND a folder for files
        folder_with_files = Folder(
            name=str(uuid.uuid4()), parent_id=parent_folder.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(folder_with_files.id)

        # AND a second folder to sync from
        folder_with_links = Folder(
            name=str(uuid.uuid4()), parent_id=parent_folder.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(folder_with_links.id)

        # AND 2 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(2)]

        # AND each file is uploaded to Synapse into `folder_with_files`
        file_entities = []
        for file in temp_files:
            schedule_for_cleanup(file)
            file_entity = syn.store(SynapseFile(path=file, parent=folder_with_files.id))
            schedule_for_cleanup(file_entity["id"])
            file_entities.append(file_entity)
            syn.store(obj=Link(targetId=file_entity.id, parent=folder_with_links.id))

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the parent folder from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=parent_folder.id, path=temp_dir, followLink=True
        )

        # THEN I expect that the result has all of the files
        assert len(sync_result) == 4

        # AND each of the files are the ones we uploaded
        for file in sync_result:
            assert file in file_entities

        # AND the manifest that is created matches the expected values
        manifest_df = pd.read_csv(os.path.join(temp_dir, MANIFEST_FILE), sep="\t")
        assert manifest_df.shape[0] == 4
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert manifest_df.shape[1] == 10
        found_matching_file = False

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            if matching_row.empty:
                continue
            else:
                found_matching_file = True
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]

            assert pd.isna(matching_row[USED_COLUMN].values[0])
            assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])
        assert found_matching_file

        # AND The directory for files contains a manifest file
        sub_directory_for_files = os.path.join(temp_dir, folder_with_files.name)
        manifest_df = pd.read_csv(
            os.path.join(sub_directory_for_files, MANIFEST_FILE), sep="\t"
        )
        assert manifest_df.shape[0] == 2
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert manifest_df.shape[1] == 10
        found_matching_file = False

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            if matching_row.empty:
                continue
            else:
                found_matching_file = True
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]

            assert pd.isna(matching_row[USED_COLUMN].values[0])
            assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])
        assert found_matching_file

        # AND The directory for links contains a manifest file
        sub_directory_for_links = os.path.join(temp_dir, folder_with_links.name)
        manifest_df = pd.read_csv(
            os.path.join(sub_directory_for_links, MANIFEST_FILE), sep="\t"
        )
        assert manifest_df.shape[0] == 2
        assert PATH_COLUMN in manifest_df.columns
        assert PARENT_COLUMN in manifest_df.columns
        assert USED_COLUMN in manifest_df.columns
        assert EXECUTED_COLUMN in manifest_df.columns
        assert ACTIVITY_NAME_COLUMN in manifest_df.columns
        assert ACTIVITY_DESCRIPTION_COLUMN in manifest_df.columns
        assert CONTENT_TYPE_COLUMN in manifest_df.columns
        assert ID_COLUMN in manifest_df.columns
        assert SYNAPSE_STORE_COLUMN in manifest_df.columns
        assert NAME_COLUMN in manifest_df.columns
        assert manifest_df.shape[1] == 10
        found_matching_file = False

        for file in sync_result:
            matching_row = manifest_df[manifest_df[PATH_COLUMN] == file[PATH_COLUMN]]
            if matching_row.empty:
                continue
            else:
                found_matching_file = True
            assert matching_row[PARENT_COLUMN].values[0] == file[PARENT_ATTRIBUTE]
            assert (
                matching_row[CONTENT_TYPE_COLUMN].values[0] == file[CONTENT_TYPE_COLUMN]
            )
            assert matching_row[ID_COLUMN].values[0] == file[ID_COLUMN]
            assert (
                matching_row[SYNAPSE_STORE_COLUMN].values[0]
                == file[SYNAPSE_STORE_COLUMN]
            )
            assert matching_row[NAME_COLUMN].values[0] == file[NAME_COLUMN]

            assert pd.isna(matching_row[USED_COLUMN].values[0])
            assert pd.isna(matching_row[EXECUTED_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_NAME_COLUMN].values[0])
            assert pd.isna(matching_row[ACTIVITY_DESCRIPTION_COLUMN].values[0])
        assert found_matching_file

    # @skip("Skip integration tests for soon to be removed code")
    def test_sync_from_synapse_dont_follow_links(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """
        Testing for this case:

        project_model (root)
        ├── folder_with_files
        │   ├── file1 (uploaded)
        │   └── file2 (uploaded)
        └── folder_with_links - This is the folder we are syncing from
            ├── link_to_file1 -> ../folder_with_files/file1
            └── link_to_file2 -> ../folder_with_files/file2
        """
        # GIVEN a folder
        folder_with_files = Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(folder_with_files.id)

        # AND a second folder to sync from
        folder_with_links = Folder(
            name=str(uuid.uuid4()), parent_id=folder_with_files.id
        ).store(synapse_client=syn)
        schedule_for_cleanup(folder_with_links.id)

        # AND 2 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(2)]

        # AND each file is uploaded to Synapse into `folder_with_files`
        file_entities = []
        for file in temp_files:
            schedule_for_cleanup(file)
            file_entity = syn.store(SynapseFile(path=file, parent=folder_with_files.id))
            schedule_for_cleanup(file_entity["id"])
            file_entities.append(file_entity)
            syn.store(obj=Link(targetId=file_entity.id, parent=folder_with_links.id))

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the parent folder from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=folder_with_links.id, path=temp_dir, followLink=False
        )

        # THEN I expect that nothing is returned as I am not following links
        assert len(sync_result) == 0

        # AND the manifest has not been created
        assert os.path.exists(os.path.join(temp_dir, MANIFEST_FILE)) is False

    # @skip("Skip integration tests for soon to be removed code")
    def test_file_sync_from_synapse(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """Tests that calling syncFromSynapse with a file entity returns the file.

        Also verifies that a manifest file is not created if the entity is a file.
        """
        # GIVEN a folder to sync from
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 1 temporary file on disk:
        file = utils.make_bogus_uuid_file()

        # AND the file is uploaded to Synapse
        schedule_for_cleanup(file)
        file_entity = syn.store(SynapseFile(path=file, parent=folder.id))
        schedule_for_cleanup(file_entity["id"])

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the content from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=file_entity, path=temp_dir
        )

        # THEN I expect that the result has the file
        assert len(sync_result) == 1

        # AND the file is the one we uploaded
        for file in sync_result:
            assert file == file_entity

        # AND the manifest has not been created
        assert os.path.exists(os.path.join(temp_dir, MANIFEST_FILE)) is False

    # @skip("Skip integration tests for soon to be removed code")
    def test_file_sync_from_synapse_specific_version(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        project_model: Project,
    ) -> None:
        """Tests that calling syncFromSynapse with a file entity with a version returns
        the requested version of the file.

        Also verifies that a manifest file is not created if the entity is a file.
        """
        # GIVEN a folder to sync from
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=syn
        )
        schedule_for_cleanup(folder.id)

        # AND 1 temporary file on disk:
        file = utils.make_bogus_uuid_file()

        # AND the file is uploaded to Synapse
        schedule_for_cleanup(file)
        file_entity_v1 = syn.store(obj=SynapseFile(path=file, parent=folder.id))
        schedule_for_cleanup(file_entity_v1["id"])
        assert file_entity_v1["versionNumber"] == 1

        # AND the version on the file is updated
        file_entity_v2 = syn.store(
            obj=SynapseFile(path=file, parent=folder.id), forceVersion=True
        )
        assert file_entity_v2["versionNumber"] == 2
        assert file_entity_v1["id"] == file_entity_v2["id"]

        # AND A temp directory to write the manifest file to
        temp_dir = tempfile.mkdtemp()

        # WHEN I sync the content from Synapse
        sync_result = synapseutils.syncFromSynapse(
            syn=syn, entity=f"{file_entity_v1['id']}.1", path=temp_dir
        )

        # THEN I expect that the result has the file
        assert len(sync_result) == 1

        # AND the file is the first version of the one we uploaded
        for file in sync_result:
            # The etag on the non-latest versions is all 0's
            del file["etag"]
            del file_entity_v1["etag"]
            # This is the first version of the file, and the new entity will have this
            # set to False
            assert file_entity_v1.properties["isLatestVersion"]
            file_entity_v1.properties["isLatestVersion"] = False
            assert file == file_entity_v1

        # AND the manifest has not been created
        assert os.path.exists(os.path.join(temp_dir, MANIFEST_FILE)) is False


def write_df_to_tsv(df: pd.DataFrame, schedule_for_cleanup: Callable[..., None]) -> str:
    tmpdir = tempfile.mkdtemp()
    schedule_for_cleanup(tmpdir)
    file_name = os.path.join(tmpdir, str(uuid.uuid4()))
    df.to_csv(file_name, sep="\t", index=False)
    assert os.path.exists(file_name)
    schedule_for_cleanup(file_name)
    return file_name
