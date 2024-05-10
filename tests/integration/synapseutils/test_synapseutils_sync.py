import uuid
import os
import time
import tempfile
import datetime
from func_timeout import FunctionTimedOut, func_set_timeout
import pandas as pd

import pytest

from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient import (
    File,
    Folder as SynapseFolder,
    Link,
    Project as SynapseProject,
    Schema,
    Synapse,
)
from synapseclient.models import Project, Folder
import synapseclient.core.utils as utils
import synapseutils

from tests.integration import QUERY_TIMEOUT_SEC

BOGUS_ACTIVITY = "bogus_activity"
BOGUS_DESCRIPTION = "bogus_description"
SYNAPSE_URL = "https://www.synapse.org"
SUB_SYNAPSE_URL = "https://www.asdf.synapse.org"
SEND_MESSAGE = False


@pytest.mark.asyncio(scope="session")
@pytest.fixture(scope="function", autouse=True)
async def test_state(syn: Synapse, schedule_for_cleanup):
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


def _makeManifest(content, schedule_for_cleanup):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".dat", delete=False) as f:
        f.write(content)
        filepath = utils.normalize_path(f.name)
    schedule_for_cleanup(filepath)
    return filepath


async def test_readManifest(test_state):
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

    async def test_sync_to_synapse_file_only(
        self, syn: Synapse, schedule_for_cleanup, project_model: Project
    ) -> None:
        # GIVEN a folder to sync to
        folder = await Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store_async()
        schedule_for_cleanup(folder.id)

        # AND 10 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(10)]
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
        tmpdir = tempfile.mkdtemp()
        schedule_for_cleanup(tmpdir)
        file_name = os.path.join(tmpdir, str(uuid.uuid4()))
        df.to_csv(file_name, sep="\t", index=False)
        assert os.path.exists(file_name)
        schedule_for_cleanup(file_name)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        await folder.sync_from_synapse_async(download_file=False)
        assert len(folder.files) == 10

        # AND each of the files are the ones we uploaded
        for file in folder.files:
            assert file.path in temp_files

    async def test_sync_to_synapse_files_with_annotations(
        self, syn: Synapse, schedule_for_cleanup, project_model: Project
    ) -> None:
        # GIVEN a folder to sync to
        folder = await Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store_async()
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
        tmpdir = tempfile.mkdtemp()
        schedule_for_cleanup(tmpdir)
        file_name = os.path.join(tmpdir, str(uuid.uuid4()))
        df.to_csv(file_name, sep="\t", index=False)
        assert os.path.exists(file_name)
        schedule_for_cleanup(file_name)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        await folder.sync_from_synapse_async(download_file=False)
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

    async def test_sync_to_synapse_with_activities(
        self, syn: Synapse, schedule_for_cleanup, project_model: Project
    ) -> None:
        # GIVEN a folder to sync to
        folder = await Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store_async()
        schedule_for_cleanup(folder.id)

        # AND 10 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(10)]
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
        tmpdir = tempfile.mkdtemp()
        schedule_for_cleanup(tmpdir)
        file_name = os.path.join(tmpdir, str(uuid.uuid4()))
        df.to_csv(file_name, sep="\t", index=False)
        assert os.path.exists(file_name)
        schedule_for_cleanup(file_name)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        await folder.sync_from_synapse_async(download_file=False)
        assert len(folder.files) == 10

        # AND each of the files are the ones we uploaded
        for file in folder.files:
            assert file.path in temp_files
            assert len(file.activity.used) == 1
            assert file.activity.used[0].url == SYNAPSE_URL
            assert len(file.activity.executed) == 1
            assert file.activity.executed[0].url == SUB_SYNAPSE_URL
            assert file.activity.name == BOGUS_ACTIVITY
            assert file.activity.description == BOGUS_DESCRIPTION

    async def test_sync_to_synapse_activities_pointing_to_files(
        self, syn: Synapse, schedule_for_cleanup, project_model: Project
    ) -> None:
        """Creates a sequence of files that are used by the next file in the sequence.
        Verifies that the files are uploaded to Synapse and that the used files are
        correctly set in the activity of the files.

        Example chain of files:
        file1 <- file2 <- file3 <- file4 <- file5
        """
        # GIVEN a folder to sync to
        folder = await Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store_async()
        schedule_for_cleanup(folder.id)

        # AND 5 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(5)]
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
        tmpdir = tempfile.mkdtemp()
        schedule_for_cleanup(tmpdir)
        file_name = os.path.join(tmpdir, str(uuid.uuid4()))
        df.to_csv(file_name, sep="\t", index=False)
        assert os.path.exists(file_name)
        schedule_for_cleanup(file_name)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        await folder.sync_from_synapse_async(download_file=False)
        assert len(folder.files) == 5

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

    async def test_sync_to_synapse_activities_pointing_to_files_and_urls(
        self, syn: Synapse, schedule_for_cleanup, project_model: Project
    ) -> None:
        """Creates a sequence of files that are used by the next file in the sequence.
        Verifies that the files are uploaded to Synapse and that the used files are
        correctly set in the activity of the files.

        Example chain of files:
        file1 <- file2 <- file3 <- file4 <- file5
        """
        # GIVEN a folder to sync to
        folder = await Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store_async()
        schedule_for_cleanup(folder.id)

        # AND 5 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(5)]
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
        tmpdir = tempfile.mkdtemp()
        schedule_for_cleanup(tmpdir)
        file_name = os.path.join(tmpdir, str(uuid.uuid4()))
        df.to_csv(file_name, sep="\t", index=False)
        assert os.path.exists(file_name)
        schedule_for_cleanup(file_name)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        await folder.sync_from_synapse_async(download_file=False)
        assert len(folder.files) == 5

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

    async def test_sync_to_synapse_all_activities_pointing_to_single_file(
        self, syn: Synapse, schedule_for_cleanup, project_model: Project
    ) -> None:
        """
        Example chain of files:
        file1 <- file2
        file1 <- file3
        file1 <- file4
        file1 <- file5
        """
        # GIVEN a folder to sync to
        folder = await Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store_async()
        schedule_for_cleanup(folder.id)

        # AND 5 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(5)]
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
        tmpdir = tempfile.mkdtemp()
        schedule_for_cleanup(tmpdir)
        file_name = os.path.join(tmpdir, str(uuid.uuid4()))
        df.to_csv(file_name, sep="\t", index=False)
        assert os.path.exists(file_name)
        schedule_for_cleanup(file_name)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        await folder.sync_from_synapse_async(download_file=False)
        assert len(folder.files) == 5

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

    async def test_sync_to_synapse_single_file_pointing_to_all_other_files(
        self, syn: Synapse, schedule_for_cleanup, project_model: Project
    ) -> None:
        """
        Example chain of files:
        file1 -> file2
        file1 -> file3
        file1 -> file4
        file1 -> file5
        """
        # GIVEN a folder to sync to
        folder = await Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store_async()
        schedule_for_cleanup(folder.id)

        # AND 5 temporary files on disk:
        temp_files = [utils.make_bogus_uuid_file() for _ in range(5)]
        for file in temp_files:
            schedule_for_cleanup(file)

        # AND the first file used/executed all the other files
        used_or_executed_files = [";".join(temp_files[1:]), "", "", "", ""]

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
        tmpdir = tempfile.mkdtemp()
        schedule_for_cleanup(tmpdir)
        file_name = os.path.join(tmpdir, str(uuid.uuid4()))
        df.to_csv(file_name, sep="\t", index=False)
        assert os.path.exists(file_name)
        schedule_for_cleanup(file_name)

        # WHEN I sync the manifest to Synapse
        synapseutils.syncToSynapse(syn, file_name, sendMessages=SEND_MESSAGE, retries=2)

        # THEN I expect that the folder has all of the files
        await folder.sync_from_synapse_async(download_file=False)
        assert len(folder.files) == 5

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
                assert len(file.activity.used) == 4
                assert file.activity.used[0].target_id in file_ids
                assert file.activity.used[1].target_id in file_ids
                assert file.activity.used[2].target_id in file_ids
                assert file.activity.used[3].target_id in file_ids
                assert len(file.activity.executed) == 4
                assert file.activity.executed[0].target_id in file_ids
                assert file.activity.executed[1].target_id in file_ids
                assert file.activity.executed[2].target_id in file_ids
                assert file.activity.executed[3].target_id in file_ids
            else:
                # AND the rest of the files do not have an activity
                assert file.activity is None


@pytest.mark.flaky(reruns=3)
async def test_syncFromSynapse(test_state):
    """This function tests recursive download as defined in syncFromSynapse
    most of the functionality of this function are already tested in the
    tests/integration/test_command_line_client::test_command_get_recursive_and_query

    which means that the only test if for path=None
    """
    # Create a Project
    project_entity = test_state.syn.store(SynapseProject(name=str(uuid.uuid4())))
    test_state.schedule_for_cleanup(project_entity.id)

    # Create a Folder in Project
    folder_entity = test_state.syn.store(
        SynapseFolder(name=str(uuid.uuid4()), parent=project_entity)
    )

    # Create and upload two files in Folder
    uploaded_paths = []
    for i in range(2):
        f = utils.make_bogus_data_file()
        uploaded_paths.append(f)
        test_state.schedule_for_cleanup(f)
        test_state.syn.store(File(f, parent=folder_entity))

    # Add a file in the project level as well
    f = utils.make_bogus_data_file()
    uploaded_paths.append(f)
    test_state.schedule_for_cleanup(f)
    entity = test_state.syn.store(File(f, parent=project_entity))

    # Update the Entity and make sure the version is incremented
    entity = test_state.syn.get(entity)
    entity = test_state.syn.store(entity, forceVersion=True)
    print(entity)
    assert entity.versionNumber == 2

    # Now get version 1 of the entity using .version syntax in the synid
    synid_with_version_1 = f"{entity.id}.1"
    entity_v1 = execute_sync_from_synapse(test_state.syn, synid_with_version_1)
    # Confirm that the entity is version 1 and not 2
    assert entity_v1[0].versionNumber == 1

    # syncFromSynapse() uses chunkedQuery() which will return results that are eventually consistent
    # but not always right after the entity is created.
    start_time = time.time()
    while len(list(test_state.syn.getChildren(project_entity))) != 2:
        assert time.time() - start_time < QUERY_TIMEOUT_SEC
        time.sleep(2)

    # Test recursive get
    try:
        output = execute_sync_from_synapse(test_state.syn, project_entity)
    except FunctionTimedOut:
        test_state.syn.logger.warning("test_syncFromSynapse timed out")
        pytest.fail("test_syncFromSynapse timed out")

    assert len(output) == len(uploaded_paths)
    for f in output:
        assert utils.normalize_path(f.path) in uploaded_paths


@pytest.mark.flaky(reruns=3)
async def test_syncFromSynapse_children_contain_non_file(test_state):
    proj = test_state.syn.store(
        SynapseProject(
            name="test_syncFromSynapse_children_non_file" + str(uuid.uuid4())
        )
    )
    test_state.schedule_for_cleanup(proj)

    temp_file = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(temp_file)
    file_entity = test_state.syn.store(
        File(
            temp_file,
            name="temp_file_test_syncFromSynapse_children_non_file" + str(uuid.uuid4()),
            parent=proj,
        )
    )

    test_state.syn.store(Schema(name="table_test_syncFromSynapse", parent=proj))

    temp_folder = tempfile.mkdtemp()
    test_state.schedule_for_cleanup(temp_folder)

    try:
        files_list = execute_sync_from_synapse(test_state.syn, proj, temp_folder)
    except FunctionTimedOut:
        test_state.syn.logger.warning(
            "test_syncFromSynapse_children_contain_non_file timed out"
        )
        pytest.fail("test_syncFromSynapse_children_contain_non_file timed out")
    assert 1 == len(files_list)
    assert file_entity == files_list[0]


@pytest.mark.flaky(reruns=3)
async def test_syncFromSynapse_Links(test_state):
    """This function tests recursive download of links as defined in syncFromSynapse
    most of the functionality of this function are already tested in the
    tests/integration/test_command_line_client::test_command_get_recursive_and_query

    which means that the only test if for path=None
    """
    # Create a Project
    project_entity = test_state.syn.store(SynapseProject(name=str(uuid.uuid4())))
    test_state.schedule_for_cleanup(project_entity.id)

    # Create a Folder in Project
    folder_entity = test_state.syn.store(
        SynapseFolder(name=str(uuid.uuid4()), parent=project_entity)
    )
    # Create a Folder hierarchy in folder_entity
    inner_folder_entity = test_state.syn.store(
        SynapseFolder(name=str(uuid.uuid4()), parent=folder_entity)
    )

    second_folder_entity = test_state.syn.store(
        SynapseFolder(name=str(uuid.uuid4()), parent=project_entity)
    )

    # Create and upload two files in Folder
    uploaded_paths = []
    for i in range(2):
        f = utils.make_bogus_data_file()
        uploaded_paths.append(f)
        test_state.schedule_for_cleanup(f)
        file_entity = test_state.syn.store(File(f, parent=project_entity))
        # Create links to inner folder
        test_state.syn.store(Link(file_entity.id, parent=folder_entity))
    # Add a file in the project level as well
    f = utils.make_bogus_data_file()
    uploaded_paths.append(f)
    test_state.schedule_for_cleanup(f)
    file_entity = test_state.syn.store(File(f, parent=second_folder_entity))
    # Create link to inner folder
    test_state.syn.store(Link(file_entity.id, parent=inner_folder_entity))

    # Test recursive get
    try:
        output = execute_sync_from_synapse(
            test_state.syn, folder_entity, followLink=True
        )
    except FunctionTimedOut:
        test_state.syn.logger.warning("test_syncFromSynapse_Links timed out")
        pytest.fail("test_syncFromSynapse_Links timed out")

    assert len(output) == len(uploaded_paths)
    for f in output:
        assert utils.normalize_path(f.path) in uploaded_paths


async def test_write_manifest_data_unicode_characters_in_rows(test_state):
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


@pytest.mark.flaky(reruns=3)
async def test_syncFromSynapse_given_file_id(test_state):
    file_path = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(file_path)
    file = test_state.syn.store(
        File(
            file_path,
            name=str(uuid.uuid4()),
            parent=test_state.project,
            synapseStore=False,
        )
    )
    try:
        all_files = execute_sync_from_synapse(test_state.syn, file.id)
    except FunctionTimedOut:
        test_state.syn.logger.warning("test_syncFromSynapse_given_file_id timed out")
        pytest.fail("test_syncFromSynapse_given_file_id timed out")

    assert 1 == len(all_files)
    assert file == all_files[0]


# When running with multiple threads it can lock up and do nothing until pipeline is killed at 6hrs
@func_set_timeout(120)
def execute_sync_from_synapse(*args, **kwargs):
    return synapseutils.syncFromSynapse(*args, **kwargs)
