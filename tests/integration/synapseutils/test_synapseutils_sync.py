import uuid
import os
import time
import tempfile
import datetime
from func_timeout import FunctionTimedOut, func_set_timeout
import pandas as pd
import numpy as np

import pytest

from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient import Entity, File, Folder, Link, Project, Schema, Synapse
import synapseclient.core.utils as utils
import synapseutils
from opentelemetry import trace

from tests.integration import QUERY_TIMEOUT_SEC


tracer = trace.get_tracer("synapseclient")


@pytest.fixture(scope="function", autouse=True)
def test_state(syn: Synapse, schedule_for_cleanup):
    class TestState:
        def __init__(self):
            self.syn = syn
            self.project = syn.store(Project(name=str(uuid.uuid4())))
            self.folder = syn.store(Folder(name=str(uuid.uuid4()), parent=self.project))
            self.schedule_for_cleanup = schedule_for_cleanup

            # Create testfiles for upload
            self.f1 = utils.make_bogus_data_file(n=10)
            self.f2 = utils.make_bogus_data_file(n=10)
            self.f3 = "https://www.synapse.org"

            self.header = "path	parent	used	executed	activityName	synapseStore	foo	date_1	datetime_1	datetime_2	datetime_3	multiple_strings	multiple_dates	multiple_bools	multiple_ints	multiple_floats\n"
            self.row1 = (
                '%s	%s	%s	"%s;https://www.example.com"	provName		bar	2020-01-01	2023-12-04T07:00:00Z	2023-12-05 23:37:02+00:00	2023-12-05 07:00:00+00:00	a,b,c,d	2020-01-01,2023-12-04T07:00:00.111Z,2023-12-05 23:37:02.333+00:00,2023-12-05 07:00:00+00:00	fAlSe,False,tRuE,True	1,2,3,4	1.2,3.4,5.6,7.8\n'
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
            self.row3 = '%s	%s	"syn12"		prov2	False	baz\n' % (self.f3, self.folder.id)
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


@tracer.start_as_current_span("test_synapseutils_sync::test_readManifest")
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


@tracer.start_as_current_span("test_synapseutils_sync::test_syncToSynapse")
# @pytest.mark.flaky(reruns=3)
def test_syncToSynapse(test_state):
    # Test upload of accurate manifest
    manifest = _makeManifest(
        test_state.header + test_state.row1 + test_state.row2 + test_state.row3,
        test_state.schedule_for_cleanup,
    )
    synapseutils.syncToSynapse(test_state.syn, manifest, sendMessages=False, retries=2)

    # syn.getChildren() used by syncFromSynapse() may intermittently have timing issues
    time.sleep(3)

    # Download using syncFromSynapse
    tmpdir = tempfile.mkdtemp()
    test_state.schedule_for_cleanup(tmpdir)
    try:
        execute_sync_from_synapse(test_state.syn, test_state.project, path=tmpdir)
    except FunctionTimedOut:
        test_state.syn.logger.warning("test_syncToSynapse timed out")
        pytest.fail("test_syncToSynapse timed out")

    orig_df = pd.read_csv(manifest, sep="\t")
    orig_df.index = [os.path.basename(p) for p in orig_df.path]
    new_df = pd.read_csv(
        os.path.join(tmpdir, synapseutils.sync.MANIFEST_FILENAME), sep="\t"
    )
    new_df.index = [os.path.basename(p) for p in new_df.path]

    assert len(orig_df) == len(new_df)
    new_df = new_df.loc[orig_df.index]

    # Validate what was uploaded is in right location
    assert new_df.parent.equals(
        orig_df.parent
    ), "Downloaded files not stored in same location"

    # Validate that annotations were set
    cols = (
        synapseutils.sync.REQUIRED_FIELDS
        + synapseutils.sync.FILE_CONSTRUCTOR_FIELDS
        + synapseutils.sync.STORE_FUNCTION_FIELDS
        + synapseutils.sync.PROVENANCE_FIELDS
    )
    orig_anots = orig_df.drop(cols, axis=1, errors="ignore")
    new_anots = new_df.drop(cols, axis=1, errors="ignore")
    assert (
        orig_anots.shape[1] == new_anots.shape[1]
    )  # Verify that we have the same number of cols

    assert new_anots.loc[:]["foo"].equals(orig_anots.loc[:]["foo"])
    # The dates in the manifest can accept a variety of formats, however we are always writing
    # them back in the same expected format. Verify they're converted correctly.
    assert new_anots.loc[:]["date_1"].tolist() == [
        "2020-01-01T00:00:00Z",
        np.nan,
        np.nan,
    ]
    assert new_anots.loc[:]["datetime_1"].tolist() == [
        "2023-12-04T07:00:00Z",
        np.nan,
        np.nan,
    ]
    assert new_anots.loc[:]["datetime_2"].tolist() == [
        "2023-12-05T23:37:02Z",
        np.nan,
        np.nan,
    ]
    assert new_anots.loc[:]["datetime_3"].tolist() == [
        "2023-12-05T07:00:00Z",
        np.nan,
        np.nan,
    ]
    assert new_anots.loc[:]["multiple_strings"].tolist() == [
        "a,b,c,d",
        np.nan,
        np.nan,
    ]
    assert new_anots.loc[:]["multiple_dates"].tolist() == [
        "2020-01-01T00:00:00Z,2023-12-04T07:00:00.111Z,2023-12-05T23:37:02.333Z,2023-12-05T07:00:00Z",
        np.nan,
        np.nan,
    ]
    assert new_anots.loc[:]["multiple_bools"].tolist() == [
        "False,False,True,True",
        np.nan,
        np.nan,
    ]
    assert new_anots.loc[:]["multiple_ints"].tolist() == [
        "1,2,3,4",
        np.nan,
        np.nan,
    ]
    assert new_anots.loc[:]["multiple_floats"].tolist() == [
        "1.2,3.4,5.6,7.8",
        np.nan,
        np.nan,
    ]

    # Validate that the Annotations uploaded to Synapse are correct when retrieving
    # them through syn.get. Also verify that they are parsed into the correct Python type
    syn_id_first_file = new_df.loc[:]["id"][0]
    synapse_file_instance = test_state.syn.get(syn_id_first_file, downloadFile=False)
    assert synapse_file_instance.foo == ["bar"]
    assert synapse_file_instance.date_1 == [
        datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    ]
    assert synapse_file_instance.datetime_1 == [
        datetime.datetime(2023, 12, 4, 7, 0, 0, tzinfo=datetime.timezone.utc)
    ]
    assert synapse_file_instance.datetime_2 == [
        datetime.datetime(2023, 12, 5, 23, 37, 2, tzinfo=datetime.timezone.utc)
    ]
    assert synapse_file_instance.datetime_3 == [
        datetime.datetime(2023, 12, 5, 7, 0, 0, tzinfo=datetime.timezone.utc)
    ]
    assert synapse_file_instance.multiple_strings == ["a", "b", "c", "d"]
    assert synapse_file_instance.multiple_dates == [
        datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2023, 12, 4, 7, 0, 0, 111000, tzinfo=datetime.timezone.utc),
        datetime.datetime(2023, 12, 5, 23, 37, 2, 333000, tzinfo=datetime.timezone.utc),
        datetime.datetime(2023, 12, 5, 7, 0, 0, tzinfo=datetime.timezone.utc),
    ]
    assert synapse_file_instance.multiple_bools == [False, False, True, True]
    assert synapse_file_instance.multiple_ints == [1, 2, 3, 4]
    assert synapse_file_instance.multiple_floats == [1.2, 3.4, 5.6, 7.8]

    # Validate that provenance is correct
    for provenanceType in ["executed", "used"]:
        # Go through each row
        for orig, new in zip(orig_df[provenanceType], new_df[provenanceType]):
            if not pd.isnull(orig) and not pd.isnull(new):
                # Must strip out white space in original because thats what happens for the new
                original = [prov.strip() for prov in orig.split(";")]
                # Convert local file paths into synId.versionNumber strings
                orig_list = [
                    "%s.%s" % (i.id, i.versionNumber) if isinstance(i, Entity) else i
                    for i in test_state.syn._convertProvenanceList(original)
                ]
                new_list = [
                    "%s.%s" % (i.id, i.versionNumber) if isinstance(i, Entity) else i
                    for i in test_state.syn._convertProvenanceList(new.split(";"))
                ]
                assert set(orig_list) == set(new_list)


@tracer.start_as_current_span("test_synapseutils_sync::test_syncFromSynapse")
@pytest.mark.flaky(reruns=3)
def test_syncFromSynapse(test_state):
    """This function tests recursive download as defined in syncFromSynapse
    most of the functionality of this function are already tested in the
    tests/integration/test_command_line_client::test_command_get_recursive_and_query

    which means that the only test if for path=None
    """
    # Create a Project
    project_entity = test_state.syn.store(Project(name=str(uuid.uuid4())))
    test_state.schedule_for_cleanup(project_entity.id)

    # Create a Folder in Project
    folder_entity = test_state.syn.store(
        Folder(name=str(uuid.uuid4()), parent=project_entity)
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
    test_state.syn.store(File(f, parent=project_entity))

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


@tracer.start_as_current_span(
    "test_synapseutils_sync::test_syncFromSynapse_children_contain_non_file"
)
@pytest.mark.flaky(reruns=3)
def test_syncFromSynapse_children_contain_non_file(test_state):
    proj = test_state.syn.store(
        Project(name="test_syncFromSynapse_children_non_file" + str(uuid.uuid4()))
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


@tracer.start_as_current_span("test_synapseutils_sync::test_syncFromSynapse_Links")
@pytest.mark.flaky(reruns=3)
def test_syncFromSynapse_Links(test_state):
    """This function tests recursive download of links as defined in syncFromSynapse
    most of the functionality of this function are already tested in the
    tests/integration/test_command_line_client::test_command_get_recursive_and_query

    which means that the only test if for path=None
    """
    # Create a Project
    project_entity = test_state.syn.store(Project(name=str(uuid.uuid4())))
    test_state.schedule_for_cleanup(project_entity.id)

    # Create a Folder in Project
    folder_entity = test_state.syn.store(
        Folder(name=str(uuid.uuid4()), parent=project_entity)
    )
    # Create a Folder hierarchy in folder_entity
    inner_folder_entity = test_state.syn.store(
        Folder(name=str(uuid.uuid4()), parent=folder_entity)
    )

    second_folder_entity = test_state.syn.store(
        Folder(name=str(uuid.uuid4()), parent=project_entity)
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


@tracer.start_as_current_span(
    "test_synapseutils_sync::test_write_manifest_data_unicode_characters_in_rows"
)
def test_write_manifest_data_unicode_characters_in_rows(test_state):
    # SYNPY-693

    named_temp_file = tempfile.NamedTemporaryFile("w")
    named_temp_file.close()
    test_state.schedule_for_cleanup(named_temp_file.name)

    keys = ["col_A", "col_B"]
    data = [{"col_A": "asdf", "col_B": "qwerty"}, {"col_A": "凵𠘨工匚口刀乇", "col_B": "丅乇丂丅"}]
    synapseutils.sync._write_manifest_data(named_temp_file.name, keys, data)

    df = pd.read_csv(named_temp_file.name, sep="\t", encoding="utf8")

    for dfrow, datarow in zip(df.itertuples(), data):
        assert datarow["col_A"] == dfrow.col_A
        assert datarow["col_B"] == dfrow.col_B


@tracer.start_as_current_span(
    "test_synapseutils_sync::test_syncFromSynapse_given_file_id"
)
@pytest.mark.flaky(reruns=3)
def test_syncFromSynapse_given_file_id(test_state):
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
