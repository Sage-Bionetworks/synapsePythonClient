import uuid
import os
import time
import tempfile
import pandas as pd

import pytest

from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient import Entity, File, Folder, Link, Project, Schema
import synapseclient.core.utils as utils
import synapseutils

from tests.integration import QUERY_TIMEOUT_SEC


@pytest.fixture(scope='module', autouse=True)
def test_state(syn, schedule_for_cleanup):
    class TestState:
        def __init__(self):
            self.syn = syn
            self.project = syn.store(Project(name=str(uuid.uuid4())))
            self.folder = syn.store(Folder(name=str(uuid.uuid4()), parent=self.project))
            self.schedule_for_cleanup = schedule_for_cleanup

            # Create testfiles for upload
            self.f1 = utils.make_bogus_data_file(n=10)
            self.f2 = utils.make_bogus_data_file(n=10)
            self.f3 = 'https://www.synapse.org'

            self.header = 'path	parent	used	executed	activityName	synapseStore	foo\n'
            self.row1 = '%s	%s	%s	"%s;https://www.example.com"	provName		bar\n' % (
                self.f1, self.project.id, self.f2, self.f3
            )
            self.row2 = '%s	%s	"syn12"	"syn123;https://www.example.com"	provName2		bar\n' % (
                self.f2, self.folder.id
            )
            self.row3 = '%s	%s	"syn12"		prov2	False	baz\n' % (self.f3, self.folder.id)
            self.row4 = '%s	%s	%s		act		2\n' % (self.f3, self.project.id, self.f1)  # Circular reference
            self.row5 = '%s	syn12					\n' % (self.f3)  # Wrong parent

    test_state = TestState()
    schedule_for_cleanup(test_state.project)
    schedule_for_cleanup(test_state.f1)
    schedule_for_cleanup(test_state.f2)

    return test_state


def _makeManifest(content, schedule_for_cleanup):
    with tempfile.NamedTemporaryFile(mode='w', suffix=".dat", delete=False) as f:
        f.write(content)
        filepath = utils.normalize_path(f.name)
    schedule_for_cleanup(filepath)
    return filepath


def test_readManifest(test_state):
    """Creates multiple manifests and verifies that they validate correctly"""
    # Test manifest with missing columns
    manifest = _makeManifest(
        '"path"\t"foo"\n#"result_data.txt"\t"syn123"',
        test_state.schedule_for_cleanup
    )
    pytest.raises(ValueError, synapseutils.sync.readManifestFile, test_state.syn, manifest)

    # Test that there are no circular references in file and that Provenance is correct
    manifest = _makeManifest(
        test_state.header + test_state.row1 + test_state.row2 + test_state.row4,
        test_state.schedule_for_cleanup
    )
    pytest.raises(RuntimeError, synapseutils.sync.readManifestFile, test_state.syn, manifest)

    # Test non existent parent
    manifest = _makeManifest(
        test_state.header + test_state.row1 + test_state.row5,
        test_state.schedule_for_cleanup
    )
    pytest.raises(SynapseHTTPError, synapseutils.sync.readManifestFile, test_state.syn, manifest)

    # Test that all files exist in manifest
    manifest = _makeManifest(
        test_state.header + test_state.row1 + test_state.row2 + '/bara/basdfasdf/8hiuu.txt	syn123\n',
        test_state.schedule_for_cleanup
    )
    pytest.raises(IOError, synapseutils.sync.readManifestFile, test_state.syn, manifest)


def test_syncToSynapse(test_state):
    # Test upload of accurate manifest
    manifest = _makeManifest(
        test_state.header + test_state.row1 + test_state.row2 + test_state.row3,
        test_state.schedule_for_cleanup
    )
    synapseutils.syncToSynapse(test_state.syn, manifest, sendMessages=False, retries=2)

    # syn.getChildren() used by syncFromSynapse() may intermittently have timing issues
    time.sleep(3)

    # Download using syncFromSynapse
    tmpdir = tempfile.mkdtemp()
    test_state.schedule_for_cleanup(tmpdir)
    synapseutils.syncFromSynapse(test_state.syn, test_state.project, path=tmpdir)

    orig_df = pd.read_csv(manifest, sep='\t')
    orig_df.index = [os.path.basename(p) for p in orig_df.path]
    new_df = pd.read_csv(os.path.join(tmpdir, synapseutils.sync.MANIFEST_FILENAME), sep='\t')
    new_df.index = [os.path.basename(p) for p in new_df.path]

    assert len(orig_df) == len(new_df)
    new_df = new_df.loc[orig_df.index]

    # Validate what was uploaded is in right location
    assert new_df.parent.equals(orig_df.parent), 'Downloaded files not stored in same location'

    # Validate that annotations were set
    cols = synapseutils.sync.REQUIRED_FIELDS + synapseutils.sync.FILE_CONSTRUCTOR_FIELDS\
        + synapseutils.sync.STORE_FUNCTION_FIELDS + synapseutils.sync.PROVENANCE_FIELDS
    orig_anots = orig_df.drop(cols, axis=1, errors='ignore')
    new_anots = new_df.drop(cols, axis=1, errors='ignore')
    assert orig_anots.shape[1] == new_anots.shape[1]  # Verify that we have the same number of cols
    assert new_anots.equals(orig_anots.loc[:, new_anots.columns]), 'Annotations different'

    # Validate that provenance is correct
    for provenanceType in ['executed', 'used']:
        # Go through each row
        for orig, new in zip(orig_df[provenanceType], new_df[provenanceType]):
            if not pd.isnull(orig) and not pd.isnull(new):
                # Convert local file paths into synId.versionNumber strings
                orig_list = ['%s.%s' % (i.id, i.versionNumber) if isinstance(i, Entity) else i
                             for i in test_state.syn._convertProvenanceList(orig.split(';'))]
                new_list = ['%s.%s' % (i.id, i.versionNumber) if isinstance(i, Entity) else i
                            for i in test_state.syn._convertProvenanceList(new.split(';'))]
                assert set(orig_list) == set(new_list)


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
    folder_entity = test_state.syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))

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
    output = synapseutils.syncFromSynapse(test_state.syn, project_entity)

    assert len(output) == len(uploaded_paths)
    for f in output:
        assert f.path in uploaded_paths


def test_syncFromSynapse__children_contain_non_file(test_state):
    proj = test_state.syn.store(Project(name="test_syncFromSynapse_children_non_file" + str(uuid.uuid4())))
    test_state.schedule_for_cleanup(proj)

    temp_file = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(temp_file)
    file_entity = test_state.syn.store(
        File(
            temp_file,
            name="temp_file_test_syncFromSynapse_children_non_file" + str(uuid.uuid4()),
            parent=proj
        )
    )

    test_state.syn.store(Schema(name="table_test_syncFromSynapse", parent=proj))

    temp_folder = tempfile.mkdtemp()
    test_state.schedule_for_cleanup(temp_folder)

    files_list = synapseutils.syncFromSynapse(test_state.syn, proj, temp_folder)
    assert 1 == len(files_list)
    assert file_entity == files_list[0]


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
    folder_entity = test_state.syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    # Create a Folder hierarchy in folder_entity
    inner_folder_entity = test_state.syn.store(Folder(name=str(uuid.uuid4()), parent=folder_entity))

    second_folder_entity = test_state.syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))

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
    output = synapseutils.syncFromSynapse(test_state.syn, folder_entity, followLink=True)

    assert len(output) == len(uploaded_paths)
    for f in output:
        assert f.path in uploaded_paths


def test_write_manifest_data__unicode_characters_in_rows(test_state):
    # SYNPY-693

    named_temp_file = tempfile.NamedTemporaryFile('w')
    named_temp_file.close()
    test_state.schedule_for_cleanup(named_temp_file.name)

    keys = ["col_A", "col_B"]
    data = [
        {'col_A': 'asdf', 'col_B': 'qwerty'},
        {'col_A': u'凵𠘨工匚口刀乇', 'col_B': u'丅乇丂丅'}
    ]
    synapseutils.sync._write_manifest_data(named_temp_file.name, keys, data)

    df = pd.read_csv(named_temp_file.name, sep='\t', encoding='utf8')

    for dfrow, datarow in zip(df.itertuples(), data):
        assert datarow['col_A'] == dfrow.col_A
        assert datarow['col_B'] == dfrow.col_B


def test_syncFromSynapse__given_file_id(test_state):
    file_path = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(file_path)
    file = test_state.syn.store(File(file_path, name=str(uuid.uuid4()), parent=test_state.project, synapseStore=False))
    all_files = synapseutils.syncFromSynapse(test_state.syn, file.id)
    assert 1 == len(all_files)
    assert file == all_files[0]
