# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import uuid
import os
import time
import tempfile

from nose.tools import assert_raises, assert_equals, assert_less, assert_in, assert_true

import synapseclient
from synapseclient import Project, Folder, File, Entity, Schema, Link
from synapseclient.exceptions import *
import synapseutils
import integration
from integration import schedule_for_cleanup, QUERY_TIMEOUT_SEC
import pandas as pd


def setup(module):

    module.syn = integration.syn

    module.project = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(module.project)
    module.folder = syn.store(Folder(name=str(uuid.uuid4()), parent=module.project))

    # Create testfiles for upload
    module.f1 = utils.make_bogus_data_file(n=10)
    module.f2 = utils.make_bogus_data_file(n=10)
    f3 = 'https://www.synapse.org'

    schedule_for_cleanup(module.f1)
    schedule_for_cleanup(module.f2)

    module.header = 'path	parent	used	executed	activityName	synapseStore	foo\n'
    module.row1 = '%s	%s	%s	"%s;https://www.example.com"	provName		bar\n' % (f1, project.id, f2, f3)
    module.row2 = '%s	%s	"syn12"	"syn123;https://www.example.com"	provName2		bar\n' % (f2, folder.id)
    module.row3 = '%s	%s	"syn12"		prov2	False	baz\n' % (f3, folder.id)
    module.row4 = '%s	%s	%s		act		2\n' % (f3, project.id, f1)  # Circular reference
    module.row5 = '%s	syn12					\n' % (f3)  # Wrong parent


def _makeManifest(content):
    with tempfile.NamedTemporaryFile(mode='w', suffix=".dat", delete=False) as f:
        f.write(content)
        filepath = utils.normalize_path(f.name)
    schedule_for_cleanup(filepath)        
    return filepath


def test_readManifest():
    """Creates multiple manifests and verifies that they validate correctly"""
    # Test manifest with missing columns
    manifest = _makeManifest('"path"\t"foo"\n#"result_data.txt"\t"syn123"')
    assert_raises(ValueError, synapseutils.sync.readManifestFile, syn, manifest)

    # Test that there are no circular references in file and that Provenance is correct
    manifest = _makeManifest(header+row1+row2+row4) 
    assert_raises(RuntimeError, synapseutils.sync.readManifestFile, syn, manifest)

    # Test non existent parent
    manifest = _makeManifest(header+row1+row5)
    assert_raises(SynapseHTTPError, synapseutils.sync.readManifestFile, syn, manifest)

    # Test that all files exist in manifest
    manifest = _makeManifest(header+row1+row2+'/bara/basdfasdf/8hiuu.txt	syn123\n') 
    assert_raises(IOError, synapseutils.sync.readManifestFile, syn, manifest)


def test_syncToSynapse():
    # Test upload of accurate manifest
    manifest = _makeManifest(header+row1+row2+row3)
    synapseutils.syncToSynapse(syn, manifest, sendMessages=False, retries=2)

    # syn.getChildren() used by syncFromSynapse() may intermittently have timing issues
    time.sleep(3)

    # Download using syncFromSynapse
    tmpdir = tempfile.mkdtemp()
    schedule_for_cleanup(tmpdir)
    entities = synapseutils.syncFromSynapse(syn, project, path=tmpdir)
    
    orig_df = pd.read_csv(manifest, sep='\t')
    orig_df.index = [os.path.basename(p) for p in orig_df.path]
    new_df = pd.read_csv(os.path.join(tmpdir, synapseutils.sync.MANIFEST_FILENAME), sep='\t')
    new_df.index = [os.path.basename(p) for p in new_df.path]

    assert_equals(len(orig_df), len(new_df))
    new_df = new_df.loc[orig_df.index]

    # Validate what was uploaded is in right location
    assert_true(new_df.parent.equals(orig_df.parent), 'Downloaded files not stored in same location')

    # Validate that annotations were set
    cols = synapseutils.sync.REQUIRED_FIELDS + synapseutils.sync.FILE_CONSTRUCTOR_FIELDS\
           + synapseutils.sync.STORE_FUNCTION_FIELDS
    orig_anots = orig_df.drop(cols, axis=1, errors='ignore')
    new_anots = new_df.drop(cols, axis=1, errors='ignore')
    assert_equals(orig_anots.shape[1], new_anots.shape[1])  # Verify that we have the same number of cols
    assert_true(new_anots.equals(orig_anots.loc[:, new_anots.columns]), 'Annotations different')
    
    # Validate that provenance is correct
    for provenanceType in ['executed', 'used']:
        # Go through each row
        for orig, new in zip(orig_df[provenanceType], new_df[provenanceType]):
            if not pd.isnull(orig) and not pd.isnull(new):
                # Convert local file paths into synId.versionNumber strings
                orig_list = ['%s.%s' % (i.id, i.versionNumber) if isinstance(i, Entity) else i
                             for i in syn._convertProvenanceList(orig.split(';'))]
                new_list = ['%s.%s' % (i.id, i.versionNumber) if isinstance(i, Entity) else i
                             for i in syn._convertProvenanceList(new.split(';'))]
                assert_equals(set(orig_list), set(new_list))

        
def test_syncFromSynapse():
    """This function tests recursive download as defined in syncFromSynapse
    most of the functionality of this function are already tested in the 
    tests/integration/test_command_line_client::test_command_get_recursive_and_query

    which means that the only test if for path=None
    """
    # Create a Project
    project_entity = syn.store(synapseclient.Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(project_entity.id)

    # Create a Folder in Project
    folder_entity = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))

    # Create and upload two files in Folder
    uploaded_paths = []
    for i in range(2):
        f = utils.make_bogus_data_file()
        uploaded_paths.append(f)
        schedule_for_cleanup(f)
        syn.store(File(f, parent=folder_entity))
    # Add a file in the project level as well
    f = utils.make_bogus_data_file()
    uploaded_paths.append(f)
    schedule_for_cleanup(f)
    syn.store(File(f, parent=project_entity))

    # syncFromSynapse() uses chunkedQuery() which will return results that are eventually consistent
    # but not always right after the entity is created.
    start_time = time.time()
    while len(list(syn.getChildren(project_entity))) != 2:
        assert_less(time.time() - start_time, QUERY_TIMEOUT_SEC)
        time.sleep(2)

    # Test recursive get
    output = synapseutils.syncFromSynapse(syn, project_entity)

    assert_equals(len(output), len(uploaded_paths))
    for f in output:
        assert_in(f.path, uploaded_paths)


def test_syncFromSynapse__children_contain_non_file():
    proj = syn.store(Project(name="test_syncFromSynapse_children_non_file" + str(uuid.uuid4())))
    schedule_for_cleanup(proj)

    temp_file = utils.make_bogus_data_file()
    schedule_for_cleanup(temp_file)
    file_entity = syn.store(File(temp_file, name="temp_file_test_syncFromSynapse_children_non_file" + str(uuid.uuid4()),
                                 parent=proj))

    syn.store(Schema(name="table_test_syncFromSynapse", parent=proj))

    temp_folder = tempfile.mkdtemp()
    schedule_for_cleanup(temp_folder)

    files_list = synapseutils.syncFromSynapse(syn, proj, temp_folder)
    assert_equals(1, len(files_list))
    assert_equals(file_entity, files_list[0])


def test_syncFromSynapse_Links():
    """This function tests recursive download of links as defined in syncFromSynapse
    most of the functionality of this function are already tested in the 
    tests/integration/test_command_line_client::test_command_get_recursive_and_query

    which means that the only test if for path=None
    """
    # Create a Project
    project_entity = syn.store(synapseclient.Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(project_entity.id)

    # Create a Folder in Project
    folder_entity = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    # Create a Folder hierarchy in folder_entity
    inner_folder_entity = syn.store(Folder(name=str(uuid.uuid4()), parent=folder_entity))

    second_folder_entity = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))

    # Create and upload two files in Folder
    uploaded_paths = []
    for i in range(2):
        f = utils.make_bogus_data_file()
        uploaded_paths.append(f)
        schedule_for_cleanup(f)
        file_entity = syn.store(File(f, parent=project_entity))
        # Create links to inner folder
        syn.store(Link(file_entity.id, parent=folder_entity))
    # Add a file in the project level as well
    f = utils.make_bogus_data_file()
    uploaded_paths.append(f)
    schedule_for_cleanup(f)
    file_entity = syn.store(File(f, parent=second_folder_entity))
    # Create link to inner folder
    syn.store(Link(file_entity.id, parent=inner_folder_entity))

    # Test recursive get
    output = synapseutils.syncFromSynapse(syn, folder_entity, followLink=True)

    assert_equals(len(output), len(uploaded_paths))
    for f in output:
        assert_in(f.path, uploaded_paths)


def test_write_manifest_data__unicode_characters_in_rows():
    # SYNPY-693

    named_temp_file = tempfile.NamedTemporaryFile('w')
    named_temp_file.close()
    schedule_for_cleanup(named_temp_file.name)

    keys = ["col_A", "col_B"]
    data = [
        {'col_A': 'asdf', 'col_B': 'qwerty'},
        {'col_A': u'凵𠘨工匚口刀乇', 'col_B': u'丅乇丂丅'}
    ]
    synapseutils.sync._write_manifest_data(named_temp_file.name, keys, data)

    df = pd.read_csv(named_temp_file.name, sep='\t', encoding='utf8')

    for dfrow, datarow in zip(df.itertuples(), data):
        assert_equals(datarow['col_A'], dfrow.col_A)
        assert_equals(datarow['col_B'], dfrow.col_B)
