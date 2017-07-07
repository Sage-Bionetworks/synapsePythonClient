# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

import uuid, filecmp, os, sys, time, tempfile

from mock import patch

from nose.tools import assert_raises, assert_equals, assert_is_none, assert_less
from nose import SkipTest

import synapseclient
from synapseclient import Project, Folder, File, Entity
from synapseclient.exceptions import *
import synapseutils
import re
import integration
from integration import schedule_for_cleanup, QUERY_TIMEOUT_SEC

def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn

    module.project = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(module.project)
    module.folder = syn.store(Folder(name=str(uuid.uuid4()), parent=module.project))

    
    #Create testfiles for upload
    module.f1 = utils.make_bogus_data_file(n=10)
    module.f2 = utils.make_bogus_data_file(n=10)
    f3 = 'https://www.synapse.org'

    schedule_for_cleanup(module.f1)
    schedule_for_cleanup(module.f2)

    module.header = 'path	parent	used	executed	activityName	synapseStore	foo\n'
    module.row1 =   '%s	%s	%s	"%s;https://www.example.com"	provName		bar\n'  %(f1, project.id, f2, f3)
    module.row2 =   '%s	%s	"syn12"	"syn123;https://www.example.com"	provName2		bar\n' %(f2, folder.id)
    module.row3 =   '%s	%s	"syn12"		prov2	False	baz\n' %(f3, folder.id)
    module.row4 =   '%s	%s	%s		act		2\n'  %(f3, project.id, f1)  #Circular reference
    module.row5 =   '%s	syn12					\n'  %(f3)  #Wrong parent


def _makeManifest(content):
    with tempfile.NamedTemporaryFile(mode='w', suffix=".dat", delete=False) as f:
        f.write(content)
        filepath = utils.normalize_path(f.name)
    schedule_for_cleanup(filepath)        
    return filepath


def test_readManifest():
    """Creates multiple manifests and verifies that they validate correctly"""
    #Test manifest with missing columns
    manifest =  _makeManifest('"path"\t"foo"\n#"result_data.txt"\t"syn123"')
    assert_raises(ValueError, synapseutils.sync.readManifestFile, syn, manifest)

    #Test that there are no circular references in file and that Provenance is correct
    manifest = _makeManifest(header+row1+row2+row4) 
    assert_raises(RuntimeError, synapseutils.sync.readManifestFile, syn, manifest)

    #Test non existent parent
    manifest = _makeManifest(header+row1+row5)
    assert_raises(SynapseHTTPError, synapseutils.sync.readManifestFile, syn, manifest)

    #Test that all files exist in manifest
    manifest = _makeManifest(header+row1+row2+'/bara/basdfasdf/8hiuu.txt	syn123\n') 
    assert_raises(IOError, synapseutils.sync.readManifestFile, syn, manifest)

def test_readManifest__sync_order_with_home_directory():
    """SYNPY-508"""
    try:
        import pandas as pd
        import pandas.util.testing as pdt
    except:
        raise SkipTest("pandas was not found. Skipping test.")

    #row1's file depends on row2's file but is listed first
    file_path1 = '~/file1.txt'
    file_path2 = '~/file2.txt'
    row1 = '%s\t%s\t%s\t""\tprovActivity1\tTrue\tsomeFooAnnotation1\n' % (file_path1, project.id, file_path2)
    row2 = '%s\t%s\t""\t""\tprovActivity2\tTrue\tsomeFooAnnotation2\n' % (file_path2, project.id)
    manifest = _makeManifest(header + row1 + row2)

    #mock isfile() to always return true to avoid having to create files in the home directory
    with patch.object(os.path, "isfile", side_effect=[True,True,True,False]): #side effect mocks values for: manfiest file, file1.txt, file2.txt, isfile(project.id) check in syn.get()
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)
        expected_order = pd.Series([os.path.normpath(os.path.expanduser(file_path2)), os.path.normpath(os.path.expanduser(file_path1))])
        pdt.assert_series_equal(expected_order, manifest_dataframe.path, check_names=False)

def test_syncToSynapse():
    synapseclient.table.test_import_pandas()
    import pandas as pd

    #Test upload of accurate manifest
    manifest = _makeManifest(header+row1+row2+row3)
    synapseutils.syncToSynapse(syn, manifest, sendMessages=False, retries=2)

    #Download using syncFromSynapse
    tmpdir = tempfile.mkdtemp()
    schedule_for_cleanup(tmpdir)
    entities = synapseutils.syncFromSynapse(syn, project, path=tmpdir)
    
    orig_df = pd.read_csv(manifest, sep='\t')
    orig_df.index = [os.path.basename(p) for p in orig_df.path]
    new_df = pd.read_csv(os.path.join(tmpdir, synapseutils.sync.MANIFEST_FILENAME), sep='\t')
    new_df.index = [os.path.basename(p) for p in new_df.path]

    assert_equals(len(orig_df), len(new_df))
    new_df = new_df.loc[orig_df.index]

    #Validate what was uploaded is in right location
    assert new_df.parent.equals(orig_df.parent), 'Downloaded files not stored in same location'

    #Validate that annotations were set
    cols = synapseutils.sync.REQUIRED_FIELDS+ synapseutils.sync.FILE_CONSTRUCTOR_FIELDS+synapseutils.sync.STORE_FUNCTION_FIELDS
    orig_anots = orig_df.drop(cols, axis=1, errors='ignore')
    new_anots = new_df.drop(cols, axis=1, errors='ignore')
    assert_equals(orig_anots.shape[1], new_anots.shape[1])  #Verify that we have the same number of cols
    assert new_anots.equals(orig_anots.loc[:, new_anots.columns]), 'Annotations different'
    
    #Validate that provenance is correct
    for provenanceType in ['executed', 'used']:
        #Go through each row 
        for orig, new in zip(orig_df[provenanceType], new_df[provenanceType]):
            if not pd.isnull(orig) and not pd.isnull(new):
                #Convert local file paths into synId.versionNumber strings
                orig_list = ['%s.%s'%(i.id, i.versionNumber) if isinstance(i, Entity) else i
                             for i in syn._convertProvenanceList(orig.split(';'))]
                new_list =  ['%s.%s' %(i.id, i.versionNumber) if isinstance(i, Entity) else i
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
        f  = utils.make_bogus_data_file()
        uploaded_paths.append(f)
        schedule_for_cleanup(f)
        file_entity = syn.store(File(f, parent=folder_entity))
    #Add a file in the project level as well
    f  = utils.make_bogus_data_file()
    uploaded_paths.append(f)
    schedule_for_cleanup(f)
    file_entity = syn.store(File(f, parent=project_entity))

    #syncFromSynapse() uses chunkedQuery() which will return results that are eventually consistent but not always right after the entity is created.
    start_time = time.time()
    while syn.query("select id from entity where id=='%s'" % file_entity.id).get('totalNumberOfResults') <= 0:
        assert_less(time.time() - start_time, QUERY_TIMEOUT_SEC)
        time.sleep(2)

    ### Test recursive get
    output = synapseutils.syncFromSynapse(syn, project_entity)

    assert len(output) == len(uploaded_paths)
    for f in output:
        assert f.path in uploaded_paths


        
    

