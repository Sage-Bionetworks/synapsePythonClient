# coding=utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import tempfile

import unit
from mock import patch
from nose import SkipTest
from nose.tools import assert_dict_equal, assert_raises, assert_equals
from builtins import str

import synapseutils
from synapseclient import Project, Schema

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

try:
    import pandas as pd
    import pandas.util.testing as pdt
    pandas_available=True
except:
    pandas_available=False

def setup(module):

    module.syn = unit.syn

def test_readManifest__sync_order_with_home_directory():
    """SYNPY-508"""
    if not pandas_available:
        raise SkipTest("pandas was not found. Skipping test.")

    #row1's file depends on row2's file but is listed first
    file_path1 = '~/file1.txt'
    file_path2 = '~/file2.txt'
    project_id = "syn123"
    header = 'path	parent	used	executed	activityName	synapseStore	foo\n'
    row1 = '%s\t%s\t%s\t""\tprovActivity1\tTrue\tsomeFooAnnotation1\n' % (file_path1, project_id, file_path2)
    row2 = '%s\t%s\t""\t""\tprovActivity2\tTrue\tsomeFooAnnotation2\n' % (file_path2, project_id)

    manifest = StringIO(header+row1+row2)
    # mock syn.get() to return a project because the final check is making sure parent is a container
    #mock isfile() to always return true to avoid having to create files in the home directory
    with patch.object(syn, "get", return_value=Project()),\
         patch.object(os.path, "isfile", side_effect=[True,True,True,False]): #side effect mocks values for: manfiest file, file1.txt, file2.txt, isfile(project.id) check in syn.get()
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)
        expected_order = pd.Series([os.path.normpath(os.path.expanduser(file_path2)), os.path.normpath(os.path.expanduser(file_path1))])
        pdt.assert_series_equal(expected_order, manifest_dataframe.path, check_names=False)


def test_readManifestFile__synapseStore_values_not_set():
    if not pandas_available:
        raise SkipTest("pandas was not found. Skipping test.")

    project_id = "syn123"
    header = 'path\tparent\n'
    path1 = os.path.abspath(os.path.expanduser('~/file1.txt'))
    path2 = 'http://www.synapse.org'
    row1 = '%s\t%s\n' % (path1, project_id)
    row2 = '%s\t%s\n' % (path2,project_id)

    expected_synapseStore = {
        str(path1): True,
        str(path2): False,
    }

    manifest = StringIO(header+row1+row2)
    with patch.object(syn, "get", return_value=Project()),\
         patch.object(os.path, "isfile", return_value=True): #side effect mocks values for: file1.txt
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)
        actual_synapseStore = (manifest_dataframe.set_index('path')['synapseStore'].to_dict())
        assert_dict_equal(expected_synapseStore, actual_synapseStore)


def test_readManifestFile__synapseStore_values_are_set():
    if not pandas_available:
        raise SkipTest("pandas was not found. Skipping test.")

    project_id = "syn123"
    header = 'path\tparent\tsynapseStore\n'
    path1 = os.path.abspath(os.path.expanduser('~/file1.txt'))
    path2 = 'http://www.synapse.org'
    path3 = os.path.abspath(os.path.expanduser('~/file3.txt'))
    path4 = 'http://www.github.com'
    path5 = os.path.abspath(os.path.expanduser('~/file5.txt'))
    path6 = 'http://www.checkoutmymixtapefam.com/fire.mp3'

    row1 = '%s\t%s\tTrue\n' % (path1, project_id)
    row2 = '%s\t%s\tTrue\n' % (path2, project_id)
    row3 = '%s\t%s\tFalse\n' % (path3, project_id)
    row4 = '%s\t%s\tFalse\n' % (path4, project_id)
    row5 = '%s\t%s\t""\n' % (path5, project_id)
    row6 = '%s\t%s\t""\n' % (path6, project_id)

    expected_synapseStore = {
        str(path1): True,
        str(path2): False,
        str(path3): False,
        str(path4): False,
        str(path5): True,
        str(path6): False
    }

    manifest = StringIO(header+row1+row2+row3+row4+row5+row6)
    with patch.object(syn, "get", return_value=Project()),\
         patch.object(os.path, "isfile", return_value=True): #mocks values for: file1.txt, file3.txt, file5.txt
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)

        actual_synapseStore = (manifest_dataframe.set_index('path')['synapseStore'].to_dict())
        assert_dict_equal(expected_synapseStore, actual_synapseStore)


def test_syncFromSynapse__non_file_Entity():
    table_schema = "syn12345"
    with patch.object(syn, "getChildren", return_value = []),\
         patch.object(syn, "get", return_value = Schema(name="asssdfa", parent="whatever")):
        assert_raises(ValueError, synapseutils.syncFromSynapse, syn, table_schema)


def test_write_manifest_data__unicode_characters_in_rows():
    # SYNPY-693
    if not pandas_available:
        raise SkipTest("pandas was not found. Skipping test.")

    named_temp_file = tempfile.NamedTemporaryFile('w')
    named_temp_file.close()
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

    os.remove(named_temp_file.name)


def test_process_manifest_rows():
    pass