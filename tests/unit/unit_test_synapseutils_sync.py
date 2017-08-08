from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os

import unit
from mock import patch
from nose import SkipTest

import synapseutils
from synapseclient import Project

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = unit.syn

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