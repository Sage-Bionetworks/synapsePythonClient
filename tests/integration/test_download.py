# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

import filecmp, os, tempfile, filecmp

import synapseclient
import synapseclient.utils as utils
import synapseclient.cache as cache
from synapseclient.exceptions import *
from synapseclient.utils import MB, GB
from synapseclient import Activity, Entity, Project, Folder, File

import integration
from integration import schedule_for_cleanup



def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn
    module.project = integration.project


def test_download_check_md5():
    entity = File(parent=project['id'])
    entity['path'] = utils.make_bogus_data_file()
    schedule_for_cleanup(entity['path'])
    entity = syn.store(entity)

    print('expected MD5:', entity['md5'])

    url = '%s/entity/%s/file' % (syn.repoEndpoint, entity.id)
    syn._downloadFile(url, destination=tempfile.gettempdir(), expected_md5=entity['md5'])

    try:
        syn._downloadFile(url, destination=tempfile.gettempdir(), expected_md5='10000000000000000000000000000001')
    except SynapseMd5MismatchError as ex1:
        print("Expected exception:", ex1)
    else:
        assert False, "Should have raised SynapseMd5MismatchError"

