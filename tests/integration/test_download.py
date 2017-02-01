# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str
from nose.tools import assert_raises

import filecmp, os, tempfile, shutil

import synapseclient
import synapseclient.utils as utils
import synapseclient.cache as cache
from synapseclient.exceptions import *
from synapseclient.utils import MB, GB
from synapseclient import Activity, Entity, Project, Folder, File

import integration
from integration import schedule_for_cleanup
import json


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

    fileResult = syn._getFileHandleDownload(entity['dataFileHandleId'],entity['id'])
    syn._downloadFileHandle(fileResult['preSignedURL'], tempfile.gettempdir(), fileResult['fileHandle'])
    assert_raises(SynapseMd5MismatchError, syn._downloadFileHandle, fileResult['preSignedURL'],
                  tempfile.gettempdir(), {'contentMd5': '100000000000000000000',
                                          'id':entity['dataFileHandleId']})

def test_resume_partial_download():
    original_file = utils.make_bogus_data_file(40000)
    original_md5 = utils.md5_for_file(original_file).hexdigest()

    entity = File(original_file, parent=project['id'])
    entity = syn.store(entity)

    ## stash the original file for comparison later
    shutil.move(original_file, original_file+'.original')
    original_file += '.original'
    schedule_for_cleanup(original_file)

    temp_dir = tempfile.gettempdir()

    url = '%s/entity/%s/file' % (syn.repoEndpoint, entity.id)
    path = syn._download(url, destination=temp_dir, fileHandleId=entity.dataFileHandleId, expected_md5=entity.md5)

    ## simulate an imcomplete download by putting the
    ## complete file back into its temporary location
    tmp_path = utils.temp_download_filename(temp_dir, entity.dataFileHandleId)
    shutil.move(path, tmp_path)

    ## ...and truncating it to some fraction of its original size
    with open(tmp_path, 'r+') as f:
        f.truncate(3*os.path.getsize(original_file)//7)

    ## this should complete the partial download
    path = syn._download(url, destination=temp_dir, fileHandleId=entity.dataFileHandleId, expected_md5=entity.md5)

    assert filecmp.cmp(original_file, path), "File comparison failed"


def test_ftp_download():
    """Test downloading an Entity that points to a file on an FTP server. """
    
    # Another test with an external reference. This is because we only need to test FTP download; not upload. Also so we don't have to maintain an FTP server just for this purpose.
    # Make an entity that points to an FTP server file.
    entity = File(parent=project['id'], name = '1KB.zip')
    fileHandle = {}
    fileHandle['externalURL'] = 'ftp://speedtest.tele2.net/1KB.zip'
    fileHandle["fileName"] = entity.name
    fileHandle["contentType"] = "application/zip"
    fileHandle["contentMd5"] = '0f343b0931126a20f133d67c2b018a3b'
    fileHandle["contentSize"] = 1024
    fileHandle["concreteType"] = "org.sagebionetworks.repo.model.file.ExternalFileHandle"
    fileHandle = syn.restPOST('/externalFileHandle', json.dumps(fileHandle), syn.fileHandleEndpoint)
    entity.dataFileHandleId = fileHandle['id']
    entity = syn.store(entity)

    # Download the entity and check that MD5 matches expected
    FTPfile = syn.get(entity.id, downloadLocation=os.getcwd(), downloadFile=True)
    assert FTPfile.md5==utils.md5_for_file(FTPfile.path).hexdigest()
    schedule_for_cleanup(entity)
    os.remove(FTPfile.path)
