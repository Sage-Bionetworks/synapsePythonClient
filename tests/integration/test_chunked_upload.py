import filecmp
import os
import sys

import synapseclient
import synapseclient.utils as utils
from synapseclient.utils import MB, GB
from synapseclient import Activity, Entity, Project, Folder, File, Data

import integration
from integration import create_project, schedule_for_cleanup


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = integration.syn

def test_round_trip():
    fh = None
    filepath = utils.make_bogus_binary_file(6*MB + 777771, verbose=True)
    print 'Made bogus file: ', filepath
    try:
        fh = syn._chunkedUploadFile(filepath, verbose=False)

        print '=' * 60
        print 'FileHandle:'
        syn.printEntity(fh)

        print 'creating project and file'
        project = create_project()
        junk = File(filepath, parent=project, dataFileHandleId=fh['id'])
        junk.properties.update(syn._createEntity(junk.properties))

        print 'downloading file'
        junk.update(syn._downloadFileEntity(junk, filepath))

        print 'comparing files'
        assert filecmp.cmp(filepath, junk.path)

        print 'ok!'

    finally:
        try:
            if 'junk' in locals():
                syn.delete(junk)
        except Exception as ex:
            print ex
        try:
            os.remove(filepath)
        except Exception as ex:
            print ex
        if fh:
            print 'Deleting fileHandle', fh['id']
            syn._deleteFileHandle(fh)

def manually_check_retry_on_key_does_not_exist():
    ## This is a manual test -- don't know how to automate this one.
    ## To run: nosetests -vs tests/integration/test_chunked_upload.py:manually_check_retry_on_key_does_not_exist
    ## We're testing the retrying of key-does-not-exist errors from S3.

    ## Expected behavior: Retries several times, getting a error message:
    ## 'The specified key does not exist.', then fails with a stack trace.
    i = 1
    filepath = utils.make_bogus_binary_file(6*MB, verbose=True)

    try:
        token = syn._createChunkedFileUploadToken(filepath, 'application/octet-stream')
        chunkRequest, url = syn._createChunkedFileUploadChunkURL(i, token)
        ## never upload the chunk, so we will get an error 'The specified key does not exist.'
        chunkResult = syn._addChunkToFile(chunkRequest, verbose=True)
    finally:
        os.remove(filepath)

