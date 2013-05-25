import os
import sys

import synapseclient
import synapseclient.utils as utils
from synapseclient.utils import MB
from synapseclient import Activity, Entity, Project, Folder, File, Data

import integration
from integration import create_project, schedule_for_cleanup


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = integration.syn

def test_chunked_file_upload():
    fh = None
    filepath = utils.make_bogus_binary_file(64*MB, verbose=True)
    print 'Made bogus file: ', filepath
    try:
        fh = syn._chunkedUploadFile(filepath, verbose=True)

        print "=" * 60
        print "FileHandle:"
        syn.printEntity(fh)
    finally:
        try:
            os.remove(filepath)
        except Exception as ex:
            print ex
        if fh:
            print 'Deleting fileHandle', fh['id']
            syn._deleteFileHandle(fh)


def test_key_does_not_exist():
    i = 1
    filepath = utils.make_bogus_binary_file(6*MB, verbose=True)

    try:
        token = syn._createChunkedFileUploadToken(filepath, "application/octet-stream")
        chunkRequest, url = syn._createChunkedFileUploadChunkURL(i, token)
        ## never upload the chunk, so we will get an error "The specified key does not exist."
        chunkResult = syn._addChunkToFile(chunkRequest, verbose=True)
    finally:
        os.remove(filepath)

