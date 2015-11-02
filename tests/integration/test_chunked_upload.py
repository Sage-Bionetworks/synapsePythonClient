# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import filecmp
import os, sys, traceback

import synapseclient
import synapseclient.utils as utils
from synapseclient.utils import MB, GB
from synapseclient import Activity, Entity, Project, Folder, File
import tempfile

import integration
from integration import schedule_for_cleanup


def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn
    module.project = integration.project

def test_round_trip():
    fh = None
    filepath = utils.make_bogus_binary_file(6*MB + 777771)
    print('Made bogus file: ', filepath)
    try:
        fh = syn._chunkedUploadFile(filepath)
        # print('FileHandle:')
        # syn.printEntity(fh)

        # Download the file and compare it with the original
        junk = File(filepath, parent=project, dataFileHandleId=fh['id'])
        junk.properties.update(syn._createEntity(junk.properties))
        junk.update(syn._downloadFileEntity(junk, filepath))
        assert filecmp.cmp(filepath, junk.path)

    finally:
        try:
            if 'junk' in locals():
                syn.delete(junk)
        except Exception:
            print(traceback.format_exc())
        try:
            os.remove(filepath)
        except Exception:
            print(traceback.format_exc())
        # if fh:
        #     # print('Deleting fileHandle', fh['id'])
        #     syn._deleteFileHandle(fh)

def manually_check_retry_on_key_does_not_exist():
    ## This is a manual test -- don't know how to automate this one.
    ## To run: nosetests -vs tests/integration/test_chunked_upload.py:manually_check_retry_on_key_does_not_exist
    ## We're testing the retrying of key-does-not-exist errors from S3.

    ## Expected behavior: Retries several times, getting a error message:
    ## 'The specified key does not exist.', then fails with a stack trace.
    i = 1
    filepath = utils.make_bogus_binary_file(6*MB)

    try:
        token = syn._createChunkedFileUploadToken(filepath, 'application/octet-stream')
        chunkRequest, url = syn._createChunkedFileUploadChunkURL(i, token)
        ## never upload the chunk, so we will get an error 'The specified key does not exist.'
        chunkResult = syn._addChunkToFile(chunkRequest)
    finally:
        os.remove(filepath)

def test_upload_string():
    ## This tests the utility that uploads  a _string_ rather than 
    ## a file on disk, to S3.
    
    fh = None
    content = "My dog has fleas.\n"
    f = tempfile.NamedTemporaryFile(mode='w', suffix=".txt", delete=False)
    f.write(content)
    f.close()
    filepath=f.name
        
    print('Made bogus file: ', filepath)
    try:
        fh = syn._uploadStringToFile(content)
        # print('FileHandle:')
        # syn.printEntity(fh)

        # Download the file and compare it with the original
        junk = File(filepath, parent=project, dataFileHandleId=fh['id'])
        junk.properties.update(syn._createEntity(junk.properties))
        junk.update(syn._downloadFileEntity(junk, filepath))
        assert filecmp.cmp(filepath, junk.path)

    finally:
        try:
            if 'junk' in locals():
                syn.delete(junk)
        except Exception:
            print(traceback.format_exc())
        try:
            os.remove(filepath)
        except Exception:
            print(traceback.format_exc())
            
