# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import filecmp
import os, sys, traceback
import json
import uuid 
from nose.tools import assert_raises, assert_is_not_none, assert_equals
import tempfile
import shutil

from synapseclient.exceptions import *
import synapseclient
import synapseclient.utils as utils
from synapseclient.utils import MB, GB
from synapseclient import Activity, Entity, Project, Folder, File

import integration
from integration import schedule_for_cleanup

DESTINATIONS =  [{"uploadType": "SFTP", 
                  "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting", 
                  "description" :'EC2 subfolder A',
                  "supportsSubfolders": True,
                  "url": "sftp://ec2-54-212-85-156.us-west-2.compute.amazonaws.com/public/pythonClientIntegration/test%20space",
                  "banner": "Uploading file to EC2\n"}, 
                 {"uploadType": "SFTP", 
                  "concreteType": "org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting", 
                  "supportsSubfolders": True,
                  "description":'EC2 subfolder B',
                  "url": "sftp://ec2-54-212-85-156.us-west-2.compute.amazonaws.com/public/pythonClientIntegration/another_location",
                  "banner": "Uploading file to EC2 version 2\n"}
                 ] 



def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn
    module.project = integration.project
    #Create the upload destinations
    destinations = [syn.restPOST('/storageLocation', body=json.dumps(x)) for x in DESTINATIONS]
    project_destination = {"concreteType": "org.sagebionetworks.repo.model.project.UploadDestinationListSetting",
                           "settingsType": "upload"}
    project_destination['projectId'] = module.project.id
    project_destination['locations'] = [dest['storageLocationId'] for dest in destinations]
    project_destination = syn.restPOST('/projectSettings', body = json.dumps(project_destination))


def test_synStore_sftpIntegration():
    """Creates a File Entity on an sftp server and add the external url. """
    filepath = utils.make_bogus_binary_file(1*MB - 777771)
    try:
        file = syn.store(File(filepath, parent=project))
        file2  = syn.get(file)
        assert file.externalURL==file2.externalURL and urlparse(file2.externalURL).scheme=='sftp'

        tmpdir = tempfile.mkdtemp()
        schedule_for_cleanup(tmpdir)

        ## test filename override
        file2.fileNameOverride = "whats_new_in_baltimore.data"
        file2 = syn.store(file2)
        ## TODO We haven't defined how filename override interacts with
        ## TODO previously cached files so, side-step that for now by
        ## TODO making sure the file is not in the cache!
        syn.cache.remove(file2.dataFileHandleId, delete=True)
        file3 = syn.get(file, downloadLocation=tmpdir)
        assert os.path.basename(file3.path) == file2.fileNameOverride

        ## test that we got an MD5 à la SYNPY-185
        assert_is_not_none(file3.md5)
        fh = syn._getFileHandle(file3.dataFileHandleId)
        assert_is_not_none(fh['contentMd5'])
        assert_equals(file3.md5, fh['contentMd5'])
    finally:
        try:
            os.remove(filepath)
        except Exception:
            print(traceback.format_exc())


def test_synGet_sftpIntegration():
    #Create file by uploading directly to sftp and creating entity from URL
    serverURL='sftp://ec2-54-212-85-156.us-west-2.compute.amazonaws.com/public/'+str(uuid.uuid1())
    filepath = utils.make_bogus_binary_file(1*MB - 777771)
    print('\n\tMade bogus file: ', filepath)
    
    url = syn._sftpUploadFile(filepath, url=serverURL)
    file = syn.store(File(path=url, parent=project, synapseStore=False))

    print('\nDownloading file', os.getcwd(), filepath)
    junk = syn.get(file, downloadLocation=os.getcwd(), downloadFile=True)
    filecmp.cmp(filepath, junk.path)


def test_utils_sftp_upload_and_download():
    """Tries to upload a file to an sftp file """
    serverURL='sftp://ec2-54-212-85-156.us-west-2.compute.amazonaws.com/public/'+str(uuid.uuid1())
    filepath = utils.make_bogus_binary_file(1*MB - 777771)

    tempdir = tempfile.mkdtemp()

    try:
        print('\n\tMade bogus file: ', filepath)
        url = syn._sftpUploadFile(filepath, url=serverURL)
        print('\tStored URL:', url)
        print('\tDownloading',)
        #Get with a specified localpath
        junk = syn._sftpDownloadFile(url, tempdir)
        print('\tComparing:', junk, filepath)
        filecmp.cmp(filepath, junk)
        #Get without specifying path
        print('\tDownloading',)
        junk2 = syn._sftpDownloadFile(url)
        print('\tComparing:', junk2, filepath)
        filecmp.cmp(filepath, junk2)
        #Get with a specified localpath as file
        print('\tDownloading',)
        junk3 = syn._sftpDownloadFile(url, os.path.join(tempdir, 'bar.dat'))
        print('\tComparing:', junk3, filepath)
        filecmp.cmp(filepath, junk3)
    finally:
        try:
            if 'junk' in locals(): os.remove(junk)
            if 'junk2' in locals(): os.remove(junk2)
            if 'junk3' in locals(): os.remove(junk3)
        except Exception:
            print(traceback.format_exc())
        try:
            os.remove(filepath)
        except Exception:
            print(traceback.format_exc())
        try:
            shutil.rmtree(tempdir)
        except Exception:
            print(traceback.format_exc())



    
