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
from synapseclient.remote_file_storage_wrappers import SFTPWrapper

import integration
from integration import schedule_for_cleanup

DESTINATIONS =  [{"uploadType": "SFTP", 
                  "description" :'EC2 subfolder A',
                  "supportsSubfolders": True,
                  "url": "sftp://ec2-54-212-85-156.us-west-2.compute.amazonaws.com/public/pythonClientIntegration/test%20space",
                  "banner": "Uploading file to EC2\n"}, 
                 {"uploadType": "SFTP", 
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
    destinations = [syn.createStorageLocationSetting('ExternalStorageLocationSetting', **x)['storageLocationId'] for x in DESTINATIONS]
    module._sftp_project_setting_id = syn.applyStorageLocationSetting(project, destinations)['id']

def teardown(module):
    syn.restDELETE('/projectSettings/%s' % module._sftp_project_setting_id)

def test_synStore_sftpIntegration():
    """Creates a File Entity on an sftp server and add the external url. """
    filepath = utils.make_bogus_binary_file(1*MB - 777771)
    try:
        file = syn.store(File(filepath, parent=project))
        file2  = syn.get(file)
        assert file.externalURL==file2.externalURL and urlparse(file2.externalURL).scheme=='sftp'

        tmpdir = tempfile.mkdtemp()
        schedule_for_cleanup(tmpdir)

        ## test that we got an MD5 à la SYNPY-185
        assert_is_not_none(file2.md5)
        fh = syn._getFileHandle(file2.dataFileHandleId)
        assert_is_not_none(fh['contentMd5'])
        assert_equals(file2.md5, fh['contentMd5'])
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

    username, password = syn._getUserCredentials(serverURL)


    url = SFTPWrapper.upload_file(filepath, url=serverURL, username=username, password=password)
    file = syn.store(File(path=url, parent=project, synapseStore=False))

    print('\nDownloading file', os.getcwd(), filepath)
    junk = syn.get(file, downloadLocation=os.getcwd(), downloadFile=True)
    filecmp.cmp(filepath, junk.path)


def test_utils_sftp_upload_and_download():
    """Tries to upload a file to an sftp file """
    serverURL='sftp://ec2-54-212-85-156.us-west-2.compute.amazonaws.com/public/'+str(uuid.uuid1())
    filepath = utils.make_bogus_binary_file(1*MB - 777771)

    tempdir = tempfile.mkdtemp()

    username, password = syn._getUserCredentials(serverURL)

    try:
        print('\n\tMade bogus file: ', filepath)
        url = SFTPWrapper.upload_file(filepath, url=serverURL, username=username, password=password)
        print('\tStored URL:', url)
        print('\tDownloading',)
        #Get with a specified localpath
        junk = SFTPWrapper.download_file(url, tempdir, username=username, password=password)
        print('\tComparing:', junk, filepath)
        filecmp.cmp(filepath, junk)
        #Get without specifying path
        print('\tDownloading',)
        junk2 = SFTPWrapper.download_file(url, username=username, password=password)
        print('\tComparing:', junk2, filepath)
        filecmp.cmp(filepath, junk2)
        #Get with a specified localpath as file
        print('\tDownloading',)
        junk3 = SFTPWrapper.download_file(url, os.path.join(tempdir, 'bar.dat'), username=username, password=password)
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



    
