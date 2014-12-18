import filecmp
import os, sys, traceback
import json
import uuid 
import urlparse
from nose.tools import assert_raises
import tempfile
import shutil

from synapseclient.exceptions import *
import synapseclient
import synapseclient.utils as utils
from synapseclient.utils import MB, GB
from synapseclient import Activity, Entity, Project, Folder, File, Data

import integration
from integration import schedule_for_cleanup

upload_destination = {"concreteType": "org.sagebionetworks.repo.model.project.UploadDestinationListSetting",
  "destinations": [
        {"uploadType": "SFTP", 
         "banner": "Uploading file to EC2\n", 
         "concreteType": "org.sagebionetworks.repo.model.project.ExternalUploadDestinationSetting", 
         "url": "sftp://ec2-54-212-85-156.us-west-2.compute.amazonaws.com/public/pythonClientIntegration/test%20space",
         "supportsSubfolders": True}, 
        {"uploadType": "SFTP", 
         "banner": "Uploading file to EC2 version 2\n", 
         "concreteType": "org.sagebionetworks.repo.model.project.ExternalUploadDestinationSetting", 
         "url": "sftp://ec2-54-212-85-156.us-west-2.compute.amazonaws.com/public/pythonClientIntegration/another_location",
         "supportsSubfolders": True}, 
        ], 
  "projectId": '', 
  "settingsType": "upload"
}


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = integration.syn
    module.project = integration.project
    upload_destination['projectId'] = module.project.id
    upload_destination['projectId'] = module.project.id
    syn.restPOST('/projectSettings', body = json.dumps(upload_destination))


def test_synStore_sftpIntegration():
    """Creates a File Entity on an sftp server and add the external url. """
    filepath = utils.make_bogus_binary_file(1*MB - 777771)
    try:
        file = syn.store(File(filepath, parent=project))
        file2  = syn.get(file)
        assert file.externalURL==file2.externalURL and urlparse.urlparse(file2.externalURL).scheme=='sftp'
    finally:
        try:
            os.remove(filepath)
        except Exception:
            print traceback.format_exc()


def test_synGet_sftpIntegration():
    #Create file by uploading directly to sftp and creating entity from URL
    serverURL='sftp://ec2-54-212-85-156.us-west-2.compute.amazonaws.com/public/'+str(uuid.uuid1())
    filepath = utils.make_bogus_binary_file(1*MB - 777771)
    print '\n\tMade bogus file: ', filepath
    
    url = syn._sftpUploadFile(filepath, url=serverURL)
    file = syn.store(File(path=url, parent=project, synapseStore=False))

    print '\nDownloading file', os.getcwd(), filepath
    junk = syn.get(file, downloadLocation=os.getcwd(), downloadFile=True)
    filecmp.cmp(filepath, junk.path)


def test_utils_sftp_upload_and_download():
    """Tries to upload a file to an sftp file """
    serverURL='sftp://ec2-54-212-85-156.us-west-2.compute.amazonaws.com/public/'+str(uuid.uuid1())
    filepath = utils.make_bogus_binary_file(1*MB - 777771)

    tempdir = tempfile.mkdtemp()

    try:
        print '\n\tMade bogus file: ', filepath
        url = syn._sftpUploadFile(filepath, url=serverURL)
        print '\tStored URL:', url
        print '\tDownloading',
        #Get with a specified localpath
        junk = syn._sftpDownloadFile(url, tempdir)
        print '\tComparing:', junk, filepath
        filecmp.cmp(filepath, junk)
        #Get without specifying path
        print '\tDownloading',
        junk2 = syn._sftpDownloadFile(url)
        print '\tComparing:', junk2, filepath
        filecmp.cmp(filepath, junk2)
        #Get with a specified localpath as file
        print '\tDownloading',
        junk3 = syn._sftpDownloadFile(url, os.path.join(tempdir, 'bar.dat'))
        print '\tComparing:', junk3, filepath
        filecmp.cmp(filepath, junk3)
    finally:
        try:
            if 'junk' in locals(): os.remove(junk)
            if 'junk2' in locals(): os.remove(junk2)
            if 'junk3' in locals(): os.remove(junk3)
        except Exception:
            print traceback.format_exc()
        try:
            os.remove(filepath)
        except Exception:
            print traceback.format_exc()
        try:
            shutil.rmtree(tempdir)
        except Exception:
            print traceback.format_exc()



    
