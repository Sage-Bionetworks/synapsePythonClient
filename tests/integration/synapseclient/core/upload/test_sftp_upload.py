import filecmp
import urllib
import os
import traceback
import uuid
import tempfile
import shutil
from nose.tools import assert_is_not_none, assert_equals

from synapseclient import *
import synapseclient.core.utils
from synapseclient.core.utils import MB
from tests import integration
from tests.integration import schedule_for_cleanup

SFTP_SERVER_PREFIX = "sftp://ec2-18-209-45-78.compute-1.amazonaws.com"
SFTP_USER_HOME_PATH = "/home/sftpuser"

DESTINATIONS = [{"uploadType": "SFTP",
                 "description": 'EC2 subfolder A',
                 "supportsSubfolders": True,
                 "url": SFTP_SERVER_PREFIX + SFTP_USER_HOME_PATH + "/folder_A",
                 "banner": "Uploading file to EC2\n"},
                {"uploadType": "SFTP",
                 "supportsSubfolders": True,
                 "description": 'EC2 subfolder B',
                 "url": SFTP_SERVER_PREFIX + SFTP_USER_HOME_PATH + "/folder_B",
                 "banner": "Uploading file to EC2 version 2\n"}
                ]


def setup(module):

    module.syn = integration.syn
    module.project = integration.project
    # Create the upload destinations
    destinations = [syn.createStorageLocationSetting('ExternalStorage', **x)['storageLocationId'] for x in DESTINATIONS]
    module._sftp_project_setting_id = syn.setStorageLocation(project, destinations)['id']


def teardown(module):
    syn.restDELETE('/projectSettings/%s' % module._sftp_project_setting_id)


def test_synStore_sftpIntegration():
    """Creates a File Entity on an sftp server and add the external url. """
    filepath = synapseclient.core.utils.make_bogus_binary_file(1 * MB - 777771)
    try:
        file = syn.store(File(filepath, parent=project))
        file2 = syn.get(file)
        assert_equals(file.externalURL, file2.externalURL)
        assert_equals(urllib.parse.urlparse(file2.externalURL).scheme, 'sftp')

        tmpdir = tempfile.mkdtemp()
        schedule_for_cleanup(tmpdir)

        # test that we got an MD5 à la SYNPY-185
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
    # Create file by uploading directly to sftp and creating entity from URL
    serverURL = SFTP_SERVER_PREFIX + SFTP_USER_HOME_PATH + '/test_synGet_sftpIntegration/' + str(uuid.uuid1())
    filepath = synapseclient.core.utils.make_bogus_binary_file(1 * MB - 777771)

    username, password = syn._getUserCredentials(SFTP_SERVER_PREFIX)

    url = synapseclient.core.remote_file_storage_wrappers.SFTPWrapper.upload_file(
        filepath, url=serverURL, username=username, password=password)
    file = syn.store(File(path=url, parent=project, synapseStore=False))

    junk = syn.get(file, downloadLocation=os.getcwd(), downloadFile=True)
    filecmp.cmp(filepath, junk.path)


def test_utils_sftp_upload_and_download():
    """Tries to upload a file to an sftp file """
    serverURL = SFTP_SERVER_PREFIX + SFTP_USER_HOME_PATH + '/test_utils_sftp_upload_and_download/' + str(uuid.uuid1())
    filepath = synapseclient.core.utils.make_bogus_binary_file(1 * MB - 777771)

    tempdir = tempfile.mkdtemp()

    username, password = syn._getUserCredentials(SFTP_SERVER_PREFIX)

    try:
        url = synapseclient.core.remote_file_storage_wrappers.SFTPWrapper.upload_file(
            filepath, url=serverURL, username=username, password=password)

        # Get with a specified localpath
        junk = synapseclient.core.remote_file_storage_wrappers.SFTPWrapper.download_file(
            url, tempdir, username=username, password=password)
        filecmp.cmp(filepath, junk)
        # Get without specifying path
        junk2 = synapseclient.core.remote_file_storage_wrappers.SFTPWrapper.download_file(
            url, username=username, password=password)
        filecmp.cmp(filepath, junk2)
        # Get with a specified localpath as file
        junk3 = synapseclient.core.remote_file_storage_wrappers.SFTPWrapper.download_file(
            url, os.path.join(tempdir, 'bar.dat'), username=username, password=password)
        filecmp.cmp(filepath, junk3)
    finally:
        try:
            if 'junk' in locals():
                os.remove(junk)
            if 'junk2' in locals():
                os.remove(junk2)
            if 'junk3' in locals():
                os.remove(junk3)
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
