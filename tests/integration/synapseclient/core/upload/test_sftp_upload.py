from urllib.parse import urlparse

import filecmp
import os
import traceback
import uuid
import tempfile
import shutil

import pytest
import unittest

from synapseclient import File
import synapseclient.core.utils as utils
from synapseclient.core.remote_file_storage_wrappers import SFTPWrapper


def get_sftp_server_prefix():
    return f"sftp://{os.environ.get('SFTP_HOST')}/"


def get_user_home_path(username):
    return f"/home/{username}"


def check_test_preconditions():
    # in order to run SFTP tests an SFTP_HOST (a domain or IP) must be defined
    # that maps to credentials in the synapse config

    skip_tests = False
    reason = ''
    if not os.environ.get('SFTP_HOST'):
        skip_tests = True
        reason = 'SFTP_HOST environment variable not set'

    return skip_tests, reason


@pytest.fixture(scope="module", autouse=True)
@unittest.skipIf(*check_test_preconditions())
def project_setting_id(request, syn, project):
    server_prefix = get_sftp_server_prefix()
    username, _ = syn._getUserCredentials(server_prefix)
    user_home_path = get_user_home_path(username)

    external_storage_destination_settings = [
        {
            "uploadType": "SFTP",
            "description": 'subfolder A',
            "supportsSubfolders": True,
            "url": server_prefix + user_home_path + "/folder_A",
            "banner": "Uploading file to EC2\n"
        },
        {
            "uploadType": "SFTP",
            "supportsSubfolders": True,
            "description": 'subfolder B',
            "url": server_prefix + username + "/folder_B",
            "banner": "Uploading file to EC2 version 2\n"
        }
    ]

    # Create the upload destinations
    destinations = [
        syn.createStorageLocationSetting('ExternalStorage', **x)['storageLocationId']
        for x in external_storage_destination_settings
    ]

    sftp_project_setting_id = syn.setStorageLocation(project, destinations)['id']

    def delete_project_setting():
        syn.restDELETE('/projectSettings/%s' % sftp_project_setting_id)
    request.addfinalizer(delete_project_setting)


@unittest.skipIf(*check_test_preconditions())
def test_synStore_sftpIntegration(syn, project, schedule_for_cleanup):
    """Creates a File Entity on an sftp server and add the external url. """
    filepath = utils.make_bogus_binary_file(1 * utils.MB - 777771)
    try:
        file = syn.store(File(filepath, parent=project))
        file2 = syn.get(file)
        assert file.externalURL == file2.externalURL
        assert urlparse(file2.externalURL).scheme == 'sftp'

        tmpdir = tempfile.mkdtemp()
        schedule_for_cleanup(tmpdir)

        # test that we got an MD5 à la SYNPY-185
        assert file2.md5 is not None
        fh = syn._get_file_handle_as_creator(file2.dataFileHandleId)
        assert fh['contentMd5'] is not None
        assert file2.md5 == fh['contentMd5']
    finally:
        try:
            os.remove(filepath)
        except Exception:
            print(traceback.format_exc())


@unittest.skipIf(*check_test_preconditions())
def test_synGet_sftpIntegration(syn, project):
    # Create file by uploading directly to sftp and creating entity from URL
    server_prefix = get_sftp_server_prefix()
    username, password = syn._getUserCredentials(server_prefix)
    server_url = server_prefix + get_user_home_path(username) + '/test_synGet_sftpIntegration/' + str(uuid.uuid1())
    filepath = utils.make_bogus_binary_file(1 * utils.MB - 777771)

    url = SFTPWrapper.upload_file(filepath, url=server_url, username=username, password=password)
    file = syn.store(File(path=url, parent=project, synapseStore=False))

    junk = syn.get(file, downloadLocation=os.getcwd(), downloadFile=True)
    filecmp.cmp(filepath, junk.path)


@unittest.skipIf(*check_test_preconditions())
def test_utils_sftp_upload_and_download(syn):
    """Tries to upload a file to an sftp file """
    server_prefix = get_sftp_server_prefix()
    username, password = syn._getUserCredentials(server_prefix)
    user_home_path = get_user_home_path(username)

    server_url = server_prefix + user_home_path + '/test_utils_sftp_upload_and_download/' + str(uuid.uuid1())
    filepath = utils.make_bogus_binary_file(1 * utils.MB - 777771)

    tempdir = tempfile.mkdtemp()

    username, password = syn._getUserCredentials(server_prefix)

    try:
        url = SFTPWrapper.upload_file(filepath, url=server_url, username=username, password=password)

        # Get with a specified localpath
        junk = SFTPWrapper.download_file(url, tempdir, username=username, password=password)
        filecmp.cmp(filepath, junk)
        # Get without specifying path
        junk2 = SFTPWrapper.download_file(url, username=username, password=password)
        filecmp.cmp(filepath, junk2)
        # Get with a specified localpath as file
        junk3 = SFTPWrapper.download_file(url, os.path.join(tempdir, 'bar.dat'), username=username, password=password)
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
