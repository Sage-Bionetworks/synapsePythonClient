import base64
import configparser
import datetime
import errno
import json
import os
from pathlib import Path
import requests
import tempfile
import urllib.request as urllib_request
import uuid

import pytest
from unittest.mock import ANY, call, create_autospec, MagicMock, Mock, patch

import synapseclient
from synapseclient.annotations import convert_old_annotation_json
from synapseclient import client
from synapseclient import (
    Activity,
    Annotations,
    Column,
    DockerRepository,
    Entity,
    EntityViewSchema,
    Schema,
    File,
    Folder,
    Team,
    SubmissionViewSchema,
    Synapse,
)
from synapseclient.core.exceptions import (
    SynapseAuthenticationError,
    SynapseError,
    SynapseFileNotFoundError,
    SynapseHTTPError,
    SynapseMd5MismatchError,
    SynapseUnmetAccessRestrictions,
)
from synapseclient.core.logging_setup import DEFAULT_LOGGER_NAME, DEBUG_LOGGER_NAME, SILENT_LOGGER_NAME
from synapseclient.core.upload import upload_functions
import synapseclient.core.utils as utils
from synapseclient.client import DEFAULT_STORAGE_LOCATION_ID
from synapseclient.core.constants import concrete_types
from synapseclient.core.credentials import UserLoginArgs
from synapseclient.core.credentials.cred_data import SynapseApiKeyCredentials
from synapseclient.core.credentials.credential_provider import SynapseCredentialsProviderChain
from synapseclient.core.models.dict_object import DictObject


class TestLogout:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.username = "asdf"
        self.credentials = SynapseApiKeyCredentials(self.username, base64.b64encode(b"api_key_doesnt_matter").decode())

    def test_logout__forgetMe_is_True(self):
        self.syn.credentials = self.credentials
        with patch.object(self.credentials, 'delete_from_keyring') as mock_delete_from_keyring:
            self.syn.logout(True)
            assert self.syn.credentials is None
            mock_delete_from_keyring.assert_called_once()

    def test_logout__forgetMe_is_False(self):
        with patch.object(self.credentials, 'delete_from_keyring') as mock_delete_from_keyring:
            self.syn.credentials = self.credentials
            self.syn.logout(False)
            assert self.syn.credentials is None
            mock_delete_from_keyring.assert_not_called()


class TestLogin:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.login_args = {'email': "AzureDiamond", "password": "hunter2"}
        self.expected_user_args = UserLoginArgs(username="AzureDiamond", password="hunter2", api_key=None,
                                                skip_cache=False)
        self.synapse_creds = SynapseApiKeyCredentials("AzureDiamond", base64.b64encode(b"*******").decode())

        self.mocked_credential_chain = create_autospec(SynapseCredentialsProviderChain)
        self.mocked_credential_chain.get_credentials.return_value = self.synapse_creds
        self.get_default_credential_chain_patcher = patch.object(client, "get_default_credential_chain",
                                                                 return_value=self.mocked_credential_chain)
        self.mocked_get_credential_chain = self.get_default_credential_chain_patcher.start()

    def teardown(self):
        self.get_default_credential_chain_patcher.stop()

    def test_login__no_credentials(self):
        self.mocked_credential_chain.get_credentials.return_value = None

        # method under test
        pytest.raises(SynapseAuthenticationError, self.syn.login, **self.login_args)

        self.mocked_get_credential_chain.assert_called_once_with()
        self.mocked_credential_chain.get_credentials.assert_called_once_with(self.syn, self.expected_user_args)

    def test_login__credentials_returned(self):
        # method under test
        self.syn.login(silent=True, **self.login_args)

        self.mocked_get_credential_chain.assert_called_once_with()
        self.mocked_credential_chain.get_credentials.assert_called_once_with(self.syn, self.expected_user_args)
        assert self.synapse_creds == self.syn.credentials

    def test_login__silentIsFalse(self):
        with patch.object(self.syn, "getUserProfile") as mocked_get_user_profile, \
                patch.object(self.syn, "logger") as mocked_logger:
            # method under test
            self.syn.login(silent=False, **self.login_args)

            mocked_get_user_profile.assert_called_once_with(refresh=True)
            mocked_logger.info.assert_called_once()

    def test_login__rememberMeIsTrue(self, mocker):
        mock_cached_sessions = mocker.patch.object(client, 'cached_sessions')
        mock_delete_stored_credentials = mocker.patch.object(client, 'delete_stored_credentials')
        mock_store_to_keyring = mocker.patch.object(self.synapse_creds, 'store_to_keyring')
        self.syn.login(silent=True, rememberMe=True)

        mock_store_to_keyring.assert_called_once()
        mock_cached_sessions.set_most_recent_user.assert_called_once_with(self.synapse_creds.username)
        mock_delete_stored_credentials.assert_called_once_with(self.synapse_creds.username)


@patch('synapseclient.Synapse._getFileHandleDownload')
@patch('synapseclient.Synapse._downloadFileHandle')
class TestPrivateGetWithEntityBundle:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def test_getWithEntityBundle__no_DOWNLOAD(self, download_file_mock, get_file_URL_and_metadata_mock):
        bundle = {
            'entity': {
                'id': 'syn10101',
                'etag': 'f826b84e-b325-48d9-a658-3929bf687129',
                'name': 'anonymous',
                'dataFileHandleId': '-1337',
                'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
                'parentId': 'syn12345'},
            'fileHandles': [],
            'annotations': {'id': 'syn10101',
                            'etag': 'f826b84e-b325-48d9-a658-3929bf687129',
                            'annotations': {}}
        }

        with patch.object(self.syn.logger, "warning") as mocked_warn:
            entity_no_download = self.syn._getWithEntityBundle(entityBundle=bundle)
            mocked_warn.assert_called_once()
            assert entity_no_download.path is None

    def test_getWithEntityBundle(self, download_file_mock, get_file_URL_and_metadata_mock):
        # Note: one thing that remains unexplained is why the previous version of
        # this test worked if you had a .cacheMap file of the form:
        # {"/Users/chris/.synapseCache/663/-1337/anonymous": "2014-09-15T22:54:57.000Z",
        #  "/var/folders/ym/p7cr7rrx4z7fw36sxv04pqh00000gq/T/tmpJ4nz8U": "2014-09-15T23:27:25.000Z"}
        # ...but failed if you didn't.

        bundle = {
            'entity': {
                'id': 'syn10101',
                'etag': 'f826b84e-b325-48d9-a658-3929bf687129',
                'name': 'anonymous',
                'dataFileHandleId': '-1337',
                'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
                'parentId': 'syn12345'},
            'fileHandles': [{
                'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
                'fileName': 'anonymous',
                'contentType': 'application/flapdoodle',
                'contentMd5': '1698d26000d60816caab15169efcd23a',
                'id': '-1337'}],
            'annotations': {'id': 'syn10101',
                            'etag': 'f826b84e-b325-48d9-a658-3929bf687129',
                            'annotations': {}}
        }

        fileHandle = bundle['fileHandles'][0]['id']
        cacheDir = self.syn.cache.get_cache_dir(fileHandle)
        # Make sure the .cacheMap file does not already exist
        cacheMap = os.path.join(cacheDir, '.cacheMap')
        if os.path.exists(cacheMap):
            os.remove(cacheMap)

        def _downloadFileHandle(fileHandleId, objectId, objectType, path, retries=5):
            # touch file at path
            with open(path, 'a'):
                os.utime(path, None)
            os.path.split(path)
            self.syn.cache.add(fileHandle, path)
            return path

        def _getFileHandleDownload(fileHandleId,  objectId, objectType='FileHandle'):
            return {'fileHandle': bundle['fileHandles'][0], 'fileHandleId': fileHandleId,
                    'preSignedURL': 'http://example.com'}

        download_file_mock.side_effect = _downloadFileHandle
        get_file_URL_and_metadata_mock.side_effect = _getFileHandleDownload

        # 1. ----------------------------------------------------------------------
        # download file to an alternate location

        temp_dir1 = tempfile.mkdtemp()

        e = self.syn._getWithEntityBundle(
            entityBundle=bundle,
            downloadLocation=temp_dir1,
            ifcollision="overwrite.local"
        )

        assert e.name == bundle["entity"]["name"]
        assert e.parentId == bundle["entity"]["parentId"]
        assert utils.normalize_path(os.path.abspath(os.path.dirname(e.path))) == utils.normalize_path(temp_dir1)
        assert bundle["fileHandles"][0]["fileName"] == os.path.basename(e.path)
        assert (
            utils.normalize_path(os.path.abspath(e.path)) ==
            utils.normalize_path(os.path.join(temp_dir1, bundle["fileHandles"][0]["fileName"]))
        )

        # 2. ----------------------------------------------------------------------
        # get without specifying downloadLocation
        e = self.syn._getWithEntityBundle(entityBundle=bundle, ifcollision="overwrite.local")

        assert e.name == bundle["entity"]["name"]
        assert e.parentId == bundle["entity"]["parentId"]
        assert bundle["fileHandles"][0]["fileName"] in e.files

        # 3. ----------------------------------------------------------------------
        # download to another location
        temp_dir2 = tempfile.mkdtemp()
        assert temp_dir2 != temp_dir1
        e = self.syn._getWithEntityBundle(
            entityBundle=bundle,
            downloadLocation=temp_dir2,
            ifcollision="overwrite.local"
        )

        assert bundle["fileHandles"][0]["fileName"] in e.files
        assert e.path is not None
        assert utils.equal_paths(os.path.dirname(e.path), temp_dir2)

        # 4. ----------------------------------------------------------------------
        # test preservation of local state
        url = 'http://foo.com/secretstuff.txt'
        # need to create a bundle with externalURL
        externalURLBundle = dict(bundle)
        externalURLBundle['fileHandles'][0]['externalURL'] = url
        e = File(name='anonymous', parentId="syn12345", synapseStore=False, externalURL=url)
        e.local_state({'zap': 'pow'})
        e = self.syn._getWithEntityBundle(entityBundle=externalURLBundle, entity=e)
        assert e.local_state()['zap'] == 'pow'
        assert e.synapseStore is False
        assert e.externalURL == url

        # TODO: add more test cases for flag combination of this method
        # TODO: separate into another test?


class TestDownloadFileHandle:
    # TODO missing tests for the other ways of downloading a file handle should be backfilled...

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    @patch.object(client, 'sts_transfer')
    def test_download_file_handle__retry_error(self, mock_sts_transfer):
        mock_sts_transfer.is_boto_sts_transfer_enabled.return_value = False

        file_handle_id = 1234
        syn_id = 'syn123'

        disk_space_error = OSError()
        disk_space_error.errno = errno.ENOSPC

        retries = 5
        for (ex, expected_attempts) in [
            (SynapseMd5MismatchError('error'), 1),
            (disk_space_error, 1),
            (ValueError('foo'), retries),
        ]:
            with patch.object(self.syn, '_getFileHandleDownload') as mock_get_file_handle_download, \
                    patch.object(self.syn, '_download_from_URL') as mock_download_from_URL:

                mock_get_file_handle_download.return_value = {
                    'fileHandle': {
                        'id': file_handle_id,
                        'concreteType': concrete_types.S3_FILE_HANDLE,
                        'contentSize': 1,
                    },
                    'preSignedURL': 'http://foo.com',
                }

                mock_download_from_URL.side_effect = ex

                with pytest.raises(ex.__class__):
                    self.syn._downloadFileHandle(
                        file_handle_id,
                        syn_id,
                        objectType='FileEntity',
                        destination='/tmp/foo',
                        retries=retries,
                    )

                assert mock_download_from_URL.call_count == expected_attempts

    @patch.object(client, 'S3ClientWrapper')
    @patch.object(client, 'sts_transfer')
    @patch.object(client, 'os')
    def test_download_file_handle__sts_boto(
            self,
            mock_os,
            mock_sts_transfer,
            mock_s3_client_wrapper,
    ):
        """Verify that we download S3 file handles using boto if the configuration specifies
        # it and if the storage location supports STS"""

        file_handle_id = 1234
        entity_id = 'syn_5678'
        bucket_name = 'fooBucket'
        key = '/tmp/fooKey'
        destination = '/tmp'
        credentials = {
            'aws_access_key_id': 'foo',
            'aws_secret_access_key': 'bar',
            'aws_session_token': 'baz',
        }

        mock_sts_transfer.is_boto_sts_transfer_enabled.return_value = True
        mock_sts_transfer.is_storage_location_sts_enabled.return_value = True

        def mock_with_boto_sts_credentials(download_fn, syn, objectId, permission):
            assert permission == 'read_only'
            assert entity_id == objectId
            return download_fn(credentials)

        mock_sts_transfer.with_boto_sts_credentials = mock_with_boto_sts_credentials

        expected_download_path = '/tmp/fooKey'
        mock_s3_client_wrapper.download_file.return_value = expected_download_path

        with patch.object(self.syn, '_getFileHandleDownload') as mock_get_file_handle_download,\
                patch.object(self.syn, 'cache') as cache:
            mock_get_file_handle_download.return_value = {
                'fileHandle': {
                    'id': file_handle_id,
                    'bucketName': bucket_name,
                    'key': key,
                    'concreteType': concrete_types.S3_FILE_HANDLE,
                    'storageLocationId': 9876
                }
            }

            # this is another opt-in download method.
            # for enabled storage locations sts should be preferred
            multi_threaded = self.syn.multi_threaded
            try:
                self.syn.multi_threaded = True
                download_path = self.syn._downloadFileHandle(
                    fileHandleId=file_handle_id,
                    objectId=entity_id,
                    objectType='FileEntity',
                    destination=destination,
                )
                # restore to whatever it was before
            finally:
                self.syn.multi_threaded = multi_threaded

            mock_os.makedirs.assert_called_once_with(mock_os.path.dirname(destination), exist_ok=True)
            cache.add.assert_called_once_with(file_handle_id, download_path)

        assert expected_download_path == download_path
        mock_s3_client_wrapper.download_file.assert_called_once_with(
            bucket_name,
            None,
            key,
            destination,
            credentials=credentials,
            show_progress=True,
            transfer_config_kwargs={'max_concurrency': self.syn.max_threads},
        )

    def test_download_file_ftp_link(self):
        """Verify downloading from an FTP Link entity"""

        file_handle_id = 1234
        entity_id = 'syn5678'
        url = 'ftp://foo.com/bar'
        destination = '/tmp'
        expected_destination = os.path.abspath(destination)

        with patch.object(self.syn, '_getFileHandleDownload') as mock_get_file_handle_download,\
                patch.object(self.syn, 'cache'),\
                patch.object(urllib_request, 'urlretrieve') as mock_url_retrieve,\
                patch.object(utils, 'md5_for_file') as mock_md5_for_file,\
                patch.object(os, 'makedirs'):

            mock_get_file_handle_download.return_value = {
                'fileHandle': {
                    'id': file_handle_id,
                    'concreteType': concrete_types.EXTERNAL_FILE_HANDLE,
                    'externalURL': url,
                    'storageLocationId': 9876,
                },
                'preSignedURL': url,
            }

            mock_md5_for_file.return_value = Mock(
                hexdigest=Mock(
                    return_value='abc123'
                )
            )

            download_path = self.syn._downloadFileHandle(
                fileHandleId=file_handle_id,
                objectId=entity_id,
                objectType='FileEntity',
                destination=destination,
            )

            mock_url_retrieve.assert_called_once_with(url, expected_destination)
            assert download_path == expected_destination

    def test_download_from_url__synapse_auth(self, mocker):
        """Verify we pass along Synapse auth headers when downloading from a Synapse repo hosted url"""

        uri = f"{self.syn.repoEndpoint}/repo/v1/entity/syn1234567/file"
        in_destination = tempfile.mktemp()

        mock_credentials = mocker.patch.object(self.syn, 'credentials')

        response = MagicMock(spec=requests.Response)
        response.status_code = 200
        response.headers = {}
        mock_get = mocker.patch.object(self.syn._requests_session, 'get')
        mock_get.return_value = response

        out_destination = self.syn._download_from_URL(uri, in_destination)
        assert mock_get.call_args[1]['auth'] is mock_credentials
        assert os.path.normpath(out_destination) == os.path.normpath(in_destination)

    def test_download_from_url__external(self, mocker):
        """Verify we do not pass along Synapse auth headers to a file download that is a not Synapse repo hosted"""

        uri = "https://not-synapse.org/foo/bar/baz"
        in_destination = tempfile.mktemp()

        mocker.patch.object(self.syn, 'credentials')

        response = MagicMock(spec=requests.Response)
        response.status_code = 200
        response.headers = {}
        mock_get = mocker.patch.object(self.syn._requests_session, 'get')
        mock_get.return_value = response

        out_destination = self.syn._download_from_URL(uri, in_destination)
        assert mock_get.call_args[1]['auth'] is None
        assert os.path.normpath(out_destination) == os.path.normpath(in_destination)

    @patch.object(Synapse, '_getFileHandleDownload')
    def test_downloadFileHandle_preserve_exception_info(self, mock_getFileHandleDownload):
        file_handle_id = 1234
        syn_id = 'syn123'

        def getFileHandleDownload_side_effect(*args):
            raise SynapseError(f'Something wrong when downloading {syn_id} in try block!')

        mock_getFileHandleDownload.side_effect = getFileHandleDownload_side_effect

        with pytest.raises(SynapseError) as ex:
            self.syn._downloadFileHandle(
                file_handle_id,
                syn_id,
                objectType='FileEntity',
                destination='/tmp/foo'
            )
        assert str(ex.value) == 'Something wrong when downloading syn123 in try block!'


class TestPrivateSubmit:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.etag = "etag"
        self.eval_id = 1
        self.eligibility_hash = 23
        self.submission = {
            'id': 123,
            'evaluationId': 1,
            'name': "entity_name",
            'entityId': "syn43",
            'versionNumber': 1,
            'teamId': 888,
            'contributors': [],
            'submitterAlias': "awesome_submission"
        }
        self.patch_restPOST = patch.object(self.syn, "restPOST", return_value=self.submission)
        self.mock_restPOST = self.patch_restPOST.start()

    def teardown(self):
        self.patch_restPOST.stop()

    def test_invalid_submission(self):
        pytest.raises(ValueError, self.syn._submit, None, self.etag, self.submission)

    def test_invalid_etag(self):
        pytest.raises(ValueError, self.syn._submit, self.submission, None, self.submission)

    def test_without_eligibility_hash(self):
        assert self.submission == self.syn._submit(self.submission, self.etag, None)
        uri = '/evaluation/submission?etag={0}'.format(self.etag)
        self.mock_restPOST.assert_called_once_with(uri, json.dumps(self.submission))

    def test_with_eligibitiy_hash(self):
        assert self.submission == self.syn._submit(self.submission, self.etag, self.eligibility_hash)
        uri = '/evaluation/submission?etag={0}&submissionEligibilityHash={1}'.format(self.etag, self.eligibility_hash)
        self.mock_restPOST.assert_called_once_with(uri, json.dumps(self.submission))


class TestSubmit:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.eval_id = '9090'
        self.contributors = None
        self.entity = {
            'versionNumber': 7,
            'id': 'syn1009',
            'etag': 'etag',
            'name': 'entity name'
        }
        self.docker = {
            'id': 'syn1009',
            'etag': 'etag',
            'repositoryName': 'entity name'
        }
        self.eval = {
            'contentSource': self.entity['id'],
            'createdOn': '2013-11-06T06:04:26.789Z',
            'etag': '86485ea1-8c89-4f24-a0a4-2f63bc011091',
            'id': self.eval_id,
            'name': 'test evaluation',
            'ownerId': '1560252',
            'status': 'OPEN',
            'submissionReceiptMessage': 'Your submission has been received.!'
        }
        self.team = {
            'id': 5,
            'name': 'Team Blue'
        }
        self.submission = {
            'id': 123,
            'evaluationId': self.eval_id,
            'name': self.entity['name'],
            'entityId': self.entity['id'],
            'versionNumber': self.entity['versionNumber'],
            'teamId': utils.id_of(self.team['id']),
            'contributors': self.contributors,
            'submitterAlias': self.team['name']
        }
        self.eligibility_hash = 23

        self.patch_private_submit = patch.object(self.syn, "_submit", return_value=self.submission)
        self.patch_getEvaluation = patch.object(self.syn, "getEvaluation", return_value=self.eval)
        self.patch_get = patch.object(self.syn, "get", return_value=self.entity)
        self.patch_getTeam = patch.object(self.syn, "getTeam", return_value=self.team)
        self.patch_get_contributors = patch.object(self.syn, "_get_contributors",
                                                   return_value=(self.contributors, self.eligibility_hash))

        self.mock_private_submit = self.patch_private_submit.start()
        self.mock_getEvaluation = self.patch_getEvaluation.start()
        self.mock_get = self.patch_get.start()
        self.mock_getTeam = self.patch_getTeam.start()
        self.mock_get_contributors = self.patch_get_contributors.start()

    def teardown(self):
        self.patch_private_submit.stop()
        self.patch_getEvaluation.stop()
        self.patch_get.stop()
        self.patch_getTeam.stop()
        self.patch_get_contributors.stop()

    def test_min_requirements(self):
        assert self.submission == self.syn.submit(self.eval_id, self.entity)

        expected_request_body = self.submission
        expected_request_body.pop('id')
        expected_request_body['teamId'] = None
        expected_request_body['submitterAlias'] = None
        expected_request_body['dockerDigest'] = None
        expected_request_body['dockerRepositoryName'] = None
        self.mock_private_submit.assert_called_once_with(expected_request_body, self.entity['etag'],
                                                         self.eligibility_hash)
        self.mock_get.assert_not_called()
        self.mock_getTeam.assert_not_called()
        self.mock_get_contributors.assert_called_once_with(self.eval_id, None)

    def test_only_entity_id_provided(self):
        assert self.submission == self.syn.submit(self.eval_id, self.entity['id'])

        expected_request_body = self.submission
        expected_request_body.pop('id')
        expected_request_body['teamId'] = None
        expected_request_body['submitterAlias'] = None
        expected_request_body['dockerDigest'] = None
        expected_request_body['dockerRepositoryName'] = None
        self.mock_private_submit.assert_called_once_with(expected_request_body, self.entity['etag'],
                                                         self.eligibility_hash)
        self.mock_get.assert_called_once_with(self.entity['id'], downloadFile=False)
        self.mock_getTeam.assert_not_called()
        self.mock_get_contributors.assert_called_once_with(self.eval_id, None)

    def test_team_is_given(self):
        assert self.submission == self.syn.submit(self.eval_id, self.entity, team=self.team['id'])

        expected_request_body = self.submission
        expected_request_body.pop('id')
        expected_request_body['dockerDigest'] = None
        expected_request_body['dockerRepositoryName'] = None
        self.mock_private_submit.assert_called_once_with(expected_request_body, self.entity['etag'],
                                                         self.eligibility_hash)
        self.mock_get.assert_not_called()
        self.mock_getTeam.assert_called_once_with(self.team['id'])
        self.mock_get_contributors.assert_called_once_with(self.eval_id, self.team)

    def test_team_not_eligible(self):
        self.mock_get_contributors.side_effect = SynapseError()
        pytest.raises(SynapseError, self.syn.submit, self.eval_id, self.entity, team=self.team['id'])
        self.mock_private_submit.assert_not_called()
        self.mock_get.assert_not_called()
        self.mock_getTeam.assert_called_once_with(self.team['id'])
        self.mock_get_contributors.assert_called_once_with(self.eval_id, self.team)

    def test_get_docker_digest_default(self):
        latest_sha = 'sha256:eeeeee'
        docker_commits = [{'tag': 'latest',
                           'digest': latest_sha}]

        with patch.object(self.syn, "_GET_paginated",
                          return_value=docker_commits) as patch_syn_get_paginated:
            digest = self.syn._get_docker_digest('syn1234')
            patch_syn_get_paginated.assert_called_once_with("/entity/syn1234/dockerTag")
            assert digest == latest_sha

    def test_get_docker_digest_specifytag(self):
        test_sha = 'sha256:ffffff'
        docker_commits = [{'tag': 'test',
                           'digest': test_sha}]
        with patch.object(self.syn, "_GET_paginated",
                          return_value=docker_commits) as patch_syn_get_paginated:
            digest = self.syn._get_docker_digest('syn1234', docker_tag="test")
            patch_syn_get_paginated.assert_called_once_with("/entity/syn1234/dockerTag")
            assert digest == test_sha

    def test_get_docker_digest_specifywrongtag(self):
        test_sha = 'sha256:ffffff'
        docker_commits = [{'tag': 'test',
                           'digest': test_sha}]
        with patch.object(self.syn, "_GET_paginated",
                          return_value=docker_commits) as patch_syn_get_paginated:
            pytest.raises(ValueError, self.syn._get_docker_digest, 'syn1234', docker_tag="foo")
            patch_syn_get_paginated.assert_called_once_with("/entity/syn1234/dockerTag")

    def test_submit_docker_nonetag(self):
        docker_entity = DockerRepository("foo", parentId="syn1000001")
        docker_entity.id = "syn123"
        docker_entity.etag = "Fake etag"

        pytest.raises(ValueError, self.syn.submit, '9090',
                      docker_entity, "George", dockerTag=None)

    def test_submit_docker(self):
        docker_entity = DockerRepository("foo", parentId="syn1000001")
        docker_entity.id = "syn123"
        docker_entity.etag = "Fake etag"

        docker_digest = 'sha256:digest'
        expected_submission = {
            'id': 123,
            'evaluationId': self.eval_id,
            'name': None,
            'entityId': docker_entity['id'],
            'versionNumber': self.entity['versionNumber'],
            'dockerDigest': docker_digest,
            'dockerRepositoryName': docker_entity['repositoryName'],
            'teamId': utils.id_of(self.team['id']),
            'contributors': self.contributors,
            'submitterAlias': self.team['name']}
        with patch.object(self.syn, "get",
                          return_value=docker_entity) as patch_syn_get, \
            patch.object(self.syn, "_get_docker_digest",
                         return_value=docker_digest) as patch_get_digest, \
            patch.object(self.syn, "_submit",
                         return_value=expected_submission):
            submission = self.syn.submit('9090', patch_syn_get, name='George')
            patch_get_digest.assert_called_once_with(docker_entity, "latest")
            assert submission == expected_submission


class TestPrivateGetContributor:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.eval_id = 111
        self.team_id = 123
        self.team = Team(name="test", id=self.team_id)
        self.hash = 3
        self.member_eligible = {
            'isEligible': True,
            'isRegistered': True,
            'isQuotaFilled': False,
            'principalId': 222,
            'hasConflictingSubmission': False
        }
        self.member_not_registered = {
            'isEligible': False,
            'isRegistered': False,
            'isQuotaFilled': False,
            'principalId': 223,
            'hasConflictingSubmission': False
        }
        self.member_quota_filled = {
            'isEligible': False,
            'isRegistered': True,
            'isQuotaFilled': True,
            'principalId': 224,
            'hasConflictingSubmission': False
        }
        self.member_has_conflict = {
            'isEligible': True,
            'isRegistered': True,
            'isQuotaFilled': False,
            'principalId': 225,
            'hasConflictingSubmission': True
        }
        self.eligibility = {
            'teamId': self.team_id,
            'evaluationId': self.eval_id,
            'teamEligibility': {
                'isEligible': True,
                'isRegistered': True,
                'isQuotaFilled': False
            },
            'membersEligibility': [
                self.member_eligible,
                self.member_not_registered,
                self.member_quota_filled,
                self.member_has_conflict],
            'eligibilityStateHash': self.hash
        }

        self.patch_restGET = patch.object(self.syn, "restGET", return_value=self.eligibility)
        self.mock_restGET = self.patch_restGET.start()

    def teardown(self):
        self.patch_restGET.stop()

    def test_none_team(self):
        assert (None, None) == self.syn._get_contributors(self.eval_id, None)
        self.mock_restGET.assert_not_called()

    def test_none_eval_id(self):
        assert (None, None) == self.syn._get_contributors(None, self.team)
        self.mock_restGET.assert_not_called()

    def test_not_registered(self):
        self.eligibility['teamEligibility']['isEligible'] = False
        self.eligibility['teamEligibility']['isRegistered'] = False
        self.patch_restGET.return_value = self.eligibility
        pytest.raises(SynapseError, self.syn._get_contributors, self.eval_id, self.team)
        uri = '/evaluation/{evalId}/team/{id}/submissionEligibility'.format(evalId=self.eval_id, id=self.team_id)
        self.mock_restGET.assert_called_once_with(uri)

    def test_quota_filled(self):
        self.eligibility['teamEligibility']['isEligible'] = False
        self.eligibility['teamEligibility']['isQuotaFilled'] = True
        self.patch_restGET.return_value = self.eligibility
        pytest.raises(SynapseError, self.syn._get_contributors, self.eval_id, self.team)
        uri = '/evaluation/{evalId}/team/{id}/submissionEligibility'.format(evalId=self.eval_id, id=self.team_id)
        self.mock_restGET.assert_called_once_with(uri)

    def test_empty_members(self):
        self.eligibility['membersEligibility'] = []
        self.patch_restGET.return_value = self.eligibility
        assert ([], self.eligibility['eligibilityStateHash']) == self.syn._get_contributors(self.eval_id, self.team)
        uri = '/evaluation/{evalId}/team/{id}/submissionEligibility'.format(evalId=self.eval_id, id=self.team_id)
        self.mock_restGET.assert_called_once_with(uri)

    def test_happy_case(self):
        contributors = [{'principalId': self.member_eligible['principalId']}]
        assert (
            (contributors, self.eligibility['eligibilityStateHash']) ==
            self.syn._get_contributors(self.eval_id, self.team)
        )
        uri = '/evaluation/{evalId}/team/{id}/submissionEligibility'.format(evalId=self.eval_id, id=self.team_id)
        self.mock_restGET.assert_called_once_with(uri)


def test_send_message(syn):
    messageBody = ("In Xanadu did Kubla Khan\n"
                   "A stately pleasure-dome decree:\n"
                   "Where Alph, the sacred river, ran\n"
                   "Through caverns measureless to man\n"
                   "Down to a sunless sea.\n")
    with patch("synapseclient.client.multipart_upload_string", return_value='7365905') as mock_upload_string, \
            patch("synapseclient.client.Synapse.restPOST") as post_mock:
        syn.sendMessage(
            userIds=[1421212],
            messageSubject="Xanadu",
            messageBody=messageBody)
        msg = json.loads(post_mock.call_args_list[0][1]['body'])
        assert msg["fileHandleId"] == "7365905", msg
        assert msg["recipients"] == [1421212], msg
        assert msg["subject"] == "Xanadu", msg
        mock_upload_string.assert_called_once_with(syn, messageBody, content_type="text/plain")


@patch("synapseclient.Synapse._getDefaultUploadDestination")
class TestPrivateUploadExternallyStoringProjects:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def test__uploadExternallyStoringProjects_external_user(self, mock_upload_destination):
        # setup
        expected_storage_location_id = "1234567"
        expected_path = "~/fake/path/file.txt"
        expected_path_expanded = os.path.expanduser(expected_path)
        expected_file_handle_id = "8786"
        mock_upload_destination.return_value = {'storageLocationId': expected_storage_location_id,
                                                'concreteType': concrete_types.EXTERNAL_S3_UPLOAD_DESTINATION}

        test_file = File(expected_path, parent="syn12345")
        max_threads = 8

        # method under test
        with patch.object(upload_functions, "multipart_upload_file",
                          return_value=expected_file_handle_id) as mocked_multipart_upload, \
                patch.object(self.syn.cache, "add") as mocked_cache_add,\
                patch.object(self.syn, "_get_file_handle_as_creator") as mocked_getFileHandle:
            upload_functions.upload_file_handle(self.syn, test_file['parentId'], test_file['path'],
                                                max_threads=max_threads)

            mock_upload_destination.assert_called_once_with(test_file['parentId'])
            mocked_multipart_upload.assert_called_once_with(
                self.syn,
                expected_path_expanded,
                content_type=None,
                storage_location_id=expected_storage_location_id,
                max_threads=max_threads,
            )
            mocked_cache_add.assert_called_once_with(expected_file_handle_id, expected_path_expanded)
            mocked_getFileHandle.assert_called_once_with(expected_file_handle_id)
            # test


def test_findEntityIdByNameAndParent__None_parent(syn):
    entity_name = "Kappa 123"
    expected_uri = "/entity/child"
    expected_body = json.dumps({"parentId": None, "entityName": entity_name})
    expected_id = "syn1234"
    return_val = {'id': expected_id}
    with patch.object(syn, "restPOST", return_value=return_val) as mocked_POST:
        entity_id = syn.findEntityId(entity_name)
        mocked_POST.assert_called_once_with(expected_uri, body=expected_body)
        assert expected_id == entity_id


def test_findEntityIdByNameAndParent__with_parent(syn):
    entity_name = "Kappa 123"
    parentId = "syn42"
    parent_entity = Folder(name="wwwwwwwwwwwwwwwwwwwwww@@@@@@@@@@@@@@@@", id=parentId, parent="fakeParent")
    expected_uri = "/entity/child"
    expected_body = json.dumps({"parentId": parentId, "entityName": entity_name})
    expected_id = "syn1234"
    return_val = {'id': expected_id}
    with patch.object(syn, "restPOST", return_value=return_val) as mocked_POST:
        entity_id = syn.findEntityId(entity_name, parent_entity)
        mocked_POST.assert_called_once_with(expected_uri, body=expected_body)
        assert expected_id == entity_id


def test_findEntityIdByNameAndParent__404_error_no_result(syn):
    entity_name = "Kappa 123"
    fake_response = DictObject({"status_code": 404})
    with patch.object(syn, "restPOST", side_effect=SynapseHTTPError(response=fake_response)):
        assert syn.findEntityId(entity_name) is None


def test_getChildren__nextPageToken(syn):
    # setup
    nextPageToken = "T O K E N"
    parent_project_id_int = 42690
    first_page = {'versionLabel': '1',
                  'name': 'firstPageResult',
                  'versionNumber': 1,
                  'benefactorId': parent_project_id_int,
                  'type': 'org.sagebionetworks.repo.model.FileEntity',
                  'id': 'syn123'}
    second_page = {'versionLabel': '1',
                   'name': 'secondPageResult',
                   'versionNumber': 1,
                   'benefactorId': parent_project_id_int,
                   'type': 'org.sagebionetworks.repo.model.Folder',
                   'id': 'syn456'}
    mock_responses = [{'page': [first_page], 'nextPageToken': nextPageToken},
                      {'page': [second_page], 'nextPageToken': None}]

    with patch.object(syn, "restPOST", side_effect=mock_responses) as mocked_POST:
        # method under test
        children_generator = syn.getChildren('syn'+str(parent_project_id_int))

        # assert check the results of the generator
        result = next(children_generator)
        assert first_page == result
        result = next(children_generator)
        assert second_page == result
        pytest.raises(StopIteration, next, children_generator)

        # check that the correct POST requests were sent
        # genrates JSOn for the expected request body
        def expected_request_JSON(token):
            return json.dumps({'parentId': 'syn' + str(parent_project_id_int),
                               'includeTypes': ["folder", "file", "table", "link", "entityview", "dockerrepo",
                                                "submissionview", "dataset", "materializedview"],
                               'sortBy': 'NAME', 'sortDirection': 'ASC', 'nextPageToken': token})
        expected_POST_url = '/entity/children'
        mocked_POST.assert_has_calls([call(expected_POST_url, body=expected_request_JSON(None)),
                                      call(expected_POST_url, body=expected_request_JSON(nextPageToken))])


def test_check_entity_restrictions__no_unmet_restriction(syn):
    with patch("warnings.warn") as mocked_warn:
        bundle = {'entity': {
            'id': 'syn123',
            'name': 'anonymous',
            'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
            'parentId': 'syn12345'},
            'restrictionInformation': {
                'hasUnmetAccessRequirement': False
            }
        }
        entity = 'syn123'
        syn._check_entity_restrictions(bundle, entity, True)
        mocked_warn.assert_not_called()


def test_check_entity_restrictions__unmet_restriction_entity_file_with_downloadFile_is_True(syn):
    with patch("warnings.warn") as mocked_warn:
        bundle = {'entity': {
            'id': 'syn123',
            'name': 'anonymous',
            'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
            'parentId': 'syn12345'},
            'entityType': 'file',
            'restrictionInformation': {
                'hasUnmetAccessRequirement': True
            }
        }
        entity = 'syn123'
        pytest.raises(SynapseUnmetAccessRestrictions, syn._check_entity_restrictions, bundle, entity, True)
    mocked_warn.assert_not_called()


def test_check_entity_restrictions__unmet_restriction_entity_project_with_downloadFile_is_True(syn):
    with patch("warnings.warn") as mocked_warn:
        bundle = {'entity': {
            'id': 'syn123',
            'name': 'anonymous',
            'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
            'parentId': 'syn12345'},
            'entityType': 'project',
            'restrictionInformation': {
                'hasUnmetAccessRequirement': True
            }
        }
        entity = 'syn123'
        syn._check_entity_restrictions(bundle, entity, True)
    mocked_warn.assert_called_with('\nThis entity has access restrictions. Please visit the web page for this entity '
                                   '(syn.onweb("syn123")). Click the downward pointing arrow next to the file\'s name '
                                   'to review and fulfill its download requirement(s).\n')


def test_check_entity_restrictions__unmet_restriction_entity_folder_with_downloadFile_is_True(syn):
    with patch("warnings.warn") as mocked_warn:
        bundle = {'entity': {
            'id': 'syn123',
            'name': 'anonymous',
            'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
            'parentId': 'syn12345'},
            'entityType': 'folder',
            'restrictionInformation': {
                'hasUnmetAccessRequirement': True
            }
        }
        entity = 'syn123'
        syn._check_entity_restrictions(bundle, entity, True)
    mocked_warn.assert_called_with('\nThis entity has access restrictions. Please visit the web page for this entity '
                                   '(syn.onweb("syn123")). Click the downward pointing arrow next to the file\'s name '
                                   'to review and fulfill its download requirement(s).\n')


def test_check_entity_restrictions__unmet_restriction_downloadFile_is_False(syn):
    with patch("warnings.warn") as mocked_warn:
        bundle = {'entity': {
            'id': 'syn123',
            'name': 'anonymous',
            'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
            'parentId': 'syn12345'},
            'entityType': 'file',
            'restrictionInformation': {
                'hasUnmetAccessRequirement': True}
        }
        entity = 'syn123'

        syn._check_entity_restrictions(bundle, entity, False)
        mocked_warn.assert_called_once()

        bundle['entityType'] = 'project'
        syn._check_entity_restrictions(bundle, entity, False)
        assert mocked_warn.call_count == 2

        bundle['entityType'] = 'folder'
        syn._check_entity_restrictions(bundle, entity, False)
        assert mocked_warn.call_count == 3


class TestGetColumns(object):

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def test_input_is_SchemaBase(self):
        get_table_colums_results = [Column(name='A'), Column(name='B')]
        with patch.object(self.syn, "getTableColumns", return_value=iter(get_table_colums_results))\
                as mock_get_table_coulmns:
            schema = EntityViewSchema(parentId="syn123")
            results = list(self.syn.getColumns(schema))
            assert get_table_colums_results == results
            mock_get_table_coulmns.assert_called_with(schema)


def test_username_property__credentials_is_None(syn):
    syn.credentials = None
    assert syn.username is None


class TestPrivateGetEntityBundle:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.bundle = {
            'entity': {
                'id': 'syn10101',
                'name': 'anonymous',
                'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
                'parentId': 'syn12345'},
            'restrictionInformation': {
                'hasUnmetAccessRequirement': {}
            }}
        self.patch_restPOST = patch.object(self.syn, 'restPOST', return_value=self.bundle)
        self.patch_restPOST.start()

    def teardown(self):
        self.patch_restPOST.stop()

    def test__getEntityBundle__with_version_as_number(self):
        assert self.bundle == self.syn._getEntityBundle("syn10101", 6)

    def test__getEntityBundle__with_version_as_string(self):
        assert self.bundle == self.syn._getEntityBundle("syn10101", '6')
        pytest.raises(ValueError, self.syn._getEntityBundle, "syn10101", 'current')

    def test_access_restrictions(self):
        with patch.object(self.syn, '_getEntityBundle', return_value={
            'annotations': {
                'etag': 'cbda8e02-a83e-4435-96d0-0af4d3684a90',
                'id': 'syn1000002',
                'annotations': {}},
            'entity': {
                'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
                'createdBy': 'Miles Dewey Davis',
                'entityType': 'org.sagebionetworks.repo.model.FileEntity',
                'etag': 'cbda8e02-a83e-4435-96d0-0af4d3684a90',
                'id': 'syn1000002',
                'name': 'so_what.mp3',
                'parentId': 'syn1000001',
                'versionLabel': '1',
                'versionNumber': 1,
                'dataFileHandleId': '42'},

            'entityType': 'org.sagebionetworks.repo.model.FileEntity',
            'fileHandles': [
                {
                    'id': '42'
                }
            ],
            'restrictionInformation': {'hasUnmetAccessRequirement': True}
        }):
            entity = self.syn.get('syn1000002', downloadFile=False)
            assert entity is not None
            assert entity.path is None

            # Downloading the file is the default, but is an error if we have unmet access requirements
            pytest.raises(SynapseUnmetAccessRestrictions, self.syn.get, 'syn1000002',
                          downloadFile=True)


def test_move(syn):
    pytest.raises(SynapseFileNotFoundError, syn.move, "abc", "syn123")

    entity = Folder(name="folder", parent="syn456")
    moved_entity = entity
    moved_entity.parentId = "syn789"
    with patch.object(syn, "get", return_value=entity) as syn_get_patch,\
            patch.object(syn, "store", return_value=moved_entity) as syn_store_patch:
        assert moved_entity == syn.move("syn123", "syn789")
        syn_get_patch.assert_called_once_with("syn123", downloadFile=False)
        syn_store_patch.assert_called_once_with(moved_entity, forceVersion=False)


def test_delete__bad_attribute(syn):
    pytest.raises(SynapseError, syn.delete, ["foo"])


def test_delete__string(syn):
    with patch.object(syn, "restDELETE") as patch_rest_delete:
        syn.delete("syn1235")
        patch_rest_delete.assert_called_once_with(uri="/entity/syn1235")


def test_delete__string_version(syn):
    with patch.object(syn, "restDELETE") as patch_rest_delete:
        syn.delete("syn1235", version=1)
        patch_rest_delete.assert_called_once_with(uri="/entity/syn1235/version/1")


def test_delete__has_synapse_delete_attr(syn):
    mock_obj = Mock()
    syn.delete(mock_obj)
    mock_obj._synapse_delete.assert_called_once()


def test_delete__entity(syn):
    entity = Folder(name="folder", parent="syn456", id="syn1111")
    with patch.object(syn, "restDELETE") as patch_rest_delete:
        syn.delete(entity)
        patch_rest_delete.assert_called_once_with("/entity/syn1111")


def test_delete__entity_version(syn):
    entity = File(name="file", parent="syn456", id="syn1111")
    with patch.object(syn, "restDELETE") as patch_rest_delete:
        syn.delete(entity, version=2)
        patch_rest_delete.assert_called_once_with("/entity/syn1111/version/2")


def test_setPermissions__default_permissions(syn):
    entity = Folder(name="folder", parent="syn456", id="syn1")
    principalId = 123
    acl = {
        'resourceAccess': []
    }
    update_acl = {
        'resourceAccess': [{u'accessType': ["READ", "DOWNLOAD"], u'principalId': principalId}]
    }
    with patch.object(syn, "_getBenefactor", return_value=entity), \
            patch.object(syn, "_getACL", return_value=acl), \
            patch.object(syn, "_storeACL", return_value=update_acl) as patch_store_acl:
        assert update_acl == syn.setPermissions(entity, principalId)
        patch_store_acl.assert_called_once_with(entity, update_acl)


def test_get_unsaved_entity(syn):
    pytest.raises(ValueError, syn.get, Folder(name="folder", parent="syn456"))


def test_get_default_view_columns_nomask(syn):
    """Test no mask passed in"""
    with patch.object(syn, "restGET") as mock_restGET:
        syn._get_default_view_columns("viewtype")
        mock_restGET.assert_called_with(
            "/column/tableview/defaults?viewEntityType=viewtype"
        )


def test_get_default_view_columns_mask(syn):
    """Test mask passed in"""
    mask = 5
    with patch.object(syn, "restGET") as mock_restGET:
        syn._get_default_view_columns("viewtype", mask)
        mock_restGET.assert_called_with(
            "/column/tableview/defaults?viewEntityType=viewtype&viewTypeMask=5"
        )


class TestCreateStorageLocationSetting:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.patch_restPOST = patch.object(self.syn, 'restPOST')
        self.mock_restPOST = self.patch_restPOST.start()

    def teardown(self):
        self.patch_restPOST.stop()

    def test_invalid(self):
        pytest.raises(ValueError, self.syn.createStorageLocationSetting, "new storage type")

    def test_ExternalObjectStorage(self):
        self.syn.createStorageLocationSetting("ExternalObjectStorage")
        expected = {
            'concreteType': 'org.sagebionetworks.repo.model.project.ExternalObjectStorageLocationSetting',
            'uploadType': 'S3'
        }
        self.mock_restPOST.assert_called_once_with('/storageLocation', body=json.dumps(expected))

    def test_ProxyStorage(self):
        self.syn.createStorageLocationSetting("ProxyStorage")
        expected = {
            'concreteType': 'org.sagebionetworks.repo.model.project.ProxyStorageLocationSettings',
            'uploadType': 'PROXYLOCAL'
        }
        self.mock_restPOST.assert_called_once_with('/storageLocation', body=json.dumps(expected))

    def test_ExternalS3Storage(self):
        self.syn.createStorageLocationSetting("ExternalS3Storage")
        expected = {
            'concreteType': 'org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting',
            'uploadType': 'S3'
        }
        self.mock_restPOST.assert_called_once_with('/storageLocation', body=json.dumps(expected))

    def test_ExternalStorage(self):
        self.syn.createStorageLocationSetting("ExternalStorage")
        expected = {
            'concreteType': 'org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting',
            'uploadType': 'SFTP'
        }
        self.mock_restPOST.assert_called_once_with('/storageLocation', body=json.dumps(expected))


class TestDownloadList:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        # self.entity = "syn123"
        # self.expected_location = {
        #     'concreteType': 'org.sagebionetworks.repo.model.project.UploadDestinationListSetting',
        #     'settingsType': 'upload',
        #     'locations': [DEFAULT_STORAGE_LOCATION_ID],
        #     'projectId': self.entity
        # }
        #self.patch_getProjectSetting = patch.object(self.syn, 'getProjectSetting', return_value=None)
        #self.mock_getProjectSetting = self.patch_getProjectSetting.start()
        self.patch_restPOST = patch.object(self.syn, 'restPOST')
        self.mock_restPOST = self.patch_restPOST.start()
        self.patch_restPUT = patch.object(self.syn, 'restPUT')
        self.mock_restPUT = self.patch_restPUT.start()
        self.patch_restDELETE = patch.object(self.syn, 'restDELETE')
        self.mock_restDELETE = self.patch_restDELETE.start()

    def teardown(self):
        # self.patch_getProjectSetting.stop()
        self.patch_restPOST.stop()
        self.patch_restPUT.stop()
        self.patch_restDELETE.stop()

    def test_clear_download_list(self):
        self.syn.clear_download_list()
        self.mock_restDELETE.assert_called_once_with("/download/list")


class TestSetStorageLocation:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.entity = "syn123"
        self.expected_location = {
            'concreteType': 'org.sagebionetworks.repo.model.project.UploadDestinationListSetting',
            'settingsType': 'upload',
            'locations': [DEFAULT_STORAGE_LOCATION_ID],
            'projectId': self.entity
        }
        self.patch_getProjectSetting = patch.object(self.syn, 'getProjectSetting', return_value=None)
        self.mock_getProjectSetting = self.patch_getProjectSetting.start()
        self.patch_restPOST = patch.object(self.syn, 'restPOST')
        self.mock_restPOST = self.patch_restPOST.start()
        self.patch_restPUT = patch.object(self.syn, 'restPUT')
        self.mock_restPUT = self.patch_restPUT.start()

    def teardown(self):
        self.patch_getProjectSetting.stop()
        self.patch_restPOST.stop()
        self.patch_restPUT.stop()

    def test_default(self):
        self.syn.setStorageLocation(self.entity, None)
        self.mock_getProjectSetting.assert_called_once_with(self.entity, 'upload')
        self.mock_restPOST.assert_called_once_with('/projectSettings', body=json.dumps(self.expected_location))

    def test_create(self):
        storage_location_id = 333
        self.expected_location['locations'] = [storage_location_id]
        self.syn.setStorageLocation(self.entity, storage_location_id)
        self.mock_getProjectSetting.assert_called_once_with(self.entity, 'upload')
        self.mock_restPOST.assert_called_once_with('/projectSettings', body=json.dumps(self.expected_location))

    def test_update(self):
        self.mock_getProjectSetting.return_value = self.expected_location
        storage_location_id = 333
        new_location = self.expected_location
        new_location['locations'] = [storage_location_id]
        self.syn.setStorageLocation(self.entity, storage_location_id)
        self.mock_getProjectSetting.assert_called_with(self.entity, 'upload')
        assert 2 == self.mock_getProjectSetting.call_count
        self.mock_restPUT.assert_called_once_with('/projectSettings', body=json.dumps(new_location))
        self.mock_restPOST.assert_not_called()


@patch('synapseclient.core.sts_transfer.get_sts_credentials')
def test_get_sts_storage_token(mock_get_sts_credentials, syn):
    """Verify get_sts_storage_token passes through to the underlying function as expected"""
    token = {'key': 'val'}
    mock_get_sts_credentials.return_value = token

    entity = 'syn_1'
    permission = 'read_write'
    output_format = 'boto'
    min_remaining_life = datetime.timedelta(hours=1)

    result = syn.get_sts_storage_token(
        entity, permission,
        output_format=output_format, min_remaining_life=min_remaining_life
    )
    assert token == result
    mock_get_sts_credentials.assert_called_once_with(
        syn, entity, permission,
        output_format=output_format, min_remaining_life=min_remaining_life
    )


class TestCreateS3StorageLocation:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def test_folder_and_parent(self):
        """Verify we fail as expected if both parent and folder are passed"""
        with pytest.raises(ValueError):
            self.syn.create_s3_storage_location(folder_name='foo', parent=Mock(), folder=Mock())

    def test_folder_or_parent(self):
        """Verify we fail as expected if neither parent or folder are passed"""
        with pytest.raises(ValueError):
            self.syn.create_s3_storage_location()

    def _create_storage_location_test(self, expected_post_body, *args, **kwargs):
        with patch.object(self.syn, 'restPOST') as mock_post,\
                patch.object(self.syn, 'setStorageLocation') as mock_set_storage_location,\
                patch.object(self.syn, 'store') as syn_store:
            mock_post.return_value = {'storageLocationId': 456}
            mock_set_storage_location.return_value = {'id': 'foo'}

            # either passed a folder or expected to create one
            expected_folder = kwargs.get('folder')
            if not expected_folder:
                expected_folder = syn_store.return_value = Mock()

            result = self.syn.create_s3_storage_location(*args, **kwargs)

            if 'folder_name' in kwargs:
                stored_folder = syn_store.call_args[0][0]
                assert stored_folder.name == kwargs['folder_name']
                assert stored_folder.parentId == kwargs['parent']
            else:
                assert not syn_store.called

            assert expected_folder == result[0]
            assert mock_post.return_value == result[1]
            assert mock_set_storage_location.return_value == result[2]

            mock_post.assert_called_with('/storageLocation', ANY)
            assert expected_post_body == json.loads(mock_post.call_args[0][1])

    def test_synapse_s3(self):
        """Verify we create a Synapse S3 storage location if bucket is not passed
        and that we don't create a new folder if a folder is passed."""
        folder = Mock()
        for sts_enabled in (True, False):
            expected_post_body = {
                'uploadType': 'S3',
                'concreteType': concrete_types.SYNAPSE_S3_STORAGE_LOCATION_SETTING,
                'stsEnabled': sts_enabled
            }

            self._create_storage_location_test(expected_post_body, folder=folder, sts_enabled=sts_enabled)

    def test_external_s3(self):
        """Verify we create an External S3 storage location if bucket details are passed
        and that we create a folder if passed a name and a parent."""
        folder_name = 'foo'
        parent = 'syn_123'
        bucket_name = 'test_bucket'
        base_key = 'foobarbaz'
        for sts_enabled in (True, False):
            expected_post_body = {
                'uploadType': 'S3',
                'concreteType': concrete_types.EXTERNAL_S3_STORAGE_LOCATION_SETTING,
                'stsEnabled': sts_enabled,
                'bucket': bucket_name,
                'baseKey': base_key,
            }

            self._create_storage_location_test(
                expected_post_body,
                folder_name=folder_name, parent=parent, sts_enabled=sts_enabled,
                bucket_name=bucket_name, base_key=base_key,
            )


class TestCreateExternalS3FileHandle:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def _s3_file_handle_test(self, **kwargs):
        with patch.object(self.syn, '_getDefaultUploadDestination') as mock_get_upload_dest,\
            patch.object(os, 'path') as mock_os_path, \
            patch.object(os, 'stat') as mock_os_stat, \
            patch.object(utils, 'md5_for_file') as mock_md5, \
            patch('mimetypes.guess_type') as mock_guess_mimetype, \
                patch.object(self.syn, 'restPOST') as mock_post:

            bucket_name = 'foo_bucket'
            s3_file_key = '/foo/bar/baz'
            file_path = '/tmp/foo'

            mock_get_upload_dest.return_value = {'storageLocationId': 123}
            mock_guess_mimetype.return_value = 'text/plain', None
            mock_post.return_value = {'foo': 'bar'}

            mock_os_path.basename.return_value = 'foo'
            mock_os_stat.return_value.st_size = 1024
            mock_md5.return_value.hexdigest.return_value = 'fakemd5'

            expected_post_body = {
                'concreteType': concrete_types.S3_FILE_HANDLE,
                'key': s3_file_key,
                'bucketName': bucket_name,
                'fileName': mock_os_path.basename.return_value,
                'contentMd5': mock_md5.return_value.hexdigest.return_value,
                'contentSize': mock_os_stat.return_value.st_size,
                'storageLocationId': 123,
                'contentType': kwargs.get('mimetype', 'text/plain')
            }

            result = self.syn.create_external_s3_file_handle(bucket_name, s3_file_key, file_path, **kwargs)
            assert mock_post.return_value == result

            if 'storage_location_id' in kwargs:
                assert not mock_get_upload_dest.called
            else:
                mock_get_upload_dest.assert_called_once_with(kwargs['parent'])

            mock_post.assert_called_once_with('/externalFileHandle/s3', ANY, endpoint=self.syn.fileHandleEndpoint)
            assert expected_post_body == json.loads(mock_post.call_args[0][1])

    def test_with_parent_entity(self):
        """If passed a parent entity we should fetch the default upload destination
        of the entity and use that as the storage location of the file handle"""
        self._s3_file_handle_test(parent=Mock())

    def test_with_storage_location_id(self):
        """If passed a storage location id we should use that.
        Also customize mimetype"""
        self._s3_file_handle_test(storage_location_id=123, mimetype='text/html')


class TestMembershipInvitation:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def setup(self):
        self.team = synapseclient.Team(id='222')
        self.userid = '123'
        self.username = "testme_username"
        self.email = "test@email.com"
        self.member_status = {'isMember': True}
        self.response = {'inviteeId': self.userid}
        self.message = "custom message"
        self.profile = {'ownerId': self.userid}

    def test_get_team_open_invitations__team(self):
        """Get team open invitations when Team is passed in"""
        with patch.object(self.syn, "_GET_paginated",
                          return_value=self.response) as patch_get_paginated:
            response = self.syn.get_team_open_invitations(self.team)
            request = "/team/{team}/openInvitation".format(team=self.team.id)
            patch_get_paginated.assert_called_once_with(request)
            assert response == self.response

    def test_get_team_open_invitations__teamid(self):
        """Get team open invitations when team id is passed in"""
        with patch.object(self.syn, "_GET_paginated",
                          return_value=self.response) as patch_get_paginated:
            response = self.syn.get_team_open_invitations(self.team.id)
            request = "/team/{team}/openInvitation".format(team=self.team.id)
            patch_get_paginated.assert_called_once_with(request)
            assert response == self.response

    def test_teamid_get_membership_status__rest_get(self):
        """Get membership status when team id is passed in"""
        with patch.object(self.syn, "restGET",
                          return_value=self.response) as patch_restget:
            response = self.syn.get_membership_status(self.userid, self.team.id)
            request = "/team/{team}/member/{user}/membershipStatus".format(
                team=self.team.id,
                user=self.userid)
            patch_restget.assert_called_once_with(request)
            assert response == self.response

    def test_delete_membership_invitation__rest_delete(self):
        """Delete open membership invitation"""
        invitationid = 1111
        with patch.object(self.syn, "restDELETE") as patch_rest_delete:
            self.syn._delete_membership_invitation(invitationid)
            request = "/membershipInvitation/{id}".format(id=invitationid)
            patch_rest_delete.assert_called_once_with(request)

    def test_team_get_membership_status__rest_get(self):
        """Get membership status when Team is passed in"""
        with patch.object(self.syn, "restGET") as patch_restget:
            self.syn.get_membership_status(self.userid, self.team)
            request = "/team/{team}/member/{user}/membershipStatus".format(
                team=self.team.id,
                user=self.userid)
            patch_restget.assert_called_once_with(request)

    def test_send_membership_invitation__rest_post(self):
        """Test membership invitation post"""
        invite_body = {'teamId': self.team.id,
                       'message': self.message,
                       'inviteeEmail': self.email,
                       'inviteeId': self.userid}
        with patch.object(self.syn, "restPOST",
                          return_value=self.response) as patch_rest_post:
            self.syn.send_membership_invitation(
                self.team.id, inviteeId=self.userid,
                inviteeEmail=self.email,
                message=self.message
            )
            patch_rest_post.assert_called_once_with("/membershipInvitation",
                                                    body=json.dumps(invite_body))

    def test_invite_to_team__bothuseremail_specified(self):
        """Raise error when user and email is passed in"""
        pytest.raises(ValueError, self.syn.invite_to_team, self.team,
                      user=self.userid, inviteeEmail=self.email)

    def test_invite_to_team__bothuseremail_notspecified(self):
        """Raise error when user and email is passed in"""
        pytest.raises(ValueError, self.syn.invite_to_team, self.team,
                      user=None, inviteeEmail=None)

    def test_invite_to_team__email(self):
        """Invite user to team via their email"""
        invite_body = {'message': self.message,
                       'inviteeEmail': self.email,
                       'inviteeId': None}
        with patch.object(self.syn, "get_team_open_invitations",
                          return_value=[]) as patch_get_invites,\
            patch.object(self.syn, "getUserProfile",
                         return_value=self.profile) as patch_get_profile,\
            patch.object(self.syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = self.syn.invite_to_team(self.team, inviteeEmail=self.email, message=self.message)
            assert invite == self.response
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_get_profile.assert_not_called()
            patch_invitation.assert_called_once_with(self.team.id,
                                                     **invite_body)

    def test_invite_to_team__user(self):
        """Invite user to team via their Synapse userid"""
        self.member_status['isMember'] = False
        invite_body = {'inviteeId': self.userid,
                       'inviteeEmail': None,
                       'message': None}
        with patch.object(self.syn, "get_membership_status",
                          return_value=self.member_status) as patch_getmem,\
            patch.object(self.syn, "get_team_open_invitations",
                         return_value=[]) as patch_get_invites,\
            patch.object(self.syn, "getUserProfile",
                         return_value=self.profile) as patch_get_profile,\
            patch.object(self.syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = self.syn.invite_to_team(self.team, user=self.userid)
            patch_getmem.assert_called_once_with(self.userid,
                                                 self.team.id)
            patch_get_profile.assert_called_once_with(self.userid)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_invitation.assert_called_once_with(self.team.id,
                                                     **invite_body)
            assert invite == self.response

    def test_invite_to_team__username(self):
        """Invite user to team via their Synapse username"""
        self.member_status['isMember'] = False
        invite_body = {'inviteeId': self.userid,
                       'inviteeEmail': None,
                       'message': None}
        with patch.object(self.syn, "get_membership_status",
                          return_value=self.member_status) as patch_getmem,\
            patch.object(self.syn, "get_team_open_invitations",
                         return_value=[]) as patch_get_invites,\
            patch.object(self.syn, "getUserProfile",
                         return_value=self.profile) as patch_get_profile,\
            patch.object(self.syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = self.syn.invite_to_team(self.team, user=self.username)
            patch_getmem.assert_called_once_with(self.userid,
                                                 self.team.id)
            patch_get_profile.assert_called_once_with(self.username)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_invitation.assert_called_once_with(self.team.id,
                                                     **invite_body)
            assert invite == self.response

    def test_invite_to_team__ismember(self):
        """None returned when user is already a member"""
        with patch.object(self.syn, "get_membership_status",
                          return_value=self.member_status) as patch_getmem,\
            patch.object(self.syn, "get_team_open_invitations",
                         return_value=[]) as patch_get_invites,\
            patch.object(self.syn, "getUserProfile",
                         return_value=self.profile) as patch_get_profile,\
            patch.object(self.syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = self.syn.invite_to_team(self.team, user=self.userid)
            patch_getmem.assert_called_once_with(self.userid,
                                                 self.team.id)
            patch_get_profile.assert_called_once_with(self.userid)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_invitation.assert_not_called()
            assert invite is None

    def test_invite_to_team__user_openinvite(self):
        """None returned when user already has an invitation"""
        self.member_status['isMember'] = False
        invite_body = {'inviteeId': self.userid}
        with patch.object(self.syn, "get_membership_status",
                          return_value=self.member_status) as patch_getmem,\
            patch.object(self.syn, "get_team_open_invitations",
                         return_value=[invite_body]) as patch_get_invites,\
            patch.object(self.syn, "getUserProfile",
                         return_value=self.profile) as patch_get_profile,\
            patch.object(self.syn, "_delete_membership_invitation") as patch_delete,\
            patch.object(self.syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = self.syn.invite_to_team(self.team, user=self.userid)
            patch_getmem.assert_called_once_with(self.userid,
                                                 self.team.id)
            patch_get_profile.assert_called_once_with(self.userid)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_invitation.assert_not_called()
            patch_delete.assert_not_called()
            assert invite is None

    def test_invite_to_team__email_openinvite(self):
        """None returned when email already has an invitation"""
        invite_body = {'inviteeEmail': self.email}
        with patch.object(self.syn, "get_team_open_invitations",
                          return_value=[invite_body]) as patch_get_invites,\
            patch.object(self.syn, "_delete_membership_invitation") as patch_delete,\
            patch.object(self.syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = self.syn.invite_to_team(self.team, inviteeEmail=self.email)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_invitation.assert_not_called()
            patch_delete.assert_not_called()
            assert invite is None
            patch_delete.assert_not_called()

    def test_invite_to_team__none_matching_invitation(self):
        """Invitation sent when no matching open invitations"""
        invite_body = {'inviteeEmail': self.email + "foo"}
        with patch.object(self.syn, "get_team_open_invitations",
                          return_value=[invite_body]) as patch_get_invites,\
            patch.object(self.syn, "_delete_membership_invitation") as patch_delete,\
            patch.object(self.syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = self.syn.invite_to_team(self.team, inviteeEmail=self.email)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_delete.assert_not_called()
            assert invite == self.response
            patch_invitation.assert_called_once()

    def test_invite_to_team__force_invite(self):
        """Invitation sent when force the invite, make sure open invitation
        is deleted"""
        open_invitations = {'inviteeEmail': self.email, 'id': '9938'}
        with patch.object(self.syn, "get_team_open_invitations",
                          return_value=[open_invitations]) as patch_get_invites,\
            patch.object(self.syn, "_delete_membership_invitation") as patch_delete,\
            patch.object(self.syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = self.syn.invite_to_team(self.team, inviteeEmail=self.email, force=True)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_delete.assert_called_once_with(open_invitations['id'])
            assert invite == self.response
            patch_invitation.assert_called_once()


class TestRestCalls:
    """Verifies the behavior of the rest[METHOD] functions on the synapse client."""

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def _method_test_complete_args(self, method, body_expected):
        """Verify we pass through to the unified _rest_call helper method with explicit args"""
        uri = '/bar'
        body = b'foo' if body_expected else None
        endpoint = 'https://foo.com'
        headers = {'foo': 'bar'}
        retryPolicy = {'retry_status_codes': [500]}
        requests_session = create_autospec(requests.Session)
        kwargs = {'stream': True}

        syn_args = [uri]
        if body_expected:
            syn_args.append(body)

        syn_kwargs = {
            'endpoint': endpoint,
            'headers': headers,
            'retryPolicy': retryPolicy,
            'requests_session': requests_session,
        }
        syn_kwargs.update(kwargs)

        syn_method = getattr(self.syn, f"rest{method.upper()}")
        with patch.object(self.syn, '_rest_call') as mock_rest_call:
            response = syn_method(*syn_args, **syn_kwargs)
            mock_rest_call.assert_called_once_with(
                method, uri, body, endpoint, headers, retryPolicy, requests_session, **kwargs
            )

        return response

    def _method_test_default_args(self, method):
        """Verify we pass through to the unified _rest_call helper method with default args"""
        uri = '/bar'

        syn_args = [uri]
        if method == 'post':
            # restPOST has a required body positional arg
            syn_args.append(None)

        syn_method = getattr(self.syn, f"rest{method.upper()}")
        with patch.object(self.syn, '_rest_call') as mock_rest_call:
            response = syn_method(*syn_args)
            mock_rest_call.assert_called_once_with(method, uri, None, None, None, {}, None)

        return response

    def test_get(self):
        self._method_test_complete_args('get', False)
        self._method_test_default_args('get')

    def test_post(self):
        self._method_test_complete_args('post', True)
        self._method_test_default_args('post')

    def test_put(self):
        self._method_test_complete_args('put', True)
        self._method_test_default_args('put')

    def test_delete(self):
        self._method_test_complete_args('delete', False)
        self._method_test_default_args('delete')

    def _rest_call_test(self, requests_session=None):
        """Verifies the behavior of the unified _rest_call function"""
        method = 'post'
        uri = '/bar'
        data = b'data'
        endpoint = 'https://foo.com'
        headers = {'foo': 'bar'}
        retryPolicy = {'retry_status_codes': [500]}
        kwargs = {'stream': True}

        requests_session = requests_session or self.syn._requests_session
        with patch.object(self.syn, '_build_uri_and_headers') as mock_build_uri_and_headers, \
                patch.object(self.syn, '_build_retry_policy') as mock_build_retry_policy, \
                patch.object(self.syn, '_handle_synapse_http_error') as mock_handle_synapse_http_error, \
                patch.object(requests_session, method) as mock_requests_call:

            mock_build_uri_and_headers.return_value = (uri, headers)
            mock_build_retry_policy.return_value = retryPolicy

            response = self.syn._rest_call(method, uri, data, endpoint, headers, retryPolicy, requests_session,
                                           **kwargs)

        mock_build_uri_and_headers.assert_called_once_with(uri, endpoint=endpoint, headers=headers)
        mock_build_retry_policy.assert_called_once_with(retryPolicy)
        mock_handle_synapse_http_error.assert_called_once_with(response)
        mock_requests_call.assert_called_once_with(uri, data=data, headers=headers, auth=self.syn.credentials, **kwargs)

        return response

    def test_rest_call__default_session(self):
        self._rest_call_test()

    def test_rest_call__custom_session(self):
        session = create_autospec(requests.Session)
        self._rest_call_test(session)

    def _rest_call_auth_test(self, **kwargs):
        method = 'get'
        uri = '/foo'
        data = b'data'
        endpoint = 'https://foo.com'
        headers = {'foo': 'bar'}
        retryPolicy = {}
        requests_session = MagicMock(spec=requests.Session)
        response = MagicMock(spec=requests.Response)
        response.status_code = 200
        requests_session.get.return_value = response

        self.syn._rest_call(method, uri, data, endpoint, headers, retryPolicy, requests_session, **kwargs)
        return requests_session.get.call_args_list[0][1]['auth']

    def test_rest_call__default_auth(self):
        """Verify that _rest_call will use the Synapse object's credentials unless overridden"""
        assert self._rest_call_auth_test() is self.syn.credentials

    def test_rest_call__passed_auth(self, mocker):
        """Verify that _rest_call will use a custom auth object if passed"""
        auth = MagicMock(spec=synapseclient.core.credentials.cred_data.SynapseCredentials)
        assert self._rest_call_auth_test(auth=auth) is auth


class TestSetAnnotations:

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    def test_not_annotation(self):
        with patch.object(self.syn, "restPUT") as mock_rest_put:
            # pass in non-annotation object
            pytest.raises(TypeError, self.syn.set_annotations, {})
            mock_rest_put.assert_not_called()

    def test_with_annotations(self):
        with patch.object(self.syn, "restPUT") as mock_rest_put:
            mock_rest_put.return_value = {'id': 'syn123',
                                          'etag': '82196a4c-d383-439a-a08a-c07090a8c147',
                                          'annotations': {'foo': {'type': 'STRING', 'value': ['bar']}}}
            # pass in non-annotation object
            self.syn.set_annotations(Annotations('syn123', '1d6c46e4-4d52-44e1-969f-e77b458d815a', {'foo': 'bar'}))
            mock_rest_put.assert_called_once_with('/entity/syn123/annotations2',
                                                  body='{"id": "syn123",'
                                                       ' "etag": "1d6c46e4-4d52-44e1-969f-e77b458d815a",'
                                                       ' "annotations": {"foo": {"type": "STRING", '
                                                       '"value": ["bar"]}}}')


def test_get_unparseable_config():
    """Verify that if the synapseConfig is not parseable we fail
    in an expected way and surface the underlying parse error."""
    config_error_msg = 'bad config'
    with patch('configparser.RawConfigParser.read') as read_config:
        read_config.side_effect = configparser.Error(config_error_msg)

        with pytest.raises(ValueError) as cm:
            Synapse(debug=False, skip_checks=True, configPath='/foo')

        # underlying error should be chained
        assert (
            config_error_msg ==
            str(cm.value.__context__))


def test_get_config_file_caching():
    """Verify we read a config file once per Synapse and are not
    parsing the file multiple times just on init."""

    with patch('configparser.RawConfigParser.read') as read_config:
        read_config.return_value = configparser.ConfigParser()

        syn1 = Synapse(debug=False, skip_checks=True, configPath='/foo')

        # additional calls shouldn't be returned via a cached value
        config1a = syn1.getConfigFile('/foo')
        config1b = syn1.getConfigFile('/foo')
        assert config1a == config1b
        assert 1 == read_config.call_count

        # however a new instance should not be cached
        Synapse(debug=False, skip_checks=True, configPath='/foo')
        assert 2 == read_config.call_count

        # but an additional call on that instance should be
        assert 2 == read_config.call_count


def test_max_threads_bounded(syn):
    """Verify we disallow setting max threads higher than our cap."""
    syn.max_threads = client.MAX_THREADS_CAP + 1
    assert syn.max_threads == client.MAX_THREADS_CAP

    syn.max_threads = 0
    assert syn.max_threads == 1


@patch('synapseclient.Synapse._get_config_section_dict')
def test_get_transfer_config(mock_config_dict):
    """Verify reading transfer.maxThreads from synapseConfig"""

    # note that RawConfigParser lower cases its option values so we
    # simulate that behavior in our mocked values here

    default_values = {'max_threads': client.DEFAULT_NUM_THREADS, 'use_boto_sts_transfers': False}

    for config_dict, expected_values in [
        # empty values get defaults
        ({}, default_values),
        ({'max_threads': '', 'use_boto_sts': ''}, default_values),
        ({'max_thraeds': None, 'use_boto_sts': None}, default_values),

        # explicit values should be parsed
        ({'max_threads': '1', 'use_boto_sts': 'True'}, {'max_threads': 1, 'use_boto_sts_transfers': True}),
        ({'max_threads': '7', 'use_boto_sts': 'true'}, {'max_threads': 7, 'use_boto_sts_transfers': True}),
        ({'max_threads': '100', 'use_boto_sts': 'false'}, {'max_threads': 100, 'use_boto_sts_transfers': False}),
    ]:
        mock_config_dict.return_value = config_dict
        syn = Synapse(skip_checks=True)
        for k, v in expected_values.items():
            assert v == getattr(syn, k)

    # invalid value for max threads should raise an error
    for invalid_max_thread_value in ('not a number', '12.2', 'true'):
        mock_config_dict.return_value = {'max_threads': invalid_max_thread_value}
        with pytest.raises(ValueError):
            Synapse(skip_checks=True)

    # invalid value for use_boto_sts should raise an error
    for invalid_max_thread_value in ('not true', '1.2', '0', 'falsey'):
        mock_config_dict.return_value = {'use_boto_sts': invalid_max_thread_value}
        with pytest.raises(ValueError):
            Synapse(skip_checks=True)


@patch('synapseclient.Synapse._get_config_section_dict')
def test_transfer_config_values_overridable(mock_config_dict):
    """Verify we can override the default transfer config values by setting them directly on the Synapse object"""

    mock_config_dict.return_value = {'max_threads': 24, 'use_boto_sts': False}
    syn = Synapse(skip_checks=True)

    assert 24 == syn.max_threads
    assert not syn.use_boto_sts_transfers

    syn.max_threads = 5
    syn.use_boto_sts_transfers = True
    assert 5 == syn.max_threads
    assert syn.use_boto_sts_transfers


def test_store__needsUploadFalse__fileHandleId_not_in_local_state(syn):
    returned_file_handle = {
        'id': '1234'
    }
    parent_id = 'syn122'
    synapse_id = 'syn123'
    etag = 'db9bc70b-1eb6-4a21-b3e8-9bf51d964031'
    returned_bundle = {'entity': {'name': 'fake_file.txt',
                                  'id': synapse_id,
                                  'etag': etag,
                                  'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
                                  'dataFileHandleId': '123412341234'},
                       'entityType': 'file',
                       'fileHandles': [{'id': '123412341234',
                                        'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle'}],
                       'annotations': {'id': synapse_id, 'etag': etag, 'annotations': {}},
                       }
    with patch.object(syn, '_getEntityBundle', return_value=returned_bundle), \
         patch.object(synapseclient.client, 'upload_file_handle', return_value=returned_file_handle), \
         patch.object(syn.cache, 'contains', return_value=True), \
         patch.object(syn, '_updateEntity'), \
         patch.object(syn, 'set_annotations'), \
         patch.object(Entity, 'create'), \
         patch.object(syn, 'get'):

        f = File('/fake_file.txt', parent=parent_id)
        syn.store(f)
        # test passes if no KeyError exception is thrown


def test_store__existing_processed_as_update(syn):
    """Test that storing an entity without its id but that matches an existing
    entity bundle will be processed as an entity update"""

    file_handle_id = '123412341234'
    returned_file_handle = {
        'id': file_handle_id
    }

    parent_id = 'syn122'
    synapse_id = 'syn123'
    etag = 'db9bc70b-1eb6-4a21-b3e8-9bf51d964031'
    file_name = 'fake_file.txt'

    existing_bundle_annotations = {
        'foo': {
            'type': 'LONG',
            'value': ['1']
        },

        # this annotation is not included in the passed which is interpreted as a deletion
        'bar': {
            'type': 'LONG',
            'value': ['2']
        },
    }
    new_annotations = {
        'foo': [3],
        'baz': [4],
    }

    returned_bundle = {
        'entity': {
            'name': file_name,
            'id': synapse_id,
            'etag': etag,
            'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
            'dataFileHandleId': file_handle_id,
        },
        'entityType': 'file',
        'fileHandles': [
            {
                'id': file_handle_id,
                'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
            }
        ],
        'annotations': {
            'id': synapse_id,
            'etag': etag,
            'annotations': existing_bundle_annotations
        },
    }

    expected_update_properties = {
        'id': synapse_id,
        'etag': etag,
        'name': file_name,
        'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
        'dataFileHandleId': file_handle_id,
        'parentId': parent_id,
        'versionComment': None,
    }
    expected_annotations = {
        'foo': [3],
        'baz': [4],
    }

    with patch.object(syn, '_getEntityBundle') as mock_get_entity_bundle, \
         patch.object(synapseclient.client, 'upload_file_handle', return_value=returned_file_handle), \
         patch.object(syn.cache, 'contains', return_value=True), \
         patch.object(syn, '_createEntity') as mock_createEntity, \
         patch.object(syn, '_updateEntity') as mock_updateEntity, \
         patch.object(syn, 'findEntityId') as mock_findEntityId, \
         patch.object(syn, 'set_annotations') as mock_set_annotations, \
         patch.object(Entity, 'create'), \
         patch.object(syn, 'get'):

        mock_get_entity_bundle.return_value = returned_bundle

        f = File(f"/{file_name}", parent=parent_id, **new_annotations)
        syn.store(f)

        mock_updateEntity.assert_called_once_with(
            expected_update_properties,
            True,  # createOrUpdate
            None,  # versionLabel
        )

        mock_set_annotations.assert_called_once_with(expected_annotations)

        assert not mock_createEntity.called
        assert not mock_findEntityId.called


def test_store__409_processed_as_update(syn):
    """Test that if we get a 409 conflict when creating an entity we re-retrieve its
    associated bundle and process it as an entity update instead."""
    file_handle_id = '123412341234'
    returned_file_handle = {
        'id': file_handle_id
    }

    parent_id = 'syn122'
    synapse_id = 'syn123'
    etag = 'db9bc70b-1eb6-4a21-b3e8-9bf51d964031'
    file_name = 'fake_file.txt'

    existing_bundle_annotations = {
        'foo': {
            'type': 'LONG',
            'value': ['1']
        },
        'bar': {
            'type': 'LONG',
            'value': ['2']
        },
    }
    new_annotations = {
        'foo': [3],
        'baz': [4],
    }

    returned_bundle = {
        'entity': {
            'name': file_name,
            'id': synapse_id,
            'etag': etag,
            'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
            'dataFileHandleId': file_handle_id,
        },
        'entityType': 'file',
        'fileHandles': [
            {
                'id': file_handle_id,
                'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
            }
        ],
        'annotations': {
            'id': synapse_id,
            'etag': etag,
            'annotations': existing_bundle_annotations
        },
    }

    expected_create_properties = {
        'name': file_name,
        'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
        'dataFileHandleId': file_handle_id,
        'parentId': parent_id,
        'versionComment': None,
    }
    expected_update_properties = {
        **expected_create_properties,
        'id': synapse_id,
        'etag': etag,
    }

    # we expect the annotations to be merged
    expected_annotations = {
        'foo': [3],
        'bar': [2],
        'baz': [4],
    }

    with patch.object(syn, '_getEntityBundle') as mock_get_entity_bundle, \
         patch.object(synapseclient.client, 'upload_file_handle', return_value=returned_file_handle), \
         patch.object(syn.cache, 'contains', return_value=True), \
         patch.object(syn, '_createEntity') as mock_createEntity, \
         patch.object(syn, '_updateEntity') as mock_updateEntity, \
         patch.object(syn, 'findEntityId') as mock_findEntityId, \
         patch.object(syn, 'set_annotations') as mock_set_annotations, \
         patch.object(Entity, 'create'), \
         patch.object(syn, 'get'):

        mock_get_entity_bundle.side_effect = [None, returned_bundle]
        mock_createEntity.side_effect = SynapseHTTPError(response=DictObject({'status_code': 409}))
        mock_findEntityId.return_value = synapse_id

        f = File(f"/{file_name}", parent=parent_id, **new_annotations)
        syn.store(f)

        mock_updateEntity.assert_called_once_with(
            expected_update_properties,
            True,  # createOrUpdate
            None,  # versionLabel
        )

        mock_set_annotations.assert_called_once_with(expected_annotations)
        mock_createEntity.assert_called_once_with(expected_create_properties)
        mock_findEntityId.assert_called_once_with(file_name, parent_id)


def test_store__no_need_to_update_annotation(syn):
    """
    Verify if the annotations don't change, no need to call set_annotation method
    """
    file_handle_id = '123412341234'
    returned_file_handle = {
        'id': file_handle_id
    }

    parent_id = 'syn122'
    synapse_id = 'syn123'
    etag = 'db9bc70b-1eb6-4a21-b3e8-9bf51d964031'
    file_name = 'fake_file.txt'

    existing_bundle_annotations = {
        'foo': {
            'type': 'LONG',
            'value': ['1']
        },
        'bar': {
            'type': 'LONG',
            'value': ['2']
        },
    }
    new_annotations = {
        'foo': [1],
        'bar': 2,
    }

    returned_bundle = {
        'entity': {
            'name': file_name,
            'id': synapse_id,
            'etag': etag,
            'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
            'dataFileHandleId': file_handle_id,
        },
        'entityType': 'file',
        'fileHandles': [
            {
                'id': file_handle_id,
                'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
            }
        ],
        'annotations': {
            'id': synapse_id,
            'etag': etag,
            'annotations': existing_bundle_annotations
        },
    }

    with patch.object(syn, '_getEntityBundle') as mock_get_entity_bundle, \
            patch.object(synapseclient.client, 'upload_file_handle', return_value=returned_file_handle), \
            patch.object(syn.cache, 'contains', return_value=True), \
            patch.object(syn, '_createEntity'), \
            patch.object(syn, '_updateEntity'), \
            patch.object(syn, 'findEntityId'), \
            patch.object(syn, 'set_annotations') as mock_set_annotations, \
            patch.object(Entity, 'create'), \
            patch.object(syn, 'get'):
        mock_get_entity_bundle.return_value = returned_bundle

        f = File(f"/{file_name}", parent=parent_id, **new_annotations)
        syn.store(f)

        mock_set_annotations.assert_not_called()


def test_store__update_versionComment(syn):
    file_handle_id = '123412341234'
    returned_file_handle = {
        'id': file_handle_id
    }

    parent_id = 'syn122'
    synapse_id = 'syn123'
    etag = 'db9bc70b-1eb6-4a21-b3e8-9bf51d964031'
    file_name = 'fake_file.txt'

    existing_bundle_annotations = {
        'foo': {
            'type': 'LONG',
            'value': ['1']
        },

        # this annotation is not included in the passed which is interpreted as a deletion
        'bar': {
            'type': 'LONG',
            'value': ['2']
        },
    }
    returned_bundle = {
        'entity': {
            'name': file_name,
            'id': synapse_id,
            'etag': etag,
            'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
            'dataFileHandleId': file_handle_id,
        },
        'entityType': 'file',
        'fileHandles': [
            {
                'id': file_handle_id,
                'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
            }
        ],
        'annotations': {
            'id': synapse_id,
            'etag': etag,
            'annotations': existing_bundle_annotations
        },
    }

    expected_update_properties = {
        'id': synapse_id,
        'etag': etag,
        'name': file_name,
        'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
        'dataFileHandleId': file_handle_id,
        'parentId': parent_id,
        'versionComment': '12345',
    }

    with patch.object(syn, '_getEntityBundle') as mock_get_entity_bundle, \
            patch.object(synapseclient.client, 'upload_file_handle', return_value=returned_file_handle), \
            patch.object(syn.cache, 'contains', return_value=True), \
            patch.object(syn, '_createEntity') as mock_createEntity, \
            patch.object(syn, '_updateEntity') as mock_updateEntity, \
            patch.object(syn, 'findEntityId') as mock_findEntityId, \
            patch.object(syn, 'set_annotations') as mock_set_annotations, \
            patch.object(Entity, 'create'), \
            patch.object(syn, 'get'):
        mock_get_entity_bundle.return_value = returned_bundle

        f = File(f"/{file_name}", parent=parent_id, versionComment='12345')
        syn.store(f)

        assert mock_set_annotations.called
        assert not mock_createEntity.called
        assert not mock_findEntityId.called

        mock_updateEntity.assert_called_once_with(
            expected_update_properties,
            True,  # createOrUpdate
            None,  # versionLabel
        )

        # entity that stores on synapse without versionComment
        f = File(f"/{file_name}", parent=parent_id)
        expected_update_properties['versionComment'] = None
        syn.store(f)

        mock_updateEntity.assert_called_with(
            expected_update_properties,
            True,  # createOrUpdate
            None,  # versionLabel
        )


def test_update_entity_version(syn):
    """Confirm behavior of entity version incrementing/labeling when invoking syn._updateEntity"""
    entity_id = 'syn123'
    entity = File(id=entity_id, parent='syn123', properties={'foo': 'bar'})
    expected_uri = f"/entity/{entity_id}"

    with patch.object(syn, 'restPUT') as mock_rest_put:
        # defaults to incrementVersion=True
        syn._updateEntity(entity)
        mock_rest_put.assert_called_with(
            expected_uri,
            body=json.dumps(utils.get_properties(entity)),
            params={'newVersion': 'true'},
        )

        # explicitly do not increment
        syn._updateEntity(entity, incrementVersion=False)
        mock_rest_put.assert_called_with(
            expected_uri,
            body=json.dumps(utils.get_properties(entity)),
            params={},
        )

        # custom versionLabel
        versionLabel = 'foo'
        expected_body_dict = utils.get_properties(entity).copy()
        expected_body_dict['versionLabel'] = versionLabel
        syn._updateEntity(entity, versionLabel=versionLabel)
        mock_rest_put.assert_called_with(
            expected_uri,
            body=json.dumps(expected_body_dict),
            params={'newVersion': 'true'},
        )


def test_store__existing_no_update(syn):
    """Test that we won't try processing a store as an update if there's an existing
    bundle if createOrUpdate is not specified."""

    file_handle_id = '123412341234'
    returned_file_handle = {
        'id': file_handle_id
    }

    parent_id = 'syn122'
    synapse_id = 'syn123'
    etag = 'db9bc70b-1eb6-4a21-b3e8-9bf51d964031'
    file_name = 'fake_file.txt'

    returned_bundle = {
        'entity': {
            'name': file_name,
            'id': synapse_id,
            'etag': etag,
            'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
            'dataFileHandleId': file_handle_id,
        },
        'entityType': 'file',
        'fileHandles': [
            {
                'id': file_handle_id,
                'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
            }
        ],
        'annotations': {}
    }

    with patch.object(syn, '_getEntityBundle') as mock_get_entity_bundle, \
         patch.object(synapseclient.client, 'upload_file_handle', return_value=returned_file_handle), \
         patch.object(syn.cache, 'contains', return_value=True), \
         patch.object(syn, '_createEntity') as mock_createEntity, \
         patch.object(syn, '_updateEntity') as mock_updatentity, \
         patch.object(syn, 'get'):

        mock_get_entity_bundle.return_value = returned_bundle
        mock_createEntity.side_effect = SynapseHTTPError(response=DictObject({'status_code': 409}))

        f = File(f"/{file_name}", parent=parent_id)

        with pytest.raises(SynapseHTTPError) as ex_cm:
            syn.store(f, createOrUpdate=False)

        assert 409 == ex_cm.value.response.status_code

        # should not have attempted an update
        assert not mock_updatentity.called


def test_get_submission_with_annotations(syn):
    """Verify a getSubmission with annotation entityBundleJSON that
    uses the old style annotations is converted to bundle v2 style
    before being used to retrieve a related entity as part of
    a getSubmission call"""

    evaluation_id = 98765,
    submission_id = 67890
    entity_id = 12345

    old_annotation_json = {
        'id': entity_id,
        'etag': str(uuid.uuid4()),
        'stringAnnotations': {
            'foo': ['bar', 'baz'],
        }
    }

    entity_bundle_json = {'annotations': old_annotation_json}
    converted_bundle_json = {
        'annotations': convert_old_annotation_json(old_annotation_json)
    }

    submission = {
        'evaluationId': evaluation_id,
        'entityId': entity_id,
        'versionNumber': 1,
        'entityBundleJSON': json.dumps(entity_bundle_json),
    }

    with patch.object(syn, 'restGET') as restGET,\
            patch.object(syn, '_getWithEntityBundle') as get_entity:

        restGET.return_value = submission
        response = syn.getSubmission(submission_id)

        restGET.assert_called_once_with(f'/evaluation/submission/{submission_id}')
        get_entity.assert_called_once_with(
            entityBundle=converted_bundle_json,
            entity=entity_id,
            submission=str(submission_id),
        )
        assert evaluation_id == response["evaluationId"]


class TestTableSnapshot:

    def test__create_table_snapshot(self, syn):
        """Testing creating table snapshots"""
        snapshot = {'snapshotVersionNumber': 2}
        with patch.object(syn, 'restPOST', return_value=snapshot) as restpost:
            syn._create_table_snapshot("syn1234", comment="foo", label="new_label",
                                       activity=2)
            restpost.assert_called_once_with(
                "/entity/syn1234/table/snapshot",
                body='{"snapshotComment": "foo", "snapshotLabel": "new_label", '
                     '"snapshotActivityId": "2"}'
            )

    def test__create_table_snapshot__no_params(self, syn):
        """Testing creating table snapshots when no optional parameters are specified"""
        snapshot = {'snapshotVersionNumber': 2}
        with patch.object(syn, 'restPOST', return_value=snapshot) as restpost:
            syn._create_table_snapshot("syn1234")
            restpost.assert_called_once_with(
                "/entity/syn1234/table/snapshot",
                body='{}'
            )

    def test__create_table_snapshot__with_activity(self, syn):
        """
        Testing creating table snapshots pass in the activity without ID property
        """
        snapshot = {'snapshotVersionNumber': 2}
        activity = Activity(name="test_activity", description="test_description")
        mock_dict = {'name': activity['name'], 'description': activity['description'], 'id': 123}
        with patch.object(syn, 'restPOST', return_value=snapshot) as restpost, \
                patch.object(syn, '_saveActivity') as mock__saveActivity:

            mock__saveActivity.return_value = mock_dict
            syn._create_table_snapshot("syn1234", comment="foo", label="new_label",
                                       activity=activity)
            mock__saveActivity.assert_called_with(activity)
            restpost.assert_called_once_with(
                '/entity/syn1234/table/snapshot',
                body='{"snapshotComment": "foo", "snapshotLabel": "new_label", "snapshotActivityId": 123}'
            )

    def test__async_table_update(self, syn):
        """Async table update"""

        snapshot = {'snapshotVersionNumber': 2}
        with patch.object(syn, '_waitForAsync', return_value=snapshot) as waitforasync:
            result = syn._async_table_update(
                "syn1234",
                create_snapshot=True,
                comment="foo",
                label="new_label",
                activity=2,
                wait=True,
            )
            waitforasync.assert_called_once_with(
                "/entity/syn1234/table/transaction/async",
                {
                    'changes': [],
                    'createSnapshot': True,
                    'snapshotOptions': {
                        'snapshotComment': 'foo',
                        'snapshotLabel': 'new_label',
                        'snapshotActivityId': 2,
                    }
                }
            )
            assert snapshot == result

        async_token = {'token': 2}
        with patch.object(syn, 'restPOST', return_value=async_token) as restpost:
            result = syn._async_table_update(
                "syn1234",
                create_snapshot=True,
                comment="foo",
                label="new_label",
                activity=2,
                wait=False,
            )
            restpost.assert_called_once_with(
                "/entity/syn1234/table/transaction/async/start",
                body='{"changes": [], "createSnapshot": true, '
                     '"snapshotOptions": {"snapshotComment": "foo", '
                     '"snapshotLabel": "new_label", '
                     '"snapshotActivityId": 2}}'
            )
            assert async_token == result

    def test_create_snapshot_version_table(self, syn):
        """Create Table snapshot"""
        table = Mock(Schema)
        snapshot_version = 3
        with patch.object(syn, 'get', return_value=table) as get,\
                patch.object(syn, '_create_table_snapshot',
                             return_value={'snapshotVersionNumber': snapshot_version}) as create:
            result = syn.create_snapshot_version("syn1234", comment="foo", label="new_label", activity=2, wait=True)
            get.assert_called_once_with(utils.id_of("syn1234"), downloadFile=False)
            create.assert_called_once_with(
                "syn1234",
                comment="foo", label="new_label",
                activity=2
            )
            assert result == snapshot_version

            result = syn.create_snapshot_version("syn1234", comment="foo", label="new_label", activity=2, wait=False)
            assert result is None

    def test_create_snapshot_version_entityview(self, syn):
        """Create Entity View snapshot"""
        views = [Mock(EntityViewSchema), Mock(SubmissionViewSchema)]
        for view in views:
            snapshot_version = 3
            with patch.object(syn, 'get', return_value=view) as get,\
                    patch.object(syn, '_async_table_update',
                                 return_value={'snapshotVersionNumber': snapshot_version}) as update:
                result = syn.create_snapshot_version(
                    "syn1234",
                    comment="foo",
                    label="new_label",
                    activity=2,
                    wait=True,
                )
                get.assert_called_once_with(utils.id_of("syn1234"), downloadFile=False)
                update.assert_called_once_with(
                    "syn1234", create_snapshot=True,
                    comment="foo", label="new_label",
                    activity=2, wait=True,
                )
                assert snapshot_version == result

            with patch.object(syn, 'get', return_value=view) as get, \
                    patch.object(syn, '_async_table_update',
                                 return_value={'token': 5}) as update:
                result = syn.create_snapshot_version(
                    "syn1234",
                    comment="foo",
                    label="new_label",
                    activity=2,
                    wait=False
                )
                get.assert_called_once_with(utils.id_of("syn1234"), downloadFile=False)
                update.assert_called_once_with(
                    "syn1234", create_snapshot=True,
                    comment="foo", label="new_label",
                    activity=2, wait=False,
                )
                assert result is None

    def test_create_snapshot_version_raiseerror(self, syn):
        """Raise error if entity view or table not passed in"""
        wrong_type = Mock()
        with patch.object(syn, 'get', return_value=wrong_type),\
             pytest.raises(ValueError, match="This function only accepts Synapse ids of Tables or Views"):
            syn.create_snapshot_version("syn1234")


def test__get_annotation_view_columns(syn):
    """Test getting a view's columns based on existing annotations"""
    page1 = {'results': [{'id': 5}],
             'nextPageToken': 'a'}
    page2 = {'results': [],
             'nextPageToken': None}
    view_scope1 = {
        'concreteType': 'org.sagebionetworks.repo.model.table.ViewColumnModelRequest',
        'viewScope': {
            'scope': ['syn1234'],
            'viewEntityType': "submissionview",
            'viewTypeMask': "0x1"
        }
    }
    view_scope2 = {
        'concreteType': 'org.sagebionetworks.repo.model.table.ViewColumnModelRequest',
        'viewScope': {
            'scope': ['syn1234'],
            'viewEntityType': "submissionview",
            'viewTypeMask': "0x1"
        },
        'nextPageToken': 'a'
    }
    call_list = [call(uri="/column/view/scope/async",
                      request=view_scope1),
                 call(uri="/column/view/scope/async",
                      request=view_scope2)]

    with patch.object(syn, "_waitForAsync",
                      side_effect=[page1, page2]) as wait_for_async:
        columns = syn._get_annotation_view_columns(
            scope_ids=['syn1234'],
            view_type="submissionview",
            view_type_mask="0x1"
        )
        wait_for_async.assert_has_calls(call_list)
        assert columns == [synapseclient.Column(id=5)]


class TestGenerateHeaders:

    def test_generate_headers(self):
        """Verify expected headers"""

        syn = Synapse(skip_checks=True)

        headers = syn._generate_headers()
        expected = {}
        expected.update(syn.default_headers)
        expected.update(synapseclient.USER_AGENT)

        assert expected == headers

    def test_generate_headers__custom_headers(self):
        """Verify that custom headers override default headers"""

        custom_headers = {
            'foo': 'bar'
        }

        syn = Synapse(skip_checks=True)

        headers = syn._generate_headers(headers=custom_headers)
        expected = {}
        expected.update(custom_headers)
        expected.update(synapseclient.USER_AGENT)

        assert expected == headers


class TestHandleSynapseHTTPError:

    def test_handle_synapse_http_error__not_logged_in(self):
        """If you are not LOGGED in a http error with an unauthenticated/forbidden
        status code should raise an SynapseAuthenticationError chained from the
        underlying SynapseHTTPError"""
        syn = Synapse(skip_checks=True)
        syn.credentials = None

        for status_code in (401, 403):
            response = Mock(status_code=status_code, headers={})

            with pytest.raises(SynapseAuthenticationError) as cm_ex:
                syn._handle_synapse_http_error(response)

            assert isinstance(cm_ex.value.__cause__, SynapseHTTPError)
            assert status_code == cm_ex.value.__cause__.response.status_code

    def test_handle_synapse_http_error__logged_in(self):
        """If you are logged in a SynapseHTTPError should be raised directly,
        even if it is an unauthenticated/forbidden error."""
        syn = Synapse(skip_checks=True)
        syn.credentials = Mock()
        for status_code in (401, 403, 404):
            response = Mock(status_code=status_code, headers={})

            with pytest.raises(SynapseHTTPError) as cm_ex:
                syn._handle_synapse_http_error(response)

            assert status_code == cm_ex.value.response.status_code


def test_ensure_download_location_is_directory(syn):
    downloadLocation = '/foo/bar/baz'
    with patch.object(client, 'os') as mock_os:
        mock_os.path.isfile.return_value = False
        syn._ensure_download_location_is_directory(downloadLocation)

        mock_os.path.isfile.return_value = True
        with pytest.raises(ValueError):
            syn._ensure_download_location_is_directory(downloadLocation)


class TestTableQuery:

    @patch.object(client, 'CsvFileTable')
    def test_table_query__csv(self, mock_csv, syn):
        query = 'select id from syn123'
        kwargs = {
            'quoteCharacter': '|',
            'downloadLocation': '/foo/bar',
        }

        expected_return = Mock()
        mock_csv.from_table_query.return_value = expected_return

        actual_return = syn.tableQuery(query, resultsAs='csv', **kwargs)
        assert expected_return == actual_return
        mock_csv.from_table_query.assert_called_once_with(syn, query, **kwargs)

    @patch.object(client, 'TableQueryResult')
    def test_table_query__rowset(self, mock_result, syn):
        query = 'select id from syn123'
        kwargs = {'isConsistent': 'true'}

        expected_return = Mock()
        mock_result.return_value = expected_return

        actual_return = syn.tableQuery(query, resultsAs='rowset', **kwargs)
        assert expected_return == actual_return
        mock_result.assert_called_once_with(syn, query, **kwargs)

    @pytest.mark.parametrize('downloadLocation', [None, '/foo/baz'])
    def test_query_table_csv(self, downloadLocation, syn):
        """Verify the behavior of _queryTableCsv, both with a user specified downloadLocation and without"""

        query = 'select id from syn123'
        quoteCharacter = '|'
        escapeCharacter = '^'
        lineEnd = '\n'
        separator = '$'
        header = True
        includeRowIdAndRowVersion = True
        file_handle_id = 123
        cache_dir = os.path.join('foo', 'bar')

        expanduser = os.path.expanduser
        expandvars = os.path.expandvars
        os_join = os.path.join
        with patch.object(syn, '_waitForAsync') as mock_waitForAsync, \
                patch.object(syn, 'cache') as mock_cache, \
                patch.object(syn, '_downloadFileHandle') as mock_download_file_handle, \
                patch.object(client, 'os') as mock_os:

            mock_download_result = {'resultsFileHandleId': file_handle_id}
            mock_waitForAsync.return_value = mock_download_result
            mock_cache.get.return_value = None
            mock_cache.get_cache_dir.return_value = cache_dir
            mock_os.path.isfile.return_value = False

            # keep this fns intact
            mock_os.path.join = os_join
            mock_os.path.expanduser = expanduser
            mock_os.path.expandvars = expandvars

            kwargs = {}
            expected_dir = cache_dir
            if downloadLocation:
                expected_dir = downloadLocation
                kwargs['downloadLocation'] = downloadLocation

            expected_file_name = f"SYNAPSE_TABLE_QUERY_{file_handle_id}.csv"
            expected_path = os_join(expected_dir, expected_file_name)
            mock_download_file_handle.return_value = expected_path

            actual_result = syn._queryTableCsv(
                query,
                quoteCharacter=quoteCharacter,
                escapeCharacter=escapeCharacter,
                lineEnd=lineEnd,
                separator=separator,
                header=header,
                includeRowIdAndRowVersion=includeRowIdAndRowVersion,
                **kwargs
            )

            mock_os.makedirs.assert_called_once_with(expected_dir, exist_ok=True)
            mock_download_file_handle.assert_called_once_with(
                file_handle_id,
                'syn123',
                'TableEntity',
                expected_path,
            )

            assert (mock_download_result, expected_path) == actual_result


class TestSilentCommandAndLogger:

    def setup(self):
        """
        Set up three different synapse objects
        """
        self.syn = Synapse(debug=False, skip_checks=True)
        self.syn_with_silent = Synapse(silent=True, debug=False, skip_checks=True)
        self.syn_with_debug = Synapse(silent=False, debug=True, skip_checks=True)

    def test_syn_silent(self):
        """
        Verify the silent property is set up correctly
        """
        assert self.syn.silent is None
        assert self.syn_with_silent.silent is True
        assert self.syn_with_debug.silent is False

    def test_syn_logger_name(self):
        """
        According to the properties silent and debug, logger name should be different
        """
        assert self.syn.logger.name == DEFAULT_LOGGER_NAME
        assert self.syn_with_silent.logger.name == SILENT_LOGGER_NAME
        assert self.syn_with_debug.logger.name == DEBUG_LOGGER_NAME

    @patch.object(client, 'cumulative_transfer_progress')
    def test_print_transfer_progress(self, mock_ctp):
        """
        Verify the private method print_transfer_progress will run with property self.silent accordingly
        """
        mock_kwargs = {
            'isBytes': False,
            'dt': 10
        }
        self.syn_with_silent._print_transfer_progress("transferred", "toBeTransferred", 'Downloading ', mock_kwargs)
        mock_ctp.printTransferProgress.assert_not_called()

        self.syn._print_transfer_progress("transferred", "toBeTransferred", 'Downloading ', mock_kwargs)
        mock_ctp.printTransferProgress.assert_called_once_with("transferred", "toBeTransferred", 'Downloading ',
                                                               mock_kwargs)


@pytest.mark.parametrize("userid", [999, 1456])
def test__get_certified_passing_record(userid, syn):
    """Test correct rest call"""
    response = {"test": 5}
    with patch.object(syn, "restGET", return_value=response) as patch_get:
        record = syn._get_certified_passing_record(userid)
        patch_get.assert_called_once_with(
            f"/user/{userid}/certifiedUserPassingRecord"
        )
        assert record == response


@pytest.mark.parametrize("response", [True, False])
def test_is_certified(response, syn):
    with patch.object(syn, "getUserProfile",
                      return_value={"ownerId": "foobar"}) as patch_get_user,\
         patch.object(syn,
                      "_get_certified_passing_record",
                      return_value={'passed': response}) as patch_get_cert:
        is_certified = syn.is_certified("test")
        patch_get_user.assert_called_once_with("test")
        patch_get_cert.assert_called_once_with("foobar")
        assert is_certified is response


def test_is_certified__no_quiz_results(syn):
    """Verify handling of a user that hasn't taken the quiz at all.
    In this case the back end returns a 404 rather than a result."""
    response = MagicMock(requests.Response)
    response.status_code = 404
    with patch.object(syn, "getUserProfile",
                      return_value={"ownerId": "foobar"}) as patch_get_user,\
         patch.object(syn,
                      "_get_certified_passing_record",
                      side_effect=SynapseHTTPError(response=response)) as patch_get_cert:
        is_certified = syn.is_certified("test")
    patch_get_user.assert_called_once_with("test")
    patch_get_cert.assert_called_once_with("foobar")
    assert is_certified is False


def test_init_change_cache_path():
    """
    Verify that the user can customize the cache path.
    The cache path is set to the default value if cache_root_dir argument is None.
    """
    cache_root_dir = '.synapseCache'
    fanout = 1000
    file_handle_id = '-1337'

    syn = Synapse(debug=False, skip_checks=True)
    expected_cache_path = os.path.join(str(Path.home()), cache_root_dir,
                                       str(int(file_handle_id) % fanout), str(file_handle_id))
    assert syn.cache.get_cache_dir(file_handle_id) == expected_cache_path

    with tempfile.TemporaryDirectory() as temp_dir_name:
        syn_changed_cache_path = Synapse(debug=False, skip_checks=True, cache_root_dir=temp_dir_name)
        expected_changed_cache_path = os.path.join(temp_dir_name, str(int(file_handle_id) % fanout),
                                                   str(file_handle_id))
        assert syn_changed_cache_path.cache.get_cache_dir(file_handle_id) == expected_changed_cache_path


def test__saveActivity__has_id(syn):
    """
    Testing saveActivity method works properly
    """

    with patch.object(syn, 'restPUT') as mock_restPUT:
        put_result = {'name': 'test_activity', 'description': 'test_description', 'id': 123}
        syn._saveActivity(put_result)

        mock_restPUT.assert_called_once_with(
            '/activity/123', '{"name": "test_activity", "description": "test_description", "id": 123}'
        )


def test__saveActivity__without_id(syn):
    """
    Testing saveActivity method pass in the argument activity without ID property
    """
    with patch.object(syn, 'restPOST') as mock_restpost:
        activity = Activity(name="test_activity", description="test_description")
        syn._saveActivity(activity)
        mock_restpost.assert_called_once_with(
            '/activity', body='{"used": [], "name": "test_activity", "description": "test_description"}'
        )


@patch('synapseclient.Synapse._saveActivity')
def test__updateActivity__with_id(mock_saveActivity, syn):
    activity = {'id': 'syn123', 'name': 'test_activity', 'description': 'test_description'}
    syn.updateActivity(activity)
    mock_saveActivity.assert_called_once_with({'id': 'syn123',
                                               'name': 'test_activity',
                                               'description': 'test_description'})


def test__updateActivity__without_id(syn):
    activity = Activity(name="test_activity", description="test_description")
    with pytest.raises(ValueError) as ve:
        syn.updateActivity(activity)
    assert str(ve.value) == "The activity you want to update must exist on Synapse"
