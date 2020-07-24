import base64
import configparser
import datetime
import json
import os
import tempfile
import urllib.request as urllib_request
import uuid

from unittest.mock import ANY, call, create_autospec, Mock, patch
from nose.tools import assert_equal, assert_false, assert_in, assert_raises, assert_is_none, assert_is_not_none, \
    assert_not_equals, assert_true

import synapseclient
from synapseclient.annotations import convert_old_annotation_json
from synapseclient import client
from synapseclient import (
    Annotations,
    Column,
    DockerRepository,
    Entity,
    EntityViewSchema,
    File,
    Folder,
    Team,
    Synapse,
)
from synapseclient.core.exceptions import (
    SynapseAuthenticationError,
    SynapseError,
    SynapseFileNotFoundError,
    SynapseHTTPError,
    SynapseUnmetAccessRestrictions,
)
from synapseclient.core.upload import upload_functions
import synapseclient.core.utils as utils
from synapseclient.client import DEFAULT_STORAGE_LOCATION_ID
from synapseclient.core.constants import concrete_types
from synapseclient.core.credentials import UserLoginArgs
from synapseclient.core.credentials.cred_data import SynapseCredentials
from synapseclient.core.credentials.credential_provider import SynapseCredentialsProviderChain
from synapseclient.core.models.dict_object import DictObject
from tests import unit


def setup(module):
    module.syn = unit.syn


class TestLogout:
    def setup(self):
        self.username = "asdf"
        self.credentials = SynapseCredentials(self.username, base64.b64encode(b"api_key_doesnt_matter").decode())

    def test_logout__forgetMe_is_True(self):
        with patch.object(client, "cached_sessions") as mock_cached_session:
            syn.credentials = self.credentials
            syn.logout(True)
            assert_is_none(syn.credentials)
            mock_cached_session.remove_api_key.assert_called_with(self.username)

    def test_logout__forgetMe_is_False(self):
        with patch.object(client, "cached_sessions") as mock_cached_session:
            syn.credentials = self.credentials
            syn.logout(False)
            assert_is_none(syn.credentials)
            mock_cached_session.remove_api_key.assert_not_called()


class TestLogin:
    def setup(self):
        self.login_args = {'email': "AzureDiamond", "password": "hunter2"}
        self.expected_user_args = UserLoginArgs(username="AzureDiamond", password="hunter2", api_key=None,
                                                skip_cache=False)
        self.synapse_creds = SynapseCredentials("AzureDiamond", base64.b64encode(b"*******").decode())

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
        assert_raises(SynapseAuthenticationError, syn.login, **self.login_args)

        self.mocked_get_credential_chain.assert_called_once_with()
        self.mocked_credential_chain.get_credentials.assert_called_once_with(syn, self.expected_user_args)

    def test_login__credentials_returned(self):
        # method under test
        syn.login(silent=True, **self.login_args)

        self.mocked_get_credential_chain.assert_called_once_with()
        self.mocked_credential_chain.get_credentials.assert_called_once_with(syn, self.expected_user_args)
        assert_equal(self.synapse_creds, syn.credentials)

    def test_login__silentIsFalse(self):
        with patch.object(syn, "getUserProfile") as mocked_get_user_profile, \
                patch.object(syn, "logger") as mocked_logger:
            # method under test
            syn.login(silent=False, **self.login_args)

            mocked_get_user_profile.assert_called_once_with(refresh=True)
            mocked_logger.info.assert_called_once()

    def test_login__rememberMeIsTrue(self):
        with patch.object(client, "cached_sessions") as mocked_cached_sessions:
            syn.login(silent=True, rememberMe=True)

            mocked_cached_sessions.set_api_key.assert_called_once_with(self.synapse_creds.username,
                                                                       self.synapse_creds.api_key)
            mocked_cached_sessions.set_most_recent_user.assert_called_once_with(self.synapse_creds.username)


@patch('synapseclient.Synapse._getFileHandleDownload')
@patch('synapseclient.Synapse._downloadFileHandle')
class TestPrivateGetWithEntityBundle:

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

        with patch.object(syn.logger, "warning") as mocked_warn:
            entity_no_download = syn._getWithEntityBundle(entityBundle=bundle)
            mocked_warn.assert_called_once()
            assert_is_none(entity_no_download.path)

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
        cacheDir = syn.cache.get_cache_dir(fileHandle)
        # Make sure the .cacheMap file does not already exist
        cacheMap = os.path.join(cacheDir, '.cacheMap')
        if os.path.exists(cacheMap):
            os.remove(cacheMap)

        def _downloadFileHandle(fileHandleId,  objectId, objectType, path, retries=5, max_threads=None):
            # touch file at path
            with open(path, 'a'):
                os.utime(path, None)
            os.path.split(path)
            syn.cache.add(fileHandle, path)
            return path

        def _getFileHandleDownload(fileHandleId,  objectId, objectType='FileHandle'):
            return {'fileHandle': bundle['fileHandles'][0], 'fileHandleId': fileHandleId,
                    'preSignedURL': 'http://example.com'}

        download_file_mock.side_effect = _downloadFileHandle
        get_file_URL_and_metadata_mock.side_effect = _getFileHandleDownload

        # 1. ----------------------------------------------------------------------
        # download file to an alternate location

        temp_dir1 = tempfile.mkdtemp()

        e = syn._getWithEntityBundle(entityBundle=bundle,
                                     downloadLocation=temp_dir1,
                                     ifcollision="overwrite.local")

        assert_equal(e.name, bundle["entity"]["name"])
        assert_equal(e.parentId, bundle["entity"]["parentId"])
        assert_equal(utils.normalize_path(os.path.abspath(os.path.dirname(e.path))), utils.normalize_path(temp_dir1))
        assert_equal(bundle["fileHandles"][0]["fileName"], os.path.basename(e.path))
        assert_equal(utils.normalize_path(os.path.abspath(e.path)),
                     utils.normalize_path(os.path.join(temp_dir1, bundle["fileHandles"][0]["fileName"])))

        # 2. ----------------------------------------------------------------------
        # get without specifying downloadLocation
        e = syn._getWithEntityBundle(entityBundle=bundle, ifcollision="overwrite.local")

        assert_equal(e.name, bundle["entity"]["name"])
        assert_equal(e.parentId, bundle["entity"]["parentId"])
        assert_in(bundle["fileHandles"][0]["fileName"], e.files)

        # 3. ----------------------------------------------------------------------
        # download to another location
        temp_dir2 = tempfile.mkdtemp()
        assert_not_equals(temp_dir2, temp_dir1)
        e = syn._getWithEntityBundle(entityBundle=bundle,
                                     downloadLocation=temp_dir2,
                                     ifcollision="overwrite.local")

        assert_in(bundle["fileHandles"][0]["fileName"], e.files)
        assert_is_not_none(e.path)
        assert_true(utils.equal_paths(os.path.dirname(e.path), temp_dir2))

        # 4. ----------------------------------------------------------------------
        # test preservation of local state
        url = 'http://foo.com/secretstuff.txt'
        # need to create a bundle with externalURL
        externalURLBundle = dict(bundle)
        externalURLBundle['fileHandles'][0]['externalURL'] = url
        e = File(name='anonymous', parentId="syn12345", synapseStore=False, externalURL=url)
        e.local_state({'zap': 'pow'})
        e = syn._getWithEntityBundle(entityBundle=externalURLBundle, entity=e)
        assert_equal(e.local_state()['zap'], 'pow')
        assert_equal(e.synapseStore, False)
        assert_equal(e.externalURL, url)

        # TODO: add more test cases for flag combination of this method
        # TODO: separate into another test?


class TestDownloadFileHandle:
    # TODO missing tests for the other ways of downloading a file handle should be backfilled...

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
            assert_equal(permission, 'read_only')
            assert_equal(entity_id, objectId)
            return download_fn(credentials)

        mock_sts_transfer.with_boto_sts_credentials = mock_with_boto_sts_credentials

        expected_download_path = '/tmp/fooKey'
        mock_s3_client_wrapper.download_file.return_value = expected_download_path

        # this is another opt-in download method.
        # for enabled storage locations sts should be preferred
        syn.multi_threaded = True

        with patch.object(syn, '_getFileHandleDownload') as mock_get_file_handle_download,\
                patch.object(syn, 'cache') as cache:
            mock_get_file_handle_download.return_value = {
                'fileHandle': {
                    'id': file_handle_id,
                    'bucketName': bucket_name,
                    'key': key,
                    'concreteType': concrete_types.S3_FILE_HANDLE,
                    'storageLocationId': 9876
                }
            }

            download_path = syn._downloadFileHandle(
                fileHandleId=file_handle_id,
                objectId=entity_id,
                objectType='FileEntity',
                destination=destination,
            )
            mock_os.makedirs.assert_called_once_with(mock_os.path.dirname(destination), exist_ok=True)
            cache.add.assert_called_once_with(file_handle_id, download_path)

        assert_equal(expected_download_path, download_path)
        mock_s3_client_wrapper.download_file.assert_called_once_with(
            bucket_name,
            None,
            key,
            destination,
            credentials=credentials,
            transfer_config_kwargs={'max_concurrency': syn.max_threads},
        )

    def test_download_file_ftp_link(self):
        """Verify downloading from an FTP Link entity"""

        file_handle_id = 1234
        entity_id = 'syn5678'
        url = 'ftp://foo.com/bar'
        destination = '/tmp'
        expected_destination = os.path.abspath(destination)

        with patch.object(syn, '_getFileHandleDownload') as mock_get_file_handle_download,\
                patch.object(syn, 'cache'),\
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

            download_path = syn._downloadFileHandle(
                fileHandleId=file_handle_id,
                objectId=entity_id,
                objectType='FileEntity',
                destination=destination,
            )

            mock_url_retrieve.assert_called_once_with(url, expected_destination)
            assert_equal(download_path, expected_destination)


class TestPrivateSubmit:

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
        self.patch_restPOST = patch.object(syn, "restPOST", return_value=self.submission)
        self.mock_restPOST = self.patch_restPOST.start()

    def teardown(self):
        self.patch_restPOST.stop()

    def test_invalid_submission(self):
        assert_raises(ValueError, syn._submit, None, self.etag, self.submission)

    def test_invalid_etag(self):
        assert_raises(ValueError, syn._submit, self.submission, None, self.submission)

    def test_without_eligibility_hash(self):
        assert_equal(self.submission, syn._submit(self.submission, self.etag, None))
        uri = '/evaluation/submission?etag={0}'.format(self.etag)
        self.mock_restPOST.assert_called_once_with(uri, json.dumps(self.submission))

    def test_with_eligibitiy_hash(self):
        assert_equal(self.submission, syn._submit(self.submission, self.etag, self.eligibility_hash))
        uri = '/evaluation/submission?etag={0}&submissionEligibilityHash={1}'.format(self.etag, self.eligibility_hash)
        self.mock_restPOST.assert_called_once_with(uri, json.dumps(self.submission))


class TestSubmit:

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

        self.patch_private_submit = patch.object(syn, "_submit", return_value=self.submission)
        self.patch_getEvaluation = patch.object(syn, "getEvaluation", return_value=self.eval)
        self.patch_get = patch.object(syn, "get", return_value=self.entity)
        self.patch_getTeam = patch.object(syn, "getTeam", return_value=self.team)
        self.patch_get_contributors = patch.object(syn, "_get_contributors",
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
        assert_equal(self.submission, syn.submit(self.eval_id, self.entity))

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
        assert_equal(self.submission, syn.submit(self.eval_id, self.entity['id']))

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
        assert_equal(self.submission, syn.submit(self.eval_id, self.entity, team=self.team['id']))

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
        assert_raises(SynapseError, syn.submit, self.eval_id, self.entity, team=self.team['id'])
        self.mock_private_submit.assert_not_called()
        self.mock_get.assert_not_called()
        self.mock_getTeam.assert_called_once_with(self.team['id'])
        self.mock_get_contributors.assert_called_once_with(self.eval_id, self.team)

    def test_get_docker_digest_default(self):
        latest_sha = 'sha256:eeeeee'
        docker_commits = [{'tag': 'latest',
                           'digest': latest_sha}]

        with patch.object(syn, "_GET_paginated",
                          return_value=docker_commits) as patch_syn_get_paginated:
            digest = syn._get_docker_digest('syn1234')
            patch_syn_get_paginated.assert_called_once_with("/entity/syn1234/dockerTag")
            assert_equal(digest, latest_sha)

    def test_get_docker_digest_specifytag(self):
        test_sha = 'sha256:ffffff'
        docker_commits = [{'tag': 'test',
                           'digest': test_sha}]
        with patch.object(syn, "_GET_paginated",
                          return_value=docker_commits) as patch_syn_get_paginated:
            digest = syn._get_docker_digest('syn1234', docker_tag="test")
            patch_syn_get_paginated.assert_called_once_with("/entity/syn1234/dockerTag")
            assert_equal(digest, test_sha)

    def test_get_docker_digest_specifywrongtag(self):
        test_sha = 'sha256:ffffff'
        docker_commits = [{'tag': 'test',
                           'digest': test_sha}]
        with patch.object(syn, "_GET_paginated",
                          return_value=docker_commits) as patch_syn_get_paginated:
            assert_raises(ValueError, syn._get_docker_digest, 'syn1234', docker_tag="foo")
            patch_syn_get_paginated.assert_called_once_with("/entity/syn1234/dockerTag")

    def test_submit_docker_nonetag(self):
        docker_entity = DockerRepository("foo", parentId="syn1000001")
        docker_entity.id = "syn123"
        docker_entity.etag = "Fake etag"

        assert_raises(ValueError, syn.submit, '9090',
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
        with patch.object(syn, "get",
                          return_value=docker_entity) as patch_syn_get, \
            patch.object(syn, "_get_docker_digest",
                         return_value=docker_digest) as patch_get_digest, \
            patch.object(syn, "_submit",
                         return_value=expected_submission):
            submission = syn.submit('9090', patch_syn_get, name='George')
            patch_get_digest.assert_called_once_with(docker_entity, "latest")
            assert_equal(submission, expected_submission)


class TestPrivateGetContributor:

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

        self.patch_restGET = patch.object(syn, "restGET", return_value=self.eligibility)
        self.mock_restGET = self.patch_restGET.start()

    def teardown(self):
        self.patch_restGET.stop()

    def test_none_team(self):
        assert_equal((None, None), syn._get_contributors(self.eval_id, None))
        self.mock_restGET.assert_not_called()

    def test_none_eval_id(self):
        assert_equal((None, None), syn._get_contributors(None, self.team))
        self.mock_restGET.assert_not_called()

    def test_not_registered(self):
        self.eligibility['teamEligibility']['isEligible'] = False
        self.eligibility['teamEligibility']['isRegistered'] = False
        self.patch_restGET.return_value = self.eligibility
        assert_raises(SynapseError, syn._get_contributors, self.eval_id, self.team)
        uri = '/evaluation/{evalId}/team/{id}/submissionEligibility'.format(evalId=self.eval_id, id=self.team_id)
        self.mock_restGET.assert_called_once_with(uri)

    def test_quota_filled(self):
        self.eligibility['teamEligibility']['isEligible'] = False
        self.eligibility['teamEligibility']['isQuotaFilled'] = True
        self.patch_restGET.return_value = self.eligibility
        assert_raises(SynapseError, syn._get_contributors, self.eval_id, self.team)
        uri = '/evaluation/{evalId}/team/{id}/submissionEligibility'.format(evalId=self.eval_id, id=self.team_id)
        self.mock_restGET.assert_called_once_with(uri)

    def test_empty_members(self):
        self.eligibility['membersEligibility'] = []
        self.patch_restGET.return_value = self.eligibility
        assert_equal(([], self.eligibility['eligibilityStateHash']), syn._get_contributors(self.eval_id, self.team))
        uri = '/evaluation/{evalId}/team/{id}/submissionEligibility'.format(evalId=self.eval_id, id=self.team_id)
        self.mock_restGET.assert_called_once_with(uri)

    def test_happy_case(self):
        contributors = [{'principalId': self.member_eligible['principalId']}]
        assert_equal((contributors, self.eligibility['eligibilityStateHash']),
                     syn._get_contributors(self.eval_id, self.team))
        uri = '/evaluation/{evalId}/team/{id}/submissionEligibility'.format(evalId=self.eval_id, id=self.team_id)
        self.mock_restGET.assert_called_once_with(uri)


def test_send_message():
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
        assert_equal(msg["fileHandleId"], "7365905", msg)
        assert_equal(msg["recipients"], [1421212], msg)
        assert_equal(msg["subject"], "Xanadu", msg)
        mock_upload_string.assert_called_once_with(syn, messageBody, content_type="text/plain")


@patch("synapseclient.Synapse._getDefaultUploadDestination")
class TestPrivateUploadExternallyStoringProjects:

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
                patch.object(syn.cache, "add") as mocked_cache_add,\
                patch.object(syn, "_get_file_handle_as_creator") as mocked_getFileHandle:
            upload_functions.upload_file_handle(syn, test_file['parentId'], test_file['path'], max_threads=max_threads)

            mock_upload_destination.assert_called_once_with(test_file['parentId'])
            mocked_multipart_upload.assert_called_once_with(
                syn,
                expected_path_expanded,
                content_type=None,
                storage_location_id=expected_storage_location_id,
                max_threads=max_threads,
            )
            mocked_cache_add.assert_called_once_with(expected_file_handle_id, expected_path_expanded)
            mocked_getFileHandle.assert_called_once_with(expected_file_handle_id)
            # test


def test_findEntityIdByNameAndParent__None_parent():
    entity_name = "Kappa 123"
    expected_uri = "/entity/child"
    expected_body = json.dumps({"parentId": None, "entityName": entity_name})
    expected_id = "syn1234"
    return_val = {'id': expected_id}
    with patch.object(syn, "restPOST", return_value=return_val) as mocked_POST:
        entity_id = syn.findEntityId(entity_name)
        mocked_POST.assert_called_once_with(expected_uri, body=expected_body)
        assert_equal(expected_id, entity_id)


def test_findEntityIdByNameAndParent__with_parent():
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
        assert_equal(expected_id, entity_id)


def test_findEntityIdByNameAndParent__404_error_no_result():
    entity_name = "Kappa 123"
    fake_response = DictObject({"status_code": 404})
    with patch.object(syn, "restPOST", side_effect=SynapseHTTPError(response=fake_response)):
        assert_is_none(syn.findEntityId(entity_name))


def test_getChildren__nextPageToken():
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
        assert_equal(first_page, result)
        result = next(children_generator)
        assert_equal(second_page, result)
        assert_raises(StopIteration, next, children_generator)

        # check that the correct POST requests were sent
        # genrates JSOn for the expected request body
        def expected_request_JSON(token):
            return json.dumps({'parentId': 'syn' + str(parent_project_id_int),
                               'includeTypes': ["folder", "file", "table", "link", "entityview", "dockerrepo"],
                               'sortBy': 'NAME', 'sortDirection': 'ASC', 'nextPageToken': token})
        expected_POST_url = '/entity/children'
        mocked_POST.assert_has_calls([call(expected_POST_url, body=expected_request_JSON(None)),
                                      call(expected_POST_url, body=expected_request_JSON(nextPageToken))])


def test_check_entity_restrictions__no_unmet_restriction():
    with patch("warnings.warn") as mocked_warn:
        restriction_requirements = {'hasUnmetAccessRequirement': False}

        syn._check_entity_restrictions(restriction_requirements, "syn123", True)

        mocked_warn.assert_not_called()


def test_check_entity_restrictions__unmet_restriction_downloadFile_is_True():
    with patch("warnings.warn") as mocked_warn:
        restriction_requirements = {'hasUnmetAccessRequirement': True}

        assert_raises(SynapseUnmetAccessRestrictions, syn._check_entity_restrictions, restriction_requirements,
                      "syn123", True)

        mocked_warn.assert_not_called()


def test_check_entity_restrictions__unmet_restriction_downloadFile_is_False():
    with patch("warnings.warn") as mocked_warn:
        restriction_requirements = {'hasUnmetAccessRequirement': True}

        syn._check_entity_restrictions(restriction_requirements, "syn123", False)

        mocked_warn.assert_called_once()


class TestGetColumns(object):
    def test_input_is_SchemaBase(self):
        get_table_colums_results = [Column(name='A'), Column(name='B')]
        with patch.object(syn, "getTableColumns", return_value=iter(get_table_colums_results))\
                as mock_get_table_coulmns:
            schema = EntityViewSchema(parentId="syn123")
            results = list(syn.getColumns(schema))
            assert_equal(get_table_colums_results, results)
            mock_get_table_coulmns.assert_called_with(schema)


def test_username_property__credentials_is_None():
    syn.credentials = None
    assert_is_none(syn.username)


class TestPrivateGetEntityBundle:
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
        self.patch_restPOST = patch.object(syn, 'restPOST', return_value=self.bundle)
        self.patch_restPOST.start()

    def teardown(self):
        self.patch_restPOST.stop()

    def test__getEntityBundle__with_version_as_number(self):
        assert_equal(self.bundle, syn._getEntityBundle("syn10101", 6))

    def test__getEntityBundle__with_version_as_string(self):
        assert_equal(self.bundle, syn._getEntityBundle("syn10101", '6'))
        assert_raises(ValueError, syn._getEntityBundle, "syn10101", 'current')

    def test_access_restrictions(self):
        with patch.object(syn, '_getEntityBundle', return_value={
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
            entity = syn.get('syn1000002', downloadFile=False)
            assert_is_not_none(entity)
            assert_is_none(entity.path)

            # Downloading the file is the default, but is an error if we have unmet access requirements
            assert_raises(SynapseUnmetAccessRestrictions, syn.get, 'syn1000002',
                          downloadFile=True)


def test_move():
    assert_raises(SynapseFileNotFoundError, syn.move, "abc", "syn123")

    entity = Folder(name="folder", parent="syn456")
    moved_entity = entity
    moved_entity.parentId = "syn789"
    with patch.object(syn, "get", return_value=entity) as syn_get_patch,\
            patch.object(syn, "store", return_value=moved_entity) as syn_store_patch:
        assert_equal(moved_entity, syn.move("syn123", "syn789"))
        syn_get_patch.assert_called_once_with("syn123", downloadFile=False)
        syn_store_patch.assert_called_once_with(moved_entity, forceVersion=False)


def test_delete__bad_attribute():
    assert_raises(SynapseError, syn.delete, ["foo"])


def test_delete__string():
    with patch.object(syn, "restDELETE") as patch_rest_delete:
        syn.delete("syn1235")
        patch_rest_delete.assert_called_once_with(uri="/entity/syn1235")


def test_delete__string_version():
    with patch.object(syn, "restDELETE") as patch_rest_delete:
        syn.delete("syn1235", version=1)
        patch_rest_delete.assert_called_once_with(uri="/entity/syn1235/version/1")


def test_delete__has_synapse_delete_attr():
    mock_obj = Mock()
    syn.delete(mock_obj)
    mock_obj._synapse_delete.assert_called_once()


def test_delete__entity():
    entity = Folder(name="folder", parent="syn456", id="syn1111")
    with patch.object(syn, "restDELETE") as patch_rest_delete:
        syn.delete(entity)
        patch_rest_delete.assert_called_once_with("/entity/syn1111")


def test_delete__entity_version():
    entity = File(name="file", parent="syn456", id="syn1111")
    with patch.object(syn, "restDELETE") as patch_rest_delete:
        syn.delete(entity, version=2)
        patch_rest_delete.assert_called_once_with("/entity/syn1111/version/2")


def test_setPermissions__default_permissions():
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
        assert_equal(update_acl, syn.setPermissions(entity, principalId))
        patch_store_acl.assert_called_once_with(entity, update_acl)


def test_get_unsaved_entity():
    assert_raises(ValueError, syn.get, Folder(name="folder", parent="syn456"))


def test_get_default_view_columns_nomask():
    """Test no mask passed in"""
    with patch.object(syn, "restGET") as mock_restGET:
        syn._get_default_view_columns("viewtype")
        mock_restGET.assert_called_with(
            "/column/tableview/defaults?viewEntityType=viewtype"
        )


def test_get_default_view_columns_mask():
    """Test mask passed in"""
    mask = 5
    with patch.object(syn, "restGET") as mock_restGET:
        syn._get_default_view_columns("viewtype", mask)
        mock_restGET.assert_called_with(
            "/column/tableview/defaults?viewEntityType=viewtype&viewTypeMask=5"
        )


class TestCreateStorageLocationSetting:

    def setup(self):
        self.patch_restPOST = patch.object(syn, 'restPOST')
        self.mock_restPOST = self.patch_restPOST.start()

    def teardown(self):
        self.patch_restPOST.stop()

    def test_invalid(self):
        assert_raises(ValueError, syn.createStorageLocationSetting, "new storage type")

    def test_ExternalObjectStorage(self):
        syn.createStorageLocationSetting("ExternalObjectStorage")
        expected = {
            'concreteType': 'org.sagebionetworks.repo.model.project.ExternalObjectStorageLocationSetting',
            'uploadType': 'S3'
        }
        self.mock_restPOST.assert_called_once_with('/storageLocation', body=json.dumps(expected))

    def test_ProxyStorage(self):
        syn.createStorageLocationSetting("ProxyStorage")
        expected = {
            'concreteType': 'org.sagebionetworks.repo.model.project.ProxyStorageLocationSettings',
            'uploadType': 'PROXYLOCAL'
        }
        self.mock_restPOST.assert_called_once_with('/storageLocation', body=json.dumps(expected))

    def test_ExternalS3Storage(self):
        syn.createStorageLocationSetting("ExternalS3Storage")
        expected = {
            'concreteType': 'org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting',
            'uploadType': 'S3'
        }
        self.mock_restPOST.assert_called_once_with('/storageLocation', body=json.dumps(expected))

    def test_ExternalStorage(self):
        syn.createStorageLocationSetting("ExternalStorage")
        expected = {
            'concreteType': 'org.sagebionetworks.repo.model.project.ExternalStorageLocationSetting',
            'uploadType': 'SFTP'
        }
        self.mock_restPOST.assert_called_once_with('/storageLocation', body=json.dumps(expected))


class TestSetStorageLocation:

    def setup(self):
        self.entity = "syn123"
        self.expected_location = {
            'concreteType': 'org.sagebionetworks.repo.model.project.UploadDestinationListSetting',
            'settingsType': 'upload',
            'locations': [DEFAULT_STORAGE_LOCATION_ID],
            'projectId': self.entity
        }
        self.patch_getProjectSetting = patch.object(syn, 'getProjectSetting', return_value=None)
        self.mock_getProjectSetting = self.patch_getProjectSetting.start()
        self.patch_restPOST = patch.object(syn, 'restPOST')
        self.mock_restPOST = self.patch_restPOST.start()
        self.patch_restPUT = patch.object(syn, 'restPUT')
        self.mock_restPUT = self.patch_restPUT.start()

    def teardown(self):
        self.patch_getProjectSetting.stop()
        self.patch_restPOST.stop()
        self.patch_restPUT.stop()

    def test_default(self):
        syn.setStorageLocation(self.entity, None)
        self.mock_getProjectSetting.assert_called_once_with(self.entity, 'upload')
        self.mock_restPOST.assert_called_once_with('/projectSettings', body=json.dumps(self.expected_location))

    def test_create(self):
        storage_location_id = 333
        self.expected_location['locations'] = [storage_location_id]
        syn.setStorageLocation(self.entity, storage_location_id)
        self.mock_getProjectSetting.assert_called_once_with(self.entity, 'upload')
        self.mock_restPOST.assert_called_once_with('/projectSettings', body=json.dumps(self.expected_location))

    def test_update(self):
        self.mock_getProjectSetting.return_value = self.expected_location
        storage_location_id = 333
        new_location = self.expected_location
        new_location['locations'] = [storage_location_id]
        syn.setStorageLocation(self.entity, storage_location_id)
        self.mock_getProjectSetting.assert_called_with(self.entity, 'upload')
        assert_equal(2, self.mock_getProjectSetting.call_count)
        self.mock_restPUT.assert_called_once_with('/projectSettings', body=json.dumps(new_location))
        self.mock_restPOST.assert_not_called()


@patch('synapseclient.core.sts_transfer.get_sts_credentials')
def test_get_sts_storage_token(mock_get_sts_credentials):
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
    assert_equal(token, result)
    mock_get_sts_credentials.assert_called_once_with(
        syn, entity, permission,
        output_format=output_format, min_remaining_life=min_remaining_life
    )


class TestCreateS3StorageLocation:

    def test_folder_and_parent(self):
        """Verify we fail as expected if both parent and folder are passed"""
        with assert_raises(ValueError):
            syn.create_s3_storage_location(folder_name='foo', parent=Mock(), folder=Mock())

    def test_folder_or_parent(self):
        """Verify we fail as expected if neither parent or folder are passed"""
        with assert_raises(ValueError):
            syn.create_s3_storage_location()

    def _create_storage_location_test(self, expected_post_body, *args, **kwargs):
        with patch.object(syn, 'restPOST') as mock_post,\
                patch.object(syn, 'setStorageLocation') as mock_set_storage_location,\
                patch.object(syn, 'store') as syn_store:
            mock_post.return_value = {'storageLocationId': 456}
            mock_set_storage_location.return_value = {'id': 'foo'}

            # either passed a folder or expected to create one
            expected_folder = kwargs.get('folder')
            if not expected_folder:
                expected_folder = syn_store.return_value = Mock()

            result = syn.create_s3_storage_location(*args, **kwargs)

            if 'folder_name' in kwargs:
                stored_folder = syn_store.call_args[0][0]
                assert_equal(stored_folder.name, kwargs['folder_name'])
                assert_equal(stored_folder.parentId, kwargs['parent'])
            else:
                assert_false(syn_store.called)

            assert_equal(expected_folder, result[0])
            assert_equal(mock_post.return_value, result[1])
            assert_equal(mock_set_storage_location.return_value, result[2])

            mock_post.assert_called_with('/storageLocation', ANY)
            assert_equal(expected_post_body, json.loads(mock_post.call_args[0][1]))

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

    def _s3_file_handle_test(self, **kwargs):
        with patch.object(syn, '_getDefaultUploadDestination') as mock_get_upload_dest,\
            patch.object(os, 'path') as mock_os_path, \
            patch.object(os, 'stat') as mock_os_stat, \
            patch.object(utils, 'md5_for_file') as mock_md5, \
            patch('mimetypes.guess_type') as mock_guess_mimetype, \
                patch.object(syn, 'restPOST') as mock_post:

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

            result = syn.create_external_s3_file_handle(bucket_name, s3_file_key, file_path, **kwargs)
            assert_equal(mock_post.return_value, result)

            if 'storage_location_id' in kwargs:
                assert_false(mock_get_upload_dest.called)
            else:
                mock_get_upload_dest.assert_called_once_with(kwargs['parent'])

            mock_post.assert_called_once_with('/externalFileHandle/s3', ANY, endpoint=syn.fileHandleEndpoint)
            assert_equal(expected_post_body, json.loads(mock_post.call_args[0][1]))

    def test_with_parent_entity(self):
        """If passed a parent entity we should fetch the default upload destination
        of the entity and use that as the storage location of the file handle"""
        self._s3_file_handle_test(parent=Mock())

    def test_with_storage_location_id(self):
        """If passed a storage location id we should use that.
        Also customize mimetype"""
        self._s3_file_handle_test(storage_location_id=123, mimetype='text/html')


class TestMembershipInvitation:

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
        with patch.object(syn, "_GET_paginated",
                          return_value=self.response) as patch_get_paginated:
            response = syn.get_team_open_invitations(self.team)
            request = "/team/{team}/openInvitation".format(team=self.team.id)
            patch_get_paginated.assert_called_once_with(request)
            assert_equal(response, self.response)

    def test_get_team_open_invitations__teamid(self):
        """Get team open invitations when team id is passed in"""
        with patch.object(syn, "_GET_paginated",
                          return_value=self.response) as patch_get_paginated:
            response = syn.get_team_open_invitations(self.team.id)
            request = "/team/{team}/openInvitation".format(team=self.team.id)
            patch_get_paginated.assert_called_once_with(request)
            assert_equal(response, self.response)

    def test_teamid_get_membership_status__rest_get(self):
        """Get membership status when team id is passed in"""
        with patch.object(syn, "restGET",
                          return_value=self.response) as patch_restget:
            response = syn.get_membership_status(self.userid, self.team.id)
            request = "/team/{team}/member/{user}/membershipStatus".format(
                team=self.team.id,
                user=self.userid)
            patch_restget.assert_called_once_with(request)
            assert_equal(response, self.response)

    def test_delete_membership_invitation__rest_delete(self):
        """Delete open membership invitation"""
        invitationid = 1111
        with patch.object(syn, "restDELETE") as patch_rest_delete:
            syn._delete_membership_invitation(invitationid)
            request = "/membershipInvitation/{id}".format(id=invitationid)
            patch_rest_delete.assert_called_once_with(request)

    def test_team_get_membership_status__rest_get(self):
        """Get membership status when Team is passed in"""
        with patch.object(syn, "restGET") as patch_restget:
            syn.get_membership_status(self.userid, self.team)
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
        with patch.object(syn, "restPOST",
                          return_value=self.response) as patch_rest_post:
            syn.send_membership_invitation(self.team.id, inviteeId=self.userid,
                                           inviteeEmail=self.email,
                                           message=self.message)
            patch_rest_post.assert_called_once_with("/membershipInvitation",
                                                    body=json.dumps(invite_body))

    def test_invite_to_team__bothuseremail_specified(self):
        """Raise error when user and email is passed in"""
        assert_raises(ValueError, syn.invite_to_team, self.team,
                      user=self.userid, inviteeEmail=self.email)

    def test_invite_to_team__bothuseremail_notspecified(self):
        """Raise error when user and email is passed in"""
        assert_raises(ValueError, syn.invite_to_team, self.team,
                      user=None, inviteeEmail=None)

    def test_invite_to_team__email(self):
        """Invite user to team via their email"""
        invite_body = {'message': self.message,
                       'inviteeEmail': self.email,
                       'inviteeId': None}
        with patch.object(syn, "get_team_open_invitations",
                          return_value=[]) as patch_get_invites,\
            patch.object(syn, "getUserProfile",
                         return_value=self.profile) as patch_get_profile,\
            patch.object(syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = syn.invite_to_team(self.team, inviteeEmail=self.email,
                                        message=self.message)
            assert_equal(invite, self.response)
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
        with patch.object(syn, "get_membership_status",
                          return_value=self.member_status) as patch_getmem,\
            patch.object(syn, "get_team_open_invitations",
                         return_value=[]) as patch_get_invites,\
            patch.object(syn, "getUserProfile",
                         return_value=self.profile) as patch_get_profile,\
            patch.object(syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = syn.invite_to_team(self.team, user=self.userid)
            patch_getmem.assert_called_once_with(self.userid,
                                                 self.team.id)
            patch_get_profile.assert_called_once_with(self.userid)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_invitation.assert_called_once_with(self.team.id,
                                                     **invite_body)
            assert_equal(invite, self.response)

    def test_invite_to_team__username(self):
        """Invite user to team via their Synapse username"""
        self.member_status['isMember'] = False
        invite_body = {'inviteeId': self.userid,
                       'inviteeEmail': None,
                       'message': None}
        with patch.object(syn, "get_membership_status",
                          return_value=self.member_status) as patch_getmem,\
            patch.object(syn, "get_team_open_invitations",
                         return_value=[]) as patch_get_invites,\
            patch.object(syn, "getUserProfile",
                         return_value=self.profile) as patch_get_profile,\
            patch.object(syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = syn.invite_to_team(self.team, user=self.username)
            patch_getmem.assert_called_once_with(self.userid,
                                                 self.team.id)
            patch_get_profile.assert_called_once_with(self.username)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_invitation.assert_called_once_with(self.team.id,
                                                     **invite_body)
            assert_equal(invite, self.response)

    def test_invite_to_team__ismember(self):
        """None returned when user is already a member"""
        with patch.object(syn, "get_membership_status",
                          return_value=self.member_status) as patch_getmem,\
            patch.object(syn, "get_team_open_invitations",
                         return_value=[]) as patch_get_invites,\
            patch.object(syn, "getUserProfile",
                         return_value=self.profile) as patch_get_profile,\
            patch.object(syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = syn.invite_to_team(self.team, user=self.userid)
            patch_getmem.assert_called_once_with(self.userid,
                                                 self.team.id)
            patch_get_profile.assert_called_once_with(self.userid)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_invitation.assert_not_called()
            assert_is_none(invite)

    def test_invite_to_team__user_openinvite(self):
        """None returned when user already has an invitation"""
        self.member_status['isMember'] = False
        invite_body = {'inviteeId': self.userid}
        with patch.object(syn, "get_membership_status",
                          return_value=self.member_status) as patch_getmem,\
            patch.object(syn, "get_team_open_invitations",
                         return_value=[invite_body]) as patch_get_invites,\
            patch.object(syn, "getUserProfile",
                         return_value=self.profile) as patch_get_profile,\
            patch.object(syn, "_delete_membership_invitation") as patch_delete,\
            patch.object(syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = syn.invite_to_team(self.team, user=self.userid)
            patch_getmem.assert_called_once_with(self.userid,
                                                 self.team.id)
            patch_get_profile.assert_called_once_with(self.userid)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_invitation.assert_not_called()
            patch_delete.assert_not_called()
            assert_is_none(invite)

    def test_invite_to_team__email_openinvite(self):
        """None returned when email already has an invitation"""
        invite_body = {'inviteeEmail': self.email}
        with patch.object(syn, "get_team_open_invitations",
                          return_value=[invite_body]) as patch_get_invites,\
            patch.object(syn, "_delete_membership_invitation") as patch_delete,\
            patch.object(syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = syn.invite_to_team(self.team, inviteeEmail=self.email)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_invitation.assert_not_called()
            patch_delete.assert_not_called()
            assert_is_none(invite)
            patch_delete.assert_not_called()

    def test_invite_to_team__none_matching_invitation(self):
        """Invitation sent when no matching open invitations"""
        invite_body = {'inviteeEmail': self.email + "foo"}
        with patch.object(syn, "get_team_open_invitations",
                          return_value=[invite_body]) as patch_get_invites,\
            patch.object(syn, "_delete_membership_invitation") as patch_delete,\
            patch.object(syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = syn.invite_to_team(self.team, inviteeEmail=self.email)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_delete.assert_not_called()
            assert_equal(invite, self.response)
            patch_invitation.assert_called_once()

    def test_invite_to_team__force_invite(self):
        """Invitation sent when force the invite, make sure open invitation
        is deleted"""
        open_invitations = {'inviteeEmail': self.email, 'id': '9938'}
        with patch.object(syn, "get_team_open_invitations",
                          return_value=[open_invitations]) as patch_get_invites,\
            patch.object(syn, "_delete_membership_invitation") as patch_delete,\
            patch.object(syn, "send_membership_invitation",
                         return_value=self.response) as patch_invitation:
            invite = syn.invite_to_team(self.team, inviteeEmail=self.email,
                                        force=True)
            patch_get_invites.assert_called_once_with(self.team.id)
            patch_delete.assert_called_once_with(open_invitations['id'])
            assert_equal(invite, self.response)
            patch_invitation.assert_called_once()


class TestRequestsSession:
    """Verify we can optionally pass in a requests.Session in kwargs
    to have the client use that session instead of the instance session."""

    def setup(self):
        self._path = '/foo'
        self._headers = {}

    def test_init(self):
        """Verify that an external requests session supplied at
        instantiation is used for calls via a Synapse object."""
        requests_session = Mock()
        requests_session.get.return_value = Mock(status_code=200)

        syn = Synapse(debug=False, skip_checks=True, requests_session=requests_session)
        syn.restGET(self._path, headers=self._headers)
        requests_session.get.assert_called_once()

    def _http_method_test(self, method):
        status_ok = Mock(status_code=200)
        with patch.object(syn._requests_session, method) as requests_call:
            requests_call.return_value = status_ok

            # make call, check that it flowed through to the instance session
            rest_call = getattr(syn, "rest{}".format(method.upper()))
            rest_call(
                self._path,
                headers=self._headers
            )
            requests_call.assert_called_once()
            requests_call.reset_mock()

            # make call, check that it flowed through to the passed session
            # (and not to the instance session)
            external_session = Mock()
            getattr(external_session, method).return_value = status_ok
            rest_call(
                self._path,
                headers=self._headers,
                requests_session=external_session
            )
            getattr(external_session, method).assert_called_once()
            requests_call.assert_not_called()

    def test_get(self):
        """Test restGET session handling"""
        self._http_method_test('get')

    def test_put(self):
        """Test restPUT session handling"""
        self._http_method_test('put')

    def test_post(self):
        """Test restPOST session handling"""
        self._http_method_test('put')

    def test_delete(self):
        """Test restDELETE session handling"""
        self._http_method_test('put')


class TestSetAnnotations:

    def test_not_annotation(self):
        with patch.object(syn, "restPUT") as mock_rest_put:
            # pass in non-annotation object
            assert_raises(TypeError, syn.set_annotations, {})
            mock_rest_put.assert_not_called()

    def test_with_annotations(self):
        with patch.object(syn, "restPUT") as mock_rest_put:
            mock_rest_put.return_value = {'id': 'syn123',
                                          'etag': '82196a4c-d383-439a-a08a-c07090a8c147',
                                          'annotations': {'foo': {'type': 'STRING', 'value': ['bar']}}}
            # pass in non-annotation object
            syn.set_annotations(Annotations('syn123', '1d6c46e4-4d52-44e1-969f-e77b458d815a', {'foo': 'bar'}))
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

        with assert_raises(ValueError) as cm:
            Synapse(debug=False, skip_checks=True, configPath='/foo')

        # underlying error should be chained
        assert_equal(
            config_error_msg,
            str(cm.exception.__context__)
        )


def test_get_config_file_caching():
    """Verify we read a config file once per Synapse and are not
    parsing the file multiple times just on init."""

    with patch('configparser.RawConfigParser.read') as read_config:
        read_config.return_value = configparser.ConfigParser()

        syn1 = Synapse(debug=False, skip_checks=True, configPath='/foo')

        # additional calls shouldn't be returned via a cached value
        config1a = syn1.getConfigFile('/foo')
        config1b = syn1.getConfigFile('/foo')
        assert_equal(config1a, config1b)
        assert_equal(1, read_config.call_count)

        # however a new instance should not be cached
        Synapse(debug=False, skip_checks=True, configPath='/foo')
        assert_equal(2, read_config.call_count)

        # but an additional call on that instance should be
        assert_equal(2, read_config.call_count)


def test_max_threads_bounded():
    """Verify we disallow setting max threads higher than our cap."""
    syn.max_threads = client.MAX_THREADS_CAP + 1
    assert_equal(syn.max_threads, client.MAX_THREADS_CAP)

    syn.max_threads = 0
    assert_equal(syn.max_threads, 1)


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
        syn = Synapse()
        for k, v in expected_values.items():
            assert_equal(v, getattr(syn, k))

    # invalid value for max threads should raise an error
    for invalid_max_thread_value in ('not a number', '12.2', 'true'):
        mock_config_dict.return_value = {'max_threads': invalid_max_thread_value}
        with assert_raises(ValueError):
            Synapse()

    # invalid value for use_boto_sts should raise an error
    for invalid_max_thread_value in ('not true', '1.2', '0', 'falsey'):
        mock_config_dict.return_value = {'use_boto_sts': invalid_max_thread_value}
        with assert_raises(ValueError):
            Synapse()


@patch('synapseclient.Synapse._get_config_section_dict')
def test_transfer_config_values_overridable(mock_config_dict):
    """Verify we can override the default transfer config values by setting them directly on the Synapse object"""

    mock_config_dict.return_value = {'max_threads': 24, 'use_boto_sts': False}
    syn = Synapse()

    assert_equal(24, syn.max_threads)
    assert_equal(False, syn.use_boto_sts_transfers)

    syn.max_threads = 5
    syn.use_boto_sts_transfers = True
    assert_equal(5, syn.max_threads)
    assert_equal(True, syn.use_boto_sts_transfers)


def test_store__needsUploadFalse__fileHandleId_not_in_local_state():
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


def test_store__existing_processed_as_update():
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

        assert_false(mock_createEntity.called)
        assert_false(mock_findEntityId.called)


def test_store__409_processed_as_update():
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


def test_store__existing_no_update():
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

        with assert_raises(SynapseHTTPError) as ex_cm:
            syn.store(f, createOrUpdate=False)

        assert_equal(409, ex_cm.exception.response.status_code)

        # should not have attempted an update
        assert_false(mock_updatentity.called)


def test_get_submission_with_annotations():
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

        assert_equal(evaluation_id, response["evaluationId"])


def test__get_annotation_view_columns():
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
        assert_equal(columns, [synapseclient.Column(id=5)])


class TestGenerateHeaders:

    def test_generate_headers__credentials(self):
        """Verify signed credentials are added to the headers when logged in"""
        url = 'http://foo.com/bar'
        signed_headers = {'foo': 'bar'}

        syn = Synapse(skip_checks=True)
        syn.credentials = Mock(
            get_signed_headers=Mock(return_value=signed_headers)
        )

        headers = syn._generate_headers(url)
        expected = {}
        expected.update(signed_headers)
        expected.update(syn.default_headers)
        expected.update(synapseclient.USER_AGENT)

        assert_equal(expected, headers)
        syn.credentials.get_signed_headers.assert_called_once_with(url)

    def test_generate_headers__no_credentials(self):
        """Verify expected headers without signing when not logged in"""
        url = 'http://foo.com/bar'

        syn = Synapse(skip_checks=True)
        syn.credentials = None

        headers = syn._generate_headers(url)
        expected = {}
        expected.update(syn.default_headers)
        expected.update(synapseclient.USER_AGENT)

        assert_equal(expected, headers)

    def test_generate_headers__custom_headers(self):
        """Verify that custom headers override default headers"""

        url = 'http://foo.com/bar'
        custom_headers = {
            'foo': 'bar'
        }

        syn = Synapse(skip_checks=True)
        syn.credentials = None

        headers = syn._generate_headers(url, headers=custom_headers)
        expected = {}
        expected.update(custom_headers)
        expected.update(synapseclient.USER_AGENT)

        assert_equal(expected, headers)


class TestHandleSynapseHTTPError:

    def test_handle_synapse_http_error__not_logged_in(self):
        """If you are not LOGGED in a http error with an unauthenticated/forbidden
        status code should raise an SynapseAuthenticationError chained from the
        underlying SynapseHTTPError"""
        syn = Synapse(skip_checks=True)
        syn.credentials = None

        for status_code in (401, 403):
            response = Mock(status_code=status_code, headers={})

            with assert_raises(SynapseAuthenticationError) as cm_ex:
                syn._handle_synapse_http_error(response)

            assert_true(isinstance(cm_ex.exception.__cause__, SynapseHTTPError))
            assert_equal(status_code, cm_ex.exception.__cause__.response.status_code)

    def test_handle_synapse_http_error__logged_in(self):
        """If you are logged in a SynapseHTTPError should be raised directly,
        even if it is an unauthenticated/forbidden error."""
        syn = Synapse(skip_checks=True)
        syn.credentials = Mock()
        for status_code in (401, 403, 404):
            response = Mock(status_code=status_code, headers={})

            with assert_raises(SynapseHTTPError) as cm_ex:
                syn._handle_synapse_http_error(response)

            assert_equal(status_code, cm_ex.exception.response.status_code)
