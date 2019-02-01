import json
import os
import tempfile
import base64
from mock import patch, call, create_autospec

from nose.tools import assert_equal, assert_in, assert_raises, assert_is_none, assert_is_not_none, \
    assert_not_equals, assert_true
from synapseclient import client

from synapseclient import *
from synapseclient.core.models.exceptions import *
from synapseclient.core.upload import upload_functions, multipart_upload_file
from synapseclient.client import DEFAULT_STORAGE_LOCATION_ID
from synapseclient.core.constants import concrete_types
from synapseclient.core.credentials import UserLoginArgs
from synapseclient.core.credentials.cred_data import SynapseCredentials
from synapseclient.core.credentials.credential_provider import SynapseCredentialsProviderChain
from synapseclient.core.models.dict_object import DictObject
from synapseclient.core.utils import id_of
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
                'name': 'anonymous',
                'dataFileHandleId': '-1337',
                'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
                'parentId': 'syn12345'},
            'fileHandles': [],
            'annotations': {}}

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
            'annotations': {}}

        fileHandle = bundle['fileHandles'][0]['id']
        cacheDir = syn.cache.get_cache_dir(fileHandle)
        # Make sure the .cacheMap file does not already exist
        cacheMap = os.path.join(cacheDir, '.cacheMap')
        if os.path.exists(cacheMap):
            os.remove(cacheMap)

        def _downloadFileHandle(fileHandleId,  objectId, objectType, path, retries=5):
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
            'teamId': id_of(self.team['id']),
            'contributors': self.contributors,
            'submitterAlias': self.team['name']
        }
        self.eligibility_hash = 23

        self.patch_private_submit = patch.object(syn, "_submit", return_value=self.submission)
        self.patch_getEvaluation = patch.object(syn, "getEvaluation", return_value=self.eval)
        self.patch_get = patch.object(syn, "get", return_value=self.entity)
        self.patch_getTeam = patch.object(syn, "getTeam", return_value= self.team)
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
        self.mock_private_submit.assert_called_once_with(expected_request_body, self.entity['etag'],
                                                         self.eligibility_hash)
        self.mock_get.assert_called_once_with(self.entity['id'], downloadFile=False)
        self.mock_getTeam.assert_not_called()
        self.mock_get_contributors.assert_called_once_with(self.eval_id, None)

    def test_team_is_given(self):
        assert_equal(self.submission, syn.submit(self.eval_id, self.entity, team=self.team['id']))

        expected_request_body = self.submission
        expected_request_body.pop('id')
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

        self.patch_restGET = patch.object(syn, "restGET", return_value=self.eligibility);
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
        assert_equal((contributors, self.eligibility['eligibilityStateHash']), syn._get_contributors(self.eval_id, self.team))
        uri = '/evaluation/{evalId}/team/{id}/submissionEligibility'.format(evalId=self.eval_id, id=self.team_id)
        self.mock_restGET.assert_called_once_with(uri)


def test_send_message():
    with patch.object(multipart_upload_file, "_multipart_upload") as up_mock,\
            patch.object(Synapse, "restPOST") as post_mock:
            up_mock.return_value = {
                'startedOn': '2016-01-22T00:00:00.000Z',
                'state': 'COMPLETED',
                'uploadId': '1234',
                'updatedOn': '2016-01-22T00:00:00.000Z',
                'partsState': '11',
                'startedBy': '377358',
                'resultFileHandleId': '7365905'}
            syn.sendMessage(
                userIds=[1421212],
                messageSubject="Xanadu",
                messageBody=("In Xanadu did Kubla Khan\n"
                             "A stately pleasure-dome decree:\n"
                             "Where Alph, the sacred river, ran\n"
                             "Through caverns measureless to man\n"
                             "Down to a sunless sea.\n"))
            msg = json.loads(post_mock.call_args_list[0][1]['body'])
            assert_equal(msg["fileHandleId"], "7365905", msg)
            assert_equal(msg["recipients"], [1421212], msg)
            assert_equal(msg["subject"], "Xanadu", msg)


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

        # method under test
        with patch.object(upload_functions, "multipart_upload",
                          return_value=expected_file_handle_id) as mocked_multipart_upload, \
                patch.object(syn.cache, "add") as mocked_cache_add,\
                patch.object(syn, "_getFileHandle") as mocked_getFileHandle:
            upload_functions.upload_file_handle(syn, test_file['parentId'], test_file['path'])

            mock_upload_destination.assert_called_once_with(test_file['parentId'])
            mocked_multipart_upload.assert_called_once_with(syn, expected_path_expanded, contentType=None,
                                                            storageLocationId=expected_storage_location_id)
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
        self.patch_restGET = patch.object(syn, 'restGET', return_value=self.bundle)
        self.patch_restGET.start()

    def teardown(self):
        self.patch_restGET.stop()

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
                'stringAnnotations': {}},
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


def test_get_default_view_columns():
    mask = 5
    with patch.object(syn, "restGET") as mock_restGET:
        syn._get_default_entity_view_columns(mask)
        mock_restGET.assert_called_with("/column/tableview/defaults?viewTypeMask=5")


def test_get_annotation_entity_view_columns():
    scope_ids = 3
    mask = 5
    view_scope = {'scope': scope_ids,
                  'viewTypeMask': mask}
    page1 = {'results': [],
             'nextPageToken': 'a'}
    page2 = {'results': [],
             'nextPageToken': None}
    call_list = [call('/column/view/scope', json.dumps(view_scope), params={}),
                 call('/column/view/scope', json.dumps(view_scope), params={'nextPageToken': 'a'})]
    with patch.object(syn, "restPOST", side_effect=[page1, page2]) as mock_restPOST:
        syn._get_annotation_entity_view_columns(scope_ids, mask)
        mock_restPOST.assert_has_calls(call_list)


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
            'concreteType':'org.sagebionetworks.repo.model.project.UploadDestinationListSetting',
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

