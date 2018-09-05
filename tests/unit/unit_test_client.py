import os
import json
import tempfile
import base64
from mock import patch, call, create_autospec

import unit
from nose.tools import assert_equal, assert_in, assert_raises, assert_is_none, assert_is_not_none, \
    assert_not_equals, assert_true

import synapseclient
from synapseclient import Evaluation, File, Folder
from synapseclient.constants import concrete_types
from synapseclient.credentials.cred_data import SynapseCredentials, UserLoginArgs
from synapseclient.credentials.credential_provider import SynapseCredentialsProviderChain
from synapseclient.exceptions import *
from synapseclient.dict_object import DictObject
from synapseclient.table import Column, EntityViewSchema
import synapseclient.upload_functions as upload_functions


def setup(module):
    module.syn = unit.syn


class TestLogout:
    def setup(self):
        self.username = "asdf"
        self.credentials = SynapseCredentials(self.username, base64.b64encode(b"api_key_doesnt_matter").decode())

    def test_logout__forgetMe_is_True(self):
        with patch.object(synapseclient.client, "cached_sessions") as mock_cached_session:
            syn.credentials = self.credentials
            syn.logout(True)
            assert_is_none(syn.credentials)
            mock_cached_session.remove_api_key.assert_called_with(self.username)

    def test_logout__forgetMe_is_False(self):
        with patch.object(synapseclient.client, "cached_sessions") as mock_cached_session:
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
        self.get_default_credential_chain_patcher = patch.object(synapseclient.client, "get_default_credential_chain",
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
        with patch.object(synapseclient.client, "cached_sessions") as mocked_cached_sessions:
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


@patch('synapseclient.Synapse.restPOST')
@patch('synapseclient.Synapse.getEvaluation')
class TestSubmit:

    def test_submit(self, *mocks):
        mocks = [item for item in mocks]
        POST_mock = mocks.pop()
        getEvaluation_mock = mocks.pop()

        # -- Unmet access rights --
        getEvaluation_mock.return_value = Evaluation(**{u'contentSource': u'syn1001',
                                                        u'createdOn': u'2013-11-06T06:04:26.789Z',
                                                        u'etag': u'86485ea1-8c89-4f24-a0a4-2f63bc011091',
                                                        u'id': u'9090',
                                                        u'name': u'test evaluation',
                                                        u'ownerId': u'1560252',
                                                        u'status': u'OPEN',
                                                        u'submissionReceiptMessage': u'mmmm yummy!'})

        # -- Normal submission --
        # insert a shim that returns the dictionary it was passed after adding a bogus id
        def shim(*args):
            assert_equal(args[0], '/evaluation/submission?etag=Fake eTag')
            submission = json.loads(args[1])
            submission['id'] = 1234
            return submission
        POST_mock.side_effect = shim

        submission = syn.submit('9090', {'versionNumber': 1337, 'id': "Whee...", 'etag': 'Fake eTag'}, name='George',
                                submitterAlias='Team X')

        assert_equal(submission.id, 1234)
        assert_equal(submission.evaluationId, '9090')
        assert_equal(submission.name, 'George')
        assert_equal(submission.submitterAlias, 'Team X')


def test_send_message():
    with patch("synapseclient.multipart_upload._multipart_upload") as up_mock,\
            patch("synapseclient.client.Synapse.restPOST") as post_mock:
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
        with patch.object(synapseclient.upload_functions, "multipart_upload",
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
            assert_raises(synapseclient.exceptions.SynapseUnmetAccessRestrictions, syn.get, 'syn1000002',
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


def test_purge_trash_can():
    with patch.object(syn, "restPUT") as mockRestPUT:
        syn.purge_trash_can()
        mockRestPUT.assert_called_once_with(uri="/trashcan/purge")


def test_get_unsaved_entity():
    assert_raises(ValueError, syn.get, Folder(name="folder", parent="syn456"))
