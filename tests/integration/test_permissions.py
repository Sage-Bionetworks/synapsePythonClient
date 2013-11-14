import ConfigParser
import json
import mock
import os
import sys
import uuid

import synapseclient
import synapseclient.utils as utils
from synapseclient import Activity, Entity, Project, Folder, File, Data

import integration
from integration import schedule_for_cleanup


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = integration.syn
    module.project = integration.project

    # Some of these tests require a second user
    config = ConfigParser.ConfigParser()
    config.read(synapseclient.client.CONFIG_FILE)
    module.other_user = {}
    try:
        other_user['email'] = config.get('test-authentication', 'username')
        other_user['password'] = config.get('test-authentication', 'password')
        other_user['principalId'] = config.get('test-authentication', 'principalId')
    except ConfigParser.Error:
        print "[test-authentication] section missing from the configuration file"

    if 'principalId' not in other_user:
        # Fall back on Chris's principalId
        other_user['principalId'] = 1421212
        other_user['email'] = 'chris.bare@sagebase.org'

def test_ACL():
    # Get the user's principalId, which is called ownerId and is
    # returned as a string, while in the ACL, it's an integer
    current_user_id = int(syn.getUserProfile()['ownerId'])

    # Verify the validity of the other user
    profile = syn.getUserProfile(other_user['principalId'])

    # Add permissions on the Project for a new user
    acl = syn.setPermissions(project, other_user['principalId'], accessType=['READ', 'CREATE', 'UPDATE'])
    
    permissions = syn.getPermissions(project, current_user_id)
    assert 'DELETE' in permissions
    assert 'CHANGE_PERMISSIONS' in permissions
    assert 'READ' in permissions
    assert 'CREATE' in permissions
    assert 'UPDATE' in permissions

    permissions = syn.getPermissions(project, other_user['principalId'])
    assert 'READ' in permissions
    assert 'CREATE' in permissions
    assert 'UPDATE' in permissions

    #Make sure it works to set/getPermissions by email
    email = other_user['email']
    acl = syn.setPermissions(project, email, accessType=['READ'])
    permissions = syn.getPermissions(project, email)
    assert 'READ' in permissions and len(permissions)==1

    #Get permissionsons of PUBLIC user
    permissions = syn.getPermissions(project)
    assert len(permissions)==0
    
    


def test_get_entity_owned_by_another_user():
    if 'email' not in other_user or 'password' not in other_user:
        sys.stderr.write('\nWarning: no test-authentication configured. skipping test_get_entity_owned_by_another.\n')
        return

    try:
        syn_other = synapseclient.Synapse(skip_checks=True)
        syn_other.login(other_user['email'], other_user['password'])

        project = Project(name=str(uuid.uuid4()))
        project = syn_other.store(project)

        filepath = utils.make_bogus_data_file()
        a_file = File(filepath, parent=project, description='asdf qwer', foo=1234)
        a_file = syn_other.store(a_file)

        current_user_id = int(syn.getUserProfile()['ownerId'])

        # Update the acl to give the current user read permissions
        syn_other.setPermissions(a_file, current_user_id, accessType=['READ'], modify_benefactor=True)

        # Test whether the benefactor's ACL was modified
        assert syn_other.getPermissions(project, current_user_id) == ['READ']

        # Add a new permission to a user with existing permissions
        # make this change on the entity itself, not its benefactor
        syn_other.setPermissions(a_file, current_user_id, accessType=['READ', 'UPDATE'], modify_benefactor=False, warn_if_inherits=False)
        permissions = syn_other.getPermissions(a_file, current_user_id)
        assert 'READ' in permissions
        assert 'UPDATE' in permissions
        assert len(permissions) == 2

        syn_other.setPermissions(a_file, current_user_id, accessType=['READ'])
        assert syn_other.getPermissions(a_file, current_user_id) == ['READ']

        other_users_file = syn.get(a_file.id)
        a_file = syn_other.get(a_file.id)

        assert other_users_file == a_file
    finally:
        syn_other.logout()


def test_access_restrictions():
    ## Bruce gives this test a 'B'. The 'A' solution would be to
    ## construct the mock value from the schemas. -jcb
    with mock.patch('synapseclient.Synapse._getEntityBundle') as _getEntityBundle_mock:
        _getEntityBundle_mock.return_value = {
            u'annotations': {
              u'etag': u'cbda8e02-a83e-4435-96d0-0af4d3684a90',
              u'id': u'syn1000002',
              u'stringAnnotations': {}},
            u'entity': {
              u'concreteType': u'org.sagebionetworks.repo.model.FileEntity',
              u'createdBy': u'Miles Dewey Davis',
              u'dataFileHandleId': u'1234',
              u'entityType': u'org.sagebionetworks.repo.model.FileEntity',
              u'etag': u'cbda8e02-a83e-4435-96d0-0af4d3684a90',
              u'id': u'syn1000002',
              u'name': u'so_what.mp3',
              u'parentId': u'syn1000001',
              u'versionLabel': u'1',
              u'versionNumber': 1},
            u'entityType': u'org.sagebionetworks.repo.model.FileEntity',
            u'fileHandles': [],
            u'unmetAccessRequirements': [{
              u'accessType': u'DOWNLOAD',
              u'concreteType': u'org.sagebionetworks.repo.model.TermsOfUseAccessRequirement',
              u'createdBy': u'377358',
              u'entityType': u'org.sagebionetworks.repo.model.TermsOfUseAccessRequirement',
              u'etag': u'1dfedff0-c3b1-472c-b9ff-1b67acb81f00',
              u'id': 2299555,
              u'subjectIds': [{u'id': u'syn1000002', u'type': u'ENTITY'}],
              u'termsOfUse': u'Use it or lose it!'}]}

        entity = syn.get('syn1000002', downloadFile=False)
        assert entity is not None
        assert entity.path is None

        ## can't download if we have unmet access requirements
        entity = syn.get('syn1000002', downloadFile=True)
        assert entity is not None
        assert entity.path is None


