import ConfigParser
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


def test_get_entity_owned_by_another_user():
    if 'email' not in other_user or 'password' not in other_user:
        sys.stderr.write('\nWarning: no test-authentication configured. skipping test_get_entity_owned_by_another.\n')
        return

    try:
        syn_other = synapseclient.Synapse()
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

