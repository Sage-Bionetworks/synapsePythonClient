# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

import json
import mock
import os
import sys
import uuid

from nose.tools import assert_raises, assert_equals, raises

import synapseclient
import synapseclient.utils as utils
from synapseclient import Activity, Entity, Project, Folder, File
from synapseclient.exceptions import SynapseUnmetAccessRestrictions
import integration
from integration import schedule_for_cleanup


def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn
    module.project = integration.project
    module.other_user = integration.other_user


def test_ACL():
    # Get the user's principalId, which is called ownerId and is
    # returned as a string, while in the ACL, it's an integer
    current_user_id = int(syn.getUserProfile()['ownerId'])

    # Verify the validity of the other user
    profile = syn.getUserProfile(other_user['principalId'])

    # Add permissions on the Project for a new user
    acl = syn.setPermissions(project, other_user['principalId'], accessType=['READ', 'CREATE', 'UPDATE'])

    ## skip this next bit if the other user is the same as the current user
    assert other_user['principalId'] != current_user_id, \
        "\nInvalid test: current user and other user are the same. Please run as a " \
        "different user or modify the [test-authentication] section of .synapseConfig\n"

    ## make sure the current user still has a full set of permissions
    permissions = syn.getPermissions(project, current_user_id)
    assert 'DELETE' in permissions
    assert 'CHANGE_PERMISSIONS' in permissions
    assert 'READ' in permissions
    assert 'CREATE' in permissions
    assert 'UPDATE' in permissions

    ## check if the permissions granted to the other user stuck
    permissions = syn.getPermissions(project, other_user['principalId'])
    assert 'READ' in permissions
    assert 'CREATE' in permissions
    assert 'UPDATE' in permissions

    #Make sure it works to set/getPermissions by username (email no longer works)
    username = other_user['username']
    acl = syn.setPermissions(project, username, accessType=['READ'])
    permissions = syn.getPermissions(project, username)
    assert 'READ' in permissions and len(permissions)==1

    ## test remove user from ACL
    acl = syn.setPermissions(project, username, None)
    permissions = syn.getPermissions(project, username)
    assert permissions == []

    #Get permissions of PUBLIC user
    permissions = syn.getPermissions(project)
    assert len(permissions)==0


def test_get_entity_owned_by_another_user():
    if 'username' not in other_user or 'password' not in other_user:
        sys.stderr.write('\nWarning: no test-authentication configured. skipping test_get_entity_owned_by_another.\n')
        return

    try:
        syn_other = synapseclient.Synapse(skip_checks=True)
        syn_other.login(other_user['username'], other_user['password'])

        project = Project(name=str(uuid.uuid4()))
        project = syn_other.store(project)

        filepath = utils.make_bogus_data_file()
        a_file = File(filepath, parent=project, description='asdf qwer', foo=1234)
        a_file = syn_other.store(a_file)

        current_user_id = int(syn.getUserProfile()['ownerId'])

        # Update the acl to give the current user read permissions
        syn_other.setPermissions(a_file, current_user_id, accessType=['READ', 'DOWNLOAD'], modify_benefactor=True)

        # Test whether the benefactor's ACL was modified
        assert_equals(set(syn_other.getPermissions(project, current_user_id)),  set(['READ', 'DOWNLOAD']))

        # Add a new permission to a user with existing permissions
        # make this change on the entity itself, not its benefactor
        syn_other.setPermissions(a_file, current_user_id, accessType=['READ', 'UPDATE', 'DOWNLOAD'], modify_benefactor=False, warn_if_inherits=False)
        permissions = syn_other.getPermissions(a_file, current_user_id)
        assert 'READ' in permissions
        assert 'UPDATE' in permissions
        assert len(permissions) == 3

        syn_other.setPermissions(a_file, current_user_id, accessType=['READ', 'DOWNLOAD'])
        assert_equals(set(syn_other.getPermissions(a_file, current_user_id)), set(['DOWNLOAD', 'READ']))

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
            'annotations': {
              'etag': 'cbda8e02-a83e-4435-96d0-0af4d3684a90',
              'id': 'syn1000002',
              'stringAnnotations': {}},
            'entity': {
              'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
              'createdBy': 'Miles Dewey Davis',
              'dataFileHandleId': '1234',
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
        }

        entity = syn.get('syn1000002', downloadFile=False)
        assert entity is not None
        assert entity.path is None

        ## Downloading the file is the default, but is an error if we have unmet access requirements
        assert_raises(synapseclient.exceptions.SynapseUnmetAccessRestrictions, syn.get, 'syn1000002', downloadFile=True)


def test_setPermissions__default_permissions():
    temp_proj = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(temp_proj)

    syn.setPermissions(temp_proj, other_user['username'])
    permissions = syn.getPermissions(temp_proj, other_user['username'])

    assert_equals(set(['READ', 'DOWNLOAD']), set(permissions))



def test_check_entity_restrictions():
    current_user_id = int(syn.getUserProfile()['ownerId'])

    #use other user to create a file
    other_syn = synapseclient.login(other_user['username'], other_user['password'])
    proj = other_syn.store(Project(name=str(uuid.uuid4())+'test_check_entity_restrictions'))
    a_file = other_syn.store(File('~/idk', parent=proj, description='A place to put my junk', foo=1000, synapseStore=False), isRestricted=True)

    #attempt to get file
    assert_raises(SynapseUnmetAccessRestrictions, syn.get, a_file.id, downloadFile=True)

    other_syn.delete(proj)

