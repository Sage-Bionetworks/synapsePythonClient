"""
Integration tests for the Synapse Client for Python
"""

## to run tests: nosetests -vs synapseclient/integration_test_Entity.py
## to run single test: nosetests -vs synapseclient/integration_test_Entity.py:test_Entity
import uuid
import os
import sys

from synapseclient import Entity, Project, Folder, File, Data
import synapseclient
import synapseclient.utils as utils


def setup_module(module):
    syn = synapseclient.Synapse(debug=False, skip_checks=True)
    syn.login()
    module.syn = syn
    module._to_cleanup = []

    print "Testing against endpoints:"
    print "  " + syn.repoEndpoint
    print "  " + syn.authEndpoint
    print "  " + syn.fileHandleEndpoint
    print "  " + syn.portalEndpoint + "\n"


def teardown_module(module):
    cleanup(module._to_cleanup)

def create_project(name=None):
    """return a newly created project that will be cleaned up during teardown"""
    if name is None:
        name = str(uuid.uuid4())
    project = {'entityType':'org.sagebionetworks.repo.model.Project', 'name':name}
    project = syn.createEntity(project)
    schedule_for_cleanup(project)
    return project

def create_data_entity(parentId):
    data = { 'entityType': 'org.sagebionetworks.repo.model.Data', 'parentId': parentId}
    return syn.createEntity(data)


def schedule_for_cleanup(item):
    """schedule a file of Synapse Entity to be deleted during teardown"""
    globals()['_to_cleanup'].append(item)

def cleanup(items):
    """cleanup junk created during testing"""
    for item in items:
        if isinstance(item, Entity) or utils.is_synapse_id(item):
            try:
                syn.delete(item)
            except Exception as ex:
                if hasattr(ex, 'response') and ex.response.status_code==404:
                    pass
                else:
                    print "Error cleaning up entity: " + str(ex)
        elif isinstance(item, basestring) and os.path.exists(item):
            try:
                os.remove(item)
            except Exception as ex:
                print ex
        else:
            sys.stderr.write('Don\'t know how to clean: %s' % str(item))

