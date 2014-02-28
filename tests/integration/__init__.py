"""
Integration tests for the Synapse Client for Python

To run all the tests      : nosetests -vs tests
To run a single test suite: nosetests -vs tests/integration
To run a single test set  : nosetests -vs tests/integration/integration_test_Entity.py
To run a single test      : nosetests -vs tests/integration/integration_test_Entity.py:test_Entity
"""

import uuid, os, sys

from synapseclient import Entity, Project, Folder, File, Data, Evaluation
import synapseclient
import synapseclient.utils as utils


def setup_module(module):
    syn = synapseclient.Synapse(debug=False, skip_checks=True)

    print "Testing against endpoints:"
    print "  " + syn.repoEndpoint
    print "  " + syn.authEndpoint
    print "  " + syn.fileHandleEndpoint
    print "  " + syn.portalEndpoint + "\n"

    syn.login()
    module.syn = syn
    module._to_cleanup = []
    
    # Make one project for all the tests to use
    project = Project(name=str(uuid.uuid4()))
    project = syn.store(project)
    schedule_for_cleanup(project)
    module.project = project


def teardown_module(module):
    cleanup(module._to_cleanup)


def schedule_for_cleanup(item):
    """schedule a file of Synapse Entity to be deleted during teardown"""
    globals()['_to_cleanup'].append(item)
    

def cleanup(items):
    """cleanup junk created during testing"""
    for item in items:
        if isinstance(item, Entity) or utils.is_synapse_id(item) or hasattr(item, 'deleteURI'):
            try:
                syn.delete(item)
            except Exception as ex:
                if hasattr(ex, 'response') and ex.response.status_code == 404:
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

