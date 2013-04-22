## integration tests for the Entity class
############################################################

## to run tests: nosetests -vs synapseclient/integration_test_Entity.py
## to run single test: nosetests -vs synapseclient/integration_test_Entity.py:test_Entity

from nose.tools import *
import synapseclient
from entity import Entity, Project, Folder, File
import uuid
import tempfile
import os
from datetime import datetime as Datetime


def setup_module(module):
    print '~' * 60
    print 'testing Entity'

    ## if testing endpoints are set in the config file, use them
    ## this was created 'cause nosetests doesn't have a good means of
    ## passing parameters to the tests
    if os.path.exists(synapseclient.client.CONFIG_FILE):
        try:
            import ConfigParser
            config = ConfigParser.ConfigParser()
            config.read(synapseclient.client.CONFIG_FILE)
            if config.has_section('testEndpoints'):
                repoEndpoint=config.get('testEndpoints', 'repo')
                authEndpoint=config.get('testEndpoints', 'auth')
                fileHandleEndpoint=config.get('testEndpoints', 'file')
                print "Testing against endpoint:"
                print "  " + repoEndpoint
                print "  " + authEndpoint
                print "  " + fileHandleEndpoint                    
        except Exception as e:
            print e

    syn = synapseclient.Synapse()
    syn.login()
    module.syn = syn
    module.projects_to_cleanup = []

def teardown_module(module):
    cleanup_projects(module.projects_to_cleanup)


def get_cached_synapse_instance():
    return globals()['syn']


def create_project(name=None):
    if name is None:
        name = str(uuid.uuid4())
    project = {'entityType':'org.sagebionetworks.repo.model.Project', 'name':name}
    project = syn.createEntity(project)
    globals()['projects_to_cleanup'].append(project)
    return project


def cleanup_projects(projects):
    for project in projects:
        try:
            syn.deleteEntity(project)
        except Exception as ex:
            print "Error cleaning up project: " + str(ex)



def test_Entity():

    syn = get_cached_synapse_instance()

    project = create_project()
    e  = Entity(name='Test object',
                description='I hope this works',
                parentId=project['id'],
                entityType='org.sagebionetworks.repo.model.Data',
                foo=123,
                bar='bat')

    annos = e.annotations

    ## save entity in Synapse
    e = syn.createEntity(e.properties)
    a = syn.setAnnotations(e, annos)

    assert e['name'] == 'Test object'
    assert a['foo'] == [123]
    assert a['bar'] == ['bat']

    e = syn.getEntity(e)
    assert e['name'] == 'Test object'
    assert e.annotations['foo'] == [123]
    assert e.annotations['bar'] == ['bat']



def test_entity_subclasses():

    syn = get_cached_synapse_instance()

    ## create a test project in Synapse and annotate it
    project = create_project()
    annos = {'foo':123, 'bar':'bat', 'date':Datetime.now()}
    a = syn.setAnnotations(project, annos)

    ## try creating a Project object from the stuff returned by Synapse
    e = syn.getEntity(project)
    a = syn.getAnnotations(e)
    p = Project(properties=e, annotations=a)

    print 'project = ' + str(project)
    print 'p = ' + str(p)

    assert p['name'] == project['name']
    assert p['id'] == project['id']
    assert 123 in p['foo']

