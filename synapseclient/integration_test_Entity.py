from nose.tools import *
import synapseclient
from entity import Entity, Project, Folder, File
import uuid
import tempfile
import os
from datetime import datetime as Datetime



def test_Entity():

    syn = synapseclient.Synapse()
    syn.login()

    try:
        project = {'entityType':'org.sagebionetworks.repo.model.Project', 'name':str(uuid.uuid4())}
        project = syn.createEntity(project)

        e  = Entity(name='Test object',
                    description='I hope this works',
                    parentId=project['id'],
                    entityType='org.sagebionetworks.repo.model.Data',
                    foo=123,
                    bar='bat')

        e1 = syn.createEntity(e.properties)
        a = syn.setAnnotations(e1, e.annotations)

    finally:
        if e1:
            syn.deleteEntity(e1)
        if project:
            syn.deleteEntity(project)


def test_entity_subclasses():

    syn = synapseclient.Synapse()
    syn.login()

    try:
        project = {'entityType':'org.sagebionetworks.repo.model.Project', 'name':str(uuid.uuid4())}
        project = syn.createEntity(project)

        annos = {'foo':123, 'bar':'bat', 'date':Datetime.now()}
        a = syn.setAnnotations(project, annos)

        e = syn.getEntity(project)
        a = syn.getAnnotations(e)
        p = Project(properties=e, annotations=a)

        print 'project = ' + str(project)
        print 'p = ' + str(p)

        assert p['name'] == project['name']
        assert p['id'] == project['id']
        assert 123 in p['foo']

    finally:
        if project:
            syn.deleteEntity(project)

