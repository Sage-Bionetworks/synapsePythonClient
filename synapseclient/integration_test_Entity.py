from nose.tools import *
import synapseclient
from entity import Entity
import uuid
import tempfile
import os


def test_Entity():

    syn = synapseclient.Synapse()
    syn.login()

    try:
        project = {'entityType':'org.sagebionetworks.repo.model.Project', 'name':str(uuid.uuid4())}
        project = syn.createEntity(project)

        e  = Entity(name='Test object',
                    description='I hope this works',
                    parentId=project['id'],
                    entityType='org.sagebionetworks.repo.model.Data')

        e1 = syn.createEntity(e.properties)
        a = syn.setAnnotations(e1, e.annotations)

    except Exception as ex:
        print ex
        try: print ex.response.text
        except: pass

    finally:
        if e1:
            syn.deleteEntity(e1)
        if project:
            syn.deleteEntity(project)

