## unit tests for python synapse client
############################################################
from nose.tools import *
from annotations import Annotations
from activity import Activity, makeUsed
from datetime import datetime
import utils


def test_annotations():
    a = Annotations(foo='bar', zoo=['zing','zaboo'], species='Platypus')
    sa = a.toSynapseAnnotations()
    # print sa
    assert sa['stringAnnotations']['foo'] == ['bar']
    assert sa['stringAnnotations']['zoo'] == ['zing','zaboo']
    assert sa['stringAnnotations']['species'] == ['Platypus']

def test_more_annotations():
    a = Annotations(foo=1234, zoo=[123.1, 456.2, 789.3], species='Platypus', birthdays=[datetime(1969,4,28), datetime(1973,12,8), datetime(2008,1,3)])
    sa = a.toSynapseAnnotations()
    # print sa
    assert sa['longAnnotations']['foo'] == [1234]
    assert sa['doubleAnnotations']['zoo'] == [123.1, 456.2, 789.3]
    assert sa['stringAnnotations']['species'] == ['Platypus']
    bdays = [datetime.utcfromtimestamp(t) for t in sa['dateAnnotations']['birthdays']]
    assert bdays == [datetime(1969,4,28), datetime(1973,12,8), datetime(2008,1,3)]

def test_round_trip_annotations():
    a = Annotations(foo=1234, zoo=[123.1, 456.2, 789.3], species='Moose', birthdays=[datetime(1969,4,28), datetime(1973,12,8), datetime(2008,1,3)])
    sa = a.toSynapseAnnotations()
    # print sa
    a2 = Annotations.fromSynapseAnnotations(sa)
    # print a2
    a = a2

def test_mixed_annotations():
    """test that toSynapseAnnotations will coerce a list of mixed types to strings"""
    a = Annotations(foo=[1, 'a', datetime(1969,4,28,11,47)])
    sa = a.toSynapseAnnotations()
    # print sa
    a2 = Annotations.fromSynapseAnnotations(sa)
    # print a2
    assert a2['foo'][0] == '1'
    assert a2['foo'][1] == 'a'
    assert a2['foo'][2].find('1969') > -1


def test_idempotent_annotations():
    """test that toSynapseAnnotations won't mess up a dictionary that's already
    in synapse-style form"""
    a = Annotations(species='Moose', n=42, birthday=datetime(1969,4,28))
    sa = a.toSynapseAnnotations()
    a2 = Annotations()
    a2.update(sa)
    sa2 = a2.toSynapseAnnotations()
    assert sa == sa2

def test_activity_creation_from_dict():
    """test that activities are created correctly from a dictionary"""
    d = {'name':'Project Fuzz',
         'description':'hipster beard dataset',
         'used':[ {'reference':{'targetId':'syn12345', 'versionNumber':42}, 'wasExecuted':True} ]}
    a = Activity(data=d)
    assert a['name'] == 'Project Fuzz'
    assert a['description'] == 'hipster beard dataset'

    usedEntities = a['used']
    assert len(usedEntities) == 1

    u = usedEntities[0]
    assert u['wasExecuted'] == True

    assert u['reference']['targetId'] == 'syn12345'
    assert u['reference']['versionNumber'] == 42

def test_activity_used_execute_methods():
    """test activity creation and used and execute methods"""
    a = Activity(name='Fuzz', description='hipster beard dataset')
    a.used({'id':'syn101', 'versionNumber':42, 'entityType': 'org.sagebionetworks.repo.model.Data'})
    a.executed('syn102', targetVersion=1)
    usedEntities = a['used']
    len(usedEntities) == 2

    assert a['name'] == 'Fuzz'
    assert a['description'] == 'hipster beard dataset'

    ## ??? are activities supposed to come back in order? Let's not count on it
    used_syn101 = None
    for usedEntity in usedEntities:
        if usedEntity['reference']['targetId'] == 'syn101':
            used_syn101 = usedEntity

    assert used_syn101['reference']['targetVersionNumber'] == 42
    assert used_syn101['wasExecuted'] == False

    used_syn102 = None
    for usedEntity in usedEntities:
        if usedEntity['reference']['targetId'] == 'syn102':
            used_syn102 = usedEntity

    assert used_syn102['reference']['targetVersionNumber'] == 1
    assert used_syn102['wasExecuted'] == True

def test_activity_creation_by_constructor():
    """test activity creation adding used entities by the constructor"""

    ue1 = {'reference':{'targetId':'syn101', 'targetVersionNumber':42}, 'wasExecuted':False}
    ue2 = {'id':'syn102', 'versionNumber':2, 'entityType': 'org.sagebionetworks.repo.model.Code'}
    ue3 = 'syn103'

    a = Activity(name='Fuzz', description='hipster beard dataset', used=[ue1, ue3], executed=[ue2])

    # print a['used']

    used_syn101 = None
    for usedEntity in a['used']:
        if usedEntity['reference']['targetId'] == 'syn101':
            used_syn101 = usedEntity

    assert used_syn101 is not None
    assert used_syn101['reference']['targetVersionNumber'] == 42
    assert used_syn101['wasExecuted'] == False

    used_syn102 = None
    for usedEntity in a['used']:
        if usedEntity['reference']['targetId'] == 'syn102':
            used_syn102 = usedEntity

    assert used_syn102 is not None
    assert used_syn102['reference']['targetVersionNumber'] == 2
    assert used_syn102['wasExecuted'] == True

    used_syn103 = None
    for usedEntity in a['used']:
        if usedEntity['reference']['targetId'] == 'syn103':
            used_syn103 = usedEntity

    assert used_syn103 is not None

def test_activity_used_url():
    """test activity creation with UsedURLs"""
    u1 = 'http://xkcd.com'
    u2 = {'name':'The Onion', 'url':'http://theonion.com'}
    u3 = {'name':'Seriously advanced code', 'url':'https://github.com/cbare/Pydoku/blob/ef88069f70823808f3462410e941326ae7ffbbe0/solver.py', 'wasExecuted':True}
    u4 = makeUsed('Heavy duty algorithm', url='https://github.com/cbare/Pydoku/blob/master/solver.py')

    a = Activity(name='Foobarbat', description='Apply foo to a bar and a bat', used=[u1, u2, u3], executed=[u4])

    used_xkcd = None
    for usedEntity in a['used']:
        if 'url' in usedEntity and usedEntity['url'] == u1:
           used_xkcd = usedEntity
           break
    assert used_xkcd is not None
    assert used_xkcd['url'] == u1
    assert used_xkcd['wasExecuted'] == False

    used_onion = None
    for usedEntity in a['used']:
        if 'url' in usedEntity and usedEntity['url'] == u2['url']:
           used_onion = usedEntity
           break
    assert used_onion is not None
    assert used_onion['name'] == 'The Onion'
    assert used_onion['wasExecuted'] == False

    used_code = None
    for usedEntity in a['used']:
        if 'name' in usedEntity and usedEntity['name'] == 'Seriously advanced code':
           used_code = usedEntity
           break
    assert used_code is not None
    assert used_code['url'] == u3['url']
    assert used_code['wasExecuted'] == u3['wasExecuted']

    used_code = None
    for usedEntity in a['used']:
        if 'name' in usedEntity and usedEntity['name'] == 'Heavy duty algorithm':
           used_code = usedEntity
           break
    assert used_code is not None
    assert used_code['url'] == u4['url']
    assert used_code['wasExecuted'] == True


def test_is_url():
    """test the ability to determine whether a string is a URL"""
    assert utils.is_url("http://mydomain.com/foo/bar/bat?asdf=1234&qewr=ooo")
    assert utils.is_url("http://xkcd.com/1193/")
    assert not utils.is_url("syn123445")    
    assert not utils.is_url("wasssuuuup???")
