## unit tests for python synapse client
############################################################
from nose.tools import *
from annotations import toSynapseAnnotations, fromSynapseAnnotations
from activity import Activity
from datetime import datetime
import utils
from utils import _findUsed


def test_annotations():
    """Test string annotations"""
    a = dict(foo='bar', zoo=['zing','zaboo'], species='Platypus')
    sa = toSynapseAnnotations(a)
    # print sa
    assert sa['stringAnnotations']['foo'] == ['bar']
    assert sa['stringAnnotations']['zoo'] == ['zing','zaboo']
    assert sa['stringAnnotations']['species'] == ['Platypus']

def test_more_annotations():
    """Test long, float and data annotations"""
    a = dict(foo=1234, zoo=[123.1, 456.2, 789.3], species='Platypus', birthdays=[datetime(1969,4,28), datetime(1973,12,8), datetime(2008,1,3)])
    sa = toSynapseAnnotations(a)
    # print sa
    assert sa['longAnnotations']['foo'] == [1234]
    assert sa['doubleAnnotations']['zoo'] == [123.1, 456.2, 789.3]
    assert sa['stringAnnotations']['species'] == ['Platypus']

    ## this part of the test is kinda fragile. It it breaks again, it should be removed
    bdays = [datetime.utcfromtimestamp(float(t)/1000.0) for t in sa['dateAnnotations']['birthdays']]
    assert bdays == [datetime(1969,4,28), datetime(1973,12,8), datetime(2008,1,3)]

def test_round_trip_annotations():
    """Test that annotations can make the round trip from a simple dictionary
    to the synapse format and back"""
    a = dict(foo=1234, zoo=[123.1, 456.2, 789.3], species='Moose', birthdays=[datetime(1969,4,28), datetime(1973,12,8), datetime(2008,1,3), datetime(2013,3,15)])
    sa = toSynapseAnnotations(a)
    # print sa
    a2 = fromSynapseAnnotations(sa)
    # print a2
    a = a2

def test_mixed_annotations():
    """test that toSynapseAnnotations will coerce a list of mixed types to strings"""
    a = dict(foo=[1, 'a', datetime(1969,4,28,11,47)])
    sa = toSynapseAnnotations(a)
    # print sa
    a2 = fromSynapseAnnotations(sa)
    # print a2
    assert a2['foo'][0] == '1'
    assert a2['foo'][1] == 'a'
    assert a2['foo'][2].find('1969') > -1


def test_idempotent_annotations():
    """test that toSynapseAnnotations won't mess up a dictionary that's already
    in the synapse format"""
    a = dict(species='Moose', n=42, birthday=datetime(1969,4,28))
    sa = toSynapseAnnotations(a)
    a2 = dict()
    a2.update(sa)
    sa2 = toSynapseAnnotations(a2)
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
    used_syn101 = _findUsed(a, lambda res: res['reference']['targetId'] == 'syn101')
    assert used_syn101['reference']['targetVersionNumber'] == 42
    assert used_syn101['wasExecuted'] == False

    used_syn102 = _findUsed(a, lambda res: res['reference']['targetId'] == 'syn102')
    assert used_syn102['reference']['targetVersionNumber'] == 1
    assert used_syn102['wasExecuted'] == True

def test_activity_creation_by_constructor():
    """test activity creation adding used entities by the constructor"""

    ue1 = {'reference':{'targetId':'syn101', 'targetVersionNumber':42}, 'wasExecuted':False}
    ue2 = {'id':'syn102', 'versionNumber':2, 'entityType': 'org.sagebionetworks.repo.model.Code'}
    ue3 = 'syn103'

    a = Activity(name='Fuzz', description='hipster beard dataset', used=[ue1, ue3], executed=[ue2])

    # print a['used']

    used_syn101 = _findUsed(a, lambda res: res['reference']['targetId'] == 'syn101')
    assert used_syn101 is not None
    assert used_syn101['reference']['targetVersionNumber'] == 42
    assert used_syn101['wasExecuted'] == False

    used_syn102 = _findUsed(a, lambda res: res['reference']['targetId'] == 'syn102')
    assert used_syn102 is not None
    assert used_syn102['reference']['targetVersionNumber'] == 2
    assert used_syn102['wasExecuted'] == True

    used_syn103 = _findUsed(a, lambda res: res['reference']['targetId'] == 'syn103')
    assert used_syn103 is not None

def test_activity_used_url():
    """test activity creation with UsedURLs"""
    u1 = 'http://xkcd.com'
    u2 = {'name':'The Onion', 'url':'http://theonion.com'}
    u3 = {'name':'Seriously advanced code', 'url':'https://github.com/cbare/Pydoku/blob/ef88069f70823808f3462410e941326ae7ffbbe0/solver.py', 'wasExecuted':True}
    u4 = {'name':'Heavy duty algorithm', 'url':'https://github.com/cbare/Pydoku/blob/master/solver.py'}

    a = Activity(name='Foobarbat', description='Apply foo to a bar and a bat', used=[u1, u2, u3], executed=[u3, u4])

    a.executed(url='http://cran.r-project.org/web/packages/glmnet/index.html', name='glm.net')
    a.used(url='http://earthquake.usgs.gov/earthquakes/feed/geojson/2.5/day', name='earthquakes')

    u = _findUsed(a, lambda res: 'url' in res and res['url']==u1)
    assert u is not None
    assert u['url'] == u1
    assert u['wasExecuted'] == False

    u = _findUsed(a, lambda res: 'name' in res and res['name']=='The Onion')
    assert u is not None
    assert u['url'] == 'http://theonion.com'
    assert u['wasExecuted'] == False

    u = _findUsed(a, lambda res: 'name' in res and res['name'] == 'Seriously advanced code')
    assert u is not None
    assert u['url'] == u3['url']
    assert u['wasExecuted'] == u3['wasExecuted']

    u = _findUsed(a, lambda res: 'name' in res and res['name'] == 'Heavy duty algorithm')
    assert u is not None
    assert u['url'] == u4['url']
    assert u['wasExecuted'] == True

    u = _findUsed(a, lambda res: 'name' in res and res['name'] == 'glm.net')
    assert u is not None
    assert u['url'] == 'http://cran.r-project.org/web/packages/glmnet/index.html'
    assert u['wasExecuted'] == True

    u = _findUsed(a, lambda res: 'name' in res and res['name'] == 'earthquakes')
    assert u is not None
    assert u['url'] == 'http://earthquake.usgs.gov/earthquakes/feed/geojson/2.5/day'
    assert u['wasExecuted'] == False


def test_activity_parameter_errors():
    """Test error handling in Activity.used()"""
    a = Activity(name='Foobarbat', description='Apply foo to a bar and a bat')
    assert_raises(Exception, a.used, ['syn12345', 'http://google.com'], url='http://amazon.com')
    assert_raises(Exception, a.used, 'syn12345', url='http://amazon.com')
    assert_raises(Exception, a.used, 'http://amazon.com', targetVersion=1)



def test_is_url():
    """test the ability to determine whether a string is a URL"""
    assert utils.is_url("http://mydomain.com/foo/bar/bat?asdf=1234&qewr=ooo")
    assert utils.is_url("http://xkcd.com/1193/")
    assert not utils.is_url("syn123445")    
    assert not utils.is_url("wasssuuuup???")

def test_id_of():
    assert utils.id_of(1) == '1'
    assert utils.id_of('syn12345') == 'syn12345'
    assert utils.id_of({'foo':1, 'id':123}) == 123
    assert utils.id_of({'foo':1, 'idzz':123}) is None
    assert utils.id_of({'properties':{'id':123}}) == 123
    assert utils.id_of({'properties':{'qq':123}}) is None

    class Foo:
        def __init__(self, id):
            self.properties = {'id':id}

    foo = Foo(123)
    assert utils.id_of(foo) == 123

