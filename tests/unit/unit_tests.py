## unit tests for python synapse client
############################################################
from datetime import datetime as Datetime
from nose.tools import assert_raises
import os

import synapseclient.utils as utils
from synapseclient.activity import Activity
from synapseclient.utils import _find_used
from synapseclient.exceptions import *


def setup():
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60

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
    a.used({'id':'syn101', 'versionNumber':42, 'concreteType': 'org.sagebionetworks.repo.model.Data'})
    a.executed('syn102', targetVersion=1)
    usedEntities = a['used']
    len(usedEntities) == 2

    assert a['name'] == 'Fuzz'
    assert a['description'] == 'hipster beard dataset'

    ## ??? are activities supposed to come back in order? Let's not count on it
    used_syn101 = _find_used(a, lambda res: res['reference']['targetId'] == 'syn101')
    assert used_syn101['reference']['targetVersionNumber'] == 42
    assert used_syn101['wasExecuted'] == False

    used_syn102 = _find_used(a, lambda res: res['reference']['targetId'] == 'syn102')
    assert used_syn102['reference']['targetVersionNumber'] == 1
    assert used_syn102['wasExecuted'] == True

def test_activity_creation_by_constructor():
    """test activity creation adding used entities by the constructor"""

    ue1 = {'reference':{'targetId':'syn101', 'targetVersionNumber':42}, 'wasExecuted':False}
    ue2 = {'id':'syn102', 'versionNumber':2, 'concreteType': 'org.sagebionetworks.repo.model.Code'}
    ue3 = 'syn103'

    a = Activity(name='Fuzz', description='hipster beard dataset', used=[ue1, ue3], executed=[ue2])

    # print a['used']

    used_syn101 = _find_used(a, lambda res: res['reference']['targetId'] == 'syn101')
    assert used_syn101 is not None
    assert used_syn101['reference']['targetVersionNumber'] == 42
    assert used_syn101['wasExecuted'] == False

    used_syn102 = _find_used(a, lambda res: res['reference']['targetId'] == 'syn102')
    assert used_syn102 is not None
    assert used_syn102['reference']['targetVersionNumber'] == 2
    assert used_syn102['wasExecuted'] == True

    used_syn103 = _find_used(a, lambda res: res['reference']['targetId'] == 'syn103')
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

    u = _find_used(a, lambda res: 'url' in res and res['url']==u1)
    assert u is not None
    assert u['url'] == u1
    assert u['wasExecuted'] == False

    u = _find_used(a, lambda res: 'name' in res and res['name']=='The Onion')
    assert u is not None
    assert u['url'] == 'http://theonion.com'
    assert u['wasExecuted'] == False

    u = _find_used(a, lambda res: 'name' in res and res['name'] == 'Seriously advanced code')
    assert u is not None
    assert u['url'] == u3['url']
    assert u['wasExecuted'] == u3['wasExecuted']

    u = _find_used(a, lambda res: 'name' in res and res['name'] == 'Heavy duty algorithm')
    assert u is not None
    assert u['url'] == u4['url']
    assert u['wasExecuted'] == True

    u = _find_used(a, lambda res: 'name' in res and res['name'] == 'glm.net')
    assert u is not None
    assert u['url'] == 'http://cran.r-project.org/web/packages/glmnet/index.html'
    assert u['wasExecuted'] == True

    u = _find_used(a, lambda res: 'name' in res and res['name'] == 'earthquakes')
    assert u is not None
    assert u['url'] == 'http://earthquake.usgs.gov/earthquakes/feed/geojson/2.5/day'
    assert u['wasExecuted'] == False


def test_activity_parameter_errors():
    """Test error handling in Activity.used()"""
    a = Activity(name='Foobarbat', description='Apply foo to a bar and a bat')
    assert_raises(SynapseMalformedEntityError, a.used, ['syn12345', 'http://google.com'], url='http://amazon.com')
    assert_raises(SynapseMalformedEntityError, a.used, 'syn12345', url='http://amazon.com')
    assert_raises(SynapseMalformedEntityError, a.used, 'http://amazon.com', targetVersion=1)


def test_is_url():
    """test the ability to determine whether a string is a URL"""
    assert utils.is_url("http://mydomain.com/foo/bar/bat?asdf=1234&qewr=ooo")
    assert utils.is_url("http://xkcd.com/1193/")
    assert not utils.is_url("syn123445")    
    assert not utils.is_url("wasssuuuup???")
    assert utils.is_url('file://foo.com/path/to/file.xyz')
    assert utils.is_url('file:///path/to/file.xyz')
    assert utils.is_url('file:/path/to/file.xyz')
    assert utils.is_url('file:///c:/WINDOWS/clock.avi')
    assert utils.is_url('file:c:/WINDOWS/clock.avi')
    assert not utils.is_url('c:/WINDOWS/ugh/ugh.ugh')

def test_windows_file_urls():
    url = 'file:///c:/WINDOWS/clock.avi'
    assert utils.is_url(url)
    assert utils.file_url_to_path(url, verify_exists=False).get('path',None) == 'c:/WINDOWS/clock.avi', utils.file_url_to_path(url)

def test_id_of():
    assert utils.id_of(1) == '1'
    assert utils.id_of('syn12345') == 'syn12345'
    assert utils.id_of({'foo':1, 'id':123}) == 123
    assert_raises(SynapseMalformedEntityError, utils.id_of, {'foo':1, 'idzz':123})
    assert utils.id_of({'properties':{'id':123}}) == 123
    assert_raises(SynapseMalformedEntityError, utils.id_of, {'properties':{'qq':123}})
    assert_raises(SynapseMalformedEntityError, utils.id_of, object())

    class Foo:
        def __init__(self, id):
            self.properties = {'id':id}

    foo = Foo(123)
    assert utils.id_of(foo) == 123

def test_guess_file_name():
    assert utils.guess_file_name('a/b') == 'b'
    assert utils.guess_file_name('file:///a/b') == 'b'
    assert utils.guess_file_name('A:/a/b') == 'b'
    assert utils.guess_file_name('B:/a/b/') == 'b'
    assert utils.guess_file_name('c:\\a\\b') == 'b'
    assert utils.guess_file_name('d:\\a\\b\\') == 'b'
    assert utils.guess_file_name('E:\\a/b') == 'b'
    assert utils.guess_file_name('F:\\a/b/') == 'b'
    assert utils.guess_file_name('/a/b') == 'b'
    assert utils.guess_file_name('/a/b/') == 'b'
    assert utils.guess_file_name('http://www.a.com/b') == 'b'
    assert utils.guess_file_name('http://www.a.com/b/') == 'b'
    assert utils.guess_file_name('http://www.a.com/b?foo=bar') == 'b'
    assert utils.guess_file_name('http://www.a.com/b/?foo=bar') == 'b'
    assert utils.guess_file_name('http://www.a.com/b?foo=bar&arga=barga') == 'b'
    assert utils.guess_file_name('http://www.a.com/b/?foo=bar&arga=barga') == 'b'

def test_version_check():
    from synapseclient.version_check import _version_tuple
    assert _version_tuple('0.5.1.dev200', levels=2) == ('0', '5')
    assert _version_tuple('0.5.1.dev200', levels=3) == ('0', '5', '1')
    assert _version_tuple('1.6', levels=3) == ('1', '6', '0')

def test_normalize_path():
    ## tests should pass on reasonable OSes and also on windows

    ## resolves relative paths
    assert len(utils.normalize_path('asdf.txt')) > 8

    ## doesn't resolve home directory references
    #assert '~' not in utils.normalize_path('~/asdf.txt')

    ## converts back slashes to forward slashes
    assert utils.normalize_path('\\windows\\why\\why\\why.txt')

    ## what's the right thing to do for None?
    assert utils.normalize_path(None) is None

def test_limit_and_offset():
    def query_params(uri):
        """Return the query params as a dict"""
        return dict([kvp.split('=') for kvp in uri.split('?')[1].split('&')])

    qp = query_params(utils._limit_and_offset('/asdf/1234', limit=10, offset=0))
    assert qp['limit'] == '10'
    assert qp['offset'] == '0'

    qp = query_params(utils._limit_and_offset('/asdf/1234?limit=5&offset=10', limit=25, offset=50))
    assert qp['limit'] == '25'
    assert qp['offset'] == '50'
    assert len(qp) == 2

    qp = query_params(utils._limit_and_offset('/asdf/1234?foo=bar', limit=10, offset=30))
    assert qp['limit'] == '10'
    assert qp['offset'] == '30'
    assert qp['foo'] == 'bar'
    assert len(qp) == 3

    qp = query_params(utils._limit_and_offset('/asdf/1234?foo=bar&a=b', limit=10))
    assert qp['limit'] == '10'
    assert 'offset' not in qp
    assert qp['foo'] == 'bar'
    assert qp['a'] == 'b'
    assert len(qp) == 3


def test_utils_extract_user_name():
    profile = {'firstName':'Madonna'}
    assert utils.extract_user_name(profile) == 'Madonna'
    profile = {'firstName':'Oscar', 'lastName':'the Grouch'}
    assert utils.extract_user_name(profile) == 'Oscar the Grouch'
    profile['displayName'] = None
    assert utils.extract_user_name(profile) == 'Oscar the Grouch'
    profile['displayName'] = ''
    assert utils.extract_user_name(profile) == 'Oscar the Grouch'
    profile['displayName'] = 'Assistant Professor Oscar the Grouch, PhD'
    assert utils.extract_user_name(profile) == 'Assistant Professor Oscar the Grouch, PhD'
    profile['userName'] = 'otg'
    assert utils.extract_user_name(profile) == 'otg'


