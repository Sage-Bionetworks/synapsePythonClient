from nose.tools import assert_raises, assert_equals, assert_false, assert_true, assert_is_not_none, assert_is_none
import sys
import inspect
from nose import SkipTest

from synapseclient import *
from synapseclient.core.exceptions import _raise_for_status
from synapseclient.core.utils import _find_used
from synapseclient.core.exceptions import *
from synapseclient.core.models.dict_object import DictObject


def test_activity_creation_from_dict():
    """test that activities are created correctly from a dictionary"""
    d = {'name': 'Project Fuzz',
         'description': 'hipster beard dataset',
         'used': [{'reference': {'targetId': 'syn12345', 'versionNumber': 42}, 'wasExecuted': True}]}
    a = Activity(data=d)
    assert_equals(a['name'], 'Project Fuzz')
    assert_equals(a['description'], 'hipster beard dataset')

    usedEntities = a['used']
    assert_equals(len(usedEntities), 1)

    u = usedEntities[0]
    assert_true(u['wasExecuted'])

    assert_equals(u['reference']['targetId'], 'syn12345')
    assert_equals(u['reference']['versionNumber'], 42)


def test_activity_used_execute_methods():
    """test activity creation and used and execute methods"""
    a = Activity(name='Fuzz', description='hipster beard dataset')
    a.used({'id': 'syn101', 'versionNumber': 42, 'concreteType': 'org.sagebionetworks.repo.model.FileEntity'})
    a.executed('syn102', targetVersion=1)
    usedEntities = a['used']
    len(usedEntities), 2

    assert_equals(a['name'], 'Fuzz')
    assert_equals(a['description'], 'hipster beard dataset')

    used_syn101 = _find_used(a, lambda res: res['reference']['targetId'] == 'syn101')
    assert_equals(used_syn101['reference']['targetVersionNumber'], 42)
    assert_false(used_syn101['wasExecuted'])

    used_syn102 = _find_used(a, lambda res: res['reference']['targetId'] == 'syn102')
    assert_equals(used_syn102['reference']['targetVersionNumber'], 1)
    assert_true(used_syn102['wasExecuted'])


def test_activity_creation_by_constructor():
    """test activity creation adding used entities by the constructor"""

    ue1 = {'reference': {'targetId': 'syn101', 'targetVersionNumber': 42}, 'wasExecuted': False}
    ue2 = {'id': 'syn102', 'versionNumber': 2, 'concreteType': 'org.sagebionetworks.repo.model.FileEntity'}
    ue3 = 'syn103'

    a = Activity(name='Fuzz', description='hipster beard dataset', used=[ue1, ue3], executed=[ue2])

    used_syn101 = _find_used(a, lambda res: res['reference']['targetId'] == 'syn101')
    assert_is_not_none(used_syn101)
    assert_equals(used_syn101['reference']['targetVersionNumber'], 42)
    assert_false(used_syn101['wasExecuted'])

    used_syn102 = _find_used(a, lambda res: res['reference']['targetId'] == 'syn102')
    assert_is_not_none(used_syn102)
    assert_equals(used_syn102['reference']['targetVersionNumber'], 2)
    assert_true(used_syn102['wasExecuted'])

    used_syn103 = _find_used(a, lambda res: res['reference']['targetId'] == 'syn103')
    assert_is_not_none(used_syn103)


def test_activity_used_url():
    """test activity creation with UsedURLs"""
    u1 = 'http://xkcd.com'
    u2 = {'name': 'The Onion', 'url': 'http://theonion.com'}
    u3 = {'name': 'Seriously advanced code',
          'url': 'https://github.com/cbare/Pydoku/blob/ef88069f70823808f3462410e941326ae7ffbbe0/solver.py',
          'wasExecuted': True}
    u4 = {'name': 'Heavy duty algorithm', 'url': 'https://github.com/cbare/Pydoku/blob/master/solver.py'}

    a = Activity(name='Foobarbat', description='Apply foo to a bar and a bat', used=[u1, u2, u3], executed=[u3, u4])

    a.executed(url='http://cran.r-project.org/web/packages/glmnet/index.html', name='glm.net')
    a.used(url='http://earthquake.usgs.gov/earthquakes/feed/geojson/2.5/day', name='earthquakes')

    u = _find_used(a, lambda res: 'url' in res and res['url'] == u1)
    assert_is_not_none(u)
    assert_equals(u['url'], u1)
    assert_false(u['wasExecuted'])

    u = _find_used(a, lambda res: 'name' in res and res['name'] == 'The Onion')
    assert_is_not_none(u)
    assert_equals(u['url'], 'http://theonion.com')
    assert_false(u['wasExecuted'])

    u = _find_used(a, lambda res: 'name' in res and res['name'] == 'Seriously advanced code')
    assert_is_not_none(u)
    assert_equals(u['url'], u3['url'])
    assert_equals(u['wasExecuted'], u3['wasExecuted'])

    u = _find_used(a, lambda res: 'name' in res and res['name'] == 'Heavy duty algorithm')
    assert_is_not_none(u)
    assert_equals(u['url'], u4['url'])
    assert_true(u['wasExecuted'])

    u = _find_used(a, lambda res: 'name' in res and res['name'] == 'glm.net')
    assert_is_not_none(u)
    assert_equals(u['url'], 'http://cran.r-project.org/web/packages/glmnet/index.html')
    assert_true(u['wasExecuted'])

    u = _find_used(a, lambda res: 'name' in res and res['name'] == 'earthquakes')
    assert_is_not_none(u)
    assert_equals(u['url'], 'http://earthquake.usgs.gov/earthquakes/feed/geojson/2.5/day')
    assert_false(u['wasExecuted'])


def test_activity_parameter_errors():
    """Test error handling in Activity.used()"""
    a = Activity(name='Foobarbat', description='Apply foo to a bar and a bat')
    assert_raises(SynapseMalformedEntityError, a.used, ['syn12345', 'http://google.com'], url='http://amazon.com')
    assert_raises(SynapseMalformedEntityError, a.used, 'syn12345', url='http://amazon.com')
    assert_raises(SynapseMalformedEntityError, a.used, 'http://amazon.com', targetVersion=1)


def test_unicode_output():
    encoding = sys.stdout.encoding if hasattr(sys.stdout, 'encoding') else 'no encoding'
    print("\nPython thinks your character encoding is:", encoding)
    if encoding and encoding.lower() in ['utf-8', 'utf-16']:
        print("ȧƈƈḗƞŧḗḓ uʍop-ǝpısdn ŧḗẋŧ ƒǿř ŧḗşŧīƞɠ")
    else:
        raise SkipTest("can't display unicode, skipping test_unicode_output...")


def test_raise_for_status():
    class FakeResponse(DictObject):
        def json(self):
            return self._json

    response = FakeResponse(
        status_code=501,
        headers={"content-type": "application/json;charset=utf-8"},
        reason="SchlumpError",
        text='{"reason":"it schlumped"}',
        _json={"reason": "it schlumped"},
        request=DictObject(
            url="http://foo.com/bar/bat",
            headers={"xyz": "pdq"},
            method="PUT",
            body="body"))

    assert_raises(SynapseHTTPError, _raise_for_status, response, verbose=False)


def _calling_module_test_helper():
    return utils.caller_module_name(inspect.currentframe())


def test_calling_module():
    # 'case' is the name of the module with which nosetests runs these tests
    # 'unit_test' is the name of the module in which this test resides
    # we made a helper so that the call order is: case.some_function_for_running_tests()
    # -> unit_test.test_calling_module() -> unit_test._calling_module_test_helper()
    # since both _calling_module_test_helper and test_calling_module are a part of the unit_test module,
    # we can test that callers of the same module do indeed are skipped
    assert_equals("case", _calling_module_test_helper())
