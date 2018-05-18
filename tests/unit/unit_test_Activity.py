from synapseclient.activity import Activity
from nose.tools import assert_equal

# SYNPY-681
def test_dict():
    a = Activity(name="test")
    assert_equal(a['name'], a.name)

