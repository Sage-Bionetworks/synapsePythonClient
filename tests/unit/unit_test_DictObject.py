from synapseclient.dict_object import DictObject
from nose.tools import assert_equals, assert_true, assert_false


def test_DictObject():
    """Test creation and property access on DictObjects"""
    d = DictObject({'args_working?': 'yes'}, a=123, b='foobar', nerds=['chris', 'jen', 'janey'])
    assert_equals(d.a, 123)
    assert_equals(d['a'], 123)
    assert_equals(d.b, 'foobar')
    assert_equals(d['b'], 'foobar')
    assert_equals(d.nerds, ['chris', 'jen', 'janey'])
    assert_true(hasattr(d, 'nerds'))
    assert_equals(d['nerds'], ['chris', 'jen', 'janey'])
    assert_false(hasattr(d, 'qwerqwer'))

    assert_true(all([key in d.keys() for key in ['args_working?', 'a', 'b', 'nerds']]))
    d.new_key = 'new value!'
    assert_equals(d['new_key'], 'new value!')
