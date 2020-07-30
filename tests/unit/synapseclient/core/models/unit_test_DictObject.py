from synapseclient.core.models.dict_object import DictObject


def test_DictObject():
    """Test creation and property access on DictObjects"""
    d = DictObject({'args_working?': 'yes'}, a=123, b='foobar', nerds=['chris', 'jen', 'janey'])
    assert d.a == 123
    assert d['a'] == 123
    assert d.b == 'foobar'
    assert d['b'] == 'foobar'
    assert d.nerds == ['chris', 'jen', 'janey']
    assert hasattr(d, 'nerds')
    assert d['nerds'] == ['chris', 'jen', 'janey']
    assert not hasattr(d, 'qwerqwer')

    assert all([key in d.keys() for key in ['args_working?', 'a', 'b', 'nerds']])
    d.new_key = 'new value!'
    assert d['new_key'] == 'new value!'
