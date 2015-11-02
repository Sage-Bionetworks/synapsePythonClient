import os
from synapseclient.dict_object import DictObject


def setup():
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)

def test_DictObject():
    """Test creation and property access on DictObjects"""
    d = DictObject({'args_working?':'yes'}, a=123, b='foobar', nerds=['chris','jen','janey'])
    assert d.a==123
    assert d['a']==123
    assert d.b=='foobar'
    assert d['b']=='foobar'
    assert d.nerds==['chris','jen','janey']
    assert hasattr(d,'nerds')
    assert d['nerds']==['chris','jen','janey']
    assert not hasattr(d,'qwerqwer')

    print(d.keys())
    assert all([key in d.keys() for key in ['args_working?', 'a', 'b', 'nerds']])
    print(d)
    d.new_key = 'new value!'
    assert d['new_key'] == 'new value!'
