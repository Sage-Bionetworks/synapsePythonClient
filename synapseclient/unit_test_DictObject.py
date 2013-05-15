from dict_object import DictObject

def test_DictObject():
  """Test creation and property access on DictObjects"""
  d = DictObject({'args_working?':'yes'}, a=123, b='foobar', nerds=['chris','jen','janey'])
  assert d.a==123
  assert d['a']==123
  assert d.b=='foobar'
  assert d['b']=='foobar'
  assert d.nerds==['chris','jen','janey']
  assert d['nerds']==['chris','jen','janey']

  print d.keys()
  assert all([key in d.keys() for key in ['args_working?', 'a', 'b', 'nerds']])
  print d
  d.new_key = 'new value!'
  assert d['new_key'] == 'new value!'
