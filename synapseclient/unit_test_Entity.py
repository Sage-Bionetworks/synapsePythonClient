from nose.tools import *
import collections
from entity import Entity


def test_Entity():
    """Test the basics of creating and accessing properties on an entity"""
    e =  Entity(name='Test object', description='I hope this works',
                annotations = dict(foo=123, nerds=['chris','jen','janey'], annotations='How confusing!'),
                properties  = dict(annotations='/repo/v1/entity/syn1234/annotations',
                               md5='cdef636522577fc8fb2de4d95875b27c',
                               parentId='syn1234'),
                entityType='org.sagebionetworks.repo.model.Data')

    assert e.parentId == 'syn1234'
    assert e['parentId'] == 'syn1234'
    assert e.properties['parentId'] == 'syn1234'
    assert e.properties.parentId =='syn1234'

    assert e.foo == 123
    assert e['foo'] == 123
    assert e.annotations['foo'] == 123
    assert e.annotations.foo == 123

    ## annotations is a bit funny, because e['annotations'] != e.annotations. 
    ## We want e.annotations to point to the annotations dictionary. But,
    ## e['annotations'] points to the property 'annotations'
    assert isinstance(e.annotations, collections.Mapping)
    assert e.properties['annotations'] == '/repo/v1/entity/syn1234/annotations'
    assert e['annotations'] == '/repo/v1/entity/syn1234/annotations'
    assert e.properties.annotations == '/repo/v1/entity/syn1234/annotations'
    assert e.annotations.annotations == 'How confusing!'
    assert e.annotations['annotations'] == 'How confusing!'

    assert e.nerds == ['chris','jen','janey']

    #print "keys = " + str(e.keys())
    assert all([ k in e for k in ['name', 'description', 'foo', 'nerds', 'annotations', 'md5', 'parentId']])

    ## test modifying props
    e.description = 'Working, so far'
    assert e['description'] == 'Working, so far'
    e['description'] = 'Wiz-bang flapdoodle'
    assert e.description == 'Wiz-bang flapdoodle'

    ## test modifying annos
    e.foo = 999
    assert e.annotations['foo'] == 999
    e['foo'] = 12345
    assert e.annotations.foo == 12345

    ## test creating a new annotation
    e['bar'] = 888

    print '\n\n'
    print '~' * 60
    print e
    print '~' * 60
    assert e.annotations['bar'] == 888
    e['bat'] = 7788
    assert e.annotations['bat'] == 7788

    ## test replacing annotations object
    e.annotations = {'splat':'a totally new set of annotations', 'foo':456}
    assert e.foo == 456
    assert e['foo'] == 456
    assert e.annotations.foo == 456
    assert e.properties['annotations'] == '/repo/v1/entity/syn1234/annotations'
    assert e['annotations'] == '/repo/v1/entity/syn1234/annotations'
    assert e.properties.annotations == '/repo/v1/entity/syn1234/annotations'



