## unit tests for python synapse client
############################################################
from nose.tools import *
from annotations import Annotations
from datetime import datetime


def test_annotations():
    a = Annotations(foo='bar', zoo=['zing','zaboo'], species='Platypus')
    sa = a.toSynapseAnnotations()
    print sa
    assert sa['stringAnnotations']['foo'] == ['bar']
    assert sa['stringAnnotations']['zoo'] == ['zing','zaboo']
    assert sa['stringAnnotations']['species'] == ['Platypus']

def test_more_annotations():
    a = Annotations(foo=1234, zoo=[123.1, 456.2, 789.3], species='Platypus', birthdays=[datetime(1969,4,28), datetime(1973,12,8), datetime(2008,1,3)])
    sa = a.toSynapseAnnotations()
    print sa
    assert sa['longAnnotations']['foo'] == [1234]
    assert sa['doubleAnnotations']['zoo'] == [123.1, 456.2, 789.3]
    assert sa['stringAnnotations']['species'] == ['Platypus']
    bdays = [datetime.utcfromtimestamp(t) for t in sa['dateAnnotations']['birthdays']]
    assert bdays == [datetime(1969,4,28), datetime(1973,12,8), datetime(2008,1,3)]

def test_round_trip_annotations():
    a = Annotations(foo=1234, zoo=[123.1, 456.2, 789.3], species='Moose', birthdays=[datetime(1969,4,28), datetime(1973,12,8), datetime(2008,1,3)])
    sa = a.toSynapseAnnotations()
    print sa
    a2 = Annotations.fromSynapseAnnotations(sa)
    print a2
    a = a2

def test_mixed_annotations():
    """test that toSynapseAnnotations will coerce a list of mixed types to strings"""
    a = Annotations(foo=[1, 'a', datetime(1969,4,28,11,47)])
    sa = a.toSynapseAnnotations()
    print sa
    a2 = Annotations.fromSynapseAnnotations(sa)
    print a2
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

