# unit tests for python synapse client
############################################################
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import OrderedDict
from datetime import datetime as Datetime
from nose.tools import assert_raises, assert_equals, assert_false, assert_true, assert_greater, assert_is_instance
from math import pi

from synapseclient.annotations import to_synapse_annotations, from_synapse_annotations,\
    to_submission_status_annotations, from_submission_status_annotations, set_privacy
from synapseclient.exceptions import *


def test_annotations():
    """Test string annotations"""
    a = dict(foo='bar', zoo=['zing', 'zaboo'], species='Platypus')
    sa = to_synapse_annotations(a)
    assert_equals(sa['stringAnnotations']['foo'], ['bar'])
    assert_equals(sa['stringAnnotations']['zoo'], ['zing', 'zaboo'])
    assert_equals(sa['stringAnnotations']['species'], ['Platypus'])


def test_annotation_name_collision():
    """Test handling of a name collisions between typed user generated and untyped
       system generated annotations, see SYNPY-203 and PLFM-3248"""

    # order is important: to repro the erro, the key uri has to come before stringAnnotations
    sa = OrderedDict()
    sa[u'uri'] = u'/entity/syn47396/annotations'
    sa[u'doubleAnnotations'] = {}
    sa[u'longAnnotations'] = {}
    sa[u'stringAnnotations'] = {
            'tissueType': ['Blood'],
            'uri': ['/repo/v1/dataset/47396']}
    sa[u'creationDate'] = u'1321168909232'
    sa[u'id'] = u'syn47396'

    a = from_synapse_annotations(sa)
    assert_equals(a['tissueType'], ['Blood'])


def test_more_annotations():
    """Test long, float and data annotations"""
    a = dict(foo=1234,
             zoo=[123.1, 456.2, 789.3],
             species='Platypus',
             birthdays=[Datetime(1969, 4, 28), Datetime(1973, 12, 8), Datetime(2008, 1, 3)],
             test_boolean=True,
             test_mo_booleans=[False, True, True, False])
    sa = to_synapse_annotations(a)
    assert_equals(sa['longAnnotations']['foo'], [1234])
    assert_equals(sa['doubleAnnotations']['zoo'], [123.1, 456.2, 789.3])
    assert_equals(sa['stringAnnotations']['species'], ['Platypus'])
    assert_equals(sa['stringAnnotations']['test_boolean'], ['true'])
    assert_equals(sa['stringAnnotations']['test_mo_booleans'], ['false', 'true', 'true', 'false'])

    # this part of the test is kinda fragile. It it breaks again, it should be removed
    bdays = [utils.from_unix_epoch_time(t) for t in sa['dateAnnotations']['birthdays']]
    assert_true(all([t in bdays for t in [Datetime(1969, 4, 28), Datetime(1973, 12, 8), Datetime(2008, 1, 3)]]))


def test_annotations_unicode():
    a = {'files': [u'tmp6y5tVr.txt'], 'cacheDir': u'/Users/chris/.synapseCache/python/syn1809087', u'foo': 1266}
    sa = to_synapse_annotations(a)
    assert_equals(sa['stringAnnotations']['cacheDir'], [u'/Users/chris/.synapseCache/python/syn1809087'])


def test_round_trip_annotations():
    """Test that annotations can make the round trip from a simple dictionary to the synapse format and back"""
    a = dict(foo=1234, zoo=[123.1, 456.2, 789.3], species='Moose',
             birthdays=[Datetime(1969, 4, 28), Datetime(1973, 12, 8), Datetime(2008, 1, 3), Datetime(2013, 3, 15)])
    sa = to_synapse_annotations(a)
    a2 = from_synapse_annotations(sa)
    a = a2


def test_mixed_annotations():
    """test that to_synapse_annotations will coerce a list of mixed types to strings"""
    a = dict(foo=[1, 'a', Datetime(1969, 4, 28, 11, 47)])
    sa = to_synapse_annotations(a)
    a2 = from_synapse_annotations(sa)
    assert_equals(a2['foo'][0], '1')
    assert_equals(a2['foo'][1], 'a')
    assert_greater(a2['foo'][2].find('1969'), -1)


def test_idempotent_annotations():
    """test that to_synapse_annotations won't mess up a dictionary that's already in the synapse format"""
    a = dict(species='Moose', n=42, birthday=Datetime(1969, 4, 28))
    sa = to_synapse_annotations(a)
    a2 = dict()
    a2.update(sa)
    sa2 = to_synapse_annotations(a2)
    assert_equals(sa, sa2)


def test_submission_status_annotations_round_trip():
    april_28_1969 = Datetime(1969, 4, 28)
    a = dict(screen_name='Bullwinkle', species='Moose', lucky=13, pi=pi, birthday=april_28_1969)
    sa = to_submission_status_annotations(a)
    assert_equals({'screen_name', 'species'}, set([kvp['key'] for kvp in sa['stringAnnos']]))
    assert_equals({'Bullwinkle', 'Moose'}, set([kvp['value'] for kvp in sa['stringAnnos']]))

    # test idempotence
    assert_equals(sa, to_submission_status_annotations(sa))

    assert_equals({'lucky', 'birthday'}, set([kvp['key'] for kvp in sa['longAnnos']]))
    for kvp in sa['longAnnos']:
        key = kvp['key']
        value = kvp['value']
        if key == 'lucky':
            assert_equals(value, 13)
        if key == 'birthday':
            assert_equals(utils.from_unix_epoch_time(value), april_28_1969)

    assert_equals({'pi'}, set([kvp['key'] for kvp in sa['doubleAnnos']]))
    assert_equals({pi}, set([kvp['value'] for kvp in sa['doubleAnnos']]))

    set_privacy(sa, key='screen_name', is_private=False)
    assert_raises(KeyError, set_privacy, sa, key='this_key_does_not_exist', is_private=False)

    for kvp in sa['stringAnnos']:
        if kvp['key'] == 'screen_name':
            assert_false(kvp['isPrivate'])

    a2 = from_submission_status_annotations(sa)
    # TODO: is there a way to convert dates back from longs automatically?
    a2['birthday'] = utils.from_unix_epoch_time(a2['birthday'])
    assert_equals(a, a2)

    # test idempotence
    assert_equals(a, from_submission_status_annotations(a))


def test_submission_status_double_annos():
    ssa = {'longAnnos':   [{'isPrivate': False, 'value': 13, 'key': 'lucky'}],
           'doubleAnnos': [{'isPrivate': False, 'value': 3, 'key': 'three'},
                           {'isPrivate': False, 'value': pi, 'key': 'pi'}]}
    # test that the double annotation 'three':3 is interpreted as a floating
    # point 3.0 rather than an integer 3
    annotations = from_submission_status_annotations(ssa)
    assert_is_instance(annotations['three'], float)
    ssa2 = to_submission_status_annotations(annotations)
    assert_equals({'three', 'pi'}, set([kvp['key'] for kvp in ssa2['doubleAnnos']]))
    assert_equals({'lucky'}, set([kvp['key'] for kvp in ssa2['longAnnos']]))
