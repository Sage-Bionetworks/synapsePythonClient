# unit tests for python synapse client
############################################################

from datetime import datetime as Datetime
from math import pi
import time
import uuid

from nose.tools import assert_raises, assert_equals, assert_false, assert_true, assert_greater, assert_is_instance

from synapseclient.annotations import (
    Annotations,
    to_synapse_annotations,
    from_synapse_annotations,
    to_submission_status_annotations,
    from_submission_status_annotations,
    convert_old_annotation_json,
    is_synapse_annotations,
    set_privacy,
)
from synapseclient.core.exceptions import *
from synapseclient.entity import File


def test_annotations():
    """Test string annotations"""
    a = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f',
                    {'foo': 'bar', 'zoo': ['zing', 'zaboo'], 'species': 'Platypus'})
    sa = to_synapse_annotations(a)
    expected = {
        'id': 'syn123',
        'etag': '7bdb83e9-a50a-46e4-987a-4962559f090f',
        'annotations': {
            'foo': {'value': ['bar'],
                    'type': 'STRING'},
            'zoo': {'value': ['zing', 'zaboo'],
                    'type': 'STRING'},
            'species': {'value': ['Platypus'],
                        'type': 'STRING'}
        }
    }
    assert_equals(expected, sa)


def test_to_synapse_annotations__require_id_and_etag():
    """Test string annotations"""
    a = {'foo': 'bar', 'zoo': ['zing', 'zaboo'], 'species': 'Platypus'}
    assert_raises(TypeError, to_synapse_annotations, a)


def test_more_annotations():
    """Test long, float and data annotations"""
    a = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f',
                    {'foo': 1234,
                     'zoo': [123.1, 456.2, 789.3],
                     'species': 'Platypus',
                     'birthdays': [Datetime(1969, 4, 28), Datetime(1973, 12, 8), Datetime(2008, 1, 3)],
                     'test_boolean': True,
                     'test_mo_booleans': [False, True, True, False]})
    sa = to_synapse_annotations(a)

    expected = {
        'id': 'syn123',
        'etag': '7bdb83e9-a50a-46e4-987a-4962559f090f',
        'annotations': {
            'foo': {'value': ['1234'],
                    'type': 'LONG'},
            'zoo': {'value': ['123.1', '456.2', '789.3'],
                    'type': 'DOUBLE'},
            'species': {'value': ['Platypus'],
                        'type': 'STRING'},
            'birthdays': {'value': ['-21427200000', '124156800000', '1199318400000'],
                          'type': 'TIMESTAMP_MS'},
            'test_boolean': {'value': ['true'],
                             'type': 'STRING'},
            'test_mo_booleans': {'value': ['false', 'true', 'true', 'false'],
                                 'type': 'STRING'}
        }
    }
    assert_equals(expected, sa)


def test_annotations_unicode():
    a = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f',
                    {'files': [u'文件.txt'], 'cacheDir': u'/Users/chris/.synapseCache/python/syn1809087', u'foo': 1266})
    sa = to_synapse_annotations(a)
    expected = {'id': 'syn123',
                'etag': '7bdb83e9-a50a-46e4-987a-4962559f090f',
                'annotations': {'cacheDir': {'type': 'STRING',
                                             'value': ['/Users/chris/.synapseCache/python/syn1809087']},
                                'files': {'type': 'STRING',
                                          'value': ['文件.txt']},
                                'foo': {'type': 'LONG',
                                        'value': ['1266']}}}
    assert_equals(expected, sa)


def test_round_trip_annotations():
    """Test that annotations can make the round trip from a simple dictionary to the synapse format and back"""
    a = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f',
                    {'foo': [1234], 'zoo': [123.1, 456.2, 789.3], 'species': ['Moose'],
                     'birthdays': [Datetime(1969, 4, 28), Datetime(1973, 12, 8), Datetime(2008, 1, 3),
                                   Datetime(2013, 3, 15)]})
    sa = to_synapse_annotations(a)
    a2 = from_synapse_annotations(sa)
    assert_equals(a, a2)
    assert_equals(a.id, a2.id)
    assert_equals(a.etag, a2.etag)


def test_mixed_annotations():
    """test that to_synapse_annotations will coerce a list of mixed types to strings"""
    a = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f',
                    {'foo': [1, 'a', Datetime(1969, 4, 28, 11, 47)]})
    sa = to_synapse_annotations(a)
    a2 = from_synapse_annotations(sa)
    assert_equals(a2['foo'][0], '1')
    assert_equals(a2['foo'][1], 'a')
    assert_greater(a2['foo'][2].find('1969'), -1)
    assert_equals('syn123', a2.id)
    assert_equals('7bdb83e9-a50a-46e4-987a-4962559f090f', a2.etag)


def test_idempotent_annotations():
    """test that to_synapse_annotations won't mess up a dictionary that's already in the synapse format"""
    a = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f',
                    {'species': 'Moose', 'n': 42, 'birthday': Datetime(1969, 4, 28)})
    sa = to_synapse_annotations(a)
    a2 = {}
    a2.update(sa)
    sa2 = to_synapse_annotations(a2)
    assert_equals(sa, sa2)


def test_submission_status_annotations_round_trip():
    april_28_1969 = Datetime(1969, 4, 28)
    a = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f',
                    {'screen_name': 'Bullwinkle', 'species': 'Moose', 'lucky': 13, 'pi': pi, 'birthday': april_28_1969})
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
    ssa = {'longAnnos': [{'isPrivate': False, 'value': 13, 'key': 'lucky'}],
           'doubleAnnos': [{'isPrivate': False, 'value': 3, 'key': 'three'},
                           {'isPrivate': False, 'value': pi, 'key': 'pi'}]}
    # test that the double annotation 'three':3 is interpreted as a floating
    # point 3.0 rather than an integer 3
    annotations = from_submission_status_annotations(ssa)
    assert_is_instance(annotations['three'], float)
    ssa2 = to_submission_status_annotations(annotations)
    assert_equals({'three', 'pi'}, set([kvp['key'] for kvp in ssa2['doubleAnnos']]))
    assert_equals({'lucky'}, set([kvp['key'] for kvp in ssa2['longAnnos']]))


def test_from_synapse_annotations__empty():
    ssa = {'id': 'syn123',
           'etag': '7bdb83e9-a50a-46e4-987a-4962559f090f',
           'annotations': {}}
    annos = from_synapse_annotations(ssa)
    assert_equals({}, annos)
    assert_equals('syn123', annos.id)
    assert_equals('7bdb83e9-a50a-46e4-987a-4962559f090f', annos.etag)


def test_to_synapse_annotations__empty():
    python_client_annos = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f', {})
    assert_equals({'id': 'syn123', 'etag': '7bdb83e9-a50a-46e4-987a-4962559f090f', 'annotations': {}},
                  to_synapse_annotations(python_client_annos))


def test_is_synapse_annotation():
    assert_true(
        is_synapse_annotations({'id': 'syn123', 'etag': '0f2977b9-0261-4811-a89e-c13e37ce4604', 'annotations': {}}))
    # missing id
    assert_false(
        is_synapse_annotations({'etag': '0f2977b9-0261-4811-a89e-c13e37ce4604', 'annotations': {}}))
    # missing etag
    assert_false(
        is_synapse_annotations({'id': 'syn123', 'annotations': {}}))
    # missing annotations
    assert_false(
        is_synapse_annotations({'id': 'syn123', 'etag': '0f2977b9-0261-4811-a89e-c13e37ce4604'}))
    # has additional keys
    assert_true(is_synapse_annotations(
        {'id': 'syn123', 'etag': '0f2977b9-0261-4811-a89e-c13e37ce4604', 'annotations': {}, 'foo': 'bar'}))

    # annotations only
    assert_false(is_synapse_annotations({'annotations': {}}))

    # annotations + other keys
    assert_false(is_synapse_annotations({'annotations': {}, 'bar': 'baz'}))

    # sanity check: Any Entity subclass has id etag and annotations,
    # but its annotations are not in the format Synapse expects
    assert_false(is_synapse_annotations(File('~/asdf.txt',
                                             id='syn123',
                                             etag='0f2977b9-0261-4811-a89e-c13e37ce4604',
                                             parentId='syn135')))


class TestAnnotations:
    def test__None_id_and_etag(self):
        # in constuctor
        assert_raises(ValueError, Annotations, 'syn123', None)
        assert_raises(ValueError, Annotations, None, '0f2977b9-0261-4811-a89e-c13e37ce4604')

        # after constuction
        annot = Annotations('syn123', '0f2977b9-0261-4811-a89e-c13e37ce4604', {'asdf': 'qwerty'})

        def change_id_to_None():
            annot.id = None

        assert_raises(ValueError, change_id_to_None)

        def change_etag_to_None():
            annot.etag = None

        assert_raises(ValueError, change_etag_to_None)

    def test_annotations(self):
        annot = Annotations('syn123', '0f2977b9-0261-4811-a89e-c13e37ce4604', {'asdf': 'qwerty'})
        assert_equals('syn123', annot.id)
        assert_equals('0f2977b9-0261-4811-a89e-c13e37ce4604', annot.etag)
        assert_equals({'asdf': 'qwerty'}, annot)


def test_convert_old_annotation_json():
    """Test converting an old style annotations dict as may be returned
    from some older APIs to the version used by entity bundle services v2"""

    now = time.time()
    old_json = {
        'id': 'foo',
        'etag': str(uuid.uuid4()),

        'stringAnnotations': {'foo': ['bar', ['baz']]},
        'longAnnotations': {'first': [1, 2, 3], 'second': [4, 5]},
        'doubleAnnotations': {'third': [5.6, 6.7]},
        'dateAnnotations': {'now': [now, now + 1]},

        'blobAnnotations': {'blobs': ['are ignored']},
        'anything else': 'is ignored',
    }

    converted = convert_old_annotation_json(old_json)

    expected = {k: old_json.get(k) for k in ('id', 'etag')}
    expected_annotations = expected['annotations'] = {}

    expected_annotations['foo'] = {
        'type': 'STRING',
        'value': old_json['stringAnnotations']['foo'],
    }
    expected_annotations['first'] = {
        'type': 'LONG',
        'value': old_json['longAnnotations']['first'],
    }
    expected_annotations['second'] = {
        'type': 'LONG',
        'value': old_json['longAnnotations']['second'],
    }
    expected_annotations['third'] = {
        'type': 'DOUBLE',
        'value': old_json['doubleAnnotations']['third'],
    }
    expected_annotations['now'] = {
        'type': 'TIMESTAMP_MS',
        'value': old_json['dateAnnotations']['now'],
    }

    assert_equals(converted, expected)
