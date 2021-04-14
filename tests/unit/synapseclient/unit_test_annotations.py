# unit tests for python synapse client
############################################################

from datetime import datetime as Datetime
from math import pi
import time
import uuid

import pytest
from unittest.mock import patch

from synapseclient import annotations
from synapseclient.annotations import (
    Annotations,
    to_synapse_annotations,
    from_synapse_annotations,
    to_submission_status_annotations,
    from_submission_status_annotations,
    convert_old_annotation_json,
    check_annotations_changed,
    is_synapse_annotations,
    set_privacy,
    _convert_to_annotations_list,
)
from synapseclient.entity import File
import synapseclient.core.utils as utils


@patch.object(annotations, '_convert_to_annotations_list')
def test_to_synapse_annotations__called_convert_to_annotations_list(mock_convert_to_annotations_list):
    """Test the helper function _convert_to_annotations_list called properly"""
    a = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f',
                    {'foo': 'bar', 'zoo': ['zing', 'zaboo'], 'species': 'Platypus'})
    to_synapse_annotations(a)
    mock_convert_to_annotations_list.assert_called_once_with({'foo': 'bar', 'zoo': ['zing', 'zaboo'],
                                                              'species': 'Platypus'})


def test_to_synapse_annotations():
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
    assert expected == sa


def test_to_synapse_annotations__require_id_and_etag():
    """Test string annotations"""
    a = {'foo': 'bar', 'zoo': ['zing', 'zaboo'], 'species': 'Platypus'}
    pytest.raises(TypeError, to_synapse_annotations, a)


def test__convert_to_annotations_list():
    """Test long, float and data annotations"""
    a = {'foo': 1234,
         'zoo': [123.1, 456.2, 789.3],
         'species': 'Platypus',
         'birthdays': [Datetime(1969, 4, 28), Datetime(1973, 12, 8), Datetime(2008, 1, 3)],
         'test_boolean': True,
         'test_mo_booleans': [False, True, True, False]}
    actual_annos = _convert_to_annotations_list(a)

    expected_annos = {'foo': {'value': ['1234'],
                              'type': 'LONG'},
                      'zoo': {'value': ['123.1', '456.2', '789.3'],
                              'type': 'DOUBLE'},
                      'species': {'value': ['Platypus'],
                                  'type': 'STRING'},
                      'birthdays': {'value': ['-21427200000', '124156800000', '1199318400000'],
                                    'type': 'TIMESTAMP_MS'},
                      'test_boolean': {'value': ['true'],
                                       'type': 'BOOLEAN'},
                      'test_mo_booleans': {'value': ['false', 'true', 'true', 'false'],
                                           'type': 'BOOLEAN'}}
    assert expected_annos == actual_annos

    a_with_single_value = {'foo': 1234,
                           'zoo': 123.1,
                           'species': 'Platypus',
                           'birthdays': Datetime(1969, 4, 28),
                           'test_boolean': True}
    actual_annos = _convert_to_annotations_list(a_with_single_value)

    expected_annos = {'foo': {'value': ['1234'],
                              'type': 'LONG'},
                      'zoo': {'value': ['123.1'],
                              'type': 'DOUBLE'},
                      'species': {'value': ['Platypus'],
                                  'type': 'STRING'},
                      'birthdays': {'value': ['-21427200000'],
                                    'type': 'TIMESTAMP_MS'},
                      'test_boolean': {'value': ['true'],
                                       'type': 'BOOLEAN'}}
    assert expected_annos == actual_annos


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
    assert expected == sa


def test_round_trip_annotations():
    """Test that annotations can make the round trip from a simple dictionary to the synapse format and back"""
    a = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f', {
        'foo': [1234],
        'zoo': [123.1, 456.2, 789.3],
        'species': ['Moose'],
        'birthdays': [
            Datetime(1969, 4, 28),
            Datetime(1973, 12, 8),
            Datetime(2008, 1, 3),
            Datetime(2013, 3, 15),
        ],
        'facts': [
            True,
            False,
        ]
    })

    sa = to_synapse_annotations(a)
    a2 = from_synapse_annotations(sa)
    assert a == a2
    assert a.id == a2.id
    assert a.etag == a2.etag


def test_mixed_annotations():
    """test that to_synapse_annotations will coerce a list of mixed types to strings"""
    a = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f',
                    {'foo': [1, 'a', Datetime(1969, 4, 28, 11, 47), True]})
    sa = to_synapse_annotations(a)
    a2 = from_synapse_annotations(sa)
    assert a2['foo'][0] == '1'
    assert a2['foo'][1] == 'a'
    assert a2['foo'][2].find('1969') > -1
    assert a2['foo'][3] == 'True'
    assert 'syn123' == a2.id
    assert '7bdb83e9-a50a-46e4-987a-4962559f090f' == a2.etag


def test_idempotent_annotations():
    """test that to_synapse_annotations won't mess up a dictionary that's already in the synapse format"""
    a = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f',
                    {'species': 'Moose', 'n': 42, 'birthday': Datetime(1969, 4, 28), 'fact': True})
    sa = to_synapse_annotations(a)
    a2 = {}
    a2.update(sa)
    sa2 = to_synapse_annotations(a2)
    assert sa == sa2


def test_submission_status_annotations_round_trip():
    april_28_1969 = Datetime(1969, 4, 28)
    a = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f',
                    {'screen_name': 'Bullwinkle', 'species': 'Moose', 'lucky': 13, 'pi': pi, 'birthday': april_28_1969})
    sa = to_submission_status_annotations(a)
    assert {'screen_name', 'species'} == set([kvp['key'] for kvp in sa['stringAnnos']])
    assert {'Bullwinkle', 'Moose'} == set([kvp['value'] for kvp in sa['stringAnnos']])

    # test idempotence
    assert sa == to_submission_status_annotations(sa)

    assert {'lucky', 'birthday'} == set([kvp['key'] for kvp in sa['longAnnos']])
    for kvp in sa['longAnnos']:
        key = kvp['key']
        value = kvp['value']
        if key == 'lucky':
            assert value == 13
        if key == 'birthday':
            assert utils.from_unix_epoch_time(value) == april_28_1969

    assert {'pi'} == set([kvp['key'] for kvp in sa['doubleAnnos']])
    assert {pi} == set([kvp['value'] for kvp in sa['doubleAnnos']])

    set_privacy(sa, key='screen_name', is_private=False)
    pytest.raises(KeyError, set_privacy, sa, key='this_key_does_not_exist', is_private=False)

    for kvp in sa['stringAnnos']:
        if kvp['key'] == 'screen_name':
            assert not kvp['isPrivate']

    a2 = from_submission_status_annotations(sa)
    # TODO: is there a way to convert dates back from longs automatically?
    a2['birthday'] = utils.from_unix_epoch_time(a2['birthday'])
    assert a == a2

    # test idempotence
    assert a == from_submission_status_annotations(a)


def test_submission_status_double_annos():
    ssa = {'longAnnos': [{'isPrivate': False, 'value': 13, 'key': 'lucky'}],
           'doubleAnnos': [{'isPrivate': False, 'value': 3, 'key': 'three'},
                           {'isPrivate': False, 'value': pi, 'key': 'pi'}]}
    # test that the double annotation 'three':3 is interpreted as a floating
    # point 3.0 rather than an integer 3
    annotations = from_submission_status_annotations(ssa)
    assert isinstance(annotations['three'], float)
    ssa2 = to_submission_status_annotations(annotations)
    assert {'three', 'pi'} == set([kvp['key'] for kvp in ssa2['doubleAnnos']])
    assert {'lucky'} == set([kvp['key'] for kvp in ssa2['longAnnos']])


def test_from_synapse_annotations__empty():
    ssa = {'id': 'syn123',
           'etag': '7bdb83e9-a50a-46e4-987a-4962559f090f',
           'annotations': {}}
    annos = from_synapse_annotations(ssa)
    assert {} == annos
    assert 'syn123' == annos.id
    assert '7bdb83e9-a50a-46e4-987a-4962559f090f' == annos.etag


def test_to_synapse_annotations__empty():
    python_client_annos = Annotations('syn123', '7bdb83e9-a50a-46e4-987a-4962559f090f', {})
    assert (
        {
            'id': 'syn123',
            'etag': '7bdb83e9-a50a-46e4-987a-4962559f090f',
            'annotations': {}
        } == to_synapse_annotations(python_client_annos)
    )


def test_is_synapse_annotation():
    assert is_synapse_annotations({'id': 'syn123', 'etag': '0f2977b9-0261-4811-a89e-c13e37ce4604', 'annotations': {}})
    # missing id
    assert not is_synapse_annotations({'etag': '0f2977b9-0261-4811-a89e-c13e37ce4604', 'annotations': {}})
    # missing etag
    assert not is_synapse_annotations({'id': 'syn123', 'annotations': {}})
    # missing annotations
    assert not is_synapse_annotations({'id': 'syn123', 'etag': '0f2977b9-0261-4811-a89e-c13e37ce4604'})
    # has additional keys
    assert is_synapse_annotations(
        {'id': 'syn123', 'etag': '0f2977b9-0261-4811-a89e-c13e37ce4604', 'annotations': {}, 'foo': 'bar'})

    # annotations only
    assert not is_synapse_annotations({'annotations': {}})

    # annotations + other keys
    assert not is_synapse_annotations({'annotations': {}, 'bar': 'baz'})

    # sanity check: Any Entity subclass has id etag and annotations,
    # but its annotations are not in the format Synapse expects
    assert not is_synapse_annotations(
        File(
            '~/asdf.txt',
            id='syn123',
            etag='0f2977b9-0261-4811-a89e-c13e37ce4604',
            parentId='syn135'
        )
    )


class TestAnnotations:
    def test__None_id_and_etag(self):
        # in constuctor
        pytest.raises(ValueError, Annotations, 'syn123', None)
        pytest.raises(ValueError, Annotations, None, '0f2977b9-0261-4811-a89e-c13e37ce4604')

        # after constuction
        annot = Annotations('syn123', '0f2977b9-0261-4811-a89e-c13e37ce4604', {'asdf': 'qwerty'})

        def change_id_to_None():
            annot.id = None

        pytest.raises(ValueError, change_id_to_None)

        def change_etag_to_None():
            annot.etag = None

        pytest.raises(ValueError, change_etag_to_None)

    def test_annotations(self):
        annot = Annotations('syn123', '0f2977b9-0261-4811-a89e-c13e37ce4604', {'asdf': 'qwerty'})
        assert 'syn123' == annot.id
        assert '0f2977b9-0261-4811-a89e-c13e37ce4604' == annot.etag
        assert {'asdf': 'qwerty'} == annot


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

    assert converted == expected


def test_convert_old_annotations_json__already_v2():
    """Test that the presence of any loose annotations key not consistent
    with v1 annotations causes annotations conversion to v2 to be short
    circuited and the dictionary passed through as-is."""

    annos_dict = {
        'id': 'foo',
        'etag': str(uuid.uuid4()),
        'creationDate': '1444559717946',
        'uri': '/entity/syn123/annotations',
    }

    # these keys are consistent with old style v1 annos
    v1_keys = {
        'stringAnnotations': ['foo'],
        'longAnnotations': [1, 2, 3],
    }
    annos_dict.update(v1_keys)

    # however this key is not so its presence reveals this to be to a v2 dict
    # (and the above keys just happen to be v1 looking keys in a v2 annotations dict
    annos_dict['v2_key'] = ['these are actually v2 annotations']

    converted = convert_old_annotation_json(annos_dict)
    assert annos_dict == converted


def test_check_annotations_changed():
    # annotations didn't change, new annotations are single key-value pair
    mock_bundle_annotations = {'annotations': {'far': {'type': 'LONG', 'value': ['123']},
                                               'boo': {'type': 'STRING', 'value': ['text']}}}
    mock_new_annotations = {'far': 123, 'boo': 'text'}
    assert not check_annotations_changed(mock_bundle_annotations, mock_new_annotations)

    # annotations didn't change, new annotations are key to list of value pair
    mock_bundle_annotations = {'annotations': {'far': {'type': 'DOUBLE', 'value': ['123.12', '456.33']},
                                               'boo': {'type': 'LONG', 'value': ['789']}}}
    mock_new_annotations = {'far': [123.12, 456.33], 'boo': 789}
    assert not check_annotations_changed(mock_bundle_annotations, mock_new_annotations)

    # verify remove the annotations, check function should return True
    mock_bundle_annotations = {'annotations': {'far': {'type': 'LONG', 'value': ['123', '456']},
                                               'boo': {'type': 'LONG', 'value': ['789']}}}
    mock_new_annotations = {'far': [123], 'boo': 789}
    assert check_annotations_changed(mock_bundle_annotations, mock_new_annotations)

    # annotations are changed
    mock_bundle_annotations = {'annotations': {'boo': {'type': 'LONG', 'value': ['456']},
                                               'far': {'type': 'LONG', 'value': ['12345']}}}
    mock_new_annotations = {'boo': 456, 'far': 789}
    assert check_annotations_changed(mock_bundle_annotations, mock_new_annotations)
