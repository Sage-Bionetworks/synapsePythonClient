import json

import pytest

from synapseclient import (Evaluation, Submission, SubmissionStatus,
                           Annotations, evaluation)


def test_Evaluation():
    """Test the construction and accessors of Evaluation objects."""

    content_source_missing_ex = 'The "contentSource" parameter must be specified'
    with pytest.raises(ValueError, match=content_source_missing_ex):
        # with no contentSource
        Evaluation(name='foo', description='bar')
    with pytest.raises(ValueError, match=content_source_missing_ex):
        # with non-synapse id contentSource
        Evaluation(name='foo', description='bar', contentSource='a')

    # Assert that the values are
    ev = Evaluation(name='foobar2', description='bar', contentSource='syn1234')
    assert ev['name'] == ev.name
    assert ev['description'] == ev.description


def test_Evaluation__getByNameURI():
    """Verify that Evaluation name URIs are properly quoted"""
    assert '/evaluation/name/foobar' == Evaluation.getByNameURI('foobar')
    assert '/evaluation/name/foo%20bar' == Evaluation.getByNameURI('foo bar')


def test_Submission():
    """Test the construction and accessors of Evaluation objects."""

    pytest.raises(KeyError, Submission, foo='bar')


def test_SubmissionStatus():
    """Test the construction and accessors of SubmissionStatus objects."""
    status = SubmissionStatus(
        id="foo", etag="bar", submissionAnnotations={"foo": "baz"}
    )
    assert status['id'] == status.id
    assert status['etag'] == status.etag
    assert status['submissionAnnotations'] == {"foo": "baz"}
    assert isinstance(status['submissionAnnotations'], Annotations)


def test_SubmissionStatus_json():
    """Test the overloaded json to changes annotations to synapse style
    annotations"""
    status = SubmissionStatus(
        id="foo", etag="bar", submissionAnnotations={"foo": "baz"}
    )
    returned_json_str = status.json()
    expected_status = {
        "etag": "bar",
        "id": "foo",
        "submissionAnnotations": {
            "annotations": {
                "foo": {
                    "type": "STRING",
                    "value": ["baz"]
                }
            },
            "etag": "bar",
            "id": "foo"
        }
    }
    expected_str = json.dumps(expected_status, sort_keys=True, indent=2,
                              ensure_ascii=True)

    assert returned_json_str == expected_str


def test__convert_to_annotation_cls_dict():
    """Test that dictionary is converted to synapseclient.Annotations"""

    annotation_cls = evaluation._convert_to_annotation_cls(
        id='5',
        etag='12',
        values={"foo": "test"}
    )
    assert isinstance(annotation_cls, Annotations)
    assert annotation_cls == {"foo": "test"}
    assert annotation_cls.id == '5'
    assert annotation_cls.etag == '12'


def test__convert_to_annotation_cls_synapse_style():
    """Test that synapse style annotations is converted to
    synapseclient.Annotations"""
    annots = {
        'id': '6',
        'etag': '123',
        'annotations': {
            'foo': {
                'type': 'STRING',
                'value': ['doo']
            }
        }
    }
    annotation_cls = evaluation._convert_to_annotation_cls(
        id='5',
        etag='12',
        values=annots
    )
    assert isinstance(annotation_cls, Annotations)
    assert annotation_cls == {"foo": ["doo"]}
    assert annotation_cls.id == '6'
    assert annotation_cls.etag == '123'


def test__convert_to_annotation_cls_annotations():
    """Test that if an Annotation cls is passed in that nothing
    is done"""
    expected = Annotations(id="5", etag="12", values={'foo': 'bar'})
    annotation_cls = evaluation._convert_to_annotation_cls(
        id='5',
        etag='12',
        values=expected
    )
    assert expected == annotation_cls
