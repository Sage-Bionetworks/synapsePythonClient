from nose.tools import assert_raises, assert_equals, assert_true

from synapseclient import (Evaluation, Submission, SubmissionStatus,
                           Annotations, evaluation)


def test_Evaluation():
    """Test the construction and accessors of Evaluation objects."""

    # Status can only be one of ['OPEN', 'PLANNED', 'CLOSED', 'COMPLETED']
    assert_raises(ValueError, Evaluation, name='foo', description='bar', status='BAH')
    assert_raises(ValueError, Evaluation, name='foo', description='bar', status='OPEN', contentSource='a')

    # Assert that the values are
    ev = Evaluation(name='foobar2', description='bar', status='OPEN', contentSource='syn1234')
    assert_equals(ev['name'], ev.name)
    assert_equals(ev['description'], ev.description)
    assert_equals(ev['status'], ev.status)


def test_Submission():
    """Test the construction and accessors of Evaluation objects."""

    assert_raises(KeyError, Submission, foo='bar')


def test_SubmissionStatus():
    """Test the construction and accessors of SubmissionStatus objects."""
    status = SubmissionStatus(
        id="foo", etag="bar", submissionAnnotations={"foo": "baz"}
    )
    assert_equals(status['id'], status.id)
    assert_equals(status['etag'], status.etag)
    assert_equals(status['submissionAnnotations'], {"foo": "baz"})
    assert_true(isinstance(status['submissionAnnotations'], Annotations))


def test_SubmissionStatus_json():
    """Test the overloaded json to changes annotations to synapse style
    annotations"""
    status = SubmissionStatus(
        id="foo", etag="bar", submissionAnnotations={"foo": "baz"}
    )
    status.json()
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
    assert_equals(status, expected_status)


def test__convert_to_annotation_cls_dict():
    """Test that dictionary is converted to synapseclient.Annotations"""

    annotation_cls = evaluation._convert_to_annotation_cls(
        id='5',
        etag='12',
        values={"foo": "test"}
    )
    assert_true(isinstance(annotation_cls, Annotations))
    assert_equals(annotation_cls, {"foo": "test"})
    assert_equals(annotation_cls.id, '5')
    assert_equals(annotation_cls.etag, '12')


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
    assert_true(isinstance(annotation_cls, Annotations))
    assert_equals(annotation_cls, {"foo": ["doo"]})
    assert_equals(annotation_cls.id, '6')
    assert_equals(annotation_cls.etag, '123')


def test__convert_to_annotation_cls_annotations():
    """Test that if an Annotation cls is passed in that nothing
    is done"""
    expected = Annotations(id="5", etag="12", values={'foo': 'bar'})
    annotation_cls = evaluation._convert_to_annotation_cls(
        id='5',
        etag='12',
        values=expected
    )
    assert_equals(expected, annotation_cls)
