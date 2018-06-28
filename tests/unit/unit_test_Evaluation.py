from nose.tools import assert_raises, assert_equals

from synapseclient.evaluation import Evaluation, Submission


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


