from nose.tools import *
from evaluation import *

def test_Evaluation():
    """Test the construction and accessors of Evaluation objects."""

    #Status can only be one of ['OPEN', 'PLANNED', 'CLOSED', 'COMPLETED']
    assert_raises(ValueError, Evaluation, name='foo', description='bar', status='BAH')

    #Assert that the values are 
    ev = Evaluation(name='foobar2', description='bar', status='OPEN')
    assert(ev['name']==ev.name)
    assert(ev['description']==ev.description)
    assert(ev['status']==ev.status)


def test_Submission():
    """Test the construction and accessors of Evaluation objects."""

    assert_raises(KeyError, Submission, foo='bar')


