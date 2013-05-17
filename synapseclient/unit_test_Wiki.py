from nose.tools import assert_raises

from synapseclient.wiki import Wiki

def test_Wiki():
    """Test the construction and accessors of Wiki objects."""

    #Wiki contstuctor only takes certain values
    assert_raises(ValueError, Wiki, title='foo')

    #Construct a wiki and test uri's
    wiki = Wiki(title ='foobar2', markdown='bar', owner={'id':'5'})
    
    
if __name__ == '__main__':
    test_Wiki()
