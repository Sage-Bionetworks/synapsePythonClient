from nose.tools import assert_raises, assert_equals
from mock import patch, mock_open
from synapseclient.wiki import Wiki

def test_Wiki():
    """Test the construction and accessors of Wiki objects."""

    #Wiki contstuctor only takes certain values
    assert_raises(ValueError, Wiki, title='foo')

    #Construct a wiki and test uri's
    wiki = Wiki(title ='foobar2', markdown='bar', owner={'id':'5'})
    
    
if __name__ == '__main__':
    test_Wiki()


def test_Wiki__with_markdown_file():
    markdown_data  = """
                    MARK DOWN MARK DOWN MARK DOWN MARK DOWN
                    AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
                    adkflajsl;kfjasd;lfkjsal;kfajslkfjasdlkfj
                    """
    markdown_path = "/somewhere/over/the/rainbow"
    with patch("synapseclient.wiki.open", mock_open(read_data=markdown_data), create=True) as mocked_open:
        wiki = Wiki(owner="doesn't matter", markdownFile=markdown_path)

        mocked_open.assert_called_once_with(markdown_path, 'r')
        mocked_open().read.assert_called_once_with()
        assert_equals(markdown_data,wiki.markdown)


def test_Wiki__markdown_and_makrdownFile_both_defined():
    with assert_raises(ValueError) as raised_error:
        Wiki(owner="doesn't matter", markdown="asdf", markdownFile="~/fakeFile.txt")
    assert_equals("Please use only one argument: markdown or markdownFile", raised_error.exception.message)

