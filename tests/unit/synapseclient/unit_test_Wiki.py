from unittest.mock import mock_open, patch

import pytest

from synapseclient import Synapse, Wiki


def test_Wiki():
    """Test the construction and accessors of Wiki objects."""

    # Wiki contstuctor only takes certain values
    pytest.raises(ValueError, Wiki, title="foo")

    # Construct a wiki and test uri's
    Wiki(title="foobar2", markdown="bar", owner={"id": "5"})


if __name__ == "__main__":
    test_Wiki()


def test_Wiki__with_markdown_file():
    markdown_data = """
                    MARK DOWN MARK DOWN MARK DOWN MARK DOWN
                    AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
                    adkflajsl;kfjasd;lfkjsal;kfajslkfjasdlkfj
                    """
    markdown_path = "/somewhere/over/the/rainbow.txt"
    with (
        patch(
            "synapseclient.wiki.open", mock_open(read_data=markdown_data), create=True
        ) as mocked_open,
        patch("os.path.isfile", return_value=True),
    ):
        # method under test
        wiki = Wiki(owner="doesn't matter", markdownFile=markdown_path)

        mocked_open.assert_called_once_with(markdown_path, "r")
        mocked_open().read.assert_called_once_with()
        assert markdown_data == wiki.markdown


def test_Wiki__markdown_and_markdownFile_both_defined():
    with pytest.raises(ValueError):
        Wiki(owner="doesn't matter", markdown="asdf", markdownFile="~/fakeFile.txt")


def test_Wiki__markdown_is_None_markdownFile_defined():
    markdown_path = "/somewhere/over/the/rainbow.txt"
    with (
        patch("synapseclient.wiki.open", mock_open(), create=True) as mocked_open,
        patch("os.path.isfile", return_value=True),
    ):
        # method under test
        Wiki(owner="doesn't matter", markdownFile=markdown_path)

        mocked_open.assert_called_once_with(markdown_path, "r")
        mocked_open().read.assert_called_once_with()


def test_Wiki__markdown_defined_markdownFile_is_None():
    markdown = "Somebody once told me the OS was gonna delete me. I'm not the largest file on the disk."

    # method under test
    wiki = Wiki(owner="doesn't matter", markdown=markdown)

    assert markdown == wiki.markdown


def test_Wiki__markdownFile_path_not_exist():
    # method under test
    with pytest.raises(ValueError):
        Wiki(
            owner="doesn't matter",
            markdownFile="/this/is/not/the/file/you/are/looking.for",
        )


def test_wiki_with_none_attachments(syn: Synapse):
    with patch.object(syn, "restPOST"):
        w = Wiki(owner="syn1", markdown="markdown", attachments=None)
        syn.store(w)


def test_wiki_with_empty_string_parent_wiki_id(syn: Synapse):
    with patch.object(syn, "restPOST") as mock_restPOST:
        # WHEN a wiki is created with an empty string parentWikiId
        w = Wiki(owner="syn1", markdown="markdown", parentWikiId="")
        # THEN the parentWikiId is set to None
        assert w.parentWikiId is None
        # WHEN the wiki is stored
        syn.store(w)
        # THEN the parentWikiId is set to None in the request
        mock_restPOST.assert_called_once_with(
            "/entity/syn1/wiki",
            '{"markdown": "markdown", "parentWikiId": null, "attachmentFileHandleIds": []}',
        )
