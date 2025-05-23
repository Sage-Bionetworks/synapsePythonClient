import os
import uuid

import pytest

import synapseclient.core.utils as utils
from synapseclient import Project, Synapse, Wiki
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.core.upload.upload_functions import upload_synapse_s3


async def test_wikiAttachment(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    # Upload a file to be attached to a Wiki
    filename = utils.make_bogus_data_file()
    attachname = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    schedule_for_cleanup(attachname)
    fileHandle = upload_synapse_s3(syn, filename)

    # Create and store a Wiki
    # The constructor should accept both file handles and file paths
    md = """
    This is a test wiki
    =======================

    Blabber jabber blah blah boo.
    """
    wiki = Wiki(
        owner=project,
        title="A Test Wiki",
        markdown=md,
        fileHandles=[fileHandle["id"]],
        attachments=[attachname],
    )
    wiki = syn.store(wiki)

    # Create a Wiki sub-page
    subwiki = Wiki(
        owner=project,
        title="A sub-wiki",
        markdown="nothing",
        parentWikiId=wiki.id,
    )
    subwiki = syn.store(subwiki)

    # Retrieve the root Wiki from Synapse
    wiki2 = syn.getWiki(project)
    # due to the new wiki api, we'll get back some new properties,
    # namely markdownFileHandleId and markdown_path, so only compare
    # properties that are in the first object
    for property_name in wiki:
        assert wiki[property_name] == wiki2[property_name]

    # Retrieve the sub Wiki from Synapse
    wiki2 = syn.getWiki(project, subpageId=subwiki.id)
    for property_name in wiki:
        assert subwiki[property_name] == wiki2[property_name]

    # Try making an update
    wiki["title"] = "A New Title"
    wiki["markdown"] = wiki["markdown"] + "\nNew stuff here!!!\n"
    syn.store(wiki)
    wiki = syn.getWiki(project)
    assert wiki["title"] == "A New Title"
    assert wiki["markdown"].endswith("\nNew stuff here!!!\n")

    # Check the Wiki's metadata
    headers = syn.getWikiHeaders(project)
    assert len(headers) == 2
    assert headers[0]["title"] in (wiki["title"], subwiki["title"])

    file_handles = syn.getWikiAttachments(wiki)
    file_names = [fh["fileName"] for fh in file_handles]
    for fn in [filename, attachname]:
        assert os.path.basename(fn) in file_names

    syn.delete(subwiki)
    syn.delete(wiki)
    pytest.raises(SynapseHTTPError, syn.getWiki, project)


async def test_create_or_update_wiki(syn: Synapse, project: Project) -> None:
    # create wiki once
    syn.store(
        Wiki(
            title="This is the title",
            owner=project,
            markdown="#Wikis are OK\n\nBlabber jabber blah blah blither blather bonk!",
        )
    )

    # for now, creating it again it will be updated
    new_title = "This is a different title"
    wiki = syn.store(
        Wiki(
            title=new_title,
            owner=project,
            markdown="#Wikis are awesome\n\nNew babble boo flabble gibber wiggle sproing!",
        ),
        createOrUpdate=True,
    )
    assert new_title == syn.getWiki(wiki.ownerId)["title"]


async def test_wiki_version(syn: Synapse, project: Project) -> None:
    # create a new project to avoid artifacts from previous tests
    project = syn.store(Project(name=str(uuid.uuid4())))
    wiki = syn.store(
        Wiki(
            title="Title version 1",
            owner=project,
            markdown="##A heading\n\nThis is version 1 of the wiki page!\n",
        )
    )

    wiki.title = "Title version 2"
    wiki.markdown = "##A heading\n\nThis is version 2 of the wiki page!\n"

    wiki = syn.store(wiki)

    w1 = syn.getWiki(owner=wiki.ownerId, subpageId=wiki.id, version=0)
    assert "version 1" in w1.title
    assert "version 1" in w1.markdown

    w2 = syn.getWiki(owner=wiki.ownerId, subpageId=wiki.id, version=1)
    assert "version 2" in w2.title
    assert "version 2" in w2.markdown


async def test_wiki_with_empty_string_parent_wiki_id(
    syn: Synapse, project: Project
) -> None:
    # GIVEN a wiki is created with an empty string parentWikiId
    # WHEN it is stored
    wiki_stored = syn.store(
        Wiki(
            title="This is the title",
            owner=project,
            markdown="#Wikis are OK\n\nBlabber jabber blah blah blither blather bonk!",
            parentWikiId="",
        )
    )
    # THEN it exists in Synapse
    wiki_retrieved = syn.getWiki(owner=wiki_stored.ownerId, subpageId=wiki_stored.id)
    # AND when we retrieve it, all attributes are set as expected
    for property_name in wiki_stored:
        assert wiki_stored[property_name] == wiki_retrieved[property_name]
