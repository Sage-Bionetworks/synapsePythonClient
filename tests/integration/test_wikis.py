# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import uuid
from nose.tools import assert_raises, assert_equal, assert_in, assert_equals, assert_true

from synapseclient.exceptions import *
from synapseclient import Project, Wiki
from synapseclient.upload_functions import upload_synapse_s3

import integration
from integration import schedule_for_cleanup


def setup(module):

    module.syn = integration.syn
    module.project = integration.project


def test_wikiAttachment():
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
    wiki = Wiki(owner=project, title='A Test Wiki', markdown=md, 
                fileHandles=[fileHandle['id']], 
                attachments=[attachname])
    wiki = syn.store(wiki)
    
    # Create a Wiki sub-page
    subwiki = Wiki(owner=project, title='A sub-wiki', 
                   markdown='nothing', parentWikiId=wiki.id)
    subwiki = syn.store(subwiki)
    
    # Retrieve the root Wiki from Synapse
    wiki2 = syn.getWiki(project)
    # due to the new wiki api, we'll get back some new properties,
    # namely markdownFileHandleId and markdown_path, so only compare
    # properties that are in the first object
    for property_name in wiki:
        assert_equal(wiki[property_name], wiki2[property_name])

    # Retrieve the sub Wiki from Synapse
    wiki2 = syn.getWiki(project, subpageId=subwiki.id)
    for property_name in wiki:
        assert_equal(subwiki[property_name], wiki2[property_name])

    # Try making an update
    wiki['title'] = 'A New Title'
    wiki['markdown'] = wiki['markdown'] + "\nNew stuff here!!!\n"
    syn.store(wiki)
    wiki = syn.getWiki(project)
    assert_equals(wiki['title'], 'A New Title')
    assert_true(wiki['markdown'].endswith("\nNew stuff here!!!\n"))

    # Check the Wiki's metadata
    headers = syn.getWikiHeaders(project)
    assert_equals(len(headers), 2)
    assert_in(headers[0]['title'], (wiki['title'], subwiki['title']))

    file_handles = syn.getWikiAttachments(wiki)
    file_names = [fh['fileName'] for fh in file_handles]
    for fn in [filename, attachname]:
        assert_in(os.path.basename(fn), file_names)

    syn.delete(subwiki)
    syn.delete(wiki)
    assert_raises(SynapseHTTPError, syn.getWiki, project)


def test_create_or_update_wiki():
    # create wiki once
    syn.store(Wiki(title='This is the title', owner=project,
                   markdown="#Wikis are OK\n\nBlabber jabber blah blah blither blather bonk!"))

    # for now, creating it again it will be updated
    new_title = 'This is a different title'
    wiki = syn.store(Wiki(title=new_title, owner=project,
                          markdown="#Wikis are awesome\n\nNew babble boo flabble gibber wiggle sproing!"),
                     createOrUpdate=True)
    assert_equal(new_title, syn.getWiki(wiki.ownerId)['title'])


def test_wiki_version():
    # create a new project to avoid artifacts from previous tests
    project = syn.store(Project(name=str(uuid.uuid4())))
    wiki = syn.store(Wiki(title='Title version 1', owner=project,
                          markdown="##A heading\n\nThis is version 1 of the wiki page!\n"))

    wiki.title = "Title version 2"
    wiki.markdown = "##A heading\n\nThis is version 2 of the wiki page!\n"

    wiki = syn.store(wiki)

    w1 = syn.getWiki(owner=wiki.ownerId, subpageId=wiki.id, version=0)
    assert_in("version 1", w1.title)
    assert_in("version 1", w1.markdown)

    w2 = syn.getWiki(owner=wiki.ownerId, subpageId=wiki.id, version=1)
    assert_in("version 2", w2.title)
    assert_in("version 2", w2.markdown)
