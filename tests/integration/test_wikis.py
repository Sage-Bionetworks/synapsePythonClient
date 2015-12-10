# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

import os, uuid
from nose.tools import assert_raises, assert_equal

import synapseclient.client as client
import synapseclient.utils as utils
from synapseclient.exceptions import *
from synapseclient import Project, File, Wiki, Activity, Evaluation

import integration
from integration import schedule_for_cleanup


def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn
    module.project = integration.project


def test_wikiAttachment():
    # Upload a file to be attached to a Wiki
    filename = utils.make_bogus_data_file()
    attachname = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    schedule_for_cleanup(attachname)
    fileHandle = syn._uploadToFileHandleService(filename)

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
    ## due to the new wiki api, we'll get back some new properties,
    ## namely markdownFileHandleId and markdown_path, so only compare
    ## properties that are in the first object
    for property_name in wiki:
        assert_equal(wiki[property_name], wiki2[property_name])

    # Retrieve the sub Wiki from Synapse
    wiki2 = syn.getWiki(project, subpageId=subwiki.id)
    for property_name in wiki:
        assert_equal(subwiki[property_name], wiki2[property_name])

    # Try making an update
    wiki['title'] = 'A New Title'
    wiki['markdown'] = wiki['markdown'] + "\nNew stuff here!!!\n"
    wiki = syn.store(wiki)
    wiki = syn.getWiki(project)
    assert wiki['title'] == 'A New Title'
    assert wiki['markdown'].endswith("\nNew stuff here!!!\n")

    # Check the Wiki's metadata
    headers = syn.getWikiHeaders(project)
    assert len(headers) == 2
    assert headers[0]['title'] in (wiki['title'], subwiki['title'])

    file_handles = syn.getWikiAttachments(wiki)
    file_names = [fh['fileName'] for fh in file_handles]
    assert all( os.path.basename(fn) in file_names for fn in [filename, attachname] )

    # # Retrieve the file attachment
    # tmpdir = tempfile.mkdtemp()
    # file_props = syn._downloadWikiAttachment(project, wiki, 
    #                         os.path.basename(filename), dest_dir=tmpdir)
    # path = file_props['path']
    # assert os.path.exists(path)
    # assert filecmp.cmp(original_path, path)

    # Clean up
    # syn._deleteFileHandle(fileHandle)
    syn.delete(subwiki)
    syn.delete(wiki)
    assert_raises(SynapseHTTPError, syn.getWiki, project)


def test_create_or_update_wiki():
    # create wiki once
    wiki = syn.store(Wiki(title='This is the title', owner=project, markdown="#Wikis are OK\n\nBlabber jabber blah blah blither blather bonk!"))

    # for now, creating it again raises an exception, see SYNR-631
    assert_raises(SynapseHTTPError,
        syn.store, 
        Wiki(title='This is a different title', owner=project, markdown="#Wikis are awesome\n\nNew babble boo flabble gibber wiggle sproing!"),
        createOrUpdate=True)


def test_wiki_version():
    ## create a new project to avoid artifacts from previous tests
    project = syn.store(Project(name=str(uuid.uuid4())))
    wiki = syn.store(Wiki(title='Title version 1', owner=project, markdown="##A heading\n\nThis is version 1 of the wiki page!\n"))

    wiki.title = "Title version 2"
    wiki.markdown = "##A heading\n\nThis is version 2 of the wiki page!\n"

    wiki = syn.store(wiki)

    w1 = syn.getWiki(owner=wiki.ownerId, subpageId=wiki.id, version=0)
    assert "version 1" in w1.title
    assert "version 1" in w1.markdown

    w2 = syn.getWiki(owner=wiki.ownerId, subpageId=wiki.id, version=1)
    assert "version 2" in w2.title
    assert "version 2" in w2.markdown

