import os
from nose.tools import assert_raises

import synapseclient.client as client
import synapseclient.utils as utils
from synapseclient.exceptions import *
from synapseclient import Project, File, Data, Code, Wiki, Activity, Evaluation

import integration
from integration import schedule_for_cleanup


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
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
    assert wiki == wiki2

    # Retrieve the sub Wiki from Synapse
    wiki2 = syn.getWiki(project, subpageId=subwiki.id)
    assert subwiki == wiki2

    # Try making an update
    wiki['title'] = 'A New Title'
    wiki['markdown'] = wiki['markdown'] + "\nNew stuff here!!!\n"
    wiki = syn.store(wiki)
    assert wiki['title'] == 'A New Title'
    assert wiki['markdown'].endswith("\nNew stuff here!!!\n")

    # Check the Wiki's metadata
    headers = syn.getWikiHeaders(project)
    assert len(headers) == 2
    assert headers[0]['title'] in (wiki['title'], subwiki['title'])

    # # Retrieve the file attachment
    # tmpdir = tempfile.mkdtemp()
    # file_props = syn._downloadWikiAttachment(project, wiki, 
    #                         os.path.basename(filename), dest_dir=tmpdir)
    # path = file_props['path']
    # assert os.path.exists(path)
    # assert filecmp.cmp(original_path, path)

    # Clean up
    # syn._deleteFileHandle(fileHandle)
    syn.delete(wiki)
    syn.delete(subwiki)
    assert_raises(SynapseHTTPError, syn.getWiki, project)
