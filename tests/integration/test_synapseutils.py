# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

import uuid, filecmp, os, sys, requests, tempfile, time
from datetime import datetime as Datetime
from nose.tools import assert_raises
from nose.plugins.attrib import attr
from mock import patch
try:
    import configparser
except ImportError:
    import ConfigParser as configparser

import synapseclient
import synapseclient.client as client
import synapseclient.utils as utils
from synapseclient import Activity, Entity, Wiki, Project, Folder, File, Link, Column, Schema, RowSet, Row
from synapseclient.exceptions import *
import synapseutils
import re
import integration
from integration import schedule_for_cleanup

def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn
    module.project = integration.project

    # Some of these tests require a second user
    config = configparser.ConfigParser()
    config.read(synapseclient.client.CONFIG_FILE)
    module.other_user = {}
    try:
        other_user['username'] = config.get('test-authentication', 'username')
        other_user['password'] = config.get('test-authentication', 'password')
        other_user['principalId'] = config.get('test-authentication', 'principalId')
    except configparser.Error:
        print("[test-authentication] section missing from the configuration file")

    if 'principalId' not in other_user:
        # Fall back on the synapse-test user
        other_user['principalId'] = 1560252
        other_user['username'] = 'synapse-test'

###Add Test for UPDATE
###Add test for existing provenance but the orig doesn't have provenance
def test_copy():
    """Tests the copy function"""

    # Create a Project
    project_entity = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(project_entity.id)
    acl = syn.setPermissions(project_entity, other_user['principalId'], accessType=['READ', 'CREATE', 'UPDATE'])
    # Create two Folders in Project
    folder_entity = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    second_folder = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    third_folder = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    schedule_for_cleanup(folder_entity.id)
    schedule_for_cleanup(second_folder.id)
    schedule_for_cleanup(third_folder.id)

    # Annotations and provenance
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    annots = {'test':['hello_world']}
    prov = Activity(name = "test",used = repo_url)
    # Create, upload, and set annotations/provenance on a file in Folder
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    file_entity = syn.store(File(filename, parent=folder_entity))
    externalURL_entity = syn.store(File(repo_url,name='rand',parent=folder_entity,synapseStore=False))
    syn.setAnnotations(file_entity,annots)
    syn.setAnnotations(externalURL_entity,annots)
    syn.setProvenance(externalURL_entity.id, prov)
    schedule_for_cleanup(file_entity.id)
    schedule_for_cleanup(externalURL_entity.id)
    # ------------------------------------
    # TEST COPY FILE
    # ------------------------------------
    output = synapseutils.copy(syn,file_entity.id,destinationId=project_entity.id)
    output_URL = synapseutils.copy(syn,externalURL_entity.id,destinationId=project_entity.id)

    #Verify that our copied files are identical
    copied_ent = syn.get(output[file_entity.id])
    copied_URL_ent = syn.get(output_URL[externalURL_entity.id],downloadFile=False)

    copied_ent_annot = syn.getAnnotations(copied_ent)
    copied_url_annot = syn.getAnnotations(copied_URL_ent)
    copied_prov = syn.getProvenance(copied_ent)
    copied_url_prov = syn.getProvenance(copied_URL_ent)
    schedule_for_cleanup(copied_ent.id)
    schedule_for_cleanup(copied_URL_ent.id)

    # TEST: set_Provenance = Traceback
    print("Test: setProvenance = Traceback")
    assert copied_prov['used'][0]['reference']['targetId'] == file_entity.id
    assert copied_url_prov['used'][0]['reference']['targetId'] == externalURL_entity.id

    # TEST: Make sure copied files are the same
    assert copied_ent_annot == annots
    assert copied_ent.dataFileHandleId == file_entity.dataFileHandleId

    # TEST: Make sure copied URLs are the same
    assert copied_url_annot == annots
    assert copied_URL_ent.externalURL == repo_url
    assert copied_URL_ent.name == 'rand'
    assert copied_URL_ent.dataFileHandleId == externalURL_entity.dataFileHandleId

    # TEST: Throw error if file is copied to a folder/project that has a file with the same filename
    assert_raises(ValueError,synapseutils.copy,syn,project_entity.id,destinationId = project_entity.id)
    assert_raises(ValueError,synapseutils.copy,syn,file_entity.id,destinationId = project_entity.id)
    assert_raises(ValueError,synapseutils.copy,syn,file_entity.id,destinationId = third_folder.id,setProvenance = "gib")

    print("Test: setProvenance = None")
    output = synapseutils.copy(syn,file_entity.id,destinationId=second_folder.id,setProvenance = None)
    assert_raises(SynapseHTTPError,syn.getProvenance,output[file_entity.id])
    schedule_for_cleanup(output[file_entity.id])

    print("Test: setProvenance = Existing")
    output_URL = synapseutils.copy(syn,externalURL_entity.id,destinationId=second_folder.id,setProvenance = "existing")
    output_prov = syn.getProvenance(output_URL[externalURL_entity.id])
    schedule_for_cleanup(output_URL[externalURL_entity.id])
    assert output_prov['name'] == prov['name']
    assert output_prov['used'] == prov['used']

    if 'username' not in other_user or 'password' not in other_user:
        sys.stderr.write('\nWarning: no test-authentication configured. skipping testing copy function when trying to copy file made by another user.\n')
        return

    try:
        print("Test: Other user copy should result in different data file handle")
        syn_other = synapseclient.Synapse(skip_checks=True)
        syn_other.login(other_user['username'], other_user['password'])

        output = synapseutils.copy(syn_other,file_entity.id,destinationId=third_folder.id)
        new_copied_ent = syn.get(output[file_entity.id])
        new_copied_ent_annot = syn.getAnnotations(new_copied_ent)
        schedule_for_cleanup(new_copied_ent.id)
        
        copied_URL_ent.externalURL = "https://www.google.com"
        copied_URL_ent = syn.store(copied_URL_ent)
        output = synapseutils.copy(syn_other,copied_URL_ent.id,destinationId=third_folder.id,version=1)
        new_copied_URL = syn.get(output[copied_URL_ent.id],downloadFile=False)
        schedule_for_cleanup(new_copied_URL.id)

        assert new_copied_ent_annot == annots
        assert new_copied_ent.dataFileHandleId != copied_ent.dataFileHandleId
        #Test if copying different versions gets you the correct file
        assert new_copied_URL.versionNumber == 1
        assert new_copied_URL.externalURL == repo_url
        assert new_copied_URL.dataFileHandleId != copied_URL_ent.dataFileHandleId
    finally:
        syn_other.logout()
    # ------------------------------------
    # TEST COPY LINKS
    # ------------------------------------
    print("Test: Copy Links")
    second_file = utils.make_bogus_data_file()
    #schedule_for_cleanup(filename)
    second_file_entity = syn.store(File(second_file, parent=project_entity))
    link_entity = Link(second_file_entity.id,parent=folder_entity.id)
    link_entity = syn.store(link_entity)

    copied_link = synu.copy(syn,link_entity.id, destinationId=second_folder.id)
    old = syn.get(link_entity.id,followLink=False)
    new = syn.get(copied_link[link_entity.id],followLink=False)
    assert old.linksTo['targetId'] == new.linksTo['targetId']
    assert old.linksTo['targetVersionNumber'] == new.linksTo['targetVersionNumber']

    schedule_for_cleanup(second_file_entity.id)
    schedule_for_cleanup(link_entity.id)
    schedule_for_cleanup(copied_link[link_entity.id])

    assert_raises(ValueError,synapseutils.copy,syn,link_entity.id,destinationId=second_folder.id)


    # ------------------------------------
    # TEST COPY TABLE
    # ------------------------------------
    second_project = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(second_project.id)
    print("Test: Copy Tables")
    cols = [Column(name='n', columnType='DOUBLE', maximumSize=50),
            Column(name='c', columnType='STRING', maximumSize=50),
            Column(name='i', columnType='INTEGER')]
    data = [[2.1,'foo',10],
            [2.2,'bar',20],
            [2.3,'baz',30]]

    schema = syn.store(Schema(name='Testing', columns=cols, parent=project_entity.id))
    row_reference_set = syn.store(RowSet(columns=cols, schema=schema, rows=[Row(r) for r in data]))

    table_map = synapseutils.copy(syn,schema.id, destinationId=second_project.id)
    copied_table = syn.tableQuery('select * from %s' %table_map[schema.id])
    rows = copied_table.asRowSet()['rows']
    # TEST: Check if all values are the same
    for i,row in enumerate(rows):
        assert row['values'] == data[i]

    assert_raises(ValueError,synapseutils.copy,syn,schema.id,destinationId=second_project.id)

    schedule_for_cleanup(schema.id)
    schedule_for_cleanup(table_map[schema.id])

    # ------------------------------------
    # TEST COPY FOLDER
    # ------------------------------------
    print("Test: Copy Folder")
    mapping = synapseutils.copy(syn,folder_entity.id,destinationId=second_project.id)
    for i in mapping:
        old = syn.get(i,downloadFile=False)
        new = syn.get(mapping[i],downloadFile=False)
        assert old.name == new.name
        assert old.annotations == new.annotations
        assert old.concreteType == new.concreteType


    assert_raises(ValueError,synu.copy,syn,folder_entity.id,destinationId=second_project.id)
    # TEST: Throw error if excludeTypes isn't in file, link and table or isn't a list
    assert_raises(ValueError,synu.copy,syn,second_folder.id,excludeTypes=["foo"])
    assert_raises(ValueError,synu.copy,syn,second_folder.id,excludeTypes="file")
    # TEST: excludeType = ["file"], only the folder is created
    second = synu.copy(syn,second_folder.id,destinationId=second_project.id,excludeTypes=["file","table","link"])

    copied_folder = syn.get(second[second_folder.id])
    assert copied_folder.name == second_folder.name
    assert len(second) == 1
    # TEST: Make sure error is thrown if foldername already exists
    assert_raises(ValueError,synapseutils.copy,syn,second_folder.id, destinationId=second_project.id)

    # ------------------------------------
    # TEST COPY PROJECT
    # ------------------------------------
    print("Test: Copy Project")
    third_project = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(third_project.id)

    mapping = synapseutils.copy(syn,project_entity.id,destinationId=third_project.id)
    for i in mapping:
        old = syn.get(i,downloadFile=False)
        new = syn.get(mapping[i],downloadFile=False)
        if not isinstance(old, Project):
            assert old.name == new.name
        assert old.annotations == new.annotations
        assert old.concreteType == new.concreteType

    # TEST: Can't copy project to a folder
    assert_raises(ValueError,synapseutils.copy,syn,project_entity.id,destinationId=second_folder.id)


def test_copyWiki():
    # Create a Project
    project_entity = syn.store(Project(name=str(uuid.uuid4())))

    schedule_for_cleanup(project_entity.id)

    folder_entity = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    schedule_for_cleanup(folder_entity.id)
    second_folder = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    schedule_for_cleanup(second_folder.id)
    third_folder = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    schedule_for_cleanup(third_folder.id)

    filename = utils.make_bogus_data_file()
    attachname = utils.make_bogus_data_file()

    schedule_for_cleanup(filename)
    file_entity = syn.store(File(filename, parent=folder_entity))
    nested_folder = syn.store(Folder(name=str(uuid.uuid4()), parent=folder_entity))
    second_file = syn.store(File(filename, parent=nested_folder))

    schedule_for_cleanup(file_entity.id)
    schedule_for_cleanup(nested_folder.id)
    schedule_for_cleanup(second_file.id)

    fileWiki = Wiki(owner=second_file, title='A Test Wiki', markdown="Test")
    fileWiki = syn.store(fileWiki)

    
    #Create mock wiki
    md = """
    This is a test wiki
    =======================

    Blabber jabber blah blah boo.
    %s
    %s
    """ %(file_entity.id,second_file.id)

    wiki = Wiki(owner=project_entity, title='A Test Wiki', markdown=md, 
                attachments=[attachname])
    wiki = syn.store(wiki)

    # Create a Wiki sub-page
    subwiki = Wiki(owner=project_entity, title='A sub-wiki', 
                   markdown='%s' % file_entity.id, parentWikiId=wiki.id)
    subwiki = syn.store(subwiki)

    second_md = """
    Testing internal links
    ======================

    [test](#!Synapse:%s/wiki/%s)

    %s)
    """ % (project_entity.id,subwiki.id, second_file.id)

    sub_subwiki = Wiki(owner=project_entity, title='A sub-sub-wiki', 
                   markdown=second_md, parentWikiId=subwiki.id,
                   attachments=[attachname])
    sub_subwiki = syn.store(sub_subwiki)

    #Copy wiki to second project
    second_project = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(second_project.id)

    fileMapping = synapseutils.copy(syn, project_entity, second_project.id, copyWikiPage=False)
    
    print("Test: copyWikiPage = False")
    assert_raises(SynapseHTTPError,syn.getWiki,second_project.id)

    first_headers = syn.getWikiHeaders(project_entity)
    second_headers = synapseutils.copyWiki(syn, project_entity.id, second_project.id, entityMap=fileMapping)

    mapping = dict()

    print("Test: Check that all wikis were copied correctly with the correct mapping")
    for index,info in enumerate(second_headers):
        mapping[first_headers[index]['id']] = info['id']
        assert first_headers[index]['title'] == info['title']
        if info.get('parentId',None) is not None:
            #Check if parent Ids are mapping correctly in the copied Wikis
            assert info['parentId'] == mapping[first_headers[index]['parentId']]

    print("Test: Check that all wikis have the correct attachments and have correct internal synapse link/file mapping")
    for index,info in enumerate(second_headers):
        #Check if markdown is the correctly mapped
        orig_wikiPage= syn.getWiki(project_entity, first_headers[index]['id'])
        new_wikiPage = syn.getWiki(second_project, info['id'])
        s = orig_wikiPage.markdown
        for oldWikiId in mapping.keys():
            oldProjectAndWikiId = "%s/wiki/%s" % (project_entity.id, oldWikiId)
            newProjectAndWikiId = "%s/wiki/%s" % (second_project.id, mapping[oldWikiId])
            s=re.sub(oldProjectAndWikiId, newProjectAndWikiId, s)
        for oldFileId in fileMapping.keys():
            s = re.sub(oldFileId, fileMapping[oldFileId], s)
        assert s == new_wikiPage.markdown
        orig_attach = syn.getWikiAttachments(orig_wikiPage)
        new_attach = syn.getWikiAttachments(new_wikiPage)
        #check that attachment file names are the same
        assert orig_attach == new_attach

    print("Test: copyWikiPage = True (Default) (Should copy all wikis including wikis on files)")
    third_project = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(third_project.id)

    copiedFile = synapseutils.copy(syn, second_file, third_project.id)
    copiedWiki = syn.getWiki(copiedFile[second_file.id])
    assert copiedWiki.title == fileWiki.title
    assert copiedWiki.markdown == fileWiki.markdown

    print("Test: entitySubPageId")
    third_header = synapseutils.copyWiki(syn, project_entity.id, third_project.id, entitySubPageId=sub_subwiki.id, destinationSubPageId=None, updateLinks=False, updateSynIds=False,entityMap=fileMapping)
    test_ent_subpage = syn.getWiki(third_project.id,third_header[0]['id'])

    print("Test: No internal links updated")
    assert test_ent_subpage.markdown == sub_subwiki.markdown
    assert test_ent_subpage.title == sub_subwiki.title

    print("Test: destinationSubPageId")
    fourth_header = synu.copyWiki(syn, project_entity.id, third_project.id, entitySubPageId=subwiki.id, destinationSubPageId=test_ent_subpage.id, updateLinks=False, updateSynIds=False,entityMap=fileMapping)
    temp = syn.getWiki(third_project.id, fourth_header[0]['id'])
    #There are issues where some title pages are blank.  This is an issue that needs to be addressed
    #assert temp.title == subwiki.title

    assert temp.markdown == subwiki.markdown

    temp = syn.getWiki(third_project.id, fourth_header[1]['id'])
    assert temp.title == sub_subwiki.title
    assert temp.markdown == sub_subwiki.markdown
    assert fourth_header[0] == third_header[0]

def test_walk():
    walked = []
    firstfile = utils.make_bogus_data_file()
    schedule_for_cleanup(firstfile)
    project_entity = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(project_entity.id)
    folder_entity = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    schedule_for_cleanup(folder_entity.id)
    second_folder = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    schedule_for_cleanup(second_folder.id)
    file_entity = syn.store(File(firstfile, parent=project_entity))
    schedule_for_cleanup(file_entity.id)

    walked.append(((project_entity.name,project_entity.id),
                   [(folder_entity.name, folder_entity.id),
                    (second_folder.name, second_folder.id)],
                   [(file_entity.name, file_entity.id)]))

    nested_folder = syn.store(Folder(name=str(uuid.uuid4()), parent=folder_entity))
    schedule_for_cleanup(nested_folder.id)
    secondfile = utils.make_bogus_data_file()
    schedule_for_cleanup(secondfile)
    second_file = syn.store(File(secondfile, parent=nested_folder))
    schedule_for_cleanup(second_file.id)
    thirdfile = utils.make_bogus_data_file()
    schedule_for_cleanup(thirdfile)
    third_file = syn.store(File(thirdfile, parent=second_folder))
    schedule_for_cleanup(third_file.id)


    walked.append(((os.path.join(project_entity.name,folder_entity.name),folder_entity.id),
                   [(nested_folder.name,nested_folder.id)],
                   []))
    walked.append(((os.path.join(os.path.join(project_entity.name,folder_entity.name),nested_folder.name),nested_folder.id),
                   [],
                   [(second_file.name,second_file.id)]))
    walked.append(((os.path.join(project_entity.name,second_folder.name),second_folder.id),
                   [],
                   [(third_file.name,third_file.id)]))


    temp = synapseutils.walk(syn, project_entity.id)
    temp = list(temp)
    #Must sort the tuples returned, because order matters for the assert
    #Folders are returned in a different ordering depending on the name
    for i in walked:
        for x in i:
            if type(x) == list:
                x = x.sort()
    for i in temp:
        assert i in walked

    print("CHECK: synapseutils.walk on a file should return empty generator")
    temp = synapseutils.walk(syn, second_file.id)
    assert list(temp) == []


def test_syncFromSynapse():
    """This function tests recursive download as defined in syncFromSynapse
    most of the functionality of this function are already tested in the 
    tests/integration/test_command_line_client::test_command_get_recursive_and_query

    which means that the only test if for path=None
    """
    # Create a Project
    project_entity = syn.store(synapseclient.Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(project_entity.id)

    # Create a Folder in Project
    folder_entity = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))

    # Create and upload two files in Folder
    uploaded_paths = []
    for i in range(2):
        f  = utils.make_bogus_data_file()
        uploaded_paths.append(f)
        schedule_for_cleanup(f)
        file_entity = syn.store(File(f, parent=folder_entity))
    #Add a file in the project level as well
    f  = utils.make_bogus_data_file()
    uploaded_paths.append(f)
    schedule_for_cleanup(f)
    file_entity = syn.store(File(f, parent=project_entity))

    ### Test recursive get
    output = synapseutils.syncFromSynapse(syn, project_entity)

    assert len(output) == len(uploaded_paths)
    for f in output:
        print(f.path)
        assert f.path in uploaded_paths
