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
from synapseclient import Activity, Entity, Project, Folder, File, Link, Column, Schema, RowSet, Row
from synapseclient.exceptions import *
import synapseutils as synu

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


def test_copy():
    """Tests the copy function"""

    # Create a Project
    project_entity = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(project_entity.id)
    acl = syn.setPermissions(project_entity, other_user['principalId'], accessType=['READ', 'CREATE', 'UPDATE'])
    # Create two Folders in Project
    folder_entity = syn.store(Folder(name=str(uuid.uuid4()),
                                                   parent=project_entity))
    second_folder = syn.store(Folder(name=str(uuid.uuid4()),
                                                   parent=project_entity))
    third_folder = syn.store(Folder(name=str(uuid.uuid4()),
                                                   parent=project_entity))
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
    output = synu.copy(syn,file_entity.id,destinationId=project_entity.id)
    output_URL = synu.copy(syn,externalURL_entity.id,destinationId=project_entity.id)

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
    assert_raises(ValueError,synu.copy,syn,project_entity.id,destinationId = project_entity.id)
    assert_raises(ValueError,synu.copy,syn,file_entity.id,destinationId = project_entity.id) 
    assert_raises(ValueError,synu.copy,syn,file_entity.id,destinationId = third_folder.id,setProvenance = "gib")

    print("Test: setProvenance = None")
    output = synu.copy(syn,file_entity.id,destinationId=second_folder.id,setProvenance = None)
    assert_raises(SynapseHTTPError,syn.getProvenance,output[file_entity.id])
    schedule_for_cleanup(output[file_entity.id])

    print("Test: setProvenance = Existing")
    output_URL = synu.copy(syn,externalURL_entity.id,destinationId=second_folder.id,setProvenance = "existing")
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

        output = synu.copy(syn_other,file_entity.id,destinationId=third_folder.id)
        new_copied_ent = syn.get(output[file_entity.id])
        new_copied_ent_annot = syn.getAnnotations(new_copied_ent)
        schedule_for_cleanup(new_copied_ent.id)
        
        copied_URL_ent.externalURL = "https://www.google.com"
        copied_URL_ent = syn.store(copied_URL_ent)
        output = synu.copy(syn_other,copied_URL_ent.id,destinationId=third_folder.id,version=1)
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
    for i in copied_link:
        old = syn.get(i)
        new = syn.get(copied_link[i])
        assert old.linksTo['targetId'] == new.linksTo['targetId']
        assert old.linksTo['targetVersionNumber'] == new.linksTo['targetVersionNumber']
    schedule_for_cleanup(second_file_entity.id)
    schedule_for_cleanup(link_entity.id)
    schedule_for_cleanup(copied_link[link_entity.id])

    assert_raises(ValueError,synu.copy,syn,link_entity.id,destinationId=second_folder.id)


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

    table_map = synu.copy(syn,schema.id, destinationId=second_project.id)
    copied_table = syn.tableQuery('select * from %s' %table_map[schema.id])
    rows = copied_table.asRowSet()['rows']
    # TEST: Check if all values are the same
    for i,row in enumerate(rows):
        assert row['values'] == data[i]

    assert_raises(ValueError,synu.copy,syn,schema.id,destinationId=second_project.id)

    schedule_for_cleanup(schema.id)
    schedule_for_cleanup(table_map[schema.id])

    # ------------------------------------
    # TEST COPY FOLDER
    # ------------------------------------
    print("Test: Copy Folder")
    mapping = synu.copy(syn,folder_entity.id,destinationId=second_project.id)
    for i in mapping:
        old = syn.get(i,downloadFile=False)
        new = syn.get(mapping[i],downloadFile=False)
        assert old.name == new.name
        assert old.annotations == new.annotations
        assert old.concreteType == new.concreteType

    assert_raises(ValueError,synu.copy,syn,folder_entity.id,destinationId=second_project.id)

    # TEST: Recursive = False, only the folder is created
    second = synu.copy(syn,second_folder.id,destinationId=second_project.id,recursive=False)
    copied_folder = syn.get(second[second_folder.id])
    assert copied_folder.name == second_folder.name
    assert len(second) == 1
    # TEST: Make sure error is thrown if foldername already exists
    assert_raises(ValueError,synu.copy,syn,second_folder.id, destinationId=second_project.id)

    # ------------------------------------
    # TEST COPY PROJECT
    # ------------------------------------
    print("Test: Copy Project")
    third_project = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(third_project.id)

    mapping = synu.copy(syn,project_entity.id,destinationId=third_project.id)
    for i in mapping:
        old = syn.get(i,downloadFile=False)
        new = syn.get(mapping[i],downloadFile=False)
        if not isinstance(old, Project):
            assert old.name == new.name
        assert old.annotations == new.annotations
        assert old.concreteType == new.concreteType

    # TEST: Can't copy project to a folder
    assert_raises(ValueError,synu.copy,syn,project_entity.id,destinationId=second_folder.id)
