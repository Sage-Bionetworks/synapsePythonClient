# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import uuid
import filecmp
import os
import tempfile
from datetime import datetime as Datetime
from nose.tools import assert_raises, assert_equal, assert_is_none, assert_not_equal, assert_is_instance, assert_true,\
    assert_false, assert_equals, assert_is_not_none, assert_raises
from mock import patch

import synapseclient
from synapseclient import Activity, Project, Folder, File, Link, DockerRepository
from synapseclient.exceptions import *
from synapseclient.upload_functions import create_external_file_handle

import integration
from integration import schedule_for_cleanup, QUERY_TIMEOUT_SEC


def setup(module):
    module.syn = integration.syn
    module.project = integration.project


def test_Entity():
    # Update the project
    project_name = str(uuid.uuid4())
    project = Project(name=project_name)
    project = syn.store(project)
    schedule_for_cleanup(project)
    project = syn.getEntity(project)
    assert_equals(project.name, project_name)
    
    # Create and get a Folder
    folder = Folder('Test Folder', parent=project, description='A place to put my junk', foo=1000)
    folder = syn.createEntity(folder)
    folder = syn.getEntity(folder)
    assert_equals(folder.name, 'Test Folder')
    assert_equals(folder.parentId, project.id)
    assert_equals(folder.description, 'A place to put my junk')
    assert_equals(folder.foo[0], 1000)
    
    # Update and get the Folder
    folder.pi = 3.14159265359
    folder.description = 'The rejects from the other folder'
    folder = syn.store(folder)
    folder = syn.get(folder)
    assert_equals(folder.name, 'Test Folder')
    assert_equals(folder.parentId, project.id)
    assert_equals(folder.description, 'The rejects from the other folder')
    assert_equals(folder.pi[0], 3.14159265359)

    # Test CRUD on Files, check unicode
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    a_file = File(path, parent=folder, description=u'Description with funny characters: Déjà vu, ประเทศไทย, 中国',
                  contentType='text/flapdoodle',
                  foo='An arbitrary value',
                  bar=[33, 44, 55],
                  bday=Datetime(2013, 3, 15),
                  band=u"Motörhead",
                  lunch=u"すし")
    a_file = syn.store(a_file)
    assert_equals(a_file.path, path)

    a_file = syn.getEntity(a_file)
    assert_equals(a_file.description,
                  u'Description with funny characters: Déjà vu, ประเทศไทย, 中国', u'description= %s'
                  % a_file.description)
    assert_equals(a_file['foo'][0], 'An arbitrary value', u'foo= %s' % a_file['foo'][0])
    assert_equals(a_file['bar'], [33, 44, 55])
    assert_equals(a_file['bday'][0], Datetime(2013, 3, 15))
    assert_equals(a_file.contentType, 'text/flapdoodle', u'contentType= %s' % a_file.contentType)
    assert_equals(a_file['band'][0], u"Motörhead", u'band= %s' % a_file['band'][0])
    assert_equals(a_file['lunch'][0], u"すし", u'lunch= %s' % a_file['lunch'][0])
    
    a_file = syn.downloadEntity(a_file)
    assert_true(filecmp.cmp(path, a_file.path))

    b_file = File(name="blah", parent=folder, dataFileHandleId=a_file.dataFileHandleId)
    b_file = syn.store(b_file)

    assert_equals(b_file.dataFileHandleId, a_file.dataFileHandleId)
    # Update the File
    a_file.path = path
    a_file['foo'] = 'Another arbitrary chunk of text data'
    a_file['new_key'] = 'A newly created value'
    a_file = syn.updateEntity(a_file)
    assert_equals(a_file['foo'][0], 'Another arbitrary chunk of text data')
    assert_equals(a_file['bar'], [33, 44, 55])
    assert_equals(a_file['bday'][0], Datetime(2013, 3, 15))
    assert_equals(a_file.new_key[0], 'A newly created value')
    assert_equals(a_file.path, path)
    assert_equals(a_file.versionNumber, 1, "unexpected version number: " + str(a_file.versionNumber))

    # Test create, store, get Links
    # If version isn't specified, targetVersionNumber should not be set
    link = Link(a_file['id'], 
                parent=project)
    link = syn.store(link)
    assert_equals(link['linksTo']['targetId'], a_file['id'])
    assert_is_none(link['linksTo'].get('targetVersionNumber'))
    assert_equals(link['linksToClassName'], a_file['concreteType'])

    link = Link(a_file['id'], 
                targetVersion=a_file.versionNumber,
                parent=project)
    link = syn.store(link)
    assert_equals(link['linksTo']['targetId'], a_file['id'])
    assert_equals(link['linksTo']['targetVersionNumber'], a_file.versionNumber)
    assert_equals(link['linksToClassName'], a_file['concreteType'])
    
    testLink = syn.get(link)
    assert_equals(testLink, link)

    link = syn.get(link, followLink=True)
    assert_equals(link['foo'][0], 'Another arbitrary chunk of text data')
    assert_equals(link['bar'], [33, 44, 55])
    assert_equals(link['bday'][0], Datetime(2013, 3, 15))
    assert_equals(link.new_key[0], 'A newly created value')
    assert_true(utils.equal_paths(link.path, path))
    assert_equals(link.versionNumber, 1, "unexpected version number: " + str(a_file.versionNumber))

    newfolder = Folder('Testing Folder', parent=project)
    newfolder = syn.store(newfolder)
    link = Link(newfolder, parent=folder.id)
    link = syn.store(link)
    assert_equals(link['linksTo']['targetId'], newfolder.id)
    assert_equals(link['linksToClassName'], newfolder['concreteType'])
    assert_is_none(link['linksTo'].get('targetVersionNumber'))

    # Upload a new File and verify
    new_path = utils.make_bogus_data_file()
    schedule_for_cleanup(new_path)
    a_file = syn.uploadFile(a_file, new_path)
    a_file = syn.downloadEntity(a_file)
    assert_true(filecmp.cmp(new_path, a_file.path))
    assert_equals(a_file.versionNumber, 2)

    # Make sure we can still get the older version of file
    old_random_data = syn.get(a_file.id, version=1)
    assert_true(filecmp.cmp(old_random_data.path, path))

    tmpdir = tempfile.mkdtemp()
    schedule_for_cleanup(tmpdir)

    # test getting the file from the cache with downloadLocation parameter (SYNPY-330)
    a_file_cached = syn.get(a_file.id, downloadLocation=tmpdir)
    assert_is_not_none(a_file_cached.path)
    assert_equal(os.path.basename(a_file_cached.path), os.path.basename(a_file.path))


def test_special_characters():
    folder = syn.store(Folder(u'Special Characters Here',
                              parent=project,
                              description=u'A test for special characters such as Déjà vu, ประเทศไทย, and 中国',
                              hindi_annotation=u'बंदर बट',
                              russian_annotation=u'Обезьяна прикладом',
                              weird_german_thing=u'Völlerei lässt grüßen'))
    assert_equals(folder.name, u'Special Characters Here')
    assert_equals(folder.parentId, project.id)
    assert_equals(folder.description, u'A test for special characters such as Déjà vu, ประเทศไทย, and 中国',
                  u'description= %s' % folder.description)
    assert_equals(folder.weird_german_thing[0], u'Völlerei lässt grüßen')
    assert_equals(folder.hindi_annotation[0], u'बंदर बट')
    assert_equals(folder.russian_annotation[0], u'Обезьяна прикладом')


def test_get_local_file():
    new_path = utils.make_bogus_data_file()
    schedule_for_cleanup(new_path)
    folder = Folder('TestFindFileFolder', parent=project, description='A place to put my junk')
    folder = syn.createEntity(folder)

    # Get an nonexistent file in Synapse
    assert_raises(SynapseError, syn.get, new_path)

    # Get a file really stored in Synapse
    ent_folder = syn.store(File(new_path, parent=folder))
    ent2 = syn.get(new_path)
    assert_equals(ent_folder.id, ent2.id)
    assert_equals(ent_folder.versionNumber, ent2.versionNumber)

    # Get a file stored in Multiple locations #should display warning
    syn.store(File(new_path, parent=project))
    syn.get(new_path)

    # Get a file stored in multiple locations with limit set
    ent = syn.get(new_path, limitSearch=folder.id)
    assert_equals(ent.id, ent_folder.id)
    assert_equals(ent.versionNumber, ent_folder.versionNumber)

    # Get a file that exists but such that limitSearch removes them and raises error
    assert_raises(SynapseError, syn.get, new_path, limitSearch='syn1')


def test_store_with_flags():
    # -- CreateOrUpdate flag for Projects --
    # If we store a project with the same name, it should become an update
    projUpdate = Project(project.name)
    projUpdate.updatedThing = 'Yep, sho\'nuf it\'s updated!'
    projUpdate = syn.store(projUpdate, createOrUpdate=True)
    assert_equals(project.id, projUpdate.id)
    assert_equals(projUpdate.updatedThing, ['Yep, sho\'nuf it\'s updated!'])

    # Store a File
    filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(filepath)
    origBogus = File(filepath, name='Bogus Test File', parent=project)
    origBogus = syn.store(origBogus, createOrUpdate=True)
    assert_equals(origBogus.versionNumber, 1)

    # Modify existing annotations by createOrUpdate
    del projUpdate['parentId']
    del projUpdate['id']
    projUpdate.updatedThing = 'Updated again'
    projUpdate.addedThing = 'Something new'
    projUpdate = syn.store(projUpdate, createOrUpdate=True)
    assert_equals(project.id, projUpdate.id)
    assert_equals(projUpdate.updatedThing, ['Updated again'])
    
    # -- ForceVersion flag --
    # Re-store the same thing and don't up the version
    mutaBogus = syn.store(origBogus, forceVersion=False)
    assert_equals(mutaBogus.versionNumber, 1)
    
    # Re-store again, essentially the same condition
    mutaBogus = syn.store(mutaBogus, createOrUpdate=True, forceVersion=False)
    assert_equals(mutaBogus.versionNumber, 1, "expected version 1 but got version %s" % mutaBogus.versionNumber)
    
    # And again, but up the version this time
    mutaBogus = syn.store(mutaBogus, forceVersion=True)
    assert_equals(mutaBogus.versionNumber, 2)

    # -- CreateOrUpdate flag for files --
    # Store a different file with the same name and parent
    # Expected behavior is that a new version of the first File will be created
    new_filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(new_filepath)
    mutaBogus.path = new_filepath
    mutaBogus = syn.store(mutaBogus, createOrUpdate=True)
    assert_equals(mutaBogus.id, origBogus.id)
    assert_equals(mutaBogus.versionNumber, 3)
    assert_false(filecmp.cmp(mutaBogus.path, filepath))

    # Make doubly sure the File was uploaded
    checkBogus = syn.get(mutaBogus.id)
    assert_equals(checkBogus.id, origBogus.id)
    assert_equals(checkBogus.versionNumber, 3)
    assert_true(filecmp.cmp(mutaBogus.path, checkBogus.path))

    # Create yet another file with the same name and parent
    # Expected behavior is raising an exception with a 409 error
    newer_filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(newer_filepath)
    badBogus = File(newer_filepath, name='Bogus Test File', parent=project)
    assert_raises(SynapseHTTPError, syn.store, badBogus, createOrUpdate=False)
    
    # -- Storing after syn.get(..., downloadFile=False) --
    ephemeralBogus = syn.get(mutaBogus, downloadFile=False)
    ephemeralBogus.description = 'Snorklewacker'
    ephemeralBogus.shoe_size = 11.5
    ephemeralBogus = syn.store(ephemeralBogus)

    ephemeralBogus = syn.get(ephemeralBogus, downloadFile=False)
    assert_equals(ephemeralBogus.description, 'Snorklewacker')
    assert_equals(ephemeralBogus.shoe_size, [11.5])


def test_get_with_downloadLocation_and_ifcollision():
    # Store a File and delete it locally
    filepath = utils.make_bogus_binary_file()
    bogus = File(filepath, name='Bogus Test File', parent=project)
    bogus = syn.store(bogus)
    os.remove(filepath)

    # Compare stuff to this one
    normalBogus = syn.get(bogus)
    
    # Download to the temp folder, should be the same
    otherBogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath))
    assert_equals(otherBogus.id, normalBogus.id)
    assert_true(filecmp.cmp(otherBogus.path, normalBogus.path))
    
    # Invalidate the downloaded file's timestamps
    os.utime(otherBogus.path, (0, 0))
    badtimestamps = os.path.getmtime(otherBogus.path)
    
    # Download again, should change the modification time
    overwriteBogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath), ifcollision="overwrite.local")
    overwriteModTime = os.path.getmtime(overwriteBogus.path)
    assert_not_equal(badtimestamps, overwriteModTime)
    
    # Download again, should not change the modification time
    otherBogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath), ifcollision="keep.local")
    assert_equal(overwriteModTime, os.path.getmtime(overwriteBogus.path))
    # "keep.local" should have made the path invalid since it is keeping a potentially modified version
    assert_is_none(otherBogus.path)
    assert_is_none(otherBogus.cacheDir)
    assert_equal(0, len(otherBogus.files))

    # Invalidate the timestamps again
    os.utime(overwriteBogus.path, (0, 0))
    badtimestamps = os.path.getmtime(overwriteBogus.path)
    
    # Download once more, but rename
    renamedBogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath), ifcollision="keep.both")
    assert_not_equal(overwriteBogus.path, renamedBogus.path)
    assert_true(filecmp.cmp(overwriteBogus.path, renamedBogus.path))
    
    # Clean up
    os.remove(overwriteBogus.path)
    os.remove(renamedBogus.path)


def test_store_activity():
    # Create a File and an Activity
    path = utils.make_bogus_binary_file()
    schedule_for_cleanup(path)
    entity = File(path, name='Hinkle horn honking holes', parent=project)
    honking = Activity(name='Hinkle horn honking', 
                       description='Nettlebed Cave is a limestone cave located on the South Island of New Zealand.')
    honking.used('http://www.flickr.com/photos/bevanbfree/3482259379/')
    honking.used('http://www.flickr.com/photos/bevanbfree/3482185673/')

    # This doesn't set the ID of the Activity
    entity = syn.store(entity, activity=honking)

    # But this does
    honking = syn.getProvenance(entity.id)

    # Verify the Activity
    assert_equals(honking['name'], 'Hinkle horn honking')
    assert_equals(len(honking['used']), 2)
    assert_equals(honking['used'][0]['concreteType'], 'org.sagebionetworks.repo.model.provenance.UsedURL')
    assert_false(honking['used'][0]['wasExecuted'])
    assert_true(honking['used'][0]['url'].startswith('http://www.flickr.com/photos/bevanbfree/3482'))
    assert_equals(honking['used'][1]['concreteType'], 'org.sagebionetworks.repo.model.provenance.UsedURL')
    assert_false(honking['used'][1]['wasExecuted'])

    # Store another Entity with the same Activity
    entity = File('http://en.wikipedia.org/wiki/File:Nettlebed_cave.jpg', 
                  name='Nettlebed Cave', parent=project, synapseStore=False)
    entity = syn.store(entity, activity=honking)

    # The Activities should match
    honking2 = syn.getProvenance(entity)
    assert_equals(honking['id'], honking2['id'])


def test_store_isRestricted_flag():
    # Store a file with access requirements
    path = utils.make_bogus_binary_file()
    schedule_for_cleanup(path)
    entity = File(path, name='Secret human data', parent=project)
    
    # We don't want to spam ACT with test emails
    with patch('synapseclient.client.Synapse._createAccessRequirementIfNone') as intercepted:
        entity = syn.store(entity, isRestricted=True)
        assert_true(intercepted.called)


def test_ExternalFileHandle():
    # Tests shouldn't have external dependencies, but this is a pretty picture of Singapore
    singapore_url = 'http://upload.wikimedia.org/wikipedia/commons/thumb/3/3e/1_singapore_city_skyline_dusk_panorama_2011.jpg/1280px-1_singapore_city_skyline_dusk_panorama_2011.jpg'
    singapore = File(singapore_url, parent=project, synapseStore=False)
    singapore = syn.store(singapore)

    # Verify the file handle
    fileHandle = syn._getFileHandle(singapore.dataFileHandleId)
    assert_equals(fileHandle['concreteType'], 'org.sagebionetworks.repo.model.file.ExternalFileHandle')
    assert_equals(fileHandle['externalURL'], singapore_url)

    # The download should occur only on the client side
    singapore = syn.get(singapore, downloadFile=True)
    assert_is_not_none(singapore.path)
    assert_equals(singapore.externalURL, singapore_url)
    assert_true(os.path.exists(singapore.path))

    # Update external URL
    singapore_2_url = 'https://upload.wikimedia.org/wikipedia/commons/a/a2/Singapore_Panorama_v2.jpg'
    singapore.externalURL = singapore_2_url
    singapore = syn.store(singapore)
    s2 = syn.get(singapore, downloadFile=False)
    assert_equal(s2.externalURL, singapore_2_url)


def test_synapseStore_flag():
    # Store a path to a local file
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    bogus = File(path, name='Totally bogus data', parent=project, synapseStore=False)
    bogus = syn.store(bogus)

    # Verify the thing can be downloaded as a URL
    bogus = syn.get(bogus, downloadFile=False)
    assert_equals(bogus.name, 'Totally bogus data')
    assert_equals(bogus.path, path, "Path: %s\nExpected: %s" % (bogus.path, path))
    assert_false(bogus.synapseStore)

    # Make sure the test runs on Windows and other OS's
    if path[0].isalpha() and path[1] == ':':
        # A Windows file URL looks like this: file:///c:/foo/bar/bat.txt
        expected_url = 'file:///' + path.replace("\\", "/")
    else:
        expected_url = 'file://' + path

    assert_equals(bogus.externalURL, expected_url, 'URL: %s\nExpected %s' % (bogus.externalURL, expected_url))

    # A file path that doesn't exist should still work
    bogus = File('/path/to/local/file1.xyz', parentId=project.id, synapseStore=False)
    bogus = syn.store(bogus)
    assert_raises(IOError, syn.get, bogus)
    assert_false(bogus.synapseStore)

    # Try a URL
    bogus = File('http://dev-versions.synapse.sagebase.org/synapsePythonClient', parent=project, synapseStore=False)
    bogus = syn.store(bogus)
    bogus = syn.get(bogus)
    assert_false(bogus.synapseStore)


def test_create_or_update_project():
    name = str(uuid.uuid4())

    project = Project(name, a=1, b=2)
    proj_for_cleanup = syn.store(project)
    schedule_for_cleanup(proj_for_cleanup)

    project = Project(name, b=3, c=4)
    project = syn.store(project)

    assert_equals(project.a, [1])
    assert_equals(project.b, [3])
    assert_equals(project.c, [4])

    project = syn.get(project.id)

    assert_equals(project.a, [1])
    assert_equals(project.b, [3])
    assert_equals(project.c, [4])

    project = Project(name, c=5, d=6)
    assert_raises(Exception, syn.store, project, createOrUpdate=False)


def test_download_file_false():
    RENAME_SUFFIX = 'blah'
    
    # Upload a file
    filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(filepath)
    schedule_for_cleanup(filepath + RENAME_SUFFIX)
    file = File(filepath, name='SYNR 619', parent=project)
    file = syn.store(file)
    
    # Now hide the file from the cache and download with downloadFile=False
    os.rename(filepath, filepath + RENAME_SUFFIX)
    file = syn.get(file.id, downloadFile=False)
    
    # Change something and reupload the file's metadata
    file.name = "Only change the name, not the file"
    reupload = syn.store(file)
    assert_is_none(reupload.path, "Path field should be null: %s" % reupload.path)
    
    # This should still get the correct file
    reupload = syn.get(reupload.id)
    assert_true(filecmp.cmp(filepath + RENAME_SUFFIX, reupload.path))
    assert_equals(reupload.name, file.name)


def test_download_file_URL_false():
    # Upload an external file handle
    fileThatExists = 'http://dev-versions.synapse.sagebase.org/synapsePythonClient'
    reupload = File(fileThatExists, synapseStore=False, parent=project)
    reupload = syn.store(reupload)
    reupload = syn.get(reupload, downloadFile=False)
    originalVersion = reupload.versionNumber
    
    # Reupload and check that the URL and version does not get mangled
    reupload = syn.store(reupload, forceVersion=False)
    assert_equals(reupload.path, fileThatExists, "Entity should still be pointing at a URL")
    assert_equals(originalVersion, reupload.versionNumber)

    # Try a URL with an extra slash at the end
    fileThatDoesntExist = 'http://dev-versions.synapse.sagebase.org/synapsePythonClient/'
    reupload.synapseStore = False
    reupload.path = fileThatDoesntExist
    reupload = syn.store(reupload)
    reupload = syn.get(reupload, downloadFile=False)
    originalVersion = reupload.versionNumber
    
    reupload = syn.store(reupload, forceVersion=False)
    assert_equals(reupload.path, fileThatDoesntExist, "Entity should still be pointing at a URL")
    assert_equals(originalVersion, reupload.versionNumber)


# SYNPY-366
def test_download_local_file_URL_path():
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)

    filehandle = create_external_file_handle(syn, path, mimetype=None, file_size=None)

    localFileEntity = syn.store(File(dataFileHandleId=filehandle['id'], parent=project))
    e = syn.get(localFileEntity.id)
    assert_equal(path, e.path)


# SYNPY-424
def test_store_file_handle_update_metadata():
    original_file_path = utils.make_bogus_data_file()
    schedule_for_cleanup(original_file_path)

    # upload the project
    entity = syn.store(File(original_file_path, parent=project))
    old_file_handle = entity._file_handle

    # create file handle to replace the old one
    replacement_file_path = utils.make_bogus_data_file()
    schedule_for_cleanup(replacement_file_path)
    new_file_handle = syn.uploadFileHandle(replacement_file_path, parent=project)

    entity.dataFileHandleId = new_file_handle['id']
    new_entity = syn.store(entity)

    # make sure _file_handle info was changed
    # (_file_handle values are all changed at once so just verifying id change is sufficient)
    assert_equal(new_file_handle['id'], new_entity._file_handle['id'])
    assert_not_equal(old_file_handle['id'], new_entity._file_handle['id'])

    # check that local_state was updated
    assert_equal(replacement_file_path, new_entity.path)
    assert_equal(os.path.dirname(replacement_file_path), new_entity.cacheDir)
    assert_equal([os.path.basename(replacement_file_path)], new_entity.files)


def test_store_DockerRepository():
    repo_name = "some/repository/path"
    docker_repo = syn.store(DockerRepository(repo_name, parent=project))
    assert_is_instance(docker_repo, DockerRepository)
    assert_false(docker_repo.isManaged)
    assert_equals(repo_name, docker_repo.repositoryName)


def test_store__changing_externalURL_by_changing_path():
    url = 'https://www.synapse.org/Portal/clear.cache.gif'
    ext = syn.store(synapseclient.File(url, name="test", parent=project, synapseStore=False))

    # perform a syn.get so the filename changes
    ext = syn.get(ext)

    # create a temp file
    temp_path = utils.make_bogus_data_file()
    schedule_for_cleanup(temp_path)

    ext.synapseStore = False
    ext.path = temp_path
    ext = syn.store(ext)

    # do a get to make sure filehandle has been updated correctly
    ext = syn.get(ext.id, downloadFile=True)

    assert_not_equal(ext.externalURL, url)
    assert_equal(utils.normalize_path(temp_path), utils.file_url_to_path(ext.externalURL))
    assert_equal(temp_path, ext.path)
    assert_equal(False, ext.synapseStore)


def test_store__changing_from_Synapse_to_externalURL_by_changing_path():
    # create a temp file
    temp_path = utils.make_bogus_data_file()
    schedule_for_cleanup(temp_path)

    ext = syn.store(synapseclient.File(temp_path, parent=project, synapseStore=True))
    ext = syn.get(ext)
    assert_equal("org.sagebionetworks.repo.model.file.S3FileHandle", ext._file_handle.concreteType)

    ext.synapseStore = False
    ext = syn.store(ext)

    # do a get to make sure filehandle has been updated correctly
    ext = syn.get(ext.id, downloadFile=True)
    assert_equal("org.sagebionetworks.repo.model.file.ExternalFileHandle", ext._file_handle.concreteType)
    assert_equal(utils.as_url(temp_path), ext.externalURL)
    assert_equal(False, ext.synapseStore)

    # swap back to synapse storage
    ext.synapseStore = True
    ext = syn.store(ext)
    # do a get to make sure filehandle has been updated correctly
    ext = syn.get(ext.id, downloadFile=True)
    assert_equal("org.sagebionetworks.repo.model.file.S3FileHandle", ext._file_handle.concreteType)
    assert_equal(None, ext.externalURL)
    assert_equal(True, ext.synapseStore)



