
import uuid
import filecmp
import os
import tempfile
from datetime import datetime as Datetime

import pytest
from unittest.mock import patch

from synapseclient.core.upload.upload_functions import create_external_file_handle
from synapseclient import Activity, DockerRepository, File, Folder, Link, Project
from synapseclient.core.exceptions import SynapseError, SynapseHTTPError
import synapseclient.core.utils as utils


def test_Entity(syn, project, schedule_for_cleanup):
    # Update the project
    project_name = str(uuid.uuid4())
    project = Project(name=project_name)
    project = syn.store(project)
    schedule_for_cleanup(project)
    project = syn.get(project)
    assert project.name == project_name

    # Create and get a Folder
    folder = Folder('Test Folder', parent=project, description='A place to put my junk', foo=1000)
    folder = syn.store(folder)
    folder = syn.get(folder)
    assert folder.name == 'Test Folder'
    assert folder.parentId == project.id
    assert folder.description == 'A place to put my junk'
    assert folder.foo[0] == 1000

    # Update and get the Folder
    folder.pi = 3.14159265359
    folder.description = 'The rejects from the other folder'
    folder = syn.store(folder)
    folder = syn.get(folder)
    assert folder.name == 'Test Folder'
    assert folder.parentId == project.id
    assert folder.description == 'The rejects from the other folder'
    assert folder.pi[0] == 3.14159265359

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
    assert a_file.path == path

    a_file = syn.get(a_file)
    assert a_file.description ==\
        u'Description with funny characters: Déjà vu, ประเทศไทย, 中国', \
        u'description= %s' % a_file.description
    assert a_file['foo'][0] == 'An arbitrary value', u'foo= %s' % a_file['foo'][0]
    assert a_file['bar'] == [33, 44, 55]
    assert a_file['bday'][0] == Datetime(2013, 3, 15)
    assert a_file.contentType == 'text/flapdoodle', u'contentType= %s' % a_file.contentType
    assert a_file['band'][0] == u"Motörhead", u'band= %s' % a_file['band'][0]
    assert a_file['lunch'][0] == u"すし", u'lunch= %s' % a_file['lunch'][0]

    a_file = syn.get(a_file)
    assert filecmp.cmp(path, a_file.path)

    b_file = File(name="blah", parent=folder, dataFileHandleId=a_file.dataFileHandleId)
    b_file = syn.store(b_file)

    assert b_file.dataFileHandleId == a_file.dataFileHandleId
    # Update the File
    a_file.path = path
    a_file['foo'] = 'Another arbitrary chunk of text data'
    a_file['new_key'] = 'A newly created value'
    a_file = syn.store(a_file, forceVersion=False)
    assert a_file['foo'][0] == 'Another arbitrary chunk of text data'
    assert a_file['bar'] == [33, 44, 55]
    assert a_file['bday'][0] == Datetime(2013, 3, 15)
    assert a_file.new_key[0] == 'A newly created value'
    assert a_file.path == path
    assert a_file.versionNumber == 1, "unexpected version number: " + str(a_file.versionNumber)

    # Test create, store, get Links
    # If version isn't specified, targetVersionNumber should not be set
    link = Link(a_file['id'],
                parent=project)
    link = syn.store(link)
    assert link['linksTo']['targetId'] == a_file['id']
    assert link['linksTo'].get('targetVersionNumber') is None
    assert link['linksToClassName'] == a_file['concreteType']

    link = Link(a_file['id'],
                targetVersion=a_file.versionNumber,
                parent=project)
    link = syn.store(link)
    assert link['linksTo']['targetId'] == a_file['id']
    assert link['linksTo']['targetVersionNumber'] == a_file.versionNumber
    assert link['linksToClassName'] == a_file['concreteType']

    testLink = syn.get(link)
    assert testLink == link

    link = syn.get(link, followLink=True)
    assert link['foo'][0] == 'Another arbitrary chunk of text data'
    assert link['bar'] == [33, 44, 55]
    assert link['bday'][0] == Datetime(2013, 3, 15)
    assert link.new_key[0] == 'A newly created value'
    assert utils.equal_paths(link.path, path)
    assert link.versionNumber == 1, "unexpected version number: " + str(a_file.versionNumber)

    newfolder = Folder('Testing Folder', parent=project)
    newfolder = syn.store(newfolder)
    link = Link(newfolder, parent=folder.id)
    link = syn.store(link)
    assert link['linksTo']['targetId'] == newfolder.id
    assert link['linksToClassName'] == newfolder['concreteType']
    assert link['linksTo'].get('targetVersionNumber') is None

    # Upload a new File and verify
    new_path = utils.make_bogus_data_file()
    schedule_for_cleanup(new_path)
    a_file.path = new_path
    a_file = syn.store(a_file)
    a_file = syn.get(a_file)
    assert filecmp.cmp(new_path, a_file.path)
    assert a_file.versionNumber == 2

    # Make sure we can still get the older version of file
    old_random_data = syn.get(a_file.id, version=1)
    assert filecmp.cmp(old_random_data.path, path)

    tmpdir = tempfile.mkdtemp()
    schedule_for_cleanup(tmpdir)

    # test getting the file from the cache with downloadLocation parameter (SYNPY-330)
    a_file_cached = syn.get(a_file.id, downloadLocation=tmpdir)
    assert a_file_cached.path is not None
    assert os.path.basename(a_file_cached.path) == os.path.basename(a_file.path)


def test_special_characters(syn, project, schedule_for_cleanup):
    folder = syn.store(Folder(u'Special Characters Here',
                              parent=project,
                              description=u'A test for special characters such as Déjà vu, ประเทศไทย, and 中国',
                              hindi_annotation=u'बंदर बट',
                              russian_annotation=u'Обезьяна прикладом',
                              weird_german_thing=u'Völlerei lässt grüßen'))
    assert folder.name == u'Special Characters Here'
    assert folder.parentId == project.id
    assert folder.description ==\
        u'A test for special characters such as Déjà vu, ประเทศไทย, and 中国',\
        u'description= %s' % folder.description
    assert folder.weird_german_thing[0] == u'Völlerei lässt grüßen'
    assert folder.hindi_annotation[0] == u'बंदर बट'
    assert folder.russian_annotation[0] == u'Обезьяна прикладом'


def test_get_local_file(syn, project, schedule_for_cleanup):
    new_path = utils.make_bogus_data_file()
    schedule_for_cleanup(new_path)
    folder = Folder('TestFindFileFolder', parent=project, description='A place to put my junk')
    folder = syn.store(folder)

    # Get an nonexistent file in Synapse
    pytest.raises(SynapseError, syn.get, new_path)

    # Get a file really stored in Synapse
    ent_folder = syn.store(File(new_path, parent=folder))
    ent2 = syn.get(new_path)
    assert ent_folder.id == ent2.id
    assert ent_folder.versionNumber == ent2.versionNumber

    # Get a file stored in Multiple locations #should display warning
    syn.store(File(new_path, parent=project))
    syn.get(new_path)

    # Get a file stored in multiple locations with limit set
    ent = syn.get(new_path, limitSearch=folder.id)
    assert ent.id == ent_folder.id
    assert ent.versionNumber == ent_folder.versionNumber

    # Get a file that exists but such that limitSearch removes them and raises error
    pytest.raises(SynapseError, syn.get, new_path, limitSearch='syn1')


def test_store_with_flags(syn, project, schedule_for_cleanup):
    # -- CreateOrUpdate flag for Projects --
    # If we store a project with the same name, it should become an update
    projUpdate = Project(project.name)
    projUpdate.updatedThing = 'Yep, sho\'nuf it\'s updated!'
    projUpdate = syn.store(projUpdate, createOrUpdate=True)
    assert project.id == projUpdate.id
    assert projUpdate.updatedThing == ['Yep, sho\'nuf it\'s updated!']

    # Store a File
    filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(filepath)
    origBogus = File(filepath, name='Bogus Test File', parent=project)
    origBogus = syn.store(origBogus, createOrUpdate=True)
    assert origBogus.versionNumber == 1

    # Modify existing annotations by createOrUpdate
    del projUpdate['parentId']
    del projUpdate['id']
    projUpdate.updatedThing = 'Updated again'
    projUpdate.addedThing = 'Something new'
    projUpdate = syn.store(projUpdate, createOrUpdate=True)
    assert project.id == projUpdate.id
    assert projUpdate.updatedThing == ['Updated again']

    # -- ForceVersion flag --
    # Re-store the same thing and don't up the version
    mutaBogus = syn.store(origBogus, forceVersion=False)
    assert mutaBogus.versionNumber == 1

    # Re-store again, essentially the same condition
    mutaBogus = syn.store(mutaBogus, createOrUpdate=True, forceVersion=False)
    assert mutaBogus.versionNumber == 1, "expected version 1 but got version %s" % mutaBogus.versionNumber

    # And again, but up the version this time
    mutaBogus = syn.store(mutaBogus, forceVersion=True)
    assert mutaBogus.versionNumber == 2

    # Create file with different contents and store it with force version false
    # This should be ignored because contents (and md5) are different
    different_filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(different_filepath)
    mutaBogus = File(different_filepath, name='Bogus Test File',
                     parent=project)
    mutaBogus = syn.store(mutaBogus, forceVersion=False)
    assert mutaBogus.versionNumber == 3

    # -- CreateOrUpdate flag for files --
    # Store a different file with the same name and parent
    # Expected behavior is that a new version of the first File will be created
    new_filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(new_filepath)
    mutaBogus.path = new_filepath
    mutaBogus = syn.store(mutaBogus, createOrUpdate=True)
    assert mutaBogus.id == origBogus.id
    assert mutaBogus.versionNumber == 4
    assert not filecmp.cmp(mutaBogus.path, filepath)

    # Make doubly sure the File was uploaded
    checkBogus = syn.get(mutaBogus.id)
    assert checkBogus.id == origBogus.id
    assert checkBogus.versionNumber == 4
    assert filecmp.cmp(mutaBogus.path, checkBogus.path)

    # Create yet another file with the same name and parent
    # Expected behavior is raising an exception with a 409 error
    newer_filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(newer_filepath)
    badBogus = File(newer_filepath, name='Bogus Test File', parent=project)
    pytest.raises(SynapseHTTPError, syn.store, badBogus, createOrUpdate=False)

    # -- Storing after syn.get(..., downloadFile=False) --
    ephemeralBogus = syn.get(mutaBogus, downloadFile=False)
    ephemeralBogus.description = 'Snorklewacker'
    ephemeralBogus.shoe_size = 11.5
    ephemeralBogus = syn.store(ephemeralBogus)

    ephemeralBogus = syn.get(ephemeralBogus, downloadFile=False)
    assert ephemeralBogus.description == 'Snorklewacker'
    assert ephemeralBogus.shoe_size == [11.5]


def test_get_with_downloadLocation_and_ifcollision(syn, project, schedule_for_cleanup):
    # Store a File and delete it locally
    filepath = utils.make_bogus_binary_file()
    bogus = File(filepath, name='Bogus Test File', parent=project)
    bogus = syn.store(bogus)
    os.remove(filepath)

    # Compare stuff to this one
    normalBogus = syn.get(bogus)

    # Download to the temp folder, should be the same
    otherBogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath))
    assert otherBogus.id == normalBogus.id
    assert filecmp.cmp(otherBogus.path, normalBogus.path)

    # Invalidate the downloaded file's timestamps
    os.utime(otherBogus.path, (0, 0))
    badtimestamps = os.path.getmtime(otherBogus.path)

    # Download again, should change the modification time
    overwriteBogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath), ifcollision="overwrite.local")
    overwriteModTime = os.path.getmtime(overwriteBogus.path)
    assert badtimestamps != overwriteModTime

    # Download again, should not change the modification time
    otherBogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath), ifcollision="keep.local")
    assert overwriteModTime == os.path.getmtime(overwriteBogus.path)
    # "keep.local" should have made the path invalid since it is keeping a potentially modified version
    assert otherBogus.path is None
    assert otherBogus.cacheDir is None
    assert 0 == len(otherBogus.files)

    # Invalidate the timestamps again
    os.utime(overwriteBogus.path, (0, 0))
    badtimestamps = os.path.getmtime(overwriteBogus.path)

    # Download once more, but rename
    renamedBogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath), ifcollision="keep.both")
    assert overwriteBogus.path != renamedBogus.path
    assert filecmp.cmp(overwriteBogus.path, renamedBogus.path)

    # Clean up
    os.remove(overwriteBogus.path)
    os.remove(renamedBogus.path)


def test_store_activity(syn, project, schedule_for_cleanup):
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
    assert honking['name'] == 'Hinkle horn honking'
    assert len(honking['used']) == 2
    assert honking['used'][0]['concreteType'] == 'org.sagebionetworks.repo.model.provenance.UsedURL'
    assert not honking['used'][0]['wasExecuted']
    assert honking['used'][0]['url'].startswith('http://www.flickr.com/photos/bevanbfree/3482')
    assert honking['used'][1]['concreteType'] == 'org.sagebionetworks.repo.model.provenance.UsedURL'
    assert not honking['used'][1]['wasExecuted']

    # Store another Entity with the same Activity
    entity = File('http://en.wikipedia.org/wiki/File:Nettlebed_cave.jpg',
                  name='Nettlebed Cave', parent=project, synapseStore=False)
    entity = syn.store(entity, activity=honking)

    # The Activities should match
    honking2 = syn.getProvenance(entity)
    assert honking['id'] == honking2['id']


def test_store_isRestricted_flag(syn, project, schedule_for_cleanup):
    # Store a file with access requirements
    path = utils.make_bogus_binary_file()
    schedule_for_cleanup(path)
    entity = File(path, name='Secret human data', parent=project)

    # We don't want to spam ACT with test emails
    with patch('synapseclient.client.Synapse._createAccessRequirementIfNone') as intercepted:
        entity = syn.store(entity, isRestricted=True)
        assert intercepted.called


def test_ExternalFileHandle(syn, project, schedule_for_cleanup):
    # Tests shouldn't have external dependencies, but this is a pretty picture of Singapore
    singapore_url = 'http://upload.wikimedia.org/wikipedia/commons/thumb/3/3e/1_singapore_city_skyline_dusk_panorama_2011.jpg/1280px-1_singapore_city_skyline_dusk_panorama_2011.jpg'  # noqa
    singapore = File(singapore_url, parent=project, synapseStore=False)
    singapore = syn.store(singapore)

    # Verify the file handle
    fileHandle = syn._get_file_handle_as_creator(singapore.dataFileHandleId)
    assert fileHandle['concreteType'] == 'org.sagebionetworks.repo.model.file.ExternalFileHandle'
    assert fileHandle['externalURL'] == singapore_url

    # The download should occur only on the client side
    singapore = syn.get(singapore, downloadFile=True)
    assert singapore.path is not None
    assert singapore.externalURL == singapore_url
    assert os.path.exists(singapore.path)

    # Update external URL
    singapore_2_url = 'https://upload.wikimedia.org/wikipedia/commons/a/a2/Singapore_Panorama_v2.jpg'
    singapore.externalURL = singapore_2_url
    singapore = syn.store(singapore)
    s2 = syn.get(singapore, downloadFile=False)
    assert s2.externalURL == singapore_2_url


def test_synapseStore_flag(syn, project, schedule_for_cleanup):
    # Store a path to a local file
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    bogus = File(path, name='Totally bogus data', parent=project, synapseStore=False)
    bogus = syn.store(bogus)

    # Verify the thing can be downloaded as a URL
    bogus = syn.get(bogus, downloadFile=False)
    assert bogus.name == 'Totally bogus data'
    assert bogus.path == path, "Path: %s\nExpected: %s" % (bogus.path, path)
    assert not bogus.synapseStore

    # Make sure the test runs on Windows and other OS's
    if path[0].isalpha() and path[1] == ':':
        # A Windows file URL looks like this: file:///c:/foo/bar/bat.txt
        expected_url = 'file:///' + path.replace("\\", "/")
    else:
        expected_url = 'file://' + path

    assert bogus.externalURL == expected_url, 'URL: %s\nExpected %s' % (bogus.externalURL, expected_url)

    # A file path that doesn't exist should still work
    bogus = File('/path/to/local/file1.xyz', parentId=project.id, synapseStore=False)
    bogus = syn.store(bogus)
    pytest.raises(IOError, syn.get, bogus)
    assert not bogus.synapseStore

    # Try a URL
    bogus = File('http://dev-versions.synapse.sagebase.org/synapsePythonClient', parent=project, synapseStore=False)
    bogus = syn.store(bogus)
    bogus = syn.get(bogus)
    assert not bogus.synapseStore


def test_create_or_update_project(syn, project, schedule_for_cleanup):
    name = str(uuid.uuid4())

    project = Project(name, a=1, b=2)
    proj_for_cleanup = syn.store(project)
    schedule_for_cleanup(proj_for_cleanup)

    project = Project(name, b=3, c=4)
    project = syn.store(project)

    assert project.a == [1]
    assert project.b == [3]
    assert project.c == [4]

    project = syn.get(project.id)

    assert project.a == [1]
    assert project.b == [3]
    assert project.c == [4]

    project = Project(name, c=5, d=6)
    pytest.raises(Exception, syn.store, project, createOrUpdate=False)


def test_download_file_false(syn, project, schedule_for_cleanup):
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
    assert reupload.path is None, "Path field should be null: %s" % reupload.path

    # This should still get the correct file
    reupload = syn.get(reupload.id)
    assert filecmp.cmp(filepath + RENAME_SUFFIX, reupload.path)
    assert reupload.name == file.name


def test_download_file_URL_false(syn, project, schedule_for_cleanup):
    # Upload an external file handle
    fileThatExists = 'http://dev-versions.synapse.sagebase.org/synapsePythonClient'
    reupload = File(fileThatExists, synapseStore=False, parent=project)
    reupload = syn.store(reupload)
    reupload = syn.get(reupload, downloadFile=False)
    originalVersion = reupload.versionNumber

    # Reupload and check that the URL and version does not get mangled
    reupload = syn.store(reupload, forceVersion=False)
    assert reupload.path == fileThatExists, "Entity should still be pointing at a URL"
    assert originalVersion == reupload.versionNumber

    # Try a URL with an extra slash at the end
    fileThatDoesntExist = 'http://dev-versions.synapse.sagebase.org/synapsePythonClient/'
    reupload.synapseStore = False
    reupload.path = fileThatDoesntExist
    reupload = syn.store(reupload)
    reupload = syn.get(reupload, downloadFile=False)
    originalVersion = reupload.versionNumber

    reupload = syn.store(reupload, forceVersion=False)
    assert reupload.path == fileThatDoesntExist, "Entity should still be pointing at a URL"
    assert originalVersion == reupload.versionNumber


# SYNPY-366
def test_download_local_file_URL_path(syn, project, schedule_for_cleanup):
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)

    filehandle = create_external_file_handle(syn, path, mimetype=None, file_size=None)

    localFileEntity = syn.store(File(dataFileHandleId=filehandle['id'], parent=project))
    e = syn.get(localFileEntity.id)
    assert path == utils.normalize_path(e.path)


# SYNPY-424
def test_store_file_handle_update_metadata(syn, project, schedule_for_cleanup):
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
    assert new_file_handle['id'] == new_entity._file_handle['id']
    assert old_file_handle['id'] != new_entity._file_handle['id']

    # check that local_state was updated
    assert replacement_file_path == new_entity.path
    assert os.path.dirname(replacement_file_path) == new_entity.cacheDir
    assert [os.path.basename(replacement_file_path)] == new_entity.files


def test_store_DockerRepository(syn, project, schedule_for_cleanup):
    repo_name = "some/repository/path"
    docker_repo = syn.store(DockerRepository(repo_name, parent=project))
    assert isinstance(docker_repo, DockerRepository)
    assert not docker_repo.isManaged
    assert repo_name == docker_repo.repositoryName


def test_store__changing_externalURL_by_changing_path(syn, project, schedule_for_cleanup):
    url = 'https://www.synapse.org/Portal/clear.cache.gif'
    ext = syn.store(File(url, name="test", parent=project, synapseStore=False))

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

    assert ext.externalURL != url
    assert utils.normalize_path(temp_path) == utils.file_url_to_path(ext.externalURL)
    assert temp_path == utils.normalize_path(ext.path)
    assert not ext.synapseStore


def test_store__changing_from_Synapse_to_externalURL_by_changing_path(syn, project, schedule_for_cleanup):
    # create a temp file
    temp_path = utils.make_bogus_data_file()
    schedule_for_cleanup(temp_path)

    ext = syn.store(File(temp_path, parent=project, synapseStore=True))
    ext = syn.get(ext)
    assert "org.sagebionetworks.repo.model.file.S3FileHandle" == ext._file_handle.concreteType

    ext.synapseStore = False
    ext = syn.store(ext)

    # do a get to make sure filehandle has been updated correctly
    ext = syn.get(ext.id, downloadFile=True)
    assert "org.sagebionetworks.repo.model.file.ExternalFileHandle" == ext._file_handle.concreteType
    assert utils.as_url(temp_path) == ext.externalURL
    assert not ext.synapseStore

    # swap back to synapse storage
    ext.synapseStore = True
    ext = syn.store(ext)
    # do a get to make sure filehandle has been updated correctly
    ext = syn.get(ext.id, downloadFile=True)
    assert "org.sagebionetworks.repo.model.file.S3FileHandle" == ext._file_handle.concreteType
    assert ext.externalURL is None
    assert ext.synapseStore
