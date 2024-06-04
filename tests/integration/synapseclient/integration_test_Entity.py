"""Integration tests for working with Entities."""

import datetime
import filecmp
import os
import tempfile
import uuid
from datetime import datetime as Datetime
from unittest.mock import patch

import pytest
from pytest_mock import MockerFixture

import synapseclient.core.download.download_functions as download_functions
import synapseclient.core.utils as utils
from synapseclient import (
    Activity,
    DockerRepository,
    File,
    Folder,
    Link,
    Project,
    Synapse,
)
from synapseclient.core.exceptions import SynapseError, SynapseHTTPError
from synapseclient.core.upload.upload_functions import create_external_file_handle


async def test_entity(syn: Synapse, project: Project, schedule_for_cleanup) -> None:
    # Update the project
    project_name = str(uuid.uuid4())
    project = Project(name=project_name)
    project = syn.store(project)
    schedule_for_cleanup(project)
    project = syn.get(project)
    assert project.name == project_name

    # Create and get a Folder
    folder = Folder(
        "Test Folder", parent=project, description="A place to put my junk", foo=1000
    )
    folder = syn.store(folder)
    folder = syn.get(folder)
    assert folder.name == "Test Folder"
    assert folder.parentId == project.id
    assert folder.description == "A place to put my junk"
    assert folder.foo[0] == 1000

    # Update and get the Folder
    folder.pi = 3.14159265359
    folder.description = "The rejects from the other folder"
    folder = syn.store(folder)
    folder = syn.get(folder)
    assert folder.name == "Test Folder"
    assert folder.parentId == project.id
    assert folder.description == "The rejects from the other folder"
    assert folder.pi[0] == 3.14159265359

    # Test CRUD on Files, check unicode
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    a_file = File(
        path,
        parent=folder,
        description="Description with funny characters: Déjà vu, ประเทศไทย, 中国",
        contentType="text/flapdoodle",
        foo="An arbitrary value",
        bar=[33, 44, 55],
        bday=Datetime(2013, 3, 15),
        band="Motörhead",
        lunch="すし",
    )
    a_file = syn.store(a_file)
    assert a_file.path == path

    a_file = syn.get(a_file)
    assert (
        a_file.description
        == "Description with funny characters: Déjà vu, ประเทศไทย, 中国"
    ), ("description= %s" % a_file.description)
    assert a_file["foo"][0] == "An arbitrary value", "foo= %s" % a_file["foo"][0]
    assert a_file["bar"] == [33, 44, 55]
    assert a_file["bday"][0] == Datetime(2013, 3, 15, tzinfo=datetime.timezone.utc)
    assert a_file.contentType == "text/flapdoodle", (
        "contentType= %s" % a_file.contentType
    )
    assert a_file["band"][0] == "Motörhead", "band= %s" % a_file["band"][0]
    assert a_file["lunch"][0] == "すし", "lunch= %s" % a_file["lunch"][0]

    a_file = syn.get(a_file)
    assert filecmp.cmp(path, a_file.path)

    b_file = File(
        name="blah" + str(uuid.uuid4()),
        parent=folder,
        dataFileHandleId=a_file.dataFileHandleId,
    )
    b_file = syn.store(b_file)

    assert b_file.dataFileHandleId == a_file.dataFileHandleId
    # Update the File
    a_file.path = path
    a_file["foo"] = "Another arbitrary chunk of text data"
    a_file["new_key"] = "A newly created value"
    a_file = syn.store(a_file, forceVersion=False)
    assert a_file["foo"][0] == "Another arbitrary chunk of text data"
    assert a_file["bar"] == [33, 44, 55]
    assert a_file["bday"][0] == Datetime(2013, 3, 15, tzinfo=datetime.timezone.utc)
    assert a_file.new_key[0] == "A newly created value"
    assert a_file.path == path
    assert a_file.versionNumber == 1, "unexpected version number: " + str(
        a_file.versionNumber
    )

    # Test create, store, get Links
    # If version isn't specified, targetVersionNumber should not be set
    link = Link(a_file["id"], parent=project)
    link = syn.store(link)
    assert link["linksTo"]["targetId"] == a_file["id"]
    assert link["linksTo"].get("targetVersionNumber") is None
    assert link["linksToClassName"] == a_file["concreteType"]

    link = Link(a_file["id"], targetVersion=a_file.versionNumber, parent=project)
    link = syn.store(link)
    assert link["linksTo"]["targetId"] == a_file["id"]
    assert link["linksTo"]["targetVersionNumber"] == a_file.versionNumber
    assert link["linksToClassName"] == a_file["concreteType"]

    test_link = syn.get(link)
    assert test_link == link

    link = syn.get(link, followLink=True)
    assert link["foo"][0] == "Another arbitrary chunk of text data"
    assert link["bar"] == [33, 44, 55]
    assert link["bday"][0] == Datetime(2013, 3, 15, tzinfo=datetime.timezone.utc)
    assert link.new_key[0] == "A newly created value"
    assert utils.equal_paths(link.path, path)
    assert link.versionNumber == 1, "unexpected version number: " + str(
        a_file.versionNumber
    )

    newfolder = Folder("Testing Folder", parent=project)
    newfolder = syn.store(newfolder)
    link = Link(newfolder, parent=folder.id)
    link = syn.store(link)
    assert link["linksTo"]["targetId"] == newfolder.id
    assert link["linksToClassName"] == newfolder["concreteType"]
    assert link["linksTo"].get("targetVersionNumber") is None

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


async def test_special_characters(syn: Synapse, project: Project) -> None:
    folder = syn.store(
        Folder(
            "Special Characters Here",
            parent=project,
            description="A test for special characters such as Déjà vu, ประเทศไทย, and 中国",
            hindi_annotation="बंदर बट",
            russian_annotation="Обезьяна прикладом",
            weird_german_thing="Völlerei lässt grüßen",
        )
    )
    assert folder.name == "Special Characters Here"
    assert folder.parentId == project.id
    assert (
        folder.description
        == "A test for special characters such as Déjà vu, ประเทศไทย, and 中国"
    ), ("description= %s" % folder.description)
    assert folder.weird_german_thing[0] == "Völlerei lässt grüßen"
    assert folder.hindi_annotation[0] == "बंदर बट"
    assert folder.russian_annotation[0] == "Обезьяна прикладом"


async def test_get_local_file(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    new_path = utils.make_bogus_data_file()
    schedule_for_cleanup(new_path)
    folder = Folder(
        "TestFindFileFolder", parent=project, description="A place to put my junk"
    )
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
    pytest.raises(SynapseError, syn.get, new_path, limitSearch="syn1")


async def test_store_with_flags(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    # -- CreateOrUpdate flag for Projects --
    # If we store a project with the same name, it should become an update
    proj_update = Project(project.name)
    proj_update.updatedThing = "Yep, sho'nuf it's updated!"
    proj_update = syn.store(proj_update, createOrUpdate=True)
    assert project.id == proj_update.id
    assert proj_update.updatedThing == ["Yep, sho'nuf it's updated!"]

    # Store a File
    filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(filepath)
    file_name = "Bogus Test File" + str(uuid.uuid4())
    orig_bogus = File(filepath, name=file_name, parent=project)
    orig_bogus = syn.store(orig_bogus, createOrUpdate=True)
    assert orig_bogus.versionNumber == 1

    # Modify existing annotations by createOrUpdate
    del proj_update["parentId"]
    del proj_update["id"]
    proj_update.updatedThing = "Updated again"
    proj_update.addedThing = "Something new"
    proj_update = syn.store(proj_update, createOrUpdate=True)
    assert project.id == proj_update.id
    assert proj_update.updatedThing == ["Updated again"]

    # -- ForceVersion flag --
    # Re-store the same thing and don't up the version
    muta_bogus = syn.store(orig_bogus, forceVersion=False)
    assert muta_bogus.versionNumber == 1

    # Re-store again, essentially the same condition
    muta_bogus = syn.store(muta_bogus, createOrUpdate=True, forceVersion=False)
    assert muta_bogus.versionNumber == 1, (
        "expected version 1 but got version %s" % muta_bogus.versionNumber
    )

    # And again, but up the version this time
    muta_bogus = syn.store(muta_bogus, forceVersion=True)
    assert muta_bogus.versionNumber == 2

    # Create file with different contents and store it with force version false
    # This should be ignored because contents (and md5) are different
    different_filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(different_filepath)
    muta_bogus = File(different_filepath, name=file_name, parent=project)
    muta_bogus = syn.store(muta_bogus, forceVersion=False)
    assert muta_bogus.versionNumber == 3

    # -- CreateOrUpdate flag for files --
    # Store a different file with the same name and parent
    # Expected behavior is that a new version of the first File will be created
    new_filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(new_filepath)
    muta_bogus.path = new_filepath
    muta_bogus = syn.store(muta_bogus, createOrUpdate=True)
    assert muta_bogus.id == orig_bogus.id
    assert muta_bogus.versionNumber == 4
    assert not filecmp.cmp(muta_bogus.path, filepath)

    # Make doubly sure the File was uploaded
    check_bogus = syn.get(muta_bogus.id)
    assert check_bogus.id == orig_bogus.id
    assert check_bogus.versionNumber == 4
    assert filecmp.cmp(muta_bogus.path, check_bogus.path)

    # Create yet another file with the same name and parent
    # Expected behavior is raising an exception with a 409 error
    newer_filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(newer_filepath)
    bad_bogus = File(newer_filepath, name=file_name, parent=project)
    pytest.raises(SynapseHTTPError, syn.store, bad_bogus, createOrUpdate=False)

    # -- Storing after syn.get(..., downloadFile=False) --
    ephemeral_bogus = syn.get(muta_bogus, downloadFile=False)
    ephemeral_bogus.description = "Snorklewacker"
    ephemeral_bogus.shoe_size = 11.5
    ephemeral_bogus = syn.store(ephemeral_bogus)

    ephemeral_bogus = syn.get(ephemeral_bogus, downloadFile=False)
    assert ephemeral_bogus.description == "Snorklewacker"
    assert ephemeral_bogus.shoe_size == [11.5]


async def test_get_with_downloadLocation_and_ifcollision(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    # Store a File and delete it locally
    filepath = utils.make_bogus_binary_file()
    bogus = File(filepath, name="Bogus Test File" + str(uuid.uuid4()), parent=project)
    bogus = syn.store(bogus)
    schedule_for_cleanup(bogus)
    os.remove(filepath)

    # Compare stuff to this one
    normal_bogus = syn.get(bogus)

    # Download to the temp folder, should be the same
    other_bogus = syn.get(bogus, downloadLocation=os.path.dirname(filepath))
    assert other_bogus.id == normal_bogus.id
    assert filecmp.cmp(other_bogus.path, normal_bogus.path)

    # Invalidate the downloaded file's timestamps
    os.utime(other_bogus.path, (0, 0))
    badtimestamps = os.path.getmtime(other_bogus.path)

    # Download again, should change the modification time
    overwrite_bogus = syn.get(
        bogus, downloadLocation=os.path.dirname(filepath), ifcollision="overwrite.local"
    )
    overwrite_mod_time = os.path.getmtime(overwrite_bogus.path)
    assert badtimestamps != overwrite_mod_time

    # Download again, should not change the modification time
    other_bogus = syn.get(
        bogus, downloadLocation=os.path.dirname(filepath), ifcollision="keep.local"
    )
    assert overwrite_mod_time == os.path.getmtime(overwrite_bogus.path)
    # "keep.local" should have made the path invalid since it is keeping a potentially modified version
    assert other_bogus.path is None
    assert other_bogus.cacheDir is None
    assert 0 == len(other_bogus.files)

    # Invalidate the timestamps again
    os.utime(overwrite_bogus.path, (0, 0))
    badtimestamps = os.path.getmtime(overwrite_bogus.path)

    # Download once more, but rename
    renamed_bogus = syn.get(
        bogus, downloadLocation=os.path.dirname(filepath), ifcollision="keep.both"
    )
    assert overwrite_bogus.path != renamed_bogus.path
    assert filecmp.cmp(overwrite_bogus.path, renamed_bogus.path)

    # Clean up
    os.remove(overwrite_bogus.path)
    os.remove(renamed_bogus.path)


async def test_get_with_cache_hit_and_miss_with_ifcollision(
    syn: Synapse, project: Project, schedule_for_cleanup, mocker: MockerFixture
) -> None:
    download_file_function = mocker.spy(download_functions, "download_by_file_handle")
    # GIVEN a File that is stored in Synapse - and removed from the local machine
    filepath = utils.make_bogus_binary_file()
    original_file_md5 = utils.md5_for_file(filepath).hexdigest()
    bogus_file = File(
        path=filepath,
        name="a_name_that_will_show_up_in_cache" + str(uuid.uuid4()),
        parent=project,
    )
    bogus_file = syn.store(bogus_file)
    schedule_for_cleanup(bogus_file)
    os.remove(filepath)

    # WHEN I get the File from synapse
    unmodified_file_from_server = syn.get(
        entity=bogus_file,
        downloadLocation=os.path.dirname(filepath),
        ifcollision="overwrite.local",
    )

    # THEN I expect the file to have been downloaded and match the original files MD5
    assert download_file_function.call_count == 1
    assert unmodified_file_from_server._file_handle["contentMd5"] == original_file_md5
    assert utils.md5_for_file(filename=filepath).hexdigest() == original_file_md5

    # AND I expect the file to be in the cache and match the original files MD5
    copy_of_file_from_server = syn.get(
        entity=bogus_file,
        downloadLocation=os.path.dirname(filepath),
        ifcollision="overwrite.local",
    )
    assert copy_of_file_from_server.id == unmodified_file_from_server.id
    assert filecmp.cmp(copy_of_file_from_server.path, unmodified_file_from_server.path)
    assert download_file_function.call_count == 1
    assert copy_of_file_from_server._file_handle["contentMd5"] == original_file_md5
    assert utils.md5_for_file(filepath).hexdigest() == original_file_md5

    # GIVEN another bogus file with the same name and non matching MD5 data
    new_bogus_file = utils.make_bogus_binary_file()
    os.remove(filepath)
    os.rename(new_bogus_file, filepath)
    assert utils.md5_for_file(filepath).hexdigest() != original_file_md5

    # WHEN I get the File from synapse
    copy_of_file_from_server = syn.get(
        entity=bogus_file,
        downloadLocation=os.path.dirname(filepath),
        ifcollision="overwrite.local",
    )

    # THEN I expect the file to have been downloaded and overwrite the file on my machine with the same name
    assert copy_of_file_from_server._file_handle["contentMd5"] == original_file_md5
    assert utils.md5_for_file(filepath).hexdigest() == original_file_md5
    assert download_file_function.call_count == 2

    os.remove(filepath)


async def test_store_activity(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    # Create a File and an Activity
    path = utils.make_bogus_binary_file()
    schedule_for_cleanup(path)
    entity = File(
        path, name="Hinkle horn honking holes" + str(uuid.uuid4()), parent=project
    )
    activity_name = "Hinkle horn honking" + str(uuid.uuid4())
    honking = Activity(
        name=activity_name,
        description="Nettlebed Cave is a limestone cave located on the South Island of New Zealand.",
    )
    honking.used("http://www.flickr.com/photos/bevanbfree/3482259379/")
    honking.used("http://www.flickr.com/photos/bevanbfree/3482185673/")

    # This doesn't set the ID of the Activity
    entity = syn.store(entity, activity=honking)

    # But this does
    honking = syn.getProvenance(entity.id)

    # Verify the Activity
    assert honking["name"] == activity_name
    assert len(honking["used"]) == 2
    assert (
        honking["used"][0]["concreteType"]
        == "org.sagebionetworks.repo.model.provenance.UsedURL"
    )
    assert not honking["used"][0]["wasExecuted"]
    assert honking["used"][0]["url"].startswith(
        "http://www.flickr.com/photos/bevanbfree/3482"
    )
    assert (
        honking["used"][1]["concreteType"]
        == "org.sagebionetworks.repo.model.provenance.UsedURL"
    )
    assert not honking["used"][1]["wasExecuted"]

    # Store another Entity with the same Activity
    entity = File(
        "http://en.wikipedia.org/wiki/File:Nettlebed_cave.jpg",
        name="Nettlebed Cave" + str(uuid.uuid4()),
        parent=project,
        synapseStore=False,
    )
    entity = syn.store(entity, activity=honking)

    # The Activities should match
    honking2 = syn.getProvenance(entity)
    assert honking["id"] == honking2["id"]


async def test_store_isRestricted_flag(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    # Store a file with access requirements
    path = utils.make_bogus_binary_file()
    schedule_for_cleanup(path)
    entity = File(path, name="Secret human data" + str(uuid.uuid4()), parent=project)

    # We don't want to spam ACT with test emails
    with patch(
        "synapseclient.client.Synapse._createAccessRequirementIfNone"
    ) as intercepted:
        entity = syn.store(entity, isRestricted=True)
        assert intercepted.called


async def test_ExternalFileHandle(syn: Synapse, project: Project) -> None:
    # Tests shouldn't have external dependencies, but this is a pretty picture of Singapore
    singapore_url = "http://upload.wikimedia.org/wikipedia/commons/thumb/3/3e/1_singapore_city_skyline_dusk_panorama_2011.jpg/1280px-1_singapore_city_skyline_dusk_panorama_2011.jpg"  # noqa
    singapore = File(singapore_url, parent=project, synapseStore=False)
    singapore = syn.store(singapore)

    # Verify the file handle
    file_handle = syn._get_file_handle_as_creator(singapore.dataFileHandleId)
    assert (
        file_handle["concreteType"]
        == "org.sagebionetworks.repo.model.file.ExternalFileHandle"
    )
    assert file_handle["externalURL"] == singapore_url
    file_handle = singapore["_file_handle"]
    assert file_handle["contentMd5"] is None
    assert (
        file_handle["concreteType"]
        == "org.sagebionetworks.repo.model.file.ExternalFileHandle"
    )

    # The download should occur only on the client side
    singapore = syn.get(singapore, downloadFile=True)
    assert singapore.path is not None
    assert singapore.externalURL == singapore_url
    assert os.path.exists(singapore.path)

    # Update external URL
    singapore_2_url = (
        "https://upload.wikimedia.org/wikipedia/commons/a/a2/Singapore_Panorama_v2.jpg"
    )
    singapore.externalURL = singapore_2_url
    singapore = syn.store(singapore)
    s2 = syn.get(singapore, downloadFile=False)
    assert s2.externalURL == singapore_2_url


async def test_synapseStore_flag(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    # Store a path to a local file
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    file_name = "Totally bogus data" + str(uuid.uuid4())
    bogus = File(
        path,
        name=file_name,
        parent=project,
        synapseStore=False,
    )
    bogus = syn.store(bogus)

    # Verify the thing can be downloaded as a URL
    bogus = syn.get(bogus, downloadFile=False)
    assert bogus.name == file_name
    assert bogus.path == path, "Path: %s\nExpected: %s" % (bogus.path, path)
    assert not bogus.synapseStore
    file_handle = bogus["_file_handle"]
    assert file_handle["contentMd5"] is not None
    assert (
        file_handle["concreteType"]
        == "org.sagebionetworks.repo.model.file.ExternalFileHandle"
    )

    # Make sure the test runs on Windows and other OS's
    if path[0].isalpha() and path[1] == ":":
        # A Windows file URL looks like this: file:///c:/foo/bar/bat.txt
        expected_url = "file:///" + path.replace("\\", "/")
    else:
        expected_url = "file://" + path

    assert bogus.externalURL == expected_url, "URL: %s\nExpected %s" % (
        bogus.externalURL,
        expected_url,
    )

    # A file path that doesn't exist should still work
    bogus = File("/path/to/local/file1.xyz", parentId=project.id, synapseStore=False)
    bogus = syn.store(bogus)
    pytest.raises(IOError, syn.get, bogus)
    assert not bogus.synapseStore

    file_handle = bogus["_file_handle"]
    assert file_handle["contentMd5"] is None
    assert (
        file_handle["concreteType"]
        == "org.sagebionetworks.repo.model.file.ExternalFileHandle"
    )

    # Try a URL
    bogus = File(
        "http://dev-versions.synapse.sagebase.org/synapsePythonClient",
        parent=project,
        synapseStore=False,
    )
    bogus = syn.store(bogus)
    bogus = syn.get(bogus)
    assert not bogus.synapseStore

    file_handle = bogus["_file_handle"]
    assert file_handle["contentMd5"] is None
    assert (
        file_handle["concreteType"]
        == "org.sagebionetworks.repo.model.file.ExternalFileHandle"
    )


async def test_create_or_update_project(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
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


async def test_download_file_false(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    rename_suffix = "blah" + str(uuid.uuid4())

    # Upload a file
    filepath = utils.make_bogus_binary_file()
    schedule_for_cleanup(filepath)
    schedule_for_cleanup(filepath + rename_suffix)
    file_name = "SYNR-619-" + str(uuid.uuid4())
    file = File(filepath, name=file_name, parent=project)
    file = syn.store(file)

    # Now hide the file from the cache and download with downloadFile=False
    os.rename(filepath, filepath + rename_suffix)
    file = syn.get(file.id, downloadFile=False)

    # Change something and reupload the file's metadata
    file.name = "Only change the name, not the file" + str(uuid.uuid4())
    reupload = syn.store(file)
    assert reupload.path is None, "Path field should be null: %s" % reupload.path

    # This should still get the correct file
    reupload = syn.get(reupload.id)
    assert filecmp.cmp(filepath + rename_suffix, reupload.path)
    assert reupload.name == file.name


async def test_download_file_URL_false(syn: Synapse, project: Project) -> None:
    # Upload an external file handle
    file_that_exists = "http://dev-versions.synapse.sagebase.org/synapsePythonClient"
    reupload = File(file_that_exists, synapseStore=False, parent=project)
    reupload = syn.store(reupload)
    reupload = syn.get(reupload, downloadFile=False)
    original_version = reupload.versionNumber

    # Reupload and check that the URL and version does not get mangled
    reupload = syn.store(reupload, forceVersion=False)
    assert reupload.path == file_that_exists, "Entity should still be pointing at a URL"
    assert original_version == reupload.versionNumber

    # Try a URL with an extra slash at the end
    file_that_doesnt_exist = (
        "http://dev-versions.synapse.sagebase.org/synapsePythonClient/"
    )
    reupload.synapseStore = False
    reupload.path = file_that_doesnt_exist
    reupload = syn.store(reupload)
    reupload = syn.get(reupload, downloadFile=False)
    original_version = reupload.versionNumber

    reupload = syn.store(reupload, forceVersion=False)
    assert (
        reupload.path == file_that_doesnt_exist
    ), "Entity should still be pointing at a URL"
    assert original_version == reupload.versionNumber


# SYNPY-366
async def test_download_local_file_URL_path(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)

    filehandle = create_external_file_handle(syn, path, mimetype=None, file_size=None)

    localFileEntity = syn.store(File(dataFileHandleId=filehandle["id"], parent=project))
    e = syn.get(localFileEntity.id)
    assert path == utils.normalize_path(e.path)


# SYNPY-424
async def test_store_file_handle_update_metadata(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    original_file_path = utils.make_bogus_data_file()
    schedule_for_cleanup(original_file_path)

    # upload the project
    entity = syn.store(File(original_file_path, parent=project))
    old_file_handle = entity._file_handle

    # create file handle to replace the old one
    replacement_file_path = utils.make_bogus_data_file()
    schedule_for_cleanup(replacement_file_path)
    new_file_handle = syn.uploadFileHandle(replacement_file_path, parent=project)

    entity.dataFileHandleId = new_file_handle["id"]
    new_entity = syn.store(entity)

    # make sure _file_handle info was changed
    # (_file_handle values are all changed at once so just verifying id change is sufficient)
    assert new_file_handle["id"] == new_entity._file_handle["id"]
    assert old_file_handle["id"] != new_entity._file_handle["id"]

    # check that local_state was updated
    assert replacement_file_path == new_entity.path
    assert os.path.dirname(replacement_file_path) == new_entity.cacheDir
    assert [os.path.basename(replacement_file_path)] == new_entity.files


async def test_store_DockerRepository(syn: Synapse, project: Project) -> None:
    repo_name = "some/repository/path"
    docker_repo = syn.store(DockerRepository(repo_name, parent=project))
    assert isinstance(docker_repo, DockerRepository)
    assert not docker_repo.isManaged
    assert repo_name == docker_repo.repositoryName


@pytest.mark.flaky(reruns=3, only_rerun=["SynapseHTTPError"])
async def test_store__changing_externalURL_by_changing_path(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    url = "https://www.synapse.org/Portal/clear.cache.gif"
    ext = syn.store(
        File(url, name="test" + str(uuid.uuid4()), parent=project, synapseStore=False)
    )

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


@pytest.mark.flaky(reruns=3, only_rerun=["SynapseHTTPError"])
async def test_store__changing_from_Synapse_to_externalURL_by_changing_path(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    # create a temp file
    temp_path = utils.make_bogus_data_file()
    schedule_for_cleanup(temp_path)

    ext = syn.store(File(temp_path, parent=project, synapseStore=True))
    ext = syn.get(ext)
    assert (
        "org.sagebionetworks.repo.model.file.S3FileHandle"
        == ext._file_handle.concreteType
    )

    ext.synapseStore = False
    ext = syn.store(ext)

    # do a get to make sure filehandle has been updated correctly
    ext = syn.get(ext.id, downloadFile=True)
    assert (
        "org.sagebionetworks.repo.model.file.ExternalFileHandle"
        == ext._file_handle.concreteType
    )
    assert utils.as_url(temp_path) == ext.externalURL
    assert not ext.synapseStore

    # swap back to synapse storage
    ext.synapseStore = True
    ext = syn.store(ext)
    # do a get to make sure filehandle has been updated correctly
    ext = syn.get(ext.id, downloadFile=True)
    assert (
        "org.sagebionetworks.repo.model.file.S3FileHandle"
        == ext._file_handle.concreteType
    )
    assert ext.externalURL is None
    assert ext.synapseStore
