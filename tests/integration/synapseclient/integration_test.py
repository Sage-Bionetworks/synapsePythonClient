import configparser
import filecmp
import json
import os
import shutil
import tempfile
import uuid
from datetime import datetime
from typing import Callable
from unittest.mock import patch

import pytest

import synapseclient.core.utils as utils
from synapseclient import (
    Activity,
    Annotations,
    Entity,
    File,
    Folder,
    Project,
    Synapse,
    Team,
    UserProfile,
    client,
)
from synapseclient.core.exceptions import SynapseHTTPError, SynapseNoCredentialsError
from synapseclient.core.version_check import version_check

PUBLIC = 273949  # PrincipalId of public "user"
AUTHENTICATED_USERS = 273948

FILE_NAME_PREFIX = "fooUploadFileEntity"
FILE_DESCRIPTION = "A test file entity"
NEW_DESCRIPTION = "new description"


async def test_login(syn):
    try:
        config = configparser.RawConfigParser()
        config.read(client.CONFIG_FILE)

        # Added a section in the synapse config
        username = config.get("authentication", "username")
        authtoken = config.get("authentication", "authtoken")

        syn.logout()
        assert syn.credentials == None

        # login with config file no username
        syn.login(silent=True)
        assert syn.credentials.username == username

        # Login with ID only from config file
        syn.login(username, silent=True)
        assert syn.credentials.username == username

        # Login with auth token
        syn.login(authToken=authtoken, silent=True)
        assert syn.credentials.username == username

        # Login with ID not matching username
        pytest.raises(SynapseNoCredentialsError, syn.login, "fakeusername")

    except configparser.Error:
        raise ValueError(
            "Please supply a username and authToken in the configuration file."
        )

    finally:
        # Login with config file
        syn.login(silent=True)


async def testCustomConfigFile(schedule_for_cleanup):
    if os.path.isfile(client.CONFIG_FILE):
        configPath = "./CONFIGFILE"
        shutil.copyfile(client.CONFIG_FILE, configPath)
        schedule_for_cleanup(configPath)

        syn2 = Synapse(configPath=configPath, cache_client=False)
        syn2.login()
    else:
        raise ValueError(
            "Please supply a username and authToken in the configuration file."
        )


async def test_entity_version(syn, project, schedule_for_cleanup):
    # Make an Entity and make sure the version is one
    entity = File(parent=project["id"])
    entity["path"] = utils.make_bogus_data_file()
    schedule_for_cleanup(entity["path"])
    entity = syn.store(entity)

    syn.set_annotations(Annotations(entity, entity.etag, {"fizzbuzz": 111222}))
    entity = syn.get(entity)
    assert entity.versionNumber == 1

    # Update the Entity and make sure the version is incremented
    entity.foo = 998877
    entity["name"] = "foobarbat"
    entity["description"] = "This is a test entity..."
    entity = syn.store(entity, forceVersion=True, versionLabel="Prada remix")
    assert entity.versionNumber == 2

    # Get the older data and verify the random stuff is still there
    annotations = syn.get_annotations(entity, version=1)
    assert annotations["fizzbuzz"][0] == 111222
    returnEntity = syn.get(entity, version=1)
    assert returnEntity.versionNumber == 1
    assert returnEntity["fizzbuzz"][0] == 111222
    assert "foo" not in returnEntity

    # Try the newer Entity
    returnEntity = syn.get(entity)
    assert returnEntity.versionNumber == 2
    assert returnEntity["foo"][0] == 998877
    assert returnEntity["name"] == "foobarbat"
    assert returnEntity["description"] == "This is a test entity..."
    assert returnEntity["versionLabel"] == "Prada remix"

    # Try retrieving the entity with synid.version notation
    entity_string = f"{entity['id']}.{entity['versionNumber']}"
    returnEntity_using_string = syn.get(entity_string)
    assert returnEntity == returnEntity_using_string

    # Try the older Entity again
    returnEntity = syn.get(entity, version=1)
    assert returnEntity.versionNumber == 1
    assert returnEntity["fizzbuzz"][0] == 111222
    assert "foo" not in returnEntity

    # Delete version 2
    syn.delete(entity, version=2)
    # Let's retrieve the entity again using its synId this time,
    # since the old entity object had versionNumber = 2
    entity_id = entity.id
    returnEntity = syn.get(entity_id)
    assert returnEntity.versionNumber == 1


async def test_md5_query(syn, project, schedule_for_cleanup):
    # Add the same Entity several times
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    repeated = File(
        path, parent=project["id"], description="Same data over and over again"
    )

    # Retrieve the data via MD5
    num = 5
    stored = []
    for i in range(num):
        repeated.name = "Repeated data %d.dat" % i
        stored.append(syn.store(repeated).id)

    # Although we expect num results, it is possible for the MD5 to be non-unique
    results = syn.md5Query(utils.md5_for_file(path).hexdigest())
    assert str(sorted(stored)) == str(sorted([res["id"] for res in results]))
    assert len(results) == num


async def test_uploadFile_given_dictionary(syn, project, schedule_for_cleanup):
    # Make a Folder Entity the old fashioned way
    folder = {
        "concreteType": Folder._synapse_entity_type,
        "parentId": project["id"],
        "name": "fooDictionary",
        "foo": 334455,
    }
    entity = syn.store(folder)

    # Download and verify that it is the same file
    entity = syn.get(entity)
    assert entity.parentId == project.id
    assert entity.foo[0] == 334455

    # Update via a dictionary
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    rareCase = {}
    rareCase.update(entity.annotations)
    rareCase.update(entity.properties)
    rareCase.update(entity.local_state())
    rareCase["description"] = "Updating with a plain dictionary should be rare."

    # Verify it works
    entity = syn.store(rareCase)
    assert entity.description == rareCase["description"]
    assert entity.name == "fooDictionary"
    syn.get(entity["id"])


async def test_upload_file_with_force_version_false(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    # GIVEN A bogus file to upload to synapse
    path = utils.make_bogus_uuid_file()
    schedule_for_cleanup(path)
    entity = File(
        name=FILE_NAME_PREFIX + str(uuid.uuid4()),
        path=path,
        parentId=project["id"],
        description=FILE_DESCRIPTION,
    )

    # AND I store the file in Synapse
    entity = syn.store(entity)
    assert entity["versionNumber"] == 1

    # AND I remove the file from the local cache map
    syn.cache.remove(entity["dataFileHandleId"])

    with patch(
        "synapseclient.client.upload_file_handle_async"
    ) as mocked_file_handle_upload:
        # WHEN I store the file again with forceVersion=False
        entity = syn.store(entity, forceVersion=False)

    # THEN I expect the version to remain the same
    assert entity["versionNumber"] == 1

    # AND I expect the file to not be uploaded again
    assert not mocked_file_handle_upload.called


async def test_upload_file_changed_with_force_version_false(
    syn: Synapse, project: Project, schedule_for_cleanup
) -> None:
    # GIVEN A bogus file to upload to synapse
    path = utils.make_bogus_uuid_file()
    schedule_for_cleanup(path)
    entity = File(
        name=FILE_NAME_PREFIX + str(uuid.uuid4()),
        path=path,
        parentId=project["id"],
        description=FILE_DESCRIPTION,
    )

    # AND I store the file in Synapse
    entity = syn.store(entity)
    assert entity["versionNumber"] == 1
    before_file_handle_id = entity["dataFileHandleId"]

    # AND I remove the file from the local cache map
    syn.cache.remove(entity["dataFileHandleId"])

    # AND I update the content of the file
    with open(entity.path, "w") as f:
        f.write("I changed the content of the file")

    # WHEN I store the file again with forceVersion=False
    entity = syn.store(entity, forceVersion=False)

    # THEN I expect the version to have been updated
    assert entity["versionNumber"] == 2

    # AND I expect the filehandle to have been updated
    assert before_file_handle_id != entity["dataFileHandleId"]


async def test_uploadFileEntity(syn, project, schedule_for_cleanup):
    # Create a FileEntity
    # Dictionaries default to FileEntity as a type
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)
    entity = File(
        name=FILE_NAME_PREFIX + str(uuid.uuid4()),
        path=fname,
        parentId=project["id"],
        description=FILE_DESCRIPTION,
    )
    entity = syn.store(entity)

    # Download and verify
    entity = syn.get(entity)

    assert entity["files"][0] == os.path.basename(fname)
    assert filecmp.cmp(fname, entity["path"])

    # Check if we upload the wrong type of file handle
    fh = syn.restGET("/entity/%s/filehandles" % entity.id)["list"][0]
    assert fh["concreteType"] == "org.sagebionetworks.repo.model.file.S3FileHandle"

    # Create a different temporary file
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)

    # Update existing FileEntity
    entity.path = fname
    entity = syn.store(entity)

    # Download and verify that it is the same file
    entity = syn.get(entity)
    assert entity["files"][0] == os.path.basename(fname)
    assert filecmp.cmp(fname, entity["path"])


async def test_download_multithreaded(syn, project, schedule_for_cleanup):
    # Create a FileEntity
    # Dictionaries default to FileEntity as a type
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)
    entity = File(
        name="testMultiThreadDownload" + str(uuid.uuid4()),
        path=fname,
        parentId=project["id"],
    )
    entity = syn.store(entity)

    # Download and verify
    syn.multi_threaded = True
    entity = syn.get(entity)

    assert entity["files"][0] == os.path.basename(fname)
    assert filecmp.cmp(fname, entity["path"])
    syn.multi_threaded = False


async def test_downloadFile(schedule_for_cleanup):
    # See if the a "wget" works
    filename = utils.download_file(
        "http://dev-versions.synapse.sagebase.org/sage_bionetworks_logo_274x128.png"
    )
    schedule_for_cleanup(filename)
    assert os.path.exists(filename)


async def test_provenance(syn, project, schedule_for_cleanup):
    # Create a File Entity
    fname = utils.make_bogus_data_file()
    schedule_for_cleanup(fname)
    data_entity = syn.store(File(fname, parent=project["id"]))

    # Create a File Entity of Code
    fd, path = tempfile.mkstemp(suffix=".py")
    with os.fdopen(fd, "w") as f:
        f.write(
            utils.normalize_lines(
                """
            ## Chris's fabulous random data generator
            ############################################################
            import random
            random.seed(12345)
            data = [random.gauss(mu=0.0, sigma=1.0) for i in range(100)]
            """
            )
        )
    schedule_for_cleanup(path)
    code_entity = syn.store(File(path, parent=project["id"]))

    # Create a new Activity asserting that the Code Entity was 'used'
    activity = Activity(name="random.gauss", description="Generate some random numbers")
    activity.used(code_entity, wasExecuted=True)
    activity.used(
        {"name": "Superhack", "url": "https://github.com/joe_coder/Superhack"},
        wasExecuted=True,
    )
    activity = syn.setProvenance(data_entity, activity)

    # Retrieve and verify the saved Provenance record
    retrieved_activity = syn.getProvenance(data_entity)
    assert retrieved_activity == activity

    # Test Activity update
    new_description = "Generate random numbers like a gangsta"
    retrieved_activity["description"] = new_description
    updated_activity = syn.updateActivity(retrieved_activity)
    assert updated_activity["name"] == retrieved_activity["name"]
    assert updated_activity["description"] == new_description

    # Test delete
    syn.deleteProvenance(data_entity)
    pytest.raises(SynapseHTTPError, syn.getProvenance, data_entity["id"])


async def test_annotations(syn, project, schedule_for_cleanup):
    # Get the annotations of an Entity
    entity = syn.store(Folder(parent=project["id"]))
    anno = syn.get_annotations(entity)
    assert hasattr(anno, "id")
    assert hasattr(anno, "etag")
    assert anno.id == entity.id
    assert anno.etag == entity.etag

    # Set the annotations, with keywords too
    anno["bogosity"] = "total"
    syn.set_annotations(
        Annotations(
            entity,
            entity.etag,
            anno,
            wazoo="Frank",
            label="Barking Pumpkin",
            shark=16776960,
        )
    )

    # Check the update
    annote = syn.get_annotations(entity)
    assert annote["bogosity"] == ["total"]
    assert annote["wazoo"] == ["Frank"]
    assert annote["label"] == ["Barking Pumpkin"]
    assert annote["shark"] == [16776960]

    # More annotation setting
    annote["primes"] = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
    annote["phat_numbers"] = [1234.5678, 8888.3333, 1212.3434, 6677.8899]
    annote["goobers"] = ["chris", "jen", "jane"]
    annote["present_time"] = datetime.now()
    annote["maybe"] = [True, False]
    syn.set_annotations(annote)

    # Check it again
    annotation = syn.get_annotations(entity)
    assert annotation["primes"] == [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
    assert annotation["phat_numbers"] == [1234.5678, 8888.3333, 1212.3434, 6677.8899]
    assert annotation["goobers"] == ["chris", "jen", "jane"]
    assert annotation["present_time"][0].strftime("%Y-%m-%d %H:%M:%S") == annote[
        "present_time"
    ].strftime("%Y-%m-%d %H:%M:%S")
    assert annotation["maybe"] == [True, False]


async def test_annotations_on_file_during_create_no_annotations(
    syn: Synapse, project: Project, schedule_for_cleanup
):
    # GIVEN a bogus file
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    file = File(path, parent=project["id"], description="test_annotations_on_file")

    # AND the file is stored
    with patch.object(syn, "set_annotations") as mock_set_annotations:
        entity = syn.store(file)
    schedule_for_cleanup(entity.id)

    # WHEN I get the annotations
    annotations = syn.get_annotations(entity)

    # THEN I expect the annotations to be empty
    assert hasattr(annotations, "id")
    assert hasattr(annotations, "etag")
    assert annotations.id == entity.id
    assert annotations.etag == entity.etag
    assert len(annotations) == 0

    # AND set_annotations has not been called
    assert not mock_set_annotations.called


async def test_annotations_on_file_during_create_with_annotations(
    syn: Synapse, project: Project, schedule_for_cleanup
):
    # GIVEN a bogus file
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)

    # AND annotations have been set on the file
    file = File(
        path,
        parent=project["id"],
        description="test_annotations_on_file",
        annotations={"label_1": "value_1", "label_2": "value_2"},
    )

    # AND the file is stored
    entity = syn.store(file)
    schedule_for_cleanup(entity.id)

    # WHEN I get the annotations
    annotations = syn.get_annotations(entity)

    # THEN I expect the annotations to have been set
    assert annotations["label_1"] == ["value_1"]
    assert annotations["label_2"] == ["value_2"]
    assert len(annotations) == 2


async def test_get_user_profile(syn):
    p1 = syn.getUserProfile()

    # get by name
    p2 = syn.getUserProfile(p1.userName)
    assert p2.userName == p1.userName

    # get by user ID
    p2 = syn.getUserProfile(p1.ownerId)
    assert p2.userName == p1.userName


async def test_findEntityIdByNameAndParent(syn, schedule_for_cleanup):
    project_name = str(uuid.uuid1())
    project_id = syn.store(Project(name=project_name))["id"]
    assert project_id == syn.findEntityId(project_name)
    schedule_for_cleanup(project_id)


async def test_getChildren(syn, schedule_for_cleanup):
    # setup a hierarchy for folders
    # PROJECT
    # |     \
    # File   Folder
    #           |
    #         File
    project_name = str(uuid.uuid1())
    test_project = syn.store(Project(name=project_name))
    folder = syn.store(Folder(name="firstFolder", parent=test_project))
    syn.store(
        File(
            path="~/doesntMatter.txt",
            name="file inside folders",
            parent=folder,
            synapseStore=False,
        )
    )
    project_file = syn.store(
        File(
            path="~/doesntMatterAgain.txt",
            name="file inside project",
            parent=test_project,
            synapseStore=False,
        )
    )
    schedule_for_cleanup(test_project)

    expected_id_set = {project_file.id, folder.id}
    children_id_set = {x["id"] for x in syn.getChildren(test_project.id)}
    assert expected_id_set == children_id_set


def testSetStorageLocation(syn, schedule_for_cleanup):
    proj = syn.store(
        Project(
            name=str(uuid.uuid4()) + "testSetStorageLocation__existing_storage_location"
        )
    )
    schedule_for_cleanup(proj)

    endpoint = "https://url.doesnt.matter.com"
    bucket = "fake-bucket-name"
    storage_location = syn.createStorageLocationSetting(
        "ExternalObjectStorage", endpointUrl=endpoint, bucket=bucket
    )
    storage_setting = syn.setStorageLocation(
        proj, storage_location["storageLocationId"]
    )
    retrieved_setting = syn.getProjectSetting(proj, "upload")
    assert storage_setting == retrieved_setting


def testMoveProject(syn, schedule_for_cleanup):
    proj1 = syn.store(Project(name=str(uuid.uuid4()) + "testMoveProject-child"))
    proj2 = syn.store(Project(name=str(uuid.uuid4()) + "testMoveProject-newParent"))
    pytest.raises(SynapseHTTPError, syn.move, proj1, proj2)
    schedule_for_cleanup(proj1)
    schedule_for_cleanup(proj2)


class TestPermissionsOnProject:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_get_acl_default(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_default_permissions: Entity = self.syn.store(
            Project(name=str(uuid.uuid4()) + "test_get_acl_default_permissions")
        )
        self.schedule_for_cleanup(project_with_default_permissions)

        # AND the user that created the project
        p1: UserProfile = self.syn.getUserProfile()

        # WHEN I get the permissions for the user on the entity
        permissions = self.syn.get_acl(project_with_default_permissions.id, p1.ownerId)

        # THEN I expect to see the default admin permissions
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_permissions) == set(permissions)

    async def test_get_acl_read_only_permissions_on_entity(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_read_only_permissions: Entity = self.syn.store(
            Project(name=str(uuid.uuid4()) + "test_get_acl_read_permissions_on_project")
        )
        self.schedule_for_cleanup(project_with_read_only_permissions)

        # AND the user that created the project
        p1: UserProfile = self.syn.getUserProfile()

        # AND the permissions for the user on the entity are set to READ only
        self.syn.setPermissions(
            project_with_read_only_permissions, p1.ownerId, ["READ"]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = self.syn.get_acl(
            project_with_read_only_permissions.id, p1.ownerId
        )

        # THEN I expect to see read only permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

    async def test_get_acl_through_team_assigned_to_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_through_single_team: Entity = self.syn.store(
            Project(
                name=str(uuid.uuid4())
                + "test_get_acl_through_team_assigned_to_user_and_project"
            )
        )

        # AND the user that created the project
        p1: UserProfile = self.syn.getUserProfile()

        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team)
        self.schedule_for_cleanup(project_with_permissions_through_single_team)

        # AND the permissions for the Team on the entity are set to all permissions except for DOWNLOAD
        self.syn.setPermissions(
            project_with_permissions_through_single_team,
            team.id,
            [
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
        )

        # AND the permissions for the user on the entity are set to NONE
        self.syn.setPermissions(
            project_with_permissions_through_single_team, p1.ownerId, []
        )

        # WHEN I get the permissions for the user on the entity
        permissions = self.syn.get_acl(
            project_with_permissions_through_single_team.id, p1.ownerId
        )

        # THEN I expect to see the permissions of the team
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
        ]
        assert set(expected_permissions) == set(permissions)

    async def test_get_acl_through_multiple_teams_assigned_to_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_through_multiple_teams: Entity = self.syn.store(
            Project(
                name=str(uuid.uuid4())
                + "test_get_acl_through_two_teams_assigned_to_user_and_project"
            )
        )

        # AND the user that created the project
        p1: UserProfile = self.syn.getUserProfile()

        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_1 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 1",
            )
        )

        # AND a second team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_2 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 2",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order
        # can matter when there are dependent resources
        self.schedule_for_cleanup(team_1)
        self.schedule_for_cleanup(team_2)
        self.schedule_for_cleanup(project_with_permissions_through_multiple_teams)

        # AND the permissions for the Team 1 on the entity are set to all permissions except for DOWNLOAD
        self.syn.setPermissions(
            project_with_permissions_through_multiple_teams,
            team_1.id,
            [
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
        )

        # AND the permissions for the Team 2 on the entity are set to only READ and DOWNLOAD
        self.syn.setPermissions(
            project_with_permissions_through_multiple_teams,
            team_2.id,
            ["READ", "DOWNLOAD"],
        )

        # AND the permissions for the user on the entity are set to NONE
        self.syn.setPermissions(
            project_with_permissions_through_multiple_teams, p1.ownerId, []
        )

        # WHEN I get the permissions for the user on the entity
        permissions = self.syn.get_acl(
            project_with_permissions_through_multiple_teams.id, p1.ownerId
        )

        # THEN I expect to see the permissions of both teams
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_permissions) == set(permissions)

    async def test_get_acl_for_project_with_public_and_registered_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_for_public_and_authenticated_users: Entity = (
            self.syn.store(
                Project(
                    name=str(uuid.uuid4())
                    + "test_get_acl_for_project_with_registered_user"
                )
            )
        )
        self.schedule_for_cleanup(
            project_with_permissions_for_public_and_authenticated_users
        )

        # AND the user that created the project
        p1: UserProfile = self.syn.getUserProfile()

        # AND the permissions for PUBLIC are set to 'READ'
        self.syn.setPermissions(
            project_with_permissions_for_public_and_authenticated_users,
            PUBLIC,
            ["READ"],
        )

        # AND the permissions for AUTHENTICATED_USERS is set to 'READ, DOWNLOAD'
        self.syn.setPermissions(
            project_with_permissions_for_public_and_authenticated_users,
            AUTHENTICATED_USERS,
            ["READ", "DOWNLOAD"],
        )

        # AND the permissions for the user on the entity do NOT include DOWNLOAD
        self.syn.setPermissions(
            project_with_permissions_for_public_and_authenticated_users,
            p1.ownerId,
            [
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
        )

        # WHEN I get the permissions for a public user on the entity
        permissions = self.syn.get_acl(
            project_with_permissions_for_public_and_authenticated_users.id
        )

        # THEN I expect to the public permissions
        expected_permissions = ["READ"]
        assert set(expected_permissions) == set(permissions)

        # and WHEN I get the permissions for an authenticated user on the entity
        permissions = self.syn.getPermissions(
            project_with_permissions_for_public_and_authenticated_users.id, p1.ownerId
        )

        # THEN I expect to see the permissions of the user, and the authenticated user, and the public user
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_permissions) == set(permissions)


class TestPermissionsOnEntityForCaller:
    """
    Test the permissions a caller has for an entity
    """

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_get_permissions_default(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_default_permissions: Entity = self.syn.store(
            Project(name=str(uuid.uuid4()) + "test_get_permissions_default_permissions")
        )
        self.schedule_for_cleanup(project_with_default_permissions)

        # WHEN I get the permissions for the user on the entity
        permissions = self.syn.get_permissions(project_with_default_permissions.id)

        # THEN I expect to see the default admin permissions
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_permissions) == set(permissions.access_types)

    async def test_get_permissions_read_only_permissions_on_entity(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_read_only_permissions: Entity = self.syn.store(
            Project(
                name=str(uuid.uuid4())
                + "test_get_permissions_read_permissions_on_project"
            )
        )
        self.schedule_for_cleanup(project_with_read_only_permissions)

        # AND the user that created the project
        p1: UserProfile = self.syn.getUserProfile()

        # AND the permissions for the user on the entity are set to READ only
        self.syn.setPermissions(
            project_with_read_only_permissions, p1.ownerId, ["READ"]
        )

        # WHEN I get the permissions for the user on the entity
        permissions = self.syn.get_permissions(project_with_read_only_permissions.id)

        # THEN I expect to see read only permissions. CHANGE_SETTINGS is bound to ownerId.
        # Since the entity is created by the Caller, the CHANGE_SETTINGS will always be True.
        expected_permissions = ["READ", "CHANGE_SETTINGS"]

        assert set(expected_permissions) == set(permissions.access_types)

    async def test_get_permissions_through_team_assigned_to_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_through_single_team: Entity = self.syn.store(
            Project(
                name=str(uuid.uuid4())
                + "test_get_permissions_through_team_assigned_to_user_and_project"
            )
        )

        # AND the user that created the project
        p1: UserProfile = self.syn.getUserProfile()

        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order can matter when there are dependent resources
        self.schedule_for_cleanup(team)
        self.schedule_for_cleanup(project_with_permissions_through_single_team)

        # AND the permissions for the Team on the entity are set to all permissions except for DOWNLOAD
        self.syn.setPermissions(
            project_with_permissions_through_single_team,
            team.id,
            [
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
        )

        # AND the permissions for the user on the entity are set to NONE
        self.syn.setPermissions(
            project_with_permissions_through_single_team, p1.ownerId, []
        )

        # WHEN I get the permissions for the user on the entity
        permissions = self.syn.get_permissions(
            project_with_permissions_through_single_team.id
        )

        # THEN I expect to see the permissions of the team
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
        ]
        assert set(expected_permissions) == set(permissions.access_types)

    async def test_get_permissions_through_multiple_teams_assigned_to_user(
        self,
    ) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_through_multiple_teams: Entity = self.syn.store(
            Project(
                name=str(uuid.uuid4())
                + "test_get_permissions_through_two_teams_assigned_to_user_and_project"
            )
        )

        # AND the user that created the project
        p1: UserProfile = self.syn.getUserProfile()

        # AND a team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_1 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 1",
            )
        )

        # AND a second team is created
        name = "My Uniquely Named Team " + str(uuid.uuid4())
        team_2 = self.syn.store(
            Team(
                name=name,
                description="A fake team for testing permissions assigned to a single project - 2",
            )
        )

        # Handle Cleanup - Note: When running this schedule for cleanup order can matter when there are dependent resources
        self.schedule_for_cleanup(team_1)
        self.schedule_for_cleanup(team_2)
        self.schedule_for_cleanup(project_with_permissions_through_multiple_teams)

        # AND the permissions for the Team 1 on the entity are set to all permissions except for DOWNLOAD
        self.syn.setPermissions(
            project_with_permissions_through_multiple_teams,
            team_1.id,
            [
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
        )

        # AND the permissions for the Team 2 on the entity are set to only READ and DOWNLOAD
        self.syn.setPermissions(
            project_with_permissions_through_multiple_teams,
            team_2.id,
            ["READ", "DOWNLOAD"],
        )

        # AND the permissions for the user on the entity are set to NONE
        self.syn.setPermissions(
            project_with_permissions_through_multiple_teams, p1.ownerId, []
        )

        # WHEN I get the permissions for the user on the entity
        permissions = self.syn.get_permissions(
            project_with_permissions_through_multiple_teams.id
        )

        # THEN I expect to see the permissions of both teams
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_permissions) == set(permissions.access_types)

    async def test_get_permissions_for_project_with_registered_user(self) -> None:
        # GIVEN a project created with default permissions of administrator
        project_with_permissions_for_authenticated_users: Entity = self.syn.store(
            Project(
                name=str(uuid.uuid4())
                + "test_get_permissions_for_project_with_registered_user"
            )
        )
        self.schedule_for_cleanup(project_with_permissions_for_authenticated_users)

        # AND the user that created the project
        p1: UserProfile = self.syn.getUserProfile()

        # AND the permissions for AUTHENTICATED_USERS is set to 'READ, DOWNLOAD'
        self.syn.setPermissions(
            project_with_permissions_for_authenticated_users,
            AUTHENTICATED_USERS,
            ["READ", "DOWNLOAD"],
        )

        # AND the permissions for the user on the entity do NOT include DOWNLOAD
        self.syn.setPermissions(
            project_with_permissions_for_authenticated_users,
            p1.ownerId,
            [
                "READ",
                "DELETE",
                "CHANGE_SETTINGS",
                "UPDATE",
                "CHANGE_PERMISSIONS",
                "CREATE",
                "MODERATE",
            ],
        )

        # and WHEN I get the permissions for the user on the entity
        permissions = self.syn.get_permissions(
            project_with_permissions_for_authenticated_users.id
        )

        # THEN I expect to see the permissions of the user, and the authenticated user
        expected_permissions = [
            "READ",
            "DELETE",
            "CHANGE_SETTINGS",
            "UPDATE",
            "CHANGE_PERMISSIONS",
            "CREATE",
            "MODERATE",
            "DOWNLOAD",
        ]
        assert set(expected_permissions) == set(permissions.access_types)


async def test_create_delete_team(syn: Synapse) -> None:
    # GIVEN information about a team I want to create
    name = "python_client_integration_test_team_" + str(uuid.uuid4())
    description = "test description"
    # WHEN I create the team
    team = syn.create_team(name=name, description=description)
    # THEN I expect create_team to return the team
    assert team.id is not None
    assert team.name == name
    assert team.description == description
    assert team.etag is not None
    assert team.createdOn is not None
    assert team.modifiedOn is not None
    assert team.createdBy is not None
    assert team.modifiedBy is not None
    # AND I expect to be able to get the team from Synapse
    assert syn.getTeam(id=team.id).name == name
    # WHEN I delete the team
    syn.delete_team(id=team.id)
    # THEN I expect it to no longer exist in Synapse
    with pytest.raises(SynapseHTTPError):
        syn.getTeam(id=team.id)


class TestAsyncRestInterfaces:
    """Test the async interfaces for the rest methods of the Synapse client."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_rest_get_async(self, project: Project) -> None:
        # GIVEN a project stored in synapse

        # WHEN I get the project from synapse
        new_project_instance = await self.syn.rest_get_async(f"/entity/{project.id}")

        # THEN I expect the project to be the same
        assert new_project_instance["id"] == project["id"]
        assert new_project_instance["name"] == project["name"]
        assert new_project_instance["etag"] == project["etag"]

    async def test_rest_post_async(self, project: Project) -> None:
        # GIVEN A bogus file to upload to synapse
        path = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(path)
        entity = File(
            name=FILE_NAME_PREFIX + str(uuid.uuid4()),
            path=path,
            parentId=project["id"],
            description=FILE_DESCRIPTION,
        )

        # AND I store the file in Synapse
        entity = self.syn.store(entity)
        self.schedule_for_cleanup(entity.id)

        # WHEN I used the rest_post_async method to get the bundle
        request = {
            "includeEntity": True,
        }
        entity_bundle = await self.syn.rest_post_async(
            uri=f"/entity/{entity.id}/bundle2",
            body=json.dumps(request),
        )

        # THEN I expect the bundle to contain the entity
        assert entity_bundle["entity"]["id"] == entity.id
        assert entity_bundle["entity"]["name"] == entity.name
        assert entity_bundle["entity"]["etag"] == entity.etag

    async def test_rest_put_async(self, project: Project) -> None:
        # GIVEN A bogus file to upload to synapse
        path = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(path)
        entity = File(
            name=FILE_NAME_PREFIX + str(uuid.uuid4()),
            path=path,
            parentId=project["id"],
            description=FILE_DESCRIPTION,
        )

        # AND I store the file in Synapse
        entity = self.syn.store(entity)
        self.schedule_for_cleanup(entity.id)

        # AND I used the rest_post_async method to get the bundle
        request = {
            "includeEntity": True,
        }
        entity_bundle = await self.syn.rest_post_async(
            uri=f"/entity/{entity.id}/bundle2",
            body=json.dumps(request),
        )

        # WHEN I updated a field on the entity
        assert entity_bundle["entity"]["description"] == FILE_DESCRIPTION
        entity_bundle["entity"]["description"] = NEW_DESCRIPTION

        # AND I PUT the updated entity back to Synapse
        updated_entity_bundle = await self.syn.rest_put_async(
            uri=f"/entity/{entity_bundle['entity']['id']}/bundle2",
            body=json.dumps({"entity": entity_bundle["entity"]}),
        )

        # THEN I expect the updated entity to have been stored to synapse
        assert updated_entity_bundle["entity"]["description"] == NEW_DESCRIPTION

        # AND getting the entity again from synapse should return the updated entity
        entity_bundle_copy = await self.syn.rest_post_async(
            uri=f"/entity/{entity.id}/bundle2",
            body=json.dumps(
                {
                    "includeEntity": True,
                }
            ),
        )
        assert entity_bundle_copy["entity"]["description"] == NEW_DESCRIPTION

    async def test_rest_delete_async(self, project: Project) -> None:
        # GIVEN A bogus file to upload to synapse
        path = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(path)
        entity = File(
            name=FILE_NAME_PREFIX + str(uuid.uuid4()),
            path=path,
            parentId=project["id"],
            description=FILE_DESCRIPTION,
        )

        # AND I store the file in Synapse
        entity = self.syn.store(entity)
        self.schedule_for_cleanup(entity.id)

        # WHEN I use rest_delete_async to delete the entity
        await self.syn.rest_delete_async(uri=f"/entity/{entity.id}")

        # AND I use the rest_post_async method to get the bundle
        request = {
            "includeEntity": True,
        }
        try:
            await self.syn.rest_post_async(
                uri=f"/entity/{entity.id}/bundle2",
                body=json.dumps(request),
            )
        except SynapseHTTPError as ex:
            # THEN I expect that nothing is found as it is in the trash can
            assert ex.response.status_code == 404
            assert f"404 Client Error: Entity {entity.id} is in trash can." in str(ex)
