"""Unit tests for the Sync utility functions"""
import csv
import datetime
import math
import os
import random
import tempfile
from io import StringIO
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, Mock, call, create_autospec, patch

import pandas as pd
import pandas.testing as pdt
import pytest

import synapseclient
import synapseutils
from synapseclient import File as SynapseFile
from synapseclient import Folder, Project, Schema, Synapse
from synapseclient.core.constants import concrete_types, method_flags
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Activity, File
from synapseutils import sync
from synapseutils.sync import _SyncUploader, _SyncUploadItem
from tests.test_utils import spy_for_async_function, spy_for_function

SYNAPSE_URL = "http://www.synapse.org"
GITHUB_URL = "http://www.github.com"
SYN_123 = "syn123"
SYN_789 = "syn789"
PROJECT_NAME = "project_name"
FOLDER_NAME = "folder_name"
PARENT_ID = "syn456"
FILE_NAME = "file_name"


class MockedSyncUploader:
    """Class for mocks in this module"""

    def __init__(self, *args, **kwargs) -> None:
        self.upload = AsyncMock()


def mock_project_dict() -> Dict[str, str]:
    """Mocking:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
    """
    return {
        "concreteType": concrete_types.PROJECT_ENTITY,
        "id": SYN_123,
        "name": FOLDER_NAME,
        "parentId": PARENT_ID,
    }


def mocked_project_rest_api_dict() -> Dict[str, Dict[str, str]]:
    """Mocking:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundle.html>
    """
    return {
        "entity": {
            "concreteType": concrete_types.PROJECT_ENTITY,
            "id": SYN_123,
            "name": FOLDER_NAME,
            "parentId": PARENT_ID,
        }
    }


def mock_folder_dict() -> Dict[str, str]:
    """Mocking:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
    """
    return {
        "concreteType": concrete_types.FOLDER_ENTITY,
        "id": SYN_123,
        "name": FOLDER_NAME,
        "parentId": PARENT_ID,
    }


def mocked_folder_rest_api_dict() -> Dict[str, Dict[str, str]]:
    """Mocking:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundle.html>
    """
    return {
        "entity": {
            "concreteType": concrete_types.FOLDER_ENTITY,
            "id": SYN_123,
            "name": FOLDER_NAME,
            "parentId": PARENT_ID,
        }
    }


def mock_file_dict(syn_id: str = SYN_123) -> Dict[str, str]:
    """Mocking:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
    """
    return {
        "concreteType": concrete_types.FILE_ENTITY,
        "id": syn_id,
        "name": FILE_NAME,
        "parentId": PARENT_ID,
        "isLatestVersion": True,
    }


def mocked_file_rest_api_dict(syn_id: str = SYN_123) -> Dict[str, Dict[str, str]]:
    """Mocking:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundle.html>
    """
    return {
        "entity": {
            "concreteType": concrete_types.FILE_ENTITY,
            "id": syn_id,
            "name": FILE_NAME,
            "parentId": PARENT_ID,
            "isLatestVersion": True,
        }
    }


def mocked_file_child(syn_id: str = SYN_123) -> Dict[str, str]:
    """Mocking:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityHeader.html>
    """
    return {
        "name": FILE_NAME,
        "id": syn_id,
        "type": concrete_types.FILE_ENTITY,
    }


def mocked_folder_child() -> Dict[str, str]:
    """Mocking:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityHeader.html>
    """
    return {
        "name": FOLDER_NAME,
        "id": SYN_123,
        "type": concrete_types.FOLDER_ENTITY,
    }


def test_read_manifest_sync_order_with_home_directory(syn: Synapse) -> None:
    """SYNPY-508"""

    # row1's file depends on row2's file but is listed first
    file_path1 = "~/file1.txt"
    file_path2 = "~/file2.txt"
    project_id = SYN_123
    header = "path	parent	used	executed	activityName	synapseStore	foo\n"
    row1 = '%s\t%s\t%s\t""\tprovActivity1\tTrue\tsomeFooAnnotation1\n' % (
        file_path1,
        project_id,
        file_path2,
    )
    row2 = '%s\t%s\t""\t""\tprovActivity2\tTrue\tsomeFooAnnotation2\n' % (
        file_path2,
        project_id,
    )

    manifest = StringIO(header + row1 + row2)
    # mock syn.get() to return a project because the final check is making sure parent is a container
    # mock isfile() to always return true to avoid having to create files in the home directory
    # side effect mocks values for: manfiest file, file1.txt, file2.txt, isfile(project.id) check in syn.get()
    with patch.object(syn, "get_async", return_value=Project()), patch.object(
        os.path, "isfile", side_effect=[True, True, True, False]
    ), patch.object(sync, "_check_size_each_file", return_value=Mock()):
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)
        expected_order = pd.Series(
            [
                os.path.normpath(os.path.expanduser(file_path2)),
                os.path.normpath(os.path.expanduser(file_path1)),
            ]
        )
        pdt.assert_series_equal(
            expected_order, manifest_dataframe.path, check_names=False
        )


def test_read_manifest_file_synapse_store_values_not_set(syn: Synapse) -> None:
    project_id = SYN_123
    header = "path\tparent\n"
    path1 = os.path.abspath(os.path.expanduser("~/file1.txt"))
    path2 = SYNAPSE_URL
    row1 = "%s\t%s\n" % (path1, project_id)
    row2 = "%s\t%s\n" % (path2, project_id)

    expected_synapseStore = {
        str(path1): True,
        str(path2): False,
    }

    manifest = StringIO(header + row1 + row2)
    with patch.object(syn, "get_async", return_value=Project()), patch.object(
        os.path, "isfile", return_value=True
    ), patch.object(
        sync, "_check_size_each_file", return_value=Mock()
    ):  # side effect mocks values for: file1.txt
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)
        actual_synapseStore = manifest_dataframe.set_index("path")[
            "synapseStore"
        ].to_dict()
        assert expected_synapseStore == actual_synapseStore


def test_read_manifest_file_synapse_store_values_are_set(syn: Synapse) -> None:
    project_id = SYN_123
    header = "path\tparent\tsynapseStore\n"
    path1 = os.path.abspath(os.path.expanduser("~/file1.txt"))
    path2 = SYNAPSE_URL
    path3 = os.path.abspath(os.path.expanduser("~/file3.txt"))
    path4 = GITHUB_URL
    path5 = os.path.abspath(os.path.expanduser("~/file5.txt"))
    path6 = "http://www.checkoutmymixtapefam.com/fire.mp3"

    row1 = "%s\t%s\tTrue\n" % (path1, project_id)
    row2 = "%s\t%s\tTrue\n" % (path2, project_id)
    row3 = "%s\t%s\tFalse\n" % (path3, project_id)
    row4 = "%s\t%s\tFalse\n" % (path4, project_id)
    row5 = '%s\t%s\t""\n' % (path5, project_id)
    row6 = '%s\t%s\t""\n' % (path6, project_id)

    expected_synapseStore = {
        str(path1): True,
        str(path2): False,
        str(path3): False,
        str(path4): False,
        str(path5): True,
        str(path6): False,
    }

    manifest = StringIO(header + row1 + row2 + row3 + row4 + row5 + row6)
    with patch.object(syn, "get_async", return_value=Project()), patch.object(
        sync, "_check_size_each_file", return_value=Mock()
    ), patch.object(
        os.path, "isfile", return_value=True
    ):  # mocks values for: file1.txt, file3.txt, file5.txt
        manifest_dataframe = synapseutils.sync.readManifestFile(syn, manifest)

        actual_synapseStore = manifest_dataframe.set_index("path")[
            "synapseStore"
        ].to_dict()
        assert expected_synapseStore == actual_synapseStore


def test_sync_from_synapse_non_file_entity(syn: Synapse) -> None:
    table_schema = "syn12345"
    with patch.object(syn, "getChildren", return_value=[]), patch.object(
        syn, "get", return_value=Schema(name="asssdfa", parent=PARENT_ID)
    ), patch(
        "synapseutils.sync.get_entity",
        new_callable=AsyncMock,
        return_value={"concreteType": concrete_types.TABLE_ENTITY},
    ):
        pytest.raises(ValueError, synapseutils.syncFromSynapse, syn, table_schema)


def test_sync_from_synapse_empty_folder(syn: Synapse) -> None:
    async def mock_get_children(*args, **kwargs):
        for child in []:
            yield child

    with patch(
        "synapseclient.models.mixins.storable_container.get_children",
        side_effect=mock_get_children,
    ), patch(
        "synapseutils.sync.get_entity",
        new_callable=AsyncMock,
        return_value=(mock_folder_dict()),
    ), patch(
        "synapseclient.api.entity_factory.get_entity_id_bundle2",
        new_callable=AsyncMock,
        return_value=(mocked_folder_rest_api_dict()),
    ):
        assert list() == synapseutils.syncFromSynapse(syn=syn, entity=SYN_123)


def test_sync_from_synapse_file_entity(syn: Synapse) -> None:
    file = SynapseFile(
        name=FILE_NAME,
        parent=PARENT_ID,
        id=SYN_123,
        properties={"isLatestVersion": True},
    )
    with patch.object(syn, "getChildren") as patch_syn_get_children, patch(
        "synapseutils.sync.get_entity",
        new_callable=AsyncMock,
        return_value=(mock_file_dict()),
    ), patch(
        "synapseclient.api.entity_factory.get_entity_id_bundle2",
        new_callable=AsyncMock,
        return_value=(mocked_file_rest_api_dict()),
    ):
        result = synapseutils.syncFromSynapse(syn, file)
        assert [file] == result
        patch_syn_get_children.assert_not_called()


def test_sync_from_synapse_folder_contains_one_file(syn: Synapse) -> None:
    folder = Folder(name=FOLDER_NAME, parent=PARENT_ID, id=SYN_123)
    file = SynapseFile(
        name=FILE_NAME,
        parent=PARENT_ID,
        id=SYN_123,
        properties={"isLatestVersion": True},
    )

    async def mock_get_children(*args, **kwargs):
        for child in [mocked_file_child()]:
            yield child

    with patch(
        "synapseclient.models.mixins.storable_container.get_children",
        side_effect=mock_get_children,
    ), patch(
        "synapseutils.sync.get_entity",
        new_callable=AsyncMock,
        side_effect=[mock_folder_dict(), mock_file_dict()],
    ), patch(
        "synapseclient.api.entity_factory.get_entity_id_bundle2",
        new_callable=AsyncMock,
        side_effect=[mocked_folder_rest_api_dict(), mocked_file_rest_api_dict()],
    ):
        result = synapseutils.syncFromSynapse(syn, folder)
        assert [file] == result


def test_sync_from_synapse_project_contains_empty_folder(syn: Synapse) -> None:
    project = Project(name=PROJECT_NAME, parent=PARENT_ID, id=SYN_123)
    file = SynapseFile(
        name=FILE_NAME,
        parent=PARENT_ID,
        id=SYN_123,
        properties={"isLatestVersion": True},
    )
    file_model = File(
        parent_id=PARENT_ID, id=SYN_123, name=FILE_NAME, is_latest_version=True
    )

    side_effects = [
        [mocked_folder_child()],
        [mocked_file_child()],
        [],
    ]
    call_count = 0

    async def mock_get_children(*args, **kwargs):
        nonlocal call_count
        for child in side_effects[call_count]:
            yield child
        call_count += 1

    with patch(
        "synapseclient.models.mixins.storable_container.get_children",
        side_effect=mock_get_children,
    ) as patch_syn_get_children, patch(
        "synapseutils.sync.get_entity",
        new_callable=AsyncMock,
        side_effect=[mock_project_dict(), mock_folder_dict(), mock_file_dict()],
    ), patch(
        "synapseclient.api.entity_factory.get_entity_id_bundle2",
        new_callable=AsyncMock,
        side_effect=[
            mocked_project_rest_api_dict(),
            mocked_folder_rest_api_dict(),
            mocked_file_rest_api_dict(),
        ],
    ), patch(
        "synapseclient.models.file.get_from_entity_factory",
        wraps=spy_for_async_function(synapseclient.models.file.get_from_entity_factory),
    ) as patch_get_file_entity:
        result = synapseutils.syncFromSynapse(syn=syn, entity=project)
        assert [file] == result
        assert patch_syn_get_children.call_count == 2
        patch_get_file_entity.assert_called_once_with(
            entity_to_update=file_model,
            synapse_id_or_path=SYN_123,
            version=None,
            if_collision=method_flags.COLLISION_OVERWRITE_LOCAL,
            limit_search=None,
            download_file=True,
            download_location=None,
            md5=None,
            synapse_client=syn,
        )


def test_sync_from_synapse_download_file_is_false(syn: Synapse) -> None:
    """
    Verify when passing the argument downloadFile is equal to False,
    syncFromSynapse won't download the file to clients' local end.
    """
    project = Project(name=PROJECT_NAME, parent=PARENT_ID, id=SYN_123)
    file = SynapseFile(
        name=FILE_NAME,
        parent=PARENT_ID,
        id=SYN_123,
        properties={"isLatestVersion": True},
    )
    file_model = File(
        parent_id=PARENT_ID,
        id=SYN_123,
        name=FILE_NAME,
        is_latest_version=True,
        download_file=False,
    )

    side_effects = [
        [mocked_folder_child(), mocked_file_child()],
        [],
    ]
    call_count = 0

    async def mock_get_children(*args, **kwargs):
        nonlocal call_count
        for child in side_effects[call_count]:
            yield child
        call_count += 1

    with patch(
        "synapseclient.models.mixins.storable_container.get_children",
        side_effect=mock_get_children,
    ) as patch_syn_get_children, patch(
        "synapseutils.sync.get_entity",
        new_callable=AsyncMock,
        side_effect=[mock_project_dict(), mock_folder_dict(), mock_file_dict()],
    ), patch(
        "synapseclient.api.entity_factory.get_entity_id_bundle2",
        new_callable=AsyncMock,
        side_effect=[
            mocked_project_rest_api_dict(),
            mocked_file_rest_api_dict(),
            mocked_folder_rest_api_dict(),
        ],
    ), patch(
        "synapseclient.models.file.get_from_entity_factory",
        wraps=spy_for_async_function(synapseclient.models.file.get_from_entity_factory),
    ) as patch_get_file_entity:
        result = synapseutils.syncFromSynapse(
            syn=syn, entity=project, downloadFile=False
        )
        assert [file] == result
        assert patch_syn_get_children.call_count == 2
        patch_get_file_entity.assert_called_once_with(
            entity_to_update=file_model,
            synapse_id_or_path=SYN_123,
            version=None,
            if_collision=method_flags.COLLISION_OVERWRITE_LOCAL,
            limit_search=None,
            download_file=False,
            download_location=None,
            md5=None,
            synapse_client=syn,
        )


def test_sync_from_synapse_manifest_is_all(
    syn: Synapse,
) -> None:
    """
    Verify manifest argument equal to "all" that pass in to syncFromSynapse, it will create root_manifest and all
    child_manifests for every layers.
    """
    # project
    #    |---> file1
    #    |---> folder
    #             |---> file2
    temp_directory_path = tempfile.mkdtemp()
    project = Project(name=PROJECT_NAME, parent=PARENT_ID, id=SYN_123)
    folder = Folder(name=FOLDER_NAME, parent=PARENT_ID, id=SYN_123)
    file = SynapseFile(
        name=FILE_NAME,
        parent=PARENT_ID,
        id=SYN_123,
        properties={"isLatestVersion": True},
    )
    file_2 = SynapseFile(
        name=FILE_NAME,
        parent=PARENT_ID,
        id=SYN_789,
        properties={"isLatestVersion": True},
    )
    file_model = File(
        parent_id=PARENT_ID, id=SYN_123, name=FILE_NAME, is_latest_version=True
    )
    file_model_2 = File(
        parent_id=PARENT_ID, id=SYN_789, name=FILE_NAME, is_latest_version=True
    )

    file_1_provenance = Activity(
        name="",
        description="",
        used=[],
        executed=[],
    )
    file_2_provenance = Activity(
        name="foo",
        description="bar",
        used=[],
        executed=[],
    )

    provenance = {
        file.id: file_1_provenance,
        file_2.id: file_2_provenance,
    }

    side_effects = [
        [mocked_folder_child(), mocked_file_child(syn_id=SYN_123)],
        [mocked_file_child(syn_id=SYN_789)],
    ]
    call_count = 0

    async def mock_get_children(*args, **kwargs):
        nonlocal call_count
        for child in side_effects[call_count]:
            yield child
        call_count += 1

    with patch(
        "synapseclient.models.mixins.storable_container.get_children",
        side_effect=mock_get_children,
    ) as patch_syn_get_children, patch(
        "synapseutils.sync.get_entity",
        new_callable=AsyncMock,
        side_effect=[
            mock_project_dict(),
            mock_file_dict(syn_id=SYN_123),
            mock_folder_dict(),
            mock_file_dict(syn_id=SYN_789),
        ],
    ), patch(
        "synapseclient.api.entity_factory.get_entity_id_bundle2",
        new_callable=AsyncMock,
        side_effect=[
            mocked_project_rest_api_dict(),
            mocked_file_rest_api_dict(syn_id=SYN_123),
            mocked_folder_rest_api_dict(),
            mocked_file_rest_api_dict(syn_id=SYN_789),
        ],
    ), patch(
        "synapseclient.models.file.get_from_entity_factory",
        wraps=spy_for_async_function(synapseclient.models.file.get_from_entity_factory),
    ) as patch_get_file_entity, patch(
        "synapseclient.models.activity.Activity.from_parent_async",
        new_callable=AsyncMock,
        side_effect=lambda parent, **kwargs: provenance.get(parent.id),
    ) as patch_activity_from_parent, patch(
        "synapseutils.sync.generate_manifest",
        wraps=spy_for_function(synapseutils.sync.generate_manifest),
    ) as generate_manifest_spy:
        result = synapseutils.syncFromSynapse(
            syn=syn, entity=project, path=temp_directory_path, manifest="all"
        )
        assert [file, file_2] == result
        expected_get_children_agrs = [
            call(
                parent=project["id"],
                include_types=[
                    "folder",
                    "file",
                    "table",
                    "entityview",
                    "dockerrepo",
                    "submissionview",
                    "dataset",
                    "datasetcollection",
                    "materializedview",
                    "virtualtable",
                ],
                synapse_client=syn,
            ),
            call(
                parent=folder["id"],
                include_types=[
                    "folder",
                    "file",
                    "table",
                    "entityview",
                    "dockerrepo",
                    "submissionview",
                    "dataset",
                    "datasetcollection",
                    "materializedview",
                    "virtualtable",
                ],
                synapse_client=syn,
            ),
        ]
        assert patch_syn_get_children.call_count == 2
        assert expected_get_children_agrs == patch_syn_get_children.call_args_list
        assert patch_get_file_entity.call_args_list == [
            call(
                entity_to_update=file_model,
                synapse_id_or_path=SYN_123,
                version=None,
                if_collision=method_flags.COLLISION_OVERWRITE_LOCAL,
                limit_search=None,
                download_file=True,
                download_location=temp_directory_path,
                md5=None,
                synapse_client=syn,
            ),
            call(
                entity_to_update=file_model_2,
                synapse_id_or_path=SYN_789,
                version=None,
                if_collision=method_flags.COLLISION_OVERWRITE_LOCAL,
                limit_search=None,
                download_file=True,
                download_location=os.path.join(temp_directory_path, FOLDER_NAME),
                md5=None,
                synapse_client=syn,
            ),
        ]

        assert generate_manifest_spy.call_count == 2
        assert patch_activity_from_parent.call_count == 2

        # Top level parent project
        generate_manifest_spy.call_args_list[0] == call(
            all_files=[file_model, file_model_2], path=temp_directory_path
        )

        expected_manifest = f"""path\tparent\tname\tid\tsynapseStore\tcontentType\tused\texecuted\tactivityName\tactivityDescription
{temp_directory_path}\tsyn456\tfile_name\tsyn123\tTrue\t\t\t\t\t
{os.path.join(temp_directory_path, FOLDER_NAME)}\tsyn456\tfile_name\tsyn789\tTrue\t\t\t\tfoo\tbar"""
        _compare_csv(
            expected_manifest,
            os.path.join(temp_directory_path, synapseutils.sync.MANIFEST_FILENAME),
        )

        # Sub folder
        generate_manifest_spy.call_args_list[1] == call(
            all_files=[file_model_2],
            path=os.path.join(temp_directory_path, FOLDER_NAME),
        )
        expected_manifest = f"""path\tparent\tname\tid\tsynapseStore\tcontentType\tused\texecuted\tactivityName\tactivityDescription
{os.path.join(temp_directory_path, FOLDER_NAME)}\tsyn456\tfile_name\tsyn789\tTrue\t\t\t\tfoo\tbar"""
        _compare_csv(
            expected_manifest,
            os.path.join(
                os.path.join(temp_directory_path, FOLDER_NAME),
                synapseutils.sync.MANIFEST_FILENAME,
            ),
        )


def test_sync_from_synapse_manifest_is_root(
    syn: Synapse,
) -> None:
    """
    Verify manifest argument equal to "root" that pass in to syncFromSynapse, it
    will create root_manifest file only.
    """
    # project
    #    |---> file1
    #    |---> folder
    #             |---> file2
    temp_directory_path = tempfile.mkdtemp()
    project = Project(name=PROJECT_NAME, parent=PARENT_ID, id=SYN_123)
    folder = Folder(name=FOLDER_NAME, parent=PARENT_ID, id=SYN_123)
    file = SynapseFile(
        name=FILE_NAME,
        parent=PARENT_ID,
        id=SYN_123,
        properties={"isLatestVersion": True},
    )
    file_2 = SynapseFile(
        name=FILE_NAME,
        parent=PARENT_ID,
        id=SYN_789,
        properties={"isLatestVersion": True},
    )
    file_model = File(
        parent_id=PARENT_ID, id=SYN_123, name=FILE_NAME, is_latest_version=True
    )
    file_model_2 = File(
        parent_id=PARENT_ID, id=SYN_789, name=FILE_NAME, is_latest_version=True
    )

    file_1_provenance = Activity(
        name="",
        description="",
        used=[],
        executed=[],
    )
    file_2_provenance = Activity(
        name="foo",
        description="bar",
        used=[],
        executed=[],
    )

    provenance = {
        file.id: file_1_provenance,
        file_2.id: file_2_provenance,
    }

    side_effects = [
        [mocked_folder_child(), mocked_file_child(syn_id=SYN_123)],
        [mocked_file_child(syn_id=SYN_789)],
    ]
    call_count = 0

    async def mock_get_children(*args, **kwargs):
        nonlocal call_count
        for child in side_effects[call_count]:
            yield child
        call_count += 1

    with patch(
        "synapseclient.models.mixins.storable_container.get_children",
        side_effect=mock_get_children,
    ) as patch_syn_get_children, patch(
        "synapseutils.sync.get_entity",
        new_callable=AsyncMock,
        side_effect=[
            mock_project_dict(),
            mock_file_dict(syn_id=SYN_123),
            mock_folder_dict(),
            mock_file_dict(syn_id=SYN_789),
        ],
    ), patch(
        "synapseclient.api.entity_factory.get_entity_id_bundle2",
        new_callable=AsyncMock,
        side_effect=[
            mocked_project_rest_api_dict(),
            mocked_file_rest_api_dict(syn_id=SYN_123),
            mocked_folder_rest_api_dict(),
            mocked_file_rest_api_dict(syn_id=SYN_789),
        ],
    ), patch(
        "synapseclient.models.file.get_from_entity_factory",
        wraps=spy_for_async_function(synapseclient.models.file.get_from_entity_factory),
    ) as patch_get_file_entity, patch(
        "synapseclient.models.activity.Activity.from_parent_async",
        new_callable=AsyncMock,
        side_effect=lambda parent, **kwargs: provenance.get(parent.id),
    ) as patch_activity_from_parent, patch(
        "synapseutils.sync.generate_manifest",
        wraps=spy_for_function(synapseutils.sync.generate_manifest),
    ) as generate_manifest_spy:
        result = synapseutils.syncFromSynapse(
            syn=syn, entity=project, path=temp_directory_path, manifest="root"
        )
        assert [file, file_2] == result
        expected_get_children_agrs = [
            call(
                parent=project["id"],
                include_types=[
                    "folder",
                    "file",
                    "table",
                    "entityview",
                    "dockerrepo",
                    "submissionview",
                    "dataset",
                    "datasetcollection",
                    "materializedview",
                    "virtualtable",
                ],
                synapse_client=syn,
            ),
            call(
                parent=folder["id"],
                include_types=[
                    "folder",
                    "file",
                    "table",
                    "entityview",
                    "dockerrepo",
                    "submissionview",
                    "dataset",
                    "datasetcollection",
                    "materializedview",
                    "virtualtable",
                ],
                synapse_client=syn,
            ),
        ]
        assert patch_syn_get_children.call_count == 2
        assert expected_get_children_agrs == patch_syn_get_children.call_args_list
        assert patch_get_file_entity.call_args_list == [
            call(
                entity_to_update=file_model,
                synapse_id_or_path=SYN_123,
                version=None,
                if_collision=method_flags.COLLISION_OVERWRITE_LOCAL,
                limit_search=None,
                download_file=True,
                download_location=temp_directory_path,
                md5=None,
                synapse_client=syn,
            ),
            call(
                entity_to_update=file_model_2,
                synapse_id_or_path=SYN_789,
                version=None,
                if_collision=method_flags.COLLISION_OVERWRITE_LOCAL,
                limit_search=None,
                download_file=True,
                download_location=os.path.join(temp_directory_path, FOLDER_NAME),
                md5=None,
                synapse_client=syn,
            ),
        ]

        assert generate_manifest_spy.call_count == 1
        assert patch_activity_from_parent.call_count == 2

        # Top level parent project
        generate_manifest_spy.call_args_list[0] == call(
            all_files=[file_model, file_model_2], path=temp_directory_path
        )

        expected_manifest = f"""path\tparent\tname\tid\tsynapseStore\tcontentType\tused\texecuted\tactivityName\tactivityDescription
{temp_directory_path}\tsyn456\tfile_name\tsyn123\tTrue\t\t\t\t\t
{os.path.join(temp_directory_path, FOLDER_NAME)}\tsyn456\tfile_name\tsyn789\tTrue\t\t\t\tfoo\tbar"""
        _compare_csv(
            expected_manifest,
            os.path.join(temp_directory_path, synapseutils.sync.MANIFEST_FILENAME),
        )


@patch.object(synapseutils.sync, "generate_manifest")
@patch.object(synapseutils.sync, "_get_file_entity_provenance_dict")
def test_sync_from_synapse_manifest_is_suppress(
    mock_get_file_entity_provenance_dict: MagicMock,
    mock_generate_manifest: MagicMock,
    syn: Synapse,
) -> None:
    """
    Verify manifest argument equal to "suppress" that pass in to syncFromSynapse, it won't create any manifest file.
    """
    # project
    #    |---> file1
    #    |---> folder
    #             |---> file2
    project = Project(name=PROJECT_NAME, parent=PARENT_ID, id=SYN_123)
    folder = Folder(name=FOLDER_NAME, parent=PARENT_ID, id=SYN_123)
    file = SynapseFile(
        name=FILE_NAME,
        parent=PARENT_ID,
        id=SYN_123,
        properties={"isLatestVersion": True},
    )
    file_2 = SynapseFile(
        name=FILE_NAME,
        parent=PARENT_ID,
        id=SYN_789,
        properties={"isLatestVersion": True},
    )
    file_model = File(
        parent_id=PARENT_ID, id=SYN_123, name=FILE_NAME, is_latest_version=True
    )
    file_model_2 = File(
        parent_id=PARENT_ID, id=SYN_789, name=FILE_NAME, is_latest_version=True
    )

    mock_get_file_entity_provenance_dict.return_value = {}

    side_effects = [
        [mocked_folder_child(), mocked_file_child(syn_id=SYN_123)],
        [mocked_file_child(syn_id=SYN_789)],
    ]
    call_count = 0

    async def mock_get_children(*args, **kwargs):
        nonlocal call_count
        for child in side_effects[call_count]:
            yield child
        call_count += 1

    with patch(
        "synapseclient.models.mixins.storable_container.get_children",
        side_effect=mock_get_children,
    ) as patch_syn_get_children, patch(
        "synapseutils.sync.get_entity",
        new_callable=AsyncMock,
        side_effect=[
            mock_project_dict(),
            mock_file_dict(syn_id=SYN_123),
            mock_folder_dict(),
            mock_file_dict(syn_id=SYN_789),
        ],
    ), patch(
        "synapseclient.api.entity_factory.get_entity_id_bundle2",
        new_callable=AsyncMock,
        side_effect=[
            mocked_project_rest_api_dict(),
            mocked_file_rest_api_dict(syn_id=SYN_123),
            mocked_folder_rest_api_dict(),
            mocked_file_rest_api_dict(syn_id=SYN_789),
        ],
    ), patch(
        "synapseclient.models.file.get_from_entity_factory",
        wraps=spy_for_async_function(synapseclient.models.file.get_from_entity_factory),
    ) as patch_get_file_entity, patch(
        "synapseclient.models.activity.Activity.from_parent_async",
        new_callable=AsyncMock,
        return_value=None,
    ) as patch_activity_from_parent:
        result = synapseutils.syncFromSynapse(
            syn=syn, entity=project, path="./", manifest="suppress"
        )
        assert [file, file_2] == result
        expected_get_children_agrs = [
            call(
                parent=project["id"],
                include_types=[
                    "folder",
                    "file",
                    "table",
                    "entityview",
                    "dockerrepo",
                    "submissionview",
                    "dataset",
                    "datasetcollection",
                    "materializedview",
                    "virtualtable",
                ],
                synapse_client=syn,
            ),
            call(
                parent=folder["id"],
                include_types=[
                    "folder",
                    "file",
                    "table",
                    "entityview",
                    "dockerrepo",
                    "submissionview",
                    "dataset",
                    "datasetcollection",
                    "materializedview",
                    "virtualtable",
                ],
                synapse_client=syn,
            ),
        ]
        assert patch_syn_get_children.call_count == 2
        assert expected_get_children_agrs == patch_syn_get_children.call_args_list
        assert patch_get_file_entity.call_args_list == [
            call(
                entity_to_update=file_model,
                synapse_id_or_path=SYN_123,
                version=None,
                if_collision=method_flags.COLLISION_OVERWRITE_LOCAL,
                limit_search=None,
                download_file=True,
                download_location="./",
                md5=None,
                synapse_client=syn,
            ),
            call(
                entity_to_update=file_model_2,
                synapse_id_or_path=SYN_789,
                version=None,
                if_collision=method_flags.COLLISION_OVERWRITE_LOCAL,
                limit_search=None,
                download_file=True,
                download_location=f"./{FOLDER_NAME}",
                md5=None,
                synapse_client=syn,
            ),
        ]

        assert mock_generate_manifest.call_count == 0
        assert patch_activity_from_parent.call_count == 2


def test_sync_from_synapse_manifest_value_is_invalid(syn) -> None:
    project = Project(name=PROJECT_NAME, parent=PARENT_ID, id=SYN_123)
    with pytest.raises(ValueError) as ve:
        synapseutils.syncFromSynapse(
            syn, project, path="./", downloadFile=False, manifest="invalid_str"
        )
    assert (
        str(ve.value)
        == 'Value of manifest option should be one of the ("all", "root", "suppress")'
    )


def _compare_csv(expected_csv_string, csv_path):
    # compare our expected csv with the one written to the given path.
    # compare parsed dictionaries vs just comparing strings to avoid newline differences across platforms
    expected = [
        r for r in csv.DictReader(StringIO(expected_csv_string), delimiter="\t")
    ]
    with open(csv_path, "r") as csv_file:
        actual = [r for r in csv.DictReader(csv_file, delimiter="\t")]
    assert expected == actual


def test_extract_file_entity_metadata__ensure_correct_row_metadata(
    syn: Synapse,
) -> None:
    # Test for SYNPY-692, where 'contentType' was incorrectly set on all rows except for the very first row.

    # create 2 file entities with different metadata
    entity1 = SynapseFile(
        parent=SYN_123,
        id="syn456",
        contentType="text/json",
        path="path1",
        name="entity1",
        synapseStore=True,
    )
    entity2 = SynapseFile(
        parent="syn789",
        id="syn890",
        contentType="text/html",
        path="path2",
        name="entity2",
        synapseStore=False,
    )
    files = [entity1, entity2]

    # we don't care about provenance metadata in this case
    with patch.object(
        synapseutils.sync, "_get_file_entity_provenance_dict", return_value={}
    ):
        # method under test
        keys, data = synapseutils.sync._extract_file_entity_metadata(syn, files)

    # compare source entity metadata gainst the extracted metadata
    for file_entity, file_row_data in zip(files, data):
        for key in keys:
            if (
                key == "parent"
            ):  # workaroundd for parent/parentId inconsistency. (SYNPY-697)
                assert file_entity.get("parentId") == file_row_data.get(key)
            else:
                assert file_entity.get(key) == file_row_data.get(key)


async def test_manifest_upload(syn: Synapse) -> None:
    """Verify behavior of synapseutils.sync._manifest_upload"""

    the_year_2000 = datetime.datetime(
        2000, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc
    )
    the_year_2001 = datetime.datetime(
        2001, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc
    )
    data = {
        "path": ["/tmp/foo", "/tmp/bar", "/tmp/baz"],
        "parent": [SYN_123, "syn456", "syn789"],
        "name": ["foo", "bar", "baz"],
        "used": [None, "/tmp/foo", "/tmp/bar"],
        "executed": [None, None, "/tmp/foo"],
        "anno_1": ["", "v1", "v2"],
        "anno_2": ["v3", "v4", ""],
        "anno_datetimes": [[the_year_2000, the_year_2001], "", ""],
        "anno_bools": [[False, True], "", ""],
        "anno_ints": [[1, 2], "", ""],
        "anno_floats": [[1.0, 2.0, 3.0, 4.0, 5.0], "", ""],
        "anno_strings": [["foo", "bar", "aaa", "bbb"], "", ""],
    }

    # any empty annotations that result from any empty csv column should not
    # be included in the upload
    expected_anno_data = [
        {
            "anno_2": "v3",
            "anno_datetimes": [the_year_2000, the_year_2001],
            "anno_bools": [False, True],
            "anno_ints": [1, 2],
            "anno_floats": [1.0, 2.0, 3.0, 4.0, 5.0],
            "anno_strings": ["foo", "bar", "aaa", "bbb"],
        },
        {"anno_1": "v1", "anno_2": "v4"},
        {"anno_1": "v2"},
        {"anno_1"},
    ]

    df = pd.DataFrame(data=data)
    mocked_uploaders = []

    def mock_uploader_constructor(*args, **kwargs):
        mock_uploader = MockedSyncUploader(*args, **kwargs)
        mocked_uploaders.append(mock_uploader)
        return mock_uploader

    with patch.object(
        synapseutils.sync, "_SyncUploadItem"
    ) as upload_item_init, patch.object(
        synapseutils.sync, "_SyncUploader", new=mock_uploader_constructor
    ):
        await synapseutils.sync._manifest_upload(syn, df)

    upload_items = []
    for i in range(3):
        expected_path = data["path"][i]
        expected_parent = data["parent"][i]
        expected_name = data["name"][i]
        expected_used = data["used"][i]
        expected_executed = data["executed"][i]
        expected_annos = expected_anno_data[i]

        upload_items.append(upload_item_init.return_value)
        upload_item_init_args = upload_item_init.call_args_list[i]
        file, used, executed = upload_item_init_args[0]
        assert file.path == expected_path
        assert file.parent_id == expected_parent
        assert file.name == expected_name
        assert file.annotations == expected_annos
        assert used == expected_used
        assert executed == expected_executed

    for mock_uploader in mocked_uploaders:
        mock_uploader.upload.assert_called_once_with(upload_items)


class TestSyncUploader:
    @patch("os.path.isfile")
    def test_order_items(self, mock_isfile: MagicMock, syn: Synapse) -> None:
        """Verfy that items are properly ordered according to their provenance."""

        def isfile(path):
            return path.startswith("/tmp")

        mock_isfile.side_effect = isfile

        # dependencies flow down
        #
        #                       /tmp/6
        #                         |
        #                  _______|_______
        #                  |             |
        #                  V             |
        #  /tmp/4        /tmp/5          |
        #    |_____________|             |
        #           |                    |
        #           V                    |
        #         /tmp/3                 |
        #           |                    |
        #           V                    V
        #         /tmp/1               /tmp/2

        item_1 = _SyncUploadItem(
            File(path="/tmp/1", parent_id=SYN_123),
            used=[],  # used
            executed=[],  # executed
            activity_name=None,
            activity_description=None,
        )
        item_2 = _SyncUploadItem(
            File(path="/tmp/2", parent_id=SYN_123),
            used=[],  # used
            executed=[],  # executed
            activity_name=None,
            activity_description=None,
        )
        item_3 = _SyncUploadItem(
            File(path="/tmp/3", parent_id=SYN_123),
            used=["/tmp/1"],  # used
            executed=[],  # executed
            activity_name=None,
            activity_description=None,
        )
        item_4 = _SyncUploadItem(
            File(path="/tmp/4", parent_id=SYN_123),
            used=[],  # used
            executed=["/tmp/3"],  # executed
            activity_name=None,
            activity_description=None,
        )
        item_5 = _SyncUploadItem(
            File(path="/tmp/5", parent_id=SYN_123),
            used=["/tmp/3"],  # used
            executed=[],  # executed
            activity_name=None,
            activity_description=None,
        )
        item_6 = _SyncUploadItem(
            File(path="/tmp/6", parent_id=SYN_123),
            used=["/tmp/5"],  # used
            executed=["/tmp/2"],  # executed
            activity_name=None,
            activity_description=None,
        )

        items = [
            item_5,
            item_6,
            item_2,
            item_3,
            item_1,
            item_4,
        ]

        random.shuffle(items)
        uploader = _SyncUploader(syn)
        ordered = uploader._build_dependency_graph(items)

        seen = set()
        for i in ordered.path_to_upload_item.values():
            assert all(p in seen for p in (i.used + i.executed))
            seen.add(i.entity.path)

    @patch("os.path.isfile")
    def test_order_items__provenance_cycle(self, isfile: MagicMock) -> None:
        """Verify that if a provenance cycle is detected we raise an error"""

        isfile.return_value = True

        items = [
            _SyncUploadItem(
                entity=File(path="/tmp/1", parent_id=SYN_123),
                used=["/tmp/2"],  # used
                executed=[],
                activity_name=None,
                activity_description=None,
            ),
            _SyncUploadItem(
                entity=File(path="/tmp/2", parent_id=SYN_123),
                used=[],  # used
                executed=["/tmp/1"],  # executed
                activity_name=None,
                activity_description=None,
            ),
        ]

        with pytest.raises(RuntimeError) as cm_ex:
            uploader = _SyncUploader(None)
            uploader._build_dependency_graph(items)
        assert "cyclic" in str(cm_ex.value)

    @patch("os.path.isfile")
    def test_order_items__provenance_file_not_uploaded(self, isfile: MagicMock) -> None:
        """Verify that if one file depends on another for provenance but that file
        is not included in the upload we raise an error."""

        isfile.return_value = True

        items = [
            _SyncUploadItem(
                entity=File(path="/tmp/1", parent_id=SYN_123),
                used=["/tmp/2"],  # used
                executed=[],
                activity_name=None,
                activity_description=None,
            ),
        ]

        with pytest.raises(ValueError) as cm_ex:
            uploader = _SyncUploader(None)
            uploader._build_dependency_graph(items)
        assert "not being uploaded" in str(cm_ex.value)

    async def test_upload_item_success(self, syn: Synapse) -> None:
        """Test successfully uploading an item"""

        uploader = _SyncUploader(syn)

        used = [SYNAPSE_URL]
        executed = [GITHUB_URL]
        entity = File(path="/tmp/file", parent_id=SYN_123)
        entity.store_async = AsyncMock(return_value=None)
        item = _SyncUploadItem(
            entity=entity,
            used=used,
            executed=executed,
            activity_name=None,
            activity_description=None,
        )

        await uploader.upload([item])

        entity.store_async.assert_called_once()

    async def test_upload_item_failure(self, syn: Synapse) -> None:
        """Verify behavior if an item upload fails.
        Exception should be raised, and appropriate threading controls should be released/notified.
        """

        uploader = _SyncUploader(syn)

        entity = File(path="/tmp/file", parent_id=SYN_123)
        entity.store_async = AsyncMock(side_effect=ValueError("Falure during upload"))

        item = _SyncUploadItem(
            entity=entity,
            used=[],
            executed=[],
            activity_name=None,
            activity_description=None,
        )

        with pytest.raises(ValueError):
            await uploader.upload([item])

    @patch("os.path.isfile")
    async def test_upload(self, mock_os_isfile: MagicMock, syn: Synapse) -> None:
        """Ensure that an upload including multiple items which depend on each other through
        provenance are all uploaded and in the expected order."""
        mock_os_isfile.return_value = True
        paths = ["/tmp/foo", "/tmp/bar", "/tmp/baz"]

        file_1 = File(path=paths[0], parent_id=SYN_123, id="syn3")
        file_1.store_async = AsyncMock(return_value=file_1)
        item_1 = _SyncUploadItem(
            entity=file_1,
            used=[],  # used
            executed=[],  # executed
            activity_name=None,
            activity_description=None,
        )
        file_2 = File(path=paths[1], parent_id=SYN_123, id="syn2")
        file_2.store_async = AsyncMock(return_value=file_2)
        item_2 = _SyncUploadItem(
            entity=file_2,
            used=["/tmp/foo"],  # used
            executed=[],  # executed
            activity_name=None,
            activity_description=None,
        )
        file_3 = File(path=paths[2], parent_id=SYN_123, id="syn1")
        file_3.store_async = AsyncMock(return_value=file_3)
        item_3 = _SyncUploadItem(
            entity=file_3,
            used=["/tmp/bar"],  # used
            executed=[],  # executed
            activity_name=None,
            activity_description=None,
        )

        items = [
            item_1,
            item_2,
            item_3,
        ]

        uploader = _SyncUploader(syn)
        await uploader.upload([item_1, item_2, item_3])

        # all three of our items should have been stored
        for i in items:
            i.entity.store_async.assert_called_once()
            assert i.entity.path in paths


class TestGetFileEntityProvenanceDict:
    """
    test synapseutils.sync._get_file_entity_provenance_dict
    """

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self):
        self.mock_syn = create_autospec(Synapse)

    def test_get_file_entity_provenance_dict__error_is_404(self) -> None:
        self.mock_syn.getProvenance.side_effect = SynapseHTTPError(
            response=Mock(status_code=404)
        )

        result_dict = synapseutils.sync._get_file_entity_provenance_dict(
            self.mock_syn, SYN_123
        )
        assert {} == result_dict

    def test_get_file_entity_provenance_dict__error_not_404(self) -> None:
        self.mock_syn.getProvenance.side_effect = SynapseHTTPError(
            response=Mock(status_code=400)
        )

        pytest.raises(
            SynapseHTTPError,
            synapseutils.sync._get_file_entity_provenance_dict,
            self.mock_syn,
            SYN_123,
        )


@patch.object(sync, "os")
def test_check_size_each_file(mock_os: MagicMock, syn: Synapse) -> None:
    """
    Verify the check_size_each_file method works correctly
    """

    project_id = SYN_123
    header = "path\tparent\n"
    path1 = os.path.abspath(os.path.expanduser("~/file1.txt"))
    path2 = SYNAPSE_URL
    path3 = os.path.abspath(os.path.expanduser("~/file3.txt"))
    path4 = GITHUB_URL

    row1 = f"{path1}\t{project_id}\n"
    row2 = f"{path2}\t{project_id}\n"
    row3 = f"{path3}\t{project_id}\n"
    row4 = f"{path4}\t{project_id}\n"

    manifest = StringIO(header + row1 + row2 + row3 + row4)
    # HACK: This is repeated because of os.path.basename is called once for every file
    # for 3 separate functions.
    mock_os.path.basename.side_effect = [
        "file1.txt",
        "www.synapse.org",
        "file3.txt",
        "www.github.com",
        "file1.txt",
        "www.synapse.org",
        "file3.txt",
        "www.github.com",
        "file1.txt",
        "www.synapse.org",
        "file3.txt",
        "www.github.com",
    ]
    mock_os.path.isfile.side_effect = [True, True, True, False]
    mock_os.path.abspath.side_effect = [path1, path3]
    mock_stat = MagicMock(spec="st_size")
    mock_os.stat.return_value = mock_stat
    mock_stat.st_size = 5

    # mock syn.get() to return a project because the final check is making sure parent is a container
    with patch.object(syn, "get_async", return_value=Project()):
        sync.readManifestFile(syn, manifest)
        mock_os.stat.call_count == 4


@patch.object(sync, "os")
def test_check_size_each_file_raise_error(mock_os: MagicMock, syn: Synapse) -> None:
    """
    Verify the check_size_each_file method raises the ValueError when the file is empty.
    """

    project_id = SYN_123
    header = "path\tparent\n"
    path1 = os.path.abspath(os.path.expanduser("~/file1.txt"))
    path2 = SYNAPSE_URL
    path3 = os.path.abspath(os.path.expanduser("~/file3.txt"))
    path4 = GITHUB_URL

    row1 = f"{path1}\t{project_id}\n"
    row2 = f"{path2}\t{project_id}\n"
    row3 = f"{path3}\t{project_id}\n"
    row4 = f"{path4}\t{project_id}\n"

    manifest = StringIO(header + row1 + row2 + row3 + row4)
    mock_os.path.isfile.side_effect = [True, True, True, False]
    mock_os.path.abspath.side_effect = [path1, path3]
    mock_os.path.basename.return_value = "file1.txt"
    mock_stat = MagicMock(spec="st_size")
    mock_os.stat.return_value = mock_stat
    mock_stat.st_size = 0
    with pytest.raises(ValueError) as ve:
        sync.readManifestFile(syn, manifest)
    assert str(
        ve.value
    ) == "File {} is empty, empty files cannot be uploaded to Synapse".format(
        "file1.txt"
    )


@patch.object(sync, "os")
def test_check_file_name(mock_os: MagicMock, syn: Synapse) -> None:
    """
    Verify the check_file_name method works correctly
    """

    project_id = SYN_123
    header = "path\tparent\tname\n"
    path1 = os.path.abspath(os.path.expanduser("~/file1.txt"))
    path2 = os.path.abspath(os.path.expanduser("~/file2.txt"))
    path3 = os.path.abspath(os.path.expanduser("~/file3.txt"))

    row1 = f"{path1}\t{project_id}\tTest_file_name.txt\n"
    row2 = f"{path2}\t{project_id}\tTest_file-name`s(1).txt\n"
    row3 = f"{path3}\t{project_id}\t\n"

    manifest = StringIO(header + row1 + row2 + row3)
    # mock isfile() to always return true to avoid having to create files in the home directory
    mock_os.path.isfile.return_value = True
    mock_os.path.abspath.side_effect = [path1, path2, path3]
    mock_os.path.basename.return_value = "file3.txt"

    # mock syn.get() to return a project because the final check is making sure parent is a container
    with patch.object(syn, "get_async", return_value=Project()):
        sync.readManifestFile(syn, manifest)


@patch.object(sync, "os")
def test_check_file_name_with_illegal_char(mock_os: MagicMock, syn: Synapse) -> None:
    """
    Verify the check_file_name method raises the ValueError when the file name contains illegal char
    """

    project_id = SYN_123
    header = "path\tparent\tname\n"
    path1 = os.path.abspath(os.path.expanduser("~/file1.txt"))
    path2 = os.path.abspath(os.path.expanduser("~/file2.txt"))
    path3 = os.path.abspath(os.path.expanduser("~/file3.txt"))
    path4 = os.path.abspath(os.path.expanduser("~/file4.txt"))

    row1 = f"{path1}\t{project_id}\tTest_file_name.txt\n"
    row2 = f"{path2}\t{project_id}\tTest_file-name`s(1).txt\n"
    row3 = f"{path3}\t{project_id}\t\n"
    illegal_name = "Test_file_name_with_#.txt"
    row4 = f"{path4}\t{project_id}\t{illegal_name}\n"

    manifest = StringIO(header + row1 + row2 + row3 + row4)
    mock_os.path.isfile.return_value = True
    mock_os.path.abspath.side_effect = [path1, path2, path3, path4]
    mock_os.path.basename.return_value = "file3.txt"

    with pytest.raises(ValueError) as ve:
        sync.readManifestFile(syn, manifest)
    assert (
        str(ve.value)
        == "File name {} cannot be stored to Synapse. Names may contain letters, numbers, spaces, "
        "underscores, hyphens, periods, plus signs, apostrophes, "
        "and parentheses".format(illegal_name)
    )


@patch.object(sync, "os")
def test_check_file_name_duplicated(mock_os: MagicMock, syn: Synapse) -> None:
    """
    Verify the check_file_name method raises the ValueError when the file name is duplicated
    """

    project_id = SYN_123
    header = "path\tparent\tname\n"
    path1 = os.path.abspath(os.path.expanduser("~/file1.txt"))
    path2 = os.path.abspath(os.path.expanduser("~/file2.txt"))
    path3 = os.path.abspath(os.path.expanduser("~/file3.txt"))
    path4 = os.path.abspath(os.path.expanduser("~/file4.txt"))

    row1 = f"{path1}\t{project_id}\tfoo\n"
    row2 = f"{path2}\t{project_id}\tfoo\n"
    row3 = f"{path3}\t{project_id}\tfoo\n"
    row4 = f"{path4}\t{project_id}\tfoo\n"

    manifest = StringIO(header + row1 + row2 + row3 + row4)
    mock_os.path.isfile.return_value = True
    mock_os.path.abspath.side_effect = [path1, path2, path3, path4]
    mock_os.path.basename.return_value = "file3.txt"

    with pytest.raises(ValueError) as ve:
        sync.readManifestFile(syn, manifest)
    assert str(ve.value) == (
        "All rows in manifest must contain a path with a unique file name and parent to upload. "
        "Files uploaded to the same folder/project (parent) must have unique file names."
    )


@patch.object(sync, "os")
def test_check_file_name_with_too_long_filename(
    mock_os: MagicMock, syn: Synapse
) -> None:
    """
    Verify the check_file_name method raises the ValueError when the file name is too long
    """

    project_id = SYN_123
    header = "path\tparent\tname\n"
    path1 = os.path.abspath(os.path.expanduser("~/file1.txt"))
    path2 = os.path.abspath(os.path.expanduser("~/file2.txt"))
    path3 = os.path.abspath(os.path.expanduser("~/file3.txt"))
    path4 = os.path.abspath(os.path.expanduser("~/file4.txt"))

    long_file_name = (
        "test_filename_too_long_test_filename_too_long_test_filename_too_long_test_filename_too_long_"
        "test_filename_too_long_test_filename_too_long_test_filename_too_long_test_filename_too_long_"
        "test_filename_too_long_test_filename_too_long_test_filename_too_long_test_filename_too_long_"
    )

    row1 = f"{path1}\t{project_id}\tTest_file_name.txt\n"
    row2 = f"{path2}\t{project_id}\tTest_file-name`s(1).txt\n"
    row3 = f"{path3}\t{project_id}\t\n"
    row4 = f"{path4}\t{project_id}\t{long_file_name}\n"

    manifest = StringIO(header + row1 + row2 + row3 + row4)
    mock_os.path.isfile.return_value = True
    mock_os.path.abspath.side_effect = [path1, path2, path3, path4]
    mock_os.path.basename.return_value = "file3.txt"

    with pytest.raises(ValueError) as ve:
        sync.readManifestFile(syn, manifest)
    assert (
        str(ve.value)
        == "File name {} cannot be stored to Synapse. Names may contain letters, numbers, spaces, "
        "underscores, hyphens, periods, plus signs, apostrophes, "
        "and parentheses".format(long_file_name)
    )


def test__create_folder(syn: Synapse) -> None:
    folder_name = "TestName"
    parent_id = SYN_123
    with patch.object(syn, "store") as patch_syn_store:
        sync._create_folder(syn, folder_name, parent_id)
        patch_syn_store.assert_called_once_with(
            {
                "name": folder_name,
                "concreteType": "org.sagebionetworks.repo.model.Folder",
                "parentId": parent_id,
            }
        )
        sync._create_folder(syn, folder_name * 2, parent_id * 2)
        patch_syn_store.assert_called_with(
            {
                "name": folder_name * 2,
                "concreteType": "org.sagebionetworks.repo.model.Folder",
                "parentId": parent_id * 2,
            }
        )


@patch.object(sync, "os")
def test__walk_directory_tree(mock_os: MagicMock, syn: Synapse) -> None:
    folder_name = "TestFolder"
    subfolder_name = "TestSubfolder"
    parent_id = SYN_123
    mock_os.walk.return_value = [
        (folder_name, [subfolder_name], ["TestFile.txt"]),
        (os.path.join(folder_name, subfolder_name), [], ["TestSubfile.txt"]),
    ]
    mock_os.stat.return_value.st_size = 1
    mock_os.path.join.side_effect = os.path.join
    with patch.object(sync, "_create_folder") as mock_create_folder:
        mock_create_folder.return_value = {"id": "syn456"}
        rows = sync._walk_directory_tree(syn, folder_name, parent_id)
        mock_os.walk.assert_called_once_with(folder_name)
        mock_create_folder.assert_called_once_with(syn, subfolder_name, parent_id)
        assert mock_os.stat.call_count == 2
        assert len(rows) == 2


def test_generate_sync_manifest(syn: Synapse) -> None:
    folder_name = "TestName"
    parent_id = SYN_123
    manifest_path = "TestFolder"
    with patch.object(
        sync, "_walk_directory_tree"
    ) as patch_walk_directory_tree, patch.object(
        sync, "_write_manifest_data"
    ) as patch_write_manifest_data:
        sync.generate_sync_manifest(syn, folder_name, parent_id, manifest_path)
        patch_walk_directory_tree.assert_called_once_with(syn, folder_name, parent_id)
        patch_write_manifest_data.assert_called_with(
            manifest_path, ["path", "parent"], patch_walk_directory_tree.return_value
        )


class TestConvertCellInManifestToPythonTypes(object):
    """
    Test the _convert_cell_in_manifest_to_python_types function for each type
    and single/multiple values.
    """

    def test_datetime_single_item(self) -> None:
        # GIVEN a single item datetime string
        datetime_string = "2020-01-01T00:00:00.000Z"
        datetime_string_in_brackets = "[2020-01-01T00:00:00.000Z]"

        # WHEN _convert_cell_in_manifest_to_python_types is called
        # THEN I expect the output to be a datetime object
        assert synapseutils.sync._convert_cell_in_manifest_to_python_types(
            datetime_string
        ) == datetime.datetime(2020, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert synapseutils.sync._convert_cell_in_manifest_to_python_types(
            datetime_string_in_brackets
        ) == datetime.datetime(2020, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)

    def test_datetime_multiple_items(self) -> None:
        # GIVEN a multiple item datetime string
        datetime_string = "[2020-01-01T00:00:00.000Z, 2020-01-02T00:00:00.000Z]"

        # WHEN _convert_cell_in_manifest_to_python_types is called
        # THEN I expect the output to be a list of datetime objects
        assert synapseutils.sync._convert_cell_in_manifest_to_python_types(
            datetime_string
        ) == [
            datetime.datetime(2020, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 1, 2, 0, 0, 0, 0, tzinfo=datetime.timezone.utc),
        ]

    def test_bool_single_item(self) -> None:
        # GIVEN a single item bool string
        bool_string = "TrUe"
        bool_string_in_brackets = "[tRue]"

        # WHEN _convert_cell_in_manifest_to_python_types is called
        # THEN I expect the output to be a bool object
        assert (
            synapseutils.sync._convert_cell_in_manifest_to_python_types(bool_string)
            is True
        )
        assert (
            synapseutils.sync._convert_cell_in_manifest_to_python_types(
                bool_string_in_brackets
            )
            is True
        )

    def test_bool_multiple_items(self) -> None:
        # GIVEN a multiple item bool string
        bool_string = "[TrUe, fAlse]"

        # WHEN _convert_cell_in_manifest_to_python_types is called
        # THEN I expect the output to be a list of bool objects
        assert synapseutils.sync._convert_cell_in_manifest_to_python_types(
            bool_string
        ) == [True, False]

    def test_int_single_item(self) -> None:
        # GIVEN a single item int string
        int_string = "123"
        int_string_in_brackets = "[123]"

        # WHEN _convert_cell_in_manifest_to_python_types is called
        # THEN I expect the output to be a int object
        assert (
            synapseutils.sync._convert_cell_in_manifest_to_python_types(int_string)
            == 123
        )
        assert (
            synapseutils.sync._convert_cell_in_manifest_to_python_types(
                int_string_in_brackets
            )
            == 123
        )

    def test_int_multiple_items(self) -> None:
        # GIVEN a multiple item int string
        int_string = "[123, 456]"

        # WHEN _convert_cell_in_manifest_to_python_types is called
        # THEN I expect the output to be a list of int objects
        assert synapseutils.sync._convert_cell_in_manifest_to_python_types(
            int_string
        ) == [123, 456]

    def test_float_single_item(self) -> None:
        # GIVEN a single item float string
        float_string = "123.456"
        float_string_in_brackets = "[123.456]"

        # WHEN _convert_cell_in_manifest_to_python_types is called
        # THEN I expect the output to be a float object
        assert math.isclose(
            synapseutils.sync._convert_cell_in_manifest_to_python_types(float_string),
            123.456,
        )
        assert math.isclose(
            synapseutils.sync._convert_cell_in_manifest_to_python_types(
                float_string_in_brackets
            ),
            123.456,
        )

    def test_float_multiple_items(self) -> None:
        # GIVEN a multiple item float string
        float_string = "     [123.456, 789.012]"

        # WHEN _convert_cell_in_manifest_to_python_types is called
        # THEN I expect the output to be a list of float objects
        result = synapseutils.sync._convert_cell_in_manifest_to_python_types(
            float_string
        )
        assert math.isclose(result[0], 123.456)
        assert math.isclose(result[1], 789.012)

    def test_string_single_item(self) -> None:
        # GIVEN a single item string
        string = "        foo"
        string_in_brackets = "       [foo]"

        # WHEN _convert_cell_in_manifest_to_python_types is called
        # THEN I expect the output to be a string object
        assert (
            synapseutils.sync._convert_cell_in_manifest_to_python_types(string) == "foo"
        )
        assert (
            synapseutils.sync._convert_cell_in_manifest_to_python_types(
                string_in_brackets
            )
            == "foo"
        )

    def test_string_multiple_items(self) -> None:
        # GIVEN a multiple item string
        string = "  [foo,    bar]"

        # WHEN _convert_cell_in_manifest_to_python_types is called
        # THEN I expect the output to be a list of string objects
        assert synapseutils.sync._convert_cell_in_manifest_to_python_types(string) == [
            "foo",
            "bar",
        ]

    def test_string_single_item_with_comma(self) -> None:
        # GIVEN a single item string
        string = "my, sentence, with, commas"

        # WHEN _convert_cell_in_manifest_to_python_types is called
        # THEN I expect the output to be a string object
        assert (
            synapseutils.sync._convert_cell_in_manifest_to_python_types(string)
            == string
        )


class TestSplitString(object):
    """
    Test the _split_string function. Note as a pre-check to calling this function
    the string would have started with brackets `[]`, but they were removed before
    calling this function.
    """

    def test_single_item(self) -> None:
        # GIVEN single item strings
        string_with_no_quotes = "foo"
        string_with_quotes = '"foo"'
        string_with_quotes_inside_string = 'foo "bar" baz'
        string_with_commas_inside_string = '"foo, bar, baz"'

        # WHEN split_strings is called

        # THEN I expect the output to treat all values as a single item
        assert synapseutils.sync._split_string(string_with_no_quotes) == [
            string_with_no_quotes
        ]
        assert synapseutils.sync._split_string(string_with_quotes) == [
            string_with_quotes
        ]
        assert synapseutils.sync._split_string(string_with_quotes_inside_string) == [
            string_with_quotes_inside_string
        ]
        assert synapseutils.sync._split_string(string_with_commas_inside_string) == [
            string_with_commas_inside_string
        ]

    def test_multiple_item(self) -> None:
        # GIVEN multiple item strings
        string_with_no_quotes = "foo, bar,    baz"
        string_with_quotes = '"foo",       "bar", "baz"'
        string_with_commas_in_some_items = '"foo, bar", baz, "foo, bar, baz"'

        # WHEN split_strings is called
        # THEN I expect the output to split the string into multiple items
        assert synapseutils.sync._split_string(string_with_no_quotes) == [
            "foo",
            "bar",
            "baz",
        ]
        assert synapseutils.sync._split_string(string_with_quotes) == [
            '"foo"',
            '"bar"',
            '"baz"',
        ]
        assert synapseutils.sync._split_string(string_with_commas_in_some_items) == [
            '"foo, bar"',
            "baz",
            '"foo, bar, baz"',
        ]
