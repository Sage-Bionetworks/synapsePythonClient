import os
import uuid

import pytest
from func_timeout import FunctionTimedOut, func_set_timeout

import synapseutils
from synapseclient import File, Folder, Project

# from unittest import skip


# @skip("Skip integration tests for soon to be removed code")
async def test_walk(syn, schedule_for_cleanup):
    try:
        execute_test_walk(syn, schedule_for_cleanup)
    except FunctionTimedOut:
        syn.logger.warning("test_walk timed out")
        pytest.skip("test_walk timed out, skipping test")


# When running with multiple threads it can lock up and do nothing until pipeline is killed at 6hrs
@func_set_timeout(120)
def execute_test_walk(syn, schedule_for_cleanup):
    # walk only ever inspects entity names/ids/structure, never downloads or reads
    # file content, so every File below uses an external URL to avoid a real upload.
    walked = []
    project_entity = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(project_entity.id)
    folder_entity = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    schedule_for_cleanup(folder_entity.id)
    second_folder = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    schedule_for_cleanup(second_folder.id)
    file_entity = syn.store(
        File(
            f"https://example.com/bogus-file-{uuid.uuid4()}.txt",
            name=f"bogus_file_{uuid.uuid4()}.txt",
            parent=project_entity,
            synapseStore=False,
        )
    )
    schedule_for_cleanup(file_entity.id)

    walked.append(
        (
            (project_entity.name, project_entity.id),
            [
                (folder_entity.name, folder_entity.id),
                (second_folder.name, second_folder.id),
            ],
            [(file_entity.name, file_entity.id)],
        )
    )

    nested_folder = syn.store(Folder(name=str(uuid.uuid4()), parent=folder_entity))
    schedule_for_cleanup(nested_folder.id)
    second_file = syn.store(
        File(
            f"https://example.com/bogus-file-{uuid.uuid4()}.txt",
            name=f"bogus_file_{uuid.uuid4()}.txt",
            parent=nested_folder,
            synapseStore=False,
        )
    )
    schedule_for_cleanup(second_file.id)
    third_file = syn.store(
        File(
            f"https://example.com/bogus-file-{uuid.uuid4()}.txt",
            name=f"bogus_file_{uuid.uuid4()}.txt",
            parent=second_folder,
            synapseStore=False,
        )
    )
    schedule_for_cleanup(third_file.id)

    walked.append(
        (
            (os.path.join(project_entity.name, folder_entity.name), folder_entity.id),
            [(nested_folder.name, nested_folder.id)],
            [],
        )
    )
    walked.append(
        (
            (
                os.path.join(
                    os.path.join(project_entity.name, folder_entity.name),
                    nested_folder.name,
                ),
                nested_folder.id,
            ),
            [],
            [(second_file.name, second_file.id)],
        )
    )
    walked.append(
        (
            (os.path.join(project_entity.name, second_folder.name), second_folder.id),
            [],
            [(third_file.name, third_file.id)],
        )
    )

    temp = synapseutils.walk(syn, project_entity.id)
    temp = list(temp)
    # Must sort the tuples returned, because order matters for the assert
    # Folders are returned in a different ordering depending on the name
    for i in walked:
        for x in i:
            if type(x) == list:
                x.sort()
    for i in temp:
        for x in i:
            if type(x) == list:
                x.sort()
        assert i in walked

    temp = synapseutils.walk(syn, second_file.id)
    assert list(temp) == []
