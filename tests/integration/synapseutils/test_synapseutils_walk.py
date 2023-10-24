import uuid
import os

import pytest

from synapseclient import File, Folder, Project
import synapseclient.core.utils as utils
import synapseutils


# This test is marked with a timeout and flaky. In some cases this test is running
# for 6+ hours and is killed by the time limit placed on how long the pipeline can run.
# signal and func_only allow for the cleanup finalizer to run even if the test is killed.
@pytest.mark.timeout(120, method="signal", func_only=True)
@pytest.mark.flaky(reruns=3)
def test_walk(syn, schedule_for_cleanup):
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
    secondfile = utils.make_bogus_data_file()
    schedule_for_cleanup(secondfile)
    second_file = syn.store(File(secondfile, parent=nested_folder))
    schedule_for_cleanup(second_file.id)
    thirdfile = utils.make_bogus_data_file()
    schedule_for_cleanup(thirdfile)
    third_file = syn.store(File(thirdfile, parent=second_folder))
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
