import uuid
import os

from nose.tools import assert_equals, assert_in

from synapseclient.core.exceptions import *
from synapseclient import *
from tests import integration
from tests.integration import schedule_for_cleanup
import synapseutils


def setup(module):
    module.syn = integration.syn
    module.project = integration.project


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

    walked.append(((project_entity.name, project_entity.id),
                   [(folder_entity.name, folder_entity.id), (second_folder.name, second_folder.id)],
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

    walked.append(((os.path.join(project_entity.name, folder_entity.name), folder_entity.id),
                   [(nested_folder.name, nested_folder.id)], []))
    walked.append(((os.path.join(os.path.join(project_entity.name, folder_entity.name), nested_folder.name),
                    nested_folder.id), [], [(second_file.name, second_file.id)]))
    walked.append(((os.path.join(project_entity.name, second_folder.name), second_folder.id), [],
                   [(third_file.name, third_file.id)]))

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
        assert_in(i, walked)

    temp = synapseutils.walk(syn, second_file.id)
    assert_equals(list(temp), [])
