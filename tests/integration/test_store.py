import os
from synapseclient import Synapse
from synapseclient import Entity, Project, Folder, File

import integration
from integration import schedule_for_cleanup


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = integration.syn
    module.project = integration.project

def test_create_or_update_for_non_versionable():
    # SYNR-582: creating a project with same name wipes off previous data
    folder = syn.store(Folder('some folder', parent=project, foo='bar', bat=123))

    try:
        attempt_overwrite_folder = syn.store(Folder('some folder', parent=project, fizz='buzz'))
    except Exception as e:
        print e
    else:
        assert False, 'Expected 409 Client Error: Conflict'

    stored_folder = syn.get(folder.id)
    assert 'foo' in stored_folder and stored_folder.foo == ['bar']
    assert 'bat' in stored_folder and stored_folder.bat == [123]
    assert 'fizz' not in stored_folder


    attempt_overwrite_folder = syn.store(Folder('some folder', parent=project, foo='bar', fizz='buzz'), createOrUpdate=True)

    stored_folder = syn.get(folder.id)
    assert 'foo' in stored_folder and stored_folder.foo == ['bar']
    assert 'bat' not in stored_folder
    assert 'fizz' in stored_folder and stored_folder.fizz == ['buzz']

