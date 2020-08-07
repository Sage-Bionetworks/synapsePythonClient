import json
import logging
import filecmp
import os
import re
import sys
import uuid
import time
import tempfile
import shutil

import pytest
from unittest.mock import patch

from synapseclient import client
from synapseclient import Annotations, Column, Evaluation, File, Folder, Project, Row, RowSet, Schema, Synapse
import synapseclient.__main__ as cmdline
import synapseclient.core.utils as utils

from io import StringIO


@pytest.fixture(scope='module')
def test_state(syn, project, schedule_for_cleanup):
    class State:
        def __init__(self):
            self.syn = syn
            self.project = project
            self.schedule_for_cleanup = schedule_for_cleanup
            self.parser = cmdline.build_parser()
            self.upload_filename = _create_temp_file_with_cleanup(schedule_for_cleanup)
            self.description_text = "'some description text'"
            self.desc_filename = _create_temp_file_with_cleanup(schedule_for_cleanup, self.description_text)
            self.update_description_text = \
                "'SOMEBODY ONCE TOLD ME THE WORLD WAS GONNA ROLL ME I AINT THE SHARPEST TOOL IN THE SHED'"
    return State()


def run(test_state, *command, **kwargs):
    """
    Sends the given command list to the command line client.

    :returns: The STDOUT output of the command.
    """

    old_stdout = sys.stdout
    capturedSTDOUT = StringIO()
    syn_client = kwargs.get('syn', test_state.syn)
    stream_handler = logging.StreamHandler(capturedSTDOUT)

    try:
        sys.stdout = capturedSTDOUT
        syn_client.logger.addHandler(stream_handler)
        sys.argv = [item for item in command]
        args = test_state.parser.parse_args()
        args.debug = True
        cmdline.perform_main(args, syn_client)
    except SystemExit:
        pass  # Prevent the test from quitting prematurely
    finally:
        sys.stdout = old_stdout
        syn_client.logger.handlers.remove(stream_handler)

    capturedSTDOUT = capturedSTDOUT.getvalue()
    return capturedSTDOUT


def parse(regex, output):
    """Returns the first match."""
    m = re.search(regex, output)
    if m:
        if len(m.groups()) > 0:
            return m.group(1).strip()
    else:
        raise Exception('ERROR parsing output: "' + str(output) + '"')


def test_command_line_client(test_state):
    print("TESTING CMD LINE CLIENT")
    # Create a Project
    output = run(test_state,
                 'synapse',
                 '--skip-checks',
                 'create',
                 '-name',
                 str(uuid.uuid4()),
                 '-description',
                 'test of command line client',
                 'Project')
    project_id = parse(r'Created entity:\s+(syn\d+)\s+', output)
    test_state.schedule_for_cleanup(project_id)

    # Create a File
    filename = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(filename)
    output = run(test_state,
                 'synapse',
                 '--skip-checks',
                 'add',
                 '-name',
                 'BogusFileEntity',
                 '-description',
                 'Bogus data to test file upload',
                 '-parentid',
                 project_id,
                 filename)
    file_entity_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)

    # Verify that we stored the file in Synapse
    f1 = test_state.syn.get(file_entity_id)
    fh = test_state.syn._get_file_handle_as_creator(f1.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.S3FileHandle'

    # Get File from the command line
    output = run(test_state,
                 'synapse',
                 '--skip-checks',
                 'get',
                 file_entity_id)
    downloaded_filename = parse(r'Downloaded file:\s+(.*)', output)
    test_state.schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)

    # Update the File
    filename = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(filename)
    output = run(test_state,
                 'synapse',
                 '--skip-checks',
                 'store',
                 '--id',
                 file_entity_id,
                 filename)

    # Get the File again
    output = run(test_state,
                 'synapse',
                 '--skip-checks',
                 'get',
                 file_entity_id)
    downloaded_filename = parse(r'Downloaded file:\s+(.*)', output)
    test_state.schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)

    # Store the same file and don't force a new version

    # Get the existing file to determine it's current version
    current_file = test_state.syn.get(file_entity_id, downloadFile=False)
    current_version = current_file.versionNumber

    # Store it without forcing version
    output = run(test_state,
                 'synapse',
                 '--skip-checks',
                 'store',
                 '--noForceVersion',
                 '--id',
                 file_entity_id,
                 filename)

    # Get the File again and check that the version did not change
    new_file = test_state.syn.get(file_entity_id, downloadFile=False)
    new_version = new_file.versionNumber
    assert current_version == new_version

    # Move the file to new folder
    folder = test_state.syn.store(Folder(parentId=project_id))
    output = run(test_state,
                 'synapse',
                 'mv',
                 '--id',
                 file_entity_id,
                 '--parentid',
                 folder.id)
    movedFile = test_state.syn.get(file_entity_id, downloadFile=False)
    assert movedFile.parentId == folder.id

    # Test Provenance
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    output = run(test_state,
                 'synapse',
                 '--skip-checks',
                 'set-provenance',
                 '-id',
                 file_entity_id,
                 '-name',
                 'TestActivity',
                 '-description',
                 'A very excellent provenance',
                 '-used',
                 file_entity_id,
                 '-executed',
                 repo_url)

    output = run(test_state,
                 'synapse',
                 '--skip-checks',
                 'get-provenance',
                 '--id',
                 file_entity_id)

    activity = json.loads(output)
    assert activity['name'] == 'TestActivity'
    assert activity['description'] == 'A very excellent provenance'

    used = utils._find_used(activity, lambda used: 'reference' in used)
    assert used['reference']['targetId'] == file_entity_id

    used = utils._find_used(activity, lambda used: 'url' in used)
    assert used['url'] == repo_url
    assert used['wasExecuted']

    # Note: Tests shouldn't have external dependencies
    #       but this is a pretty picture of Singapore
    singapore_url = 'http://upload.wikimedia.org/wikipedia/commons/' \
                    'thumb/3/3e/1_singapore_city_skyline_dusk_panorama_2011.jpg' \
                    '/1280px-1_singapore_city_skyline_dusk_panorama_2011.jpg'

    # Test external file handle
    output = run(test_state,
                 'synapse',
                 '--skip-checks',
                 'add',
                 '-name',
                 'Singapore',
                 '-description',
                 'A nice picture of Singapore',
                 '-parentid',
                 project_id,
                 singapore_url)
    exteral_entity_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)

    # Verify that we created an external file handle
    f2 = test_state.syn.get(exteral_entity_id)
    fh = test_state.syn._get_file_handle_as_creator(f2.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.ExternalFileHandle'

    output = run(test_state,
                 'synapse',
                 '--skip-checks',
                 'get',
                 exteral_entity_id)
    downloaded_filename = parse(r'Downloaded file:\s+(.*)', output)
    test_state.schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)

    # Delete the Project
    run(test_state,
        'synapse'
        '--skip-checks', 'delete', project_id)


def test_command_line_client_annotations(test_state):
    # Create a Project
    output = run(test_state,
                 'synapse',
                 '--skip-checks',
                 'create',
                 '-name',
                 str(uuid.uuid4()),
                 '-description',
                 'test of command line client',
                 'Project')
    project_id = parse(r'Created entity:\s+(syn\d+)\s+', output)
    test_state.schedule_for_cleanup(project_id)

    # Create a File
    filename = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(filename)
    output = run(test_state,
                 'synapse',
                 '--skip-checks',
                 'add',
                 '-name',
                 'BogusFileEntity',
                 '-description',
                 'Bogus data to test file upload',
                 '-parentid',
                 project_id,
                 filename)
    file_entity_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)

    # Test setting annotations
    run(test_state,
        'synapse'
        '--skip-checks',
        'set-annotations',
        '--id',
        file_entity_id,
        '--annotations',
        '{"foo": 1, "bar": "1", "baz": [1, 2, 3]}')

    # Test getting annotations
    # check that the three things set are correct
    # This test should be adjusted to check for equality of the
    # whole annotation dictionary once the issue of other
    # attributes (creationDate, eTag, id, uri) being returned is resolved
    # See: https://sagebionetworks.jira.com/browse/SYNPY-175

    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'get-annotations',
                 '--id',
                 file_entity_id)

    annotations = json.loads(output)
    assert annotations['foo'] == [1]
    assert annotations['bar'] == [u"1"]
    assert annotations['baz'] == [1, 2, 3]

    # Test setting annotations by replacing existing ones.
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'set-annotations',
                 '--id',
                 file_entity_id,
                 '--annotations',
                 '{"foo": 2}',
                 '--replace')

    # Test that the annotation was updated
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'get-annotations',
                 '--id',
                 file_entity_id)

    annotations = json.loads(output)

    assert annotations['foo'] == [2]

    # Since this replaces the existing annotations, previous values
    # Should not be available.
    pytest.raises(KeyError, lambda key: annotations[key], 'bar')
    pytest.raises(KeyError, lambda key: annotations[key], 'baz')

    # Test running add command to set annotations on a new object
    filename2 = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(filename2)
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'add',
                 '-name',
                 'BogusData2',
                 '-description',
                 'Bogus data to test file upload with add and add annotations',
                 '-parentid',
                 project_id,
                 '--annotations',
                 '{"foo": 123}',
                 filename2)

    file_entity_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)

    # Test that the annotation was updated
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'get-annotations',
                 '--id',
                 file_entity_id)

    annotations = json.loads(output)
    assert annotations['foo'] == [123]

    # Test running store command to set annotations on a new object
    filename3 = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(filename3)
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'store',
                 '--name',
                 'BogusData3',
                 '--description',
                 '\"Bogus data to test file upload with store and add annotations\"',
                 '--parentid',
                 project_id,
                 '--annotations',
                 '{"foo": 456}',
                 filename3)

    file_entity_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)

    # Test that the annotation was updated
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'get-annotations',
                 '--id',
                 file_entity_id)

    annotations = json.loads(output)
    assert annotations['foo'] == [456]


def test_command_line_store_and_submit(test_state):
    # Create a Project
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'store',
                 '--name',
                 str(uuid.uuid4()),
                 '--description',
                 'test of store command',
                 '--type',
                 'Project')
    project_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)
    test_state.schedule_for_cleanup(project_id)

    # Create and upload a file
    filename = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(filename)
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'store',
                 '--description',
                 'Bogus data to test file upload',
                 '--parentid',
                 project_id,
                 '--file',
                 filename)
    file_entity_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)

    # Verify that we stored the file in Synapse
    f1 = test_state.syn.get(file_entity_id)
    fh = test_state.syn._get_file_handle_as_creator(f1.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.S3FileHandle'

    # Test that entity is named after the file it contains
    assert f1.name == os.path.basename(filename)

    # Create an Evaluation to submit to
    eval = Evaluation(name=str(uuid.uuid4()), contentSource=project_id)
    eval = test_state.syn.store(eval)
    test_state.schedule_for_cleanup(eval)

    # Submit a bogus file
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'submit',
                 '--evaluation',
                 eval.id,
                 '--name',
                 'Some random name',
                 '--entity',
                 file_entity_id)
    parse(r'Submitted \(id: (\d+)\) entity:\s+', output)

    # testing different commmand line options for submitting to an evaluation
    # submitting to an evaluation by evaluationID
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'submit',
                 '--evalID',
                 eval.id,
                 '--name',
                 'Some random name',
                 '--alias',
                 'My Team',
                 '--entity',
                 file_entity_id)
    parse(r'Submitted \(id: (\d+)\) entity:\s+', output)

    # Update the file
    filename = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(filename)
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'store',
                 '--id',
                 file_entity_id,
                 '--file',
                 filename)
    updated_entity_id = parse(r'Updated entity:\s+(syn\d+)', output)
    test_state.schedule_for_cleanup(updated_entity_id)

    # Submit an updated bogus file and this time by evaluation name
    run(test_state,
        'synapse'
        '--skip-checks',
        'submit',
        '--evaluationName',
        eval.name,
        '--entity',
        file_entity_id)

    # Tests shouldn't have external dependencies, but here it's required
    ducky_url = 'https://www.synapse.org/Portal/clear.cache.gif'

    # Test external file handle
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 'store',
                 '--name',
                 'Rubber Ducky',
                 '--description',
                 'I like rubber duckies',
                 '--parentid',
                 project_id,
                 '--file',
                 ducky_url)
    exteral_entity_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)
    test_state.schedule_for_cleanup(exteral_entity_id)

    # Verify that we created an external file handle
    f2 = test_state.syn.get(exteral_entity_id)
    fh = test_state.syn._get_file_handle_as_creator(f2.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.ExternalFileHandle'

    # submit an external file to an evaluation and use provenance
    filename = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(filename)
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    run(test_state,
        'synapse'
        '--skip-checks', 'submit',
        '--evalID', eval.id,
        '--file', filename,
        '--parent', project_id,
        '--used', exteral_entity_id,
        '--executed', repo_url)

    # Delete project
    run(test_state,
        'synapse'
        '--skip-checks', 'delete', project_id)


def test_command_get_recursive_and_query(test_state):
    """Tests the 'synapse get -r' and 'synapse get -q' functions"""

    project_entity = test_state.project

    # Create Folders in Project
    folder_entity = test_state.syn.store(Folder(name=str(uuid.uuid4()),
                                                parent=project_entity))

    folder_entity2 = test_state.syn.store(Folder(name=str(uuid.uuid4()),
                                                 parent=folder_entity))

    # Create and upload two files in sub-Folder
    uploaded_paths = []
    file_entities = []

    for i in range(2):
        f = utils.make_bogus_data_file()
        uploaded_paths.append(f)
        test_state.schedule_for_cleanup(f)
        file_entity = File(f, parent=folder_entity2)
        file_entity = test_state.syn.store(file_entity)
        file_entities.append(file_entity)
        test_state.schedule_for_cleanup(f)

    # Add a file in the Folder as well
    f = utils.make_bogus_data_file()
    uploaded_paths.append(f)
    test_state.schedule_for_cleanup(f)
    file_entity = File(f, parent=folder_entity)
    file_entity = test_state.syn.store(file_entity)
    file_entities.append(file_entity)

    # get -r uses syncFromSynapse() which uses getChildren(), which is not immediately consistent,
    # but faster than chunked queries.
    time.sleep(2)
    # Test recursive get
    run(test_state,
        'synapse'
        '--skip-checks', 'get', '-r', folder_entity.id)
    # Verify that we downloaded files:
    new_paths = [os.path.join('.', folder_entity2.name, os.path.basename(f)) for f in uploaded_paths[:-1]]
    new_paths.append(os.path.join('.', os.path.basename(uploaded_paths[-1])))
    test_state.schedule_for_cleanup(folder_entity.name)
    for downloaded, uploaded in zip(new_paths, uploaded_paths):
        assert os.path.exists(downloaded)
        assert filecmp.cmp(downloaded, uploaded)
        test_state.schedule_for_cleanup(downloaded)

    # Test query get using a Table with an entity column
    # This should be replaced when Table File Views are implemented in the client
    cols = [Column(name='id', columnType='ENTITYID')]

    schema1 = test_state.syn.store(Schema(name='Foo Table', columns=cols, parent=project_entity))
    test_state.schedule_for_cleanup(schema1.id)

    data1 = [[x.id] for x in file_entities]

    test_state.syn.store(RowSet(schema=schema1, rows=[Row(r) for r in data1]))

    time.sleep(3)  # get -q are eventually consistent
    # Test Table/View query get
    run(test_state,
        'synapse'
        '--skip-checks', 'get', '-q',
        "select id from %s" % schema1.id)
    # Verify that we downloaded files:
    new_paths = [os.path.join('.', os.path.basename(f)) for f in uploaded_paths[:-1]]
    new_paths.append(os.path.join('.', os.path.basename(uploaded_paths[-1])))
    test_state.schedule_for_cleanup(folder_entity.name)
    for downloaded, uploaded in zip(new_paths, uploaded_paths):
        assert os.path.exists(downloaded)
        assert filecmp.cmp(downloaded, uploaded)
        test_state.schedule_for_cleanup(downloaded)

    test_state.schedule_for_cleanup(new_paths[0])


def test_command_copy(test_state):
    """Tests the 'synapse cp' function"""

    # Create a Project
    project_entity = test_state.syn.store(Project(name=str(uuid.uuid4())))
    test_state.schedule_for_cleanup(project_entity.id)

    # Create a Folder in Project
    folder_entity = test_state.syn.store(Folder(name=str(uuid.uuid4()),
                                         parent=project_entity))
    test_state.schedule_for_cleanup(folder_entity.id)
    # Create and upload a file in Folder
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    annots = {'test': ['hello_world']}
    # Create, upload, and set annotations on a file in Folder
    filename = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(filename)
    file_entity = test_state.syn.store(File(filename, parent=folder_entity))
    externalURL_entity = test_state.syn.store(File(repo_url, name='rand', parent=folder_entity, synapseStore=False))
    test_state.syn.set_annotations(Annotations(file_entity, file_entity.etag, annots))
    test_state.syn.set_annotations(Annotations(externalURL_entity, externalURL_entity.etag, annots))
    test_state.schedule_for_cleanup(file_entity.id)
    test_state.schedule_for_cleanup(externalURL_entity.id)

    # Test cp function
    output = run(test_state,
                 'synapse'
                 '--skip-checks', 'cp', file_entity.id, '--destinationId', project_entity.id)
    output_URL = run(test_state,
                     'synapse'
                     '--skip-checks', 'cp', externalURL_entity.id, '--destinationId', project_entity.id)

    copied_id = parse(r'Copied syn\d+ to (syn\d+)', output)
    copied_URL_id = parse(r'Copied syn\d+ to (syn\d+)', output_URL)

    # Verify that our copied files are identical
    copied_ent = test_state.syn.get(copied_id)
    copied_URL_ent = test_state.syn.get(copied_URL_id, downloadFile=False)
    test_state.schedule_for_cleanup(copied_id)
    test_state.schedule_for_cleanup(copied_URL_id)
    copied_ent_annot = test_state.syn.get_annotations(copied_id)
    copied_url_annot = test_state.syn.get_annotations(copied_URL_id)

    copied_prov = test_state.syn.getProvenance(copied_id)['used'][0]['reference']['targetId']
    copied_url_prov = test_state.syn.getProvenance(copied_URL_id)['used'][0]['reference']['targetId']

    # Make sure copied files are the same
    assert copied_prov == file_entity.id
    assert copied_ent_annot == annots
    assert copied_ent.properties.dataFileHandleId == file_entity.properties.dataFileHandleId

    # Make sure copied URLs are the same
    assert copied_url_prov == externalURL_entity.id
    assert copied_url_annot == annots
    assert copied_URL_ent.externalURL == repo_url
    assert copied_URL_ent.name == 'rand'
    assert copied_URL_ent.properties.dataFileHandleId == externalURL_entity.properties.dataFileHandleId

    # Verify that errors are being thrown when a
    # file is copied to a folder/project that has a file with the same filename
    pytest.raises(ValueError, run, test_state, 'synapse', '--debug', '--skip-checks', 'cp', file_entity.id,
                  '--destinationId', project_entity.id)


def test_command_line_using_paths(test_state):
    # Create a Project
    project_entity = test_state.syn.store(Project(name=str(uuid.uuid4())))
    test_state.schedule_for_cleanup(project_entity.id)

    # Create a Folder in Project
    folder_entity = test_state.syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))

    # Create and upload a file in Folder
    filename = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(filename)
    file_entity = test_state.syn.store(File(filename, parent=folder_entity))

    # Verify that we can use show with a filename
    output = run(test_state,
                 'synapse'
                 '--skip-checks', 'show', filename)
    id = parse(r'File: %s\s+\((syn\d+)\)\s+' % os.path.split(filename)[1], output)
    assert file_entity.id == id

    # Verify that limitSearch works by making sure we get the file entity
    # that's inside the folder
    file_entity2 = test_state.syn.store(File(filename, parent=project_entity))
    output = run(test_state,
                 'synapse'
                 '--skip-checks', 'get',
                 '--limitSearch', folder_entity.id,
                 filename)
    id = parse(r'Associated file: .* with synapse ID (syn\d+)', output)
    name = parse(r'Associated file: (.*) with synapse ID syn\d+', output)
    assert file_entity.id == id
    assert utils.equal_paths(name, filename)

    # Verify that set-provenance works with filepath
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    output = run(test_state,
                 'synapse'
                 '--skip-checks', 'set-provenance',
                 '-id', file_entity2.id,
                 '-name', 'TestActivity',
                 '-description', 'A very excellent provenance',
                 '-used', filename,
                 '-executed', repo_url,
                 '-limitSearch', folder_entity.id)
    parse(r'Set provenance record (\d+) on entity syn\d+', output)

    output = run(test_state,
                 'synapse'
                 '--skip-checks', 'get-provenance',
                 '-id', file_entity2.id)
    activity = json.loads(output)
    assert activity['name'] == 'TestActivity'
    assert activity['description'] == 'A very excellent provenance'

    # Verify that store works with provenance specified with filepath
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    filename2 = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(filename2)
    output = run(test_state,
                 'synapse'
                 '--skip-checks', 'add', filename2,
                 '-parentid', project_entity.id,
                 '-used', filename,
                 '-executed', '%s %s' % (repo_url, filename))
    entity_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)
    output = run(test_state,
                 'synapse'
                 '--skip-checks', 'get-provenance',
                 '-id', entity_id)
    activity = json.loads(output)
    a = [a for a in activity['used'] if not a['wasExecuted']]
    assert a[0]['reference']['targetId'] in [file_entity.id, file_entity2.id]

    # Test associate command
    # I have two files in Synapse filename and filename2
    path = tempfile.mkdtemp()
    test_state.schedule_for_cleanup(path)
    shutil.copy(filename, path)
    shutil.copy(filename2, path)
    run(test_state,
        'synapse'
        '--skip-checks', 'associate', path, '-r')
    run(test_state,
        'synapse'
        '--skip-checks', 'show', filename)


def test_table_query(test_state):
    """Test command line ability to do table query."""

    cols = [Column(name='name', columnType='STRING', maximumSize=1000),
            Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']),
            Column(name='x', columnType='DOUBLE'),
            Column(name='age', columnType='INTEGER'),
            Column(name='cartoon', columnType='BOOLEAN')]

    project_entity = test_state.project

    schema1 = test_state.syn.store(Schema(name=str(uuid.uuid4()), columns=cols, parent=project_entity))
    test_state.schedule_for_cleanup(schema1.id)

    data1 = [['Chris',  'bar', 11.23, 45, False],
             ['Jen',    'bat', 14.56, 40, False],
             ['Jane',   'bat', 17.89,  6, False],
             ['Henry',  'bar', 10.12,  1, False]]

    test_state.syn.store(RowSet(schema=schema1, rows=[Row(r) for r in data1]))

    # Test query
    output = run(test_state,
                 'synapse'
                 '--skip-checks', 'query',
                 'select * from %s' % schema1.id)

    output_rows = output.rstrip("\n").split("\n")

    # Check the length of the output
    assert len(output_rows) == 5, "got %s rows" % (len(output_rows),)

    # Check that headers are correct.
    # Should be column names in schema plus the ROW_ID and ROW_VERSION
    my_headers_set = output_rows[0].split("\t")
    expected_headers_set = ["ROW_ID", "ROW_VERSION"] + list(map(lambda x: x.name, cols))
    assert my_headers_set == expected_headers_set, "%r != %r" % (my_headers_set, expected_headers_set)


def test_login(test_state):
    alt_syn = Synapse()
    username = "username"
    password = "password"
    with patch.object(alt_syn, "login") as mock_login, \
            patch.object(alt_syn, "getUserProfile", return_value={"userName": "test_user", "ownerId": "ownerId"})\
            as mock_get_user_profile:
        run(test_state,
            'synapse'
            '--skip-checks', 'login',
            '-u', username,
            '-p', password,
            '--rememberMe',
            syn=alt_syn)
        mock_login.assert_called_once_with(username, password, forced=True, rememberMe=True, silent=False)
        mock_get_user_profile.assert_called_once_with()


def test_configPath(test_state):
    """Test using a user-specified configPath for Synapse configuration file."""

    tmp_config_file = tempfile.NamedTemporaryFile(suffix='.synapseConfig', delete=False)
    shutil.copyfile(client.CONFIG_FILE, tmp_config_file.name)

    # Create a File
    filename = utils.make_bogus_data_file()
    test_state.schedule_for_cleanup(filename)
    output = run(test_state,
                 'synapse'
                 '--skip-checks',
                 '--configPath',
                 tmp_config_file.name,
                 'add',
                 '-name',
                 'BogusFileEntityTwo',
                 '-description',
                 'Bogus data to test file upload',
                 '-parentid',
                 test_state.project.id,
                 filename)
    file_entity_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)

    # Verify that we stored the file in Synapse
    f1 = test_state.syn.get(file_entity_id)
    fh = test_state.syn._get_file_handle_as_creator(f1.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.S3FileHandle'


def _description_wiki_check(syn, run_output, expected_description):
    entity_id = parse(r'Created.* entity:\s+(syn\d+)\s+', run_output)
    wiki = syn.getWiki(entity_id)
    assert expected_description == wiki.markdown


def _create_temp_file_with_cleanup(schedule_for_cleanup, specific_file_text=None):
    if specific_file_text:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as file:
            file.write(specific_file_text)
            filename = file.name
    else:
        filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    return filename


def test_create__with_description(test_state):
    output = run(test_state,
                 'synapse',
                 'create',
                 'Folder',
                 '-name',
                 str(uuid.uuid4()),
                 '-parentid',
                 test_state.project.id,
                 '--description',
                 test_state.description_text
                 )
    _description_wiki_check(test_state.syn, output, test_state.description_text)


def test_store__with_description(test_state):
    output = run(test_state,
                 'synapse',
                 'store',
                 test_state.upload_filename,
                 '-name',
                 str(uuid.uuid4()),
                 '-parentid',
                 test_state.project.id,
                 '--description',
                 test_state.description_text
                 )
    _description_wiki_check(test_state.syn, output, test_state.description_text)


def test_add__with_description(test_state):
    output = run(test_state,
                 'synapse',
                 'add',
                 test_state.upload_filename,
                 '-name',
                 str(uuid.uuid4()),
                 '-parentid',
                 test_state.project.id,
                 '--description',
                 test_state.description_text
                 )
    _description_wiki_check(test_state.syn, output, test_state.description_text)


def test_create__with_descriptionFile(test_state):
    output = run(test_state,
                 'synapse',
                 'create',
                 'Folder',
                 '-name',
                 str(uuid.uuid4()),
                 '-parentid',
                 test_state.project.id,
                 '--descriptionFile',
                 test_state.desc_filename
                 )
    _description_wiki_check(test_state.syn, output, test_state.description_text)


def test_store__with_descriptionFile(test_state):
    output = run(test_state,
                 'synapse',
                 'store',
                 test_state.upload_filename,
                 '-name',
                 str(uuid.uuid4()),
                 '-parentid',
                 test_state.project.id,
                 '--descriptionFile',
                 test_state.desc_filename
                 )
    _description_wiki_check(test_state.syn, output, test_state.description_text)


def test_add__with_descriptionFile(test_state):
    output = run(test_state,
                 'synapse',
                 'add',
                 test_state.upload_filename,
                 '-name',
                 str(uuid.uuid4()),
                 '-parentid',
                 test_state.project.id,
                 '--descriptionFile',
                 test_state.desc_filename
                 )
    _description_wiki_check(test_state.syn, output, test_state.description_text)


def test_create__update_description(test_state):
    name = str(uuid.uuid4())
    output = run(test_state,
                 'synapse',
                 'create',
                 'Folder',
                 '-name',
                 name,
                 '-parentid',
                 test_state.project.id,
                 '--descriptionFile',
                 test_state.desc_filename
                 )
    _description_wiki_check(test_state.syn, output, test_state.description_text)
    output = run(test_state,
                 'synapse',
                 'create',
                 'Folder',
                 '-name',
                 name,
                 '-parentid',
                 test_state.project.id,
                 '--description',
                 test_state.update_description_text
                 )
    _description_wiki_check(test_state.syn, output, test_state.update_description_text)


def test_store__update_description(test_state):
    name = str(uuid.uuid4())
    output = run(test_state,
                 'synapse',
                 'store',
                 test_state.upload_filename,
                 '-name',
                 name,
                 '-parentid',
                 test_state.project.id,
                 '--descriptionFile',
                 test_state.desc_filename
                 )
    _description_wiki_check(test_state.syn, output, test_state.description_text)
    output = run(test_state,
                 'synapse',
                 'store',
                 test_state.upload_filename,
                 '-name',
                 name,
                 '-parentid',
                 test_state.project.id,
                 '--description',
                 test_state.update_description_text
                 )
    _description_wiki_check(test_state.syn, output, test_state.update_description_text)


def test_add__update_description(test_state):
    name = str(uuid.uuid4())
    output = run(test_state,
                 'synapse',
                 'add',
                 test_state.upload_filename,
                 '-name',
                 name,
                 '-parentid',
                 test_state.project.id,
                 '--descriptionFile',
                 test_state.desc_filename
                 )
    _description_wiki_check(test_state.syn, output, test_state.description_text)
    output = run(test_state,
                 'synapse',
                 'add',
                 test_state.upload_filename,
                 '-name',
                 name,
                 '-parentid',
                 test_state.project.id,
                 '--description',
                 test_state.update_description_text
                 )
    _description_wiki_check(test_state.syn, output, test_state.update_description_text)


def test_create__same_project_name(test_state):
    """Test creating project that already exists returns the existing project.

    """

    name = str(uuid.uuid4())
    output_first = run(test_state,
                       'synapse',
                       'create',
                       '--name',
                       name,
                       'Project')

    entity_id_first = parse(r'Created entity:\s+(syn\d+)\s+',
                            output_first)

    test_state.schedule_for_cleanup(entity_id_first)

    output_second = run(test_state,
                        'synapse',
                        'create',
                        '--name',
                        name,
                        'Project')

    entity_id_second = parse(r'Created entity:\s+(syn\d+)\s+',
                             output_second)

    assert entity_id_first == entity_id_second


def test_storeTable__csv(test_state):
    output = run(test_state,
                 'synapse',
                 'store-table',
                 '--csv',
                 test_state.desc_filename,
                 '--name',
                 str(uuid.uuid4()),
                 '--parentid',
                 test_state.project.id
                 )
    mapping = json.loads(output)
    test_state.schedule_for_cleanup(mapping['tableId'])
