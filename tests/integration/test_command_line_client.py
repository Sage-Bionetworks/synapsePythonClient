# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str
import six
import logging
import filecmp
import os
import re
import sys
import uuid
import json
import time
from nose.plugins.attrib import attr
from nose.tools import assert_raises, assert_equals, assert_less
import tempfile
import shutil
from mock import patch
import synapseclient
import synapseclient.client as client
import synapseclient.utils as utils
import synapseclient.__main__ as cmdline
from synapseclient.evaluation import Evaluation

import integration
from integration import schedule_for_cleanup, QUERY_TIMEOUT_SEC

if six.PY2:
    from StringIO import StringIO
else:
    from io import StringIO


def setup_module(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn
    module.project = integration.project

    module.parser = cmdline.build_parser()

    #used for --description and --descriptionFile tests
    module.upload_filename = _create_temp_file_with_cleanup()
    module.description_text = "'some description text'"
    module.desc_filename = _create_temp_file_with_cleanup(module.description_text)
    module.update_description_text = "'SOMEBODY ONCE TOLD ME THE WORLD WAS GONNA ROLL ME I AINT THE SHARPEST TOOL IN THE SHED'"
    module.other_user = integration.other_user



def run(*command, **kwargs):
    """
    Sends the given command list to the command line client.

    :returns: The STDOUT output of the command.
    """

    print(' '.join(command))
    old_stdout = sys.stdout
    capturedSTDOUT = StringIO()
    syn_client = kwargs.get('syn', syn)
    stream_handler = logging.StreamHandler(capturedSTDOUT)

    try:
        sys.stdout = capturedSTDOUT
        syn_client.logger.addHandler(stream_handler)
        sys.argv = [item for item in command]
        args = parser.parse_args()
        args.debug = True
        cmdline.perform_main(args, syn_client)
    except SystemExit:
        pass # Prevent the test from quitting prematurely
    finally:
        sys.stdout = old_stdout
        syn_client.logger.handlers.remove(stream_handler)


    capturedSTDOUT = capturedSTDOUT.getvalue()
    # print(capturedSTDOUT)
    return capturedSTDOUT


def parse(regex, output):
    """Returns the first match."""
    m = re.search(regex, output)
    if m:
        if len(m.groups()) > 0:
            return m.group(1).strip()
    else:
        raise Exception('ERROR parsing output: "' + str(output) + '"')


def test_command_line_client():
    # Create a Project
    output = run('synapse',
                 '--skip-checks',
                 'create',
                 '-name',
                 str(uuid.uuid4()),
                 '-description',
                 'test of command line client',
                 'Project')
    project_id = parse(r'Created entity:\s+(syn\d+)\s+', output)
    schedule_for_cleanup(project_id)

    # Create a File
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    output = run('synapse',
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
    f1 = syn.get(file_entity_id)
    fh = syn._getFileHandle(f1.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.S3FileHandle'

    # Get File from the command line
    output = run('synapse',
                 '--skip-checks',
                 'get',
                 file_entity_id)
    downloaded_filename = parse(r'Downloaded file:\s+(.*)', output)
    schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)


    # Update the File
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    output = run('synapse',
                 '--skip-checks',
                 'store',
                 '--id',
                 file_entity_id,
                 filename)
    updated_entity_id = parse(r'Created/Updated entity:\s+(syn\d+)', output)

    # Get the File again
    output = run('synapse',
                 '--skip-checks',
                 'get',
                 file_entity_id)
    downloaded_filename = parse(r'Downloaded file:\s+(.*)', output)
    schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)

    # Test query
    output = run('synapse',
                 '--skip-checks',
                 'query',
                 'select id, name from entity where parentId=="%s"' % project_id)
    assert 'BogusFileEntity' in output
    assert file_entity_id in output


    # Move the file to new folder
    folder = syn.store(synapseclient.Folder(parentId=project_id))
    output = run('synapse',
                 'mv',
                 '--id',
                 file_entity_id,
                 '--parentid',
                 folder.id)
    downloaded_filename = parse(r'Moved\s+(.*)', output)
    movedFile = syn.get(file_entity_id, downloadFile=False)
    assert movedFile.parentId == folder.id


    # Test Provenance
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    output = run('synapse',
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
    activity_id = parse(r'Set provenance record (\d+) on entity syn\d+', output)

    output = run('synapse',
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
    assert used['wasExecuted'] == True

    # Note: Tests shouldn't have external dependencies
    #       but this is a pretty picture of Singapore
    singapore_url = 'http://upload.wikimedia.org/wikipedia/commons/' \
                    'thumb/3/3e/1_singapore_city_skyline_dusk_panorama_2011.jpg' \
                    '/1280px-1_singapore_city_skyline_dusk_panorama_2011.jpg'

    # Test external file handle
    output = run('synapse',
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
    f2 = syn.get(exteral_entity_id)
    fh = syn._getFileHandle(f2.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.ExternalFileHandle'

    output = run('synapse',
                 '--skip-checks',
                 'get',
                 exteral_entity_id)
    downloaded_filename = parse(r'Downloaded file:\s+(.*)', output)
    schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)

    # Delete the Project
    output = run('synapse',
                 '--skip-checks',
                 'delete',
                 project_id)


def test_command_line_client_annotations():
    # Create a Project
    output = run('synapse',
                 '--skip-checks',
                 'create',
                 '-name',
                 str(uuid.uuid4()),
                 '-description',
                 'test of command line client',
                 'Project')
    project_id = parse(r'Created entity:\s+(syn\d+)\s+', output)
    schedule_for_cleanup(project_id)

    # Create a File
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    output = run('synapse',
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
    output = run('synapse',
                 '--skip-checks',
                 'set-annotations',
                 '--id',
                 file_entity_id,
                 '--annotations',
                 '{"foo": 1, "bar": "1", "baz": [1, 2, 3]}',
    )

    # Test getting annotations
    # check that the three things set are correct
    # This test should be adjusted to check for equality of the
    # whole annotation dictionary once the issue of other
    # attributes (creationDate, eTag, id, uri) being returned is resolved
    # See: https://sagebionetworks.jira.com/browse/SYNPY-175

    output = run('synapse',
                 '--skip-checks',
                 'get-annotations',
                 '--id',
                 file_entity_id
             )

    annotations = json.loads(output)
    assert annotations['foo'] == [1]
    assert annotations['bar'] == [u"1"]
    assert annotations['baz'] == [1, 2, 3]

    # Test setting annotations by replacing existing ones.
    output = run('synapse',
                 '--skip-checks',
                 'set-annotations',
                 '--id',
                 file_entity_id,
                 '--annotations',
                 '{"foo": 2}',
                 '--replace'
    )

    # Test that the annotation was updated
    output = run('synapse',
                 '--skip-checks',
                 'get-annotations',
                 '--id',
                 file_entity_id
             )

    annotations = json.loads(output)

    assert annotations['foo'] == [2]

    # Since this replaces the existing annotations, previous values
    # Should not be available.
    assert_raises(KeyError, lambda key: annotations[key], 'bar')
    assert_raises(KeyError, lambda key: annotations[key], 'baz')

    # Test running add command to set annotations on a new object
    filename2 = utils.make_bogus_data_file()
    schedule_for_cleanup(filename2)
    output = run('synapse',
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
    output = run('synapse',
                 '--skip-checks',
                 'get-annotations',
                 '--id',
                 file_entity_id
             )

    annotations = json.loads(output)
    assert annotations['foo'] == [123]

    # Test running store command to set annotations on a new object
    filename3 = utils.make_bogus_data_file()
    schedule_for_cleanup(filename3)
    output = run('synapse',
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
    output = run('synapse',
                 '--skip-checks',
                 'get-annotations',
                 '--id',
                 file_entity_id
             )

    annotations = json.loads(output)
    assert annotations['foo'] == [456]


def test_command_line_store_and_submit():
    # Create a Project
    output = run('synapse',
                 '--skip-checks',
                 'store',
                 '--name',
                 str(uuid.uuid4()),
                 '--description',
                 'test of store command',
                 '--type',
                 'Project')
    project_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)
    schedule_for_cleanup(project_id)

    # Create and upload a file
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    output = run('synapse',
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
    f1 = syn.get(file_entity_id)
    fh = syn._getFileHandle(f1.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.S3FileHandle'

    # Test that entity is named after the file it contains
    assert f1.name == os.path.basename(filename)

    # Create an Evaluation to submit to
    eval = Evaluation(name=str(uuid.uuid4()), contentSource=project_id)
    eval = syn.store(eval)
    schedule_for_cleanup(eval)

    # Submit a bogus file
    output = run('synapse',
                 '--skip-checks',
                 'submit',
                 '--evaluation',
                 eval.id,
                 '--name',
                 'Some random name',
                 '--entity',
                 file_entity_id)
    submission_id = parse(r'Submitted \(id: (\d+)\) entity:\s+', output)

    #testing different commmand line options for submitting to an evaluation
    #. submitting to an evaluation by evaluationID
    output = run('synapse',
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
    submission_id = parse(r'Submitted \(id: (\d+)\) entity:\s+', output)


    # Update the file
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    output = run('synapse',
                 '--skip-checks',
                 'store',
                 '--id',
                 file_entity_id,
                 '--file',
                 filename)
    updated_entity_id = parse(r'Updated entity:\s+(syn\d+)', output)
    schedule_for_cleanup(updated_entity_id)

    # Submit an updated bogus file and this time by evaluation name
    output = run('synapse',
                 '--skip-checks',
                 'submit',
                 '--evaluationName',
                 eval.name,
                 '--entity',
                 file_entity_id)
    submission_id = parse(r'Submitted \(id: (\d+)\) entity:\s+', output)

    # Tests shouldn't have external dependencies, but here it's required
    ducky_url = 'https://www.synapse.org/Portal/clear.cache.gif'

    # Test external file handle
    output = run('synapse',
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
    schedule_for_cleanup(exteral_entity_id)

    # Verify that we created an external file handle
    f2 = syn.get(exteral_entity_id)
    fh = syn._getFileHandle(f2.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.ExternalFileHandle'

    #submit an external file to an evaluation and use provenance
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    output = run('synapse',
                 '--skip-checks',
                 'submit',
                 '--evalID',
                 eval.id,
                 '--file',
                 filename,
                 '--parent',
                 project_id,
                 '--used',
                 exteral_entity_id,
                 '--executed',
                 repo_url
                 )
    submission_id = parse(r'Submitted \(id: (\d+)\) entity:\s+', output)

    # Delete project
    output = run('synapse',
                 '--skip-checks',
                 'delete',
                 project_id)


def test_command_get_recursive_and_query():
    """Tests the 'synapse get -r' and 'synapse get -q' functions"""

    project_entity = project

    # Create Folders in Project
    folder_entity = syn.store(synapseclient.Folder(name=str(uuid.uuid4()),
                                                   parent=project_entity))

    folder_entity2 = syn.store(synapseclient.Folder(name=str(uuid.uuid4()),
                                                    parent=folder_entity))

    # Create and upload two files in sub-Folder
    uploaded_paths = []
    file_entities = []

    for i in range(2):
        f  = utils.make_bogus_data_file()
        uploaded_paths.append(f)
        schedule_for_cleanup(f)
        file_entity = synapseclient.File(f, parent=folder_entity2)
        file_entity = syn.store(file_entity)
        file_entities.append(file_entity)
        schedule_for_cleanup(f)


    #Add a file in the Folder as well
    f  = utils.make_bogus_data_file()
    uploaded_paths.append(f)
    schedule_for_cleanup(f)
    file_entity = synapseclient.File(f, parent=folder_entity)
    file_entity = syn.store(file_entity)
    file_entities.append(file_entity)

    time.sleep(2) # get -r uses syncFromSynapse() which uses getChildren(), which is not immediately consistent, but faster than chunked queries.
    ### Test recursive get
    output = run('synapse', '--skip-checks',
                 'get', '-r',
                 folder_entity.id)
    #Verify that we downloaded files:
    new_paths = [os.path.join('.', folder_entity2.name, os.path.basename(f)) for f in uploaded_paths[:-1]]
    new_paths.append(os.path.join('.', os.path.basename(uploaded_paths[-1])))
    schedule_for_cleanup(folder_entity.name)
    for downloaded, uploaded in zip(new_paths, uploaded_paths):
        print(uploaded, downloaded)
        assert os.path.exists(downloaded)
        assert filecmp.cmp(downloaded, uploaded)
        schedule_for_cleanup(downloaded)


    time.sleep(3) # get -q uses chunkedQuery which are eventually consistent
    ### Test query get
    ### Note: We're not querying on annotations because tests can fail if there
    ###       are lots of jobs queued as happens when staging is syncing
    output = run('synapse', '--skip-checks',
                 'get', '-q', "select id from file where parentId=='%s'" %
                 folder_entity2.id)
    #Verify that we downloaded files from folder_entity2
    new_paths = [os.path.join('.', os.path.basename(f)) for f in uploaded_paths[:-1]]
    for downloaded, uploaded in zip(new_paths, uploaded_paths[:-1]):
        print(uploaded, downloaded)
        assert os.path.exists(downloaded)
        assert filecmp.cmp(downloaded, uploaded)
        schedule_for_cleanup(downloaded)

    schedule_for_cleanup(new_paths[0])

    ### Test query get using a Table with an entity column
    ### This should be replaced when Table File Views are implemented in the client
    cols = []
    cols.append(synapseclient.Column(name='id', columnType='ENTITYID'))

    schema1 = syn.store(synapseclient.Schema(name='Foo Table', columns=cols, parent=project_entity))
    schedule_for_cleanup(schema1.id)

    data1 =[[x.id] for x in file_entities]

    print(data1)

    row_reference_set1 = syn.store(synapseclient.RowSet(schema=schema1,
                                   rows=[synapseclient.Row(r) for r in data1]))

    time.sleep(3) # get -q uses chunkedQuery which are eventually consistent
    ### Test Table/View query get
    output = run('synapse', '--skip-checks', 'get', '-q',
                 "select id from %s" % schema1.id)
    #Verify that we downloaded files:
    new_paths = [os.path.join('.', os.path.basename(f)) for f in uploaded_paths[:-1]]
    new_paths.append(os.path.join('.', os.path.basename(uploaded_paths[-1])))
    schedule_for_cleanup(folder_entity.name)
    for downloaded, uploaded in zip(new_paths, uploaded_paths):
        print(uploaded, downloaded)
        assert os.path.exists(downloaded)
        assert filecmp.cmp(downloaded, uploaded)
        schedule_for_cleanup(downloaded)

    schedule_for_cleanup(new_paths[0])

def test_command_copy():
    """Tests the 'synapse cp' function"""

    # Create a Project
    project_entity = syn.store(synapseclient.Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(project_entity.id)

    # Create a Folder in Project
    folder_entity = syn.store(synapseclient.Folder(name=str(uuid.uuid4()),
                                                   parent=project_entity))
    schedule_for_cleanup(folder_entity.id)
    # Create and upload a file in Folder
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    annots = {'test':['hello_world']}
    # Create, upload, and set annotations on a file in Folder
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    file_entity = syn.store(synapseclient.File(filename, parent=folder_entity))
    externalURL_entity = syn.store(synapseclient.File(repo_url,name='rand',parent=folder_entity,synapseStore=False))
    syn.setAnnotations(file_entity,annots)
    syn.setAnnotations(externalURL_entity,annots)
    schedule_for_cleanup(file_entity.id)
    schedule_for_cleanup(externalURL_entity.id)

    ### Test cp function
    output = run('synapse', '--skip-checks',
                 'cp',file_entity.id,
                 '--destinationId',project_entity.id)
    output_URL = run('synapse', '--skip-checks',
                 'cp',externalURL_entity.id,
                 '--destinationId',project_entity.id)

    copied_id = parse(r'Copied syn\d+ to (syn\d+)',output)
    copied_URL_id = parse(r'Copied syn\d+ to (syn\d+)',output_URL)

    #Verify that our copied files are identical
    copied_ent = syn.get(copied_id)
    copied_URL_ent = syn.get(copied_URL_id,downloadFile=False)
    schedule_for_cleanup(copied_id)
    schedule_for_cleanup(copied_URL_id)
    copied_ent_annot = syn.getAnnotations(copied_id)
    copied_url_annot = syn.getAnnotations(copied_URL_id)

    copied_prov = syn.getProvenance(copied_id)['used'][0]['reference']['targetId']
    copied_url_prov = syn.getProvenance(copied_URL_id)['used'][0]['reference']['targetId']

    #Make sure copied files are the same
    assert copied_prov == file_entity.id
    assert copied_ent_annot == annots
    assert copied_ent.properties.dataFileHandleId == file_entity.properties.dataFileHandleId

    #Make sure copied URLs are the same
    assert copied_url_prov == externalURL_entity.id
    assert copied_url_annot == annots
    assert copied_URL_ent.externalURL == repo_url
    assert copied_URL_ent.name == 'rand'
    assert copied_URL_ent.properties.dataFileHandleId == externalURL_entity.properties.dataFileHandleId

    #Verify that errors are being thrown when a
    #file is copied to a folder/project that has a file with the same filename
    assert_raises(ValueError,run, 'synapse', '--debug', '--skip-checks',
                 'cp',file_entity.id,
                 '--destinationId',project_entity.id)

def test_command_line_using_paths():
    # Create a Project
    project_entity = syn.store(synapseclient.Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(project_entity.id)

    # Create a Folder in Project
    folder_entity = syn.store(synapseclient.Folder(name=str(uuid.uuid4()), parent=project_entity))

    # Create and upload a file in Folder
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    file_entity = syn.store(synapseclient.File(filename, parent=folder_entity))

    # Verify that we can use show with a filename
    output = run('synapse', '--skip-checks', 'show', filename)
    id = parse(r'File: %s\s+\((syn\d+)\)\s+' %os.path.split(filename)[1], output)
    assert file_entity.id == id

    # Verify that limitSearch works by making sure we get the file entity
    # that's inside the folder
    file_entity2 = syn.store(synapseclient.File(filename, parent=project_entity))
    output = run('synapse', '--skip-checks', 'get',
                 '--limitSearch', folder_entity.id,
                 filename)
    print("output = \"", output, "\"")
    id = parse(r'Associated file: .* with synapse ID (syn\d+)', output)
    name = parse(r'Associated file: (.*) with synapse ID syn\d+', output)
    assert_equals(file_entity.id, id)
    assert utils.equal_paths(name, filename)

    #Verify that set-provenance works with filepath
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    output = run('synapse', '--skip-checks', 'set-provenance',
                 '-id', file_entity2.id,
                 '-name', 'TestActivity',
                 '-description', 'A very excellent provenance',
                 '-used', filename,
                 '-executed', repo_url,
                 '-limitSearch', folder_entity.id)
    activity_id = parse(r'Set provenance record (\d+) on entity syn\d+', output)

    output = run('synapse', '--skip-checks', 'get-provenance',
                 '-id', file_entity2.id)
    activity = json.loads(output)
    assert activity['name'] == 'TestActivity'
    assert activity['description'] == 'A very excellent provenance'

    #Verify that store works with provenance specified with filepath
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    filename2 = utils.make_bogus_data_file()
    schedule_for_cleanup(filename2)
    output = run('synapse', '--skip-checks', 'add', filename2,
                 '-parentid', project_entity.id,
                 '-used', filename,
                 '-executed', '%s %s' %(repo_url, filename))
    entity_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)
    output = run('synapse', '--skip-checks', 'get-provenance',
                 '-id', entity_id)
    activity = json.loads(output)
    a = [a for a in activity['used'] if a['wasExecuted']==False]
    assert a[0]['reference']['targetId'] in [file_entity.id, file_entity2.id]

    #Test associate command
    #I have two files in Synapse filename and filename2
    path = tempfile.mkdtemp()
    schedule_for_cleanup(path)
    shutil.copy(filename, path)
    shutil.copy(filename2, path)
    output = run('synapse', '--skip-checks', 'associate', path, '-r')
    output = run('synapse', '--skip-checks', 'show', filename)

def test_table_query():
    """Test command line ability to do table query.

    """

    cols = []
    cols.append(synapseclient.Column(name='name', columnType='STRING', maximumSize=1000))
    cols.append(synapseclient.Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']))
    cols.append(synapseclient.Column(name='x', columnType='DOUBLE'))
    cols.append(synapseclient.Column(name='age', columnType='INTEGER'))
    cols.append(synapseclient.Column(name='cartoon', columnType='BOOLEAN'))

    project_entity = project

    schema1 = syn.store(synapseclient.Schema(name=str(uuid.uuid4()), columns=cols, parent=project_entity))
    schedule_for_cleanup(schema1.id)

    data1 =[['Chris',  'bar', 11.23, 45, False],
            ['Jen',    'bat', 14.56, 40, False],
            ['Jane',   'bat', 17.89,  6, False],
            ['Henry',  'bar', 10.12,  1, False]]

    row_reference_set1 = syn.store(synapseclient.RowSet(schema=schema1,
                                   rows=[synapseclient.Row(r) for r in data1]))

    # Test query
    output = run('synapse', '--skip-checks', 'query',
                 'select * from %s' % schema1.id)

    output_rows = output.rstrip("\n").split("\n")

    # Check the length of the output
    assert len(output_rows) == 5, "got %s rows" % (len(output_rows),)

    # Check that headers are correct.
    # Should be column names in schema plus the ROW_ID and ROW_VERSION
    my_headers_set = output_rows[0].split("\t")
    expected_headers_set = ["ROW_ID", "ROW_VERSION"] + list(map(lambda x: x.name, cols))
    assert my_headers_set == expected_headers_set, "%r != %r" % (my_headers_set, expected_headers_set)

def test_login():
    if not other_user['username']:
        raise SkipTest("Skipping test for login command: No [test-authentication] in %s" % client.CONFIG_FILE)

    with patch("synapseclient.client.Synapse._writeSessionCache") as write_session_cache_mock:
        alt_syn = synapseclient.Synapse()
        output = run('synapse', '--skip-checks', 'login',
                     '-u', other_user['username'],
                     '-p', other_user['password'],
                     '--rememberMe',
                     syn=alt_syn)
        cached_sessions = write_session_cache_mock.call_args[0][0]
        assert cached_sessions["<mostRecent>"] == other_user['username']
        assert other_user['username'] in cached_sessions
        assert alt_syn.username == other_user['username']
        assert alt_syn.apiKey is not None


def test_configPath():
    """Test using a user-specified configPath for Synapse configuration file.

    """

    tmp_config_file = tempfile.NamedTemporaryFile(suffix='.synapseConfig', delete=False)
    shutil.copyfile(synapseclient.client.CONFIG_FILE, tmp_config_file.name)

    # Create a File
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    output = run('synapse',
                 '--skip-checks',
                 '--configPath',
                 tmp_config_file.name,
                 'add',
                 '-name',
                 'BogusFileEntityTwo',
                 '-description',
                 'Bogus data to test file upload',
                 '-parentid',
                 project.id,
                 filename)
    file_entity_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)

    # Verify that we stored the file in Synapse
    f1 = syn.get(file_entity_id)
    fh = syn._getFileHandle(f1.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.S3FileHandle'


def _description_wiki_check(run_output, expected_description):
    entity_id = parse(r'Created.* entity:\s+(syn\d+)\s+', run_output)
    wiki = syn.getWiki(entity_id)
    assert_equals(expected_description, wiki.markdown)


def _create_temp_file_with_cleanup(specific_file_text = None):
    if specific_file_text:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as file:
            file.write(specific_file_text)
            filename = file.name
    else:
        filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    return filename


def test_create__with_description():
    output = run('synapse',
                 'create',
                 'Folder',
                 '-name',
                 str(uuid.uuid4()),
                 '-parentid',
                 project.id,
                 '--description',
                 description_text
                 )
    _description_wiki_check(output, description_text)


def test_store__with_description():
    output = run('synapse',
                 'store',
                 upload_filename,
                 '-name',
                 str(uuid.uuid4()),
                 '-parentid',
                 project.id,
                 '--description',
                 description_text
                 )
    _description_wiki_check(output, description_text)


def test_add__with_description():
    output = run('synapse',
                 'add',
                 upload_filename,
                 '-name',
                 str(uuid.uuid4()),
                 '-parentid',
                 project.id,
                 '--description',
                 description_text
                 )
    _description_wiki_check(output, description_text)


def test_create__with_descriptionFile():
    output = run('synapse',
                 'create',
                 'Folder',
                 '-name',
                 str(uuid.uuid4()),
                 '-parentid',
                 project.id,
                 '--descriptionFile',
                 desc_filename
                 )
    _description_wiki_check(output, description_text)


def test_store__with_descriptionFile():
    output = run('synapse',
                 'store',
                 upload_filename,
                 '-name',
                 str(uuid.uuid4()),
                 '-parentid',
                 project.id,
                 '--descriptionFile',
                 desc_filename
                 )
    _description_wiki_check(output, description_text)


def test_add__with_descriptionFile():
    output = run('synapse',
                 'add',
                 upload_filename,
                 '-name',
                 str(uuid.uuid4()),
                 '-parentid',
                 project.id,
                 '--descriptionFile',
                 desc_filename
                 )
    _description_wiki_check(output, description_text)


def test_create__update_description():
    name = str(uuid.uuid4())
    output = run('synapse',
                 'create',
                 'Folder',
                 '-name',
                 name,
                 '-parentid',
                 project.id,
                 '--descriptionFile',
                 desc_filename
                 )
    _description_wiki_check(output, description_text)
    output = run('synapse',
                 'create',
                 'Folder',
                 '-name',
                 name,
                 '-parentid',
                 project.id,
                 '--description',
                 update_description_text
                 )
    _description_wiki_check(output, update_description_text)

def test_store__update_description():
    name = str(uuid.uuid4())
    output = run('synapse',
                 'store',
                 upload_filename,
                 '-name',
                 name,
                 '-parentid',
                 project.id,
                 '--descriptionFile',
                 desc_filename
                 )
    _description_wiki_check(output, description_text)
    output = run('synapse',
                 'store',
                 upload_filename,
                 '-name',
                 name,
                 '-parentid',
                 project.id,
                 '--description',
                 update_description_text
                 )
    _description_wiki_check(output, update_description_text)

def test_add__update_description():
    name = str(uuid.uuid4())
    output = run('synapse',
                 'add',
                 upload_filename,
                 '-name',
                 name,
                 '-parentid',
                 project.id,
                 '--descriptionFile',
                 desc_filename
                 )
    _description_wiki_check(output, description_text)
    output = run('synapse',
                 'add',
                 upload_filename,
                 '-name',
                 name,
                 '-parentid',
                 project.id,
                 '--description',
                 update_description_text
                 )
    _description_wiki_check(output, update_description_text)
