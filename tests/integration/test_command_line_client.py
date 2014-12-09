import filecmp
import os
import re
import sys
import uuid
import json
from cStringIO import StringIO
from nose.plugins.attrib import attr
from nose.tools import assert_raises
import tempfile
import shutil

import synapseclient
import synapseclient.utils as utils
import synapseclient.__main__ as cmdline
from synapseclient.evaluation import Evaluation

import integration
from integration import schedule_for_cleanup


def setup_module(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = integration.syn
    module.parser = cmdline.build_parser()


def run(*command):
    """
    Sends the given command list to the command line client.
    
    :returns: The STDOUT output of the command.
    """
    
    print ' '.join(command)
    old_stdout = sys.stdout
    capturedSTDOUT = StringIO()
    try:
        sys.stdout = capturedSTDOUT
        sys.argv = [item for item in command]
        args = parser.parse_args()
        cmdline.perform_main(args, syn)
    except SystemExit:
        pass # Prevent the test from quitting prematurely
    finally:
        sys.stdout = old_stdout
        
    capturedSTDOUT = capturedSTDOUT.getvalue()
    print capturedSTDOUT
    return capturedSTDOUT
    

def parse(regex, output):
    """Returns the first match."""
    
    m = re.search(regex, output)
    if m:
        if len(m.groups()) > 0:
            return m.group(1).strip()
    else:
        raise Exception('ERROR parsing output: ' + str(output))


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
    downloaded_filename = parse(r'Creating\s+(.*)', output)
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
    downloaded_filename = parse(r'Creating\s+(.*)', output)
    schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)

    # Create a deprecated Data object
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    output = run('synapse', 
                 '--skip-checks', 
                 'add', 
                 '-name', 
                 'BogusData', 
                 '-description', 
                 'Bogus data to test file upload',
                 '-type', 
                 'Data', 
                 '-parentid', 
                 project_id, 
                 filename)
    data_entity_id = parse(r'Created/Updated entity:\s+(syn\d+)\s+', output)

    # Get the Data object back
    output = run('synapse', 
                 '--skip-checks', 
                 'get', 
                 data_entity_id)
    downloaded_filename = parse(r'Creating\s+(.*)', output)
    schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)

    # Update the Data object
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    output = run('synapse', 
                '--skip-checks', 
                'store', 
                '--id', 
                data_entity_id, 
                filename)
    updated_entity_id = parse(r'Updated entity:\s+(syn\d+)', output)
    
    # Get the Data object back again
    output = run('synapse', 
                 '--skip-checks', 
                 'get', 
                 updated_entity_id)
    downloaded_filename = parse(r'Creating\s+(.*)', output)
    schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)

    # Test query
    output = run('synapse', 
                 '--skip-checks', 
                 'query', 
                 'select id, name from entity where parentId=="%s"' % project_id)
    assert 'BogusData' in output
    assert data_entity_id in output
    assert 'BogusFileEntity' in output
    assert file_entity_id in output

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
                 data_entity_id, 
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
    assert used['reference']['targetId'] == data_entity_id
    
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
                 '-type', 
                 'File', 
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
    downloaded_filename = parse(r'Creating\s+(.*)', output)
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

    try:
        bar = annotations['bar']
    except KeyError:
        pass

    try:
        baz = annotations['baz']
    except KeyError:
        pass

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
                 '-type', 
                 'Data', 
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
                 '--type', 
                 'File', 
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
    syn.joinEvaluation(eval)
    schedule_for_cleanup(eval)
    
    # Submit a bogus file
    output = run('synapse', 
                 '--skip-checks',
                 'submit',
                 '--evaluation',
                 eval.id, 
                 '--name',
                 'Some random name',
                 '--teamName',
                 'My Team',
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
                 '--teamName',
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
                 '--pid',
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
    
    #Verify that limitSearch works by using get
    #Store same file in project as well
    file_entity2 = syn.store(synapseclient.File(filename, parent=project_entity))
    output = run('synapse', '--skip-checks', 'get', 
                 '--limitSearch', folder_entity.id, 
                 filename)
    name = parse(r'Creating\s+\.\%s(%s)\s+' % (os.path.sep, os.path.split(filename)[1]), output)
    assert name == os.path.split(filename)[1]
    schedule_for_cleanup('./'+name)

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
