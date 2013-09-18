import filecmp
import os
import re
import sys
import uuid
import json
from cStringIO import StringIO

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
    file_entity_id = parse(r'Created entity:\s+(syn\d+)\s+', output)

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
                 'update', 
                 '-id', 
                 file_entity_id, 
                 filename)
    updated_entity_id = parse(r'Updated entity:\s+(syn\d+)', output)

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
    data_entity_id = parse(r'Created entity:\s+(syn\d+)\s+', output)

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
                'update', 
                '-id', 
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
                 '-id', 
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
    exteral_entity_id = parse(r'Created entity:\s+(syn\d+)\s+', output)

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
    project_id = parse(r'Created entity:\s+(syn\d+)\s+', output)
    schedule_for_cleanup(project_id)

    # Create and upload a file
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    output = run('synapse',
                 '--skip-checks',
                 'store',
                 '--name',
                 'BogusFileEntity',
                 '--description',
                 'Bogus data to test file upload', 
                 '--parentid',
                 project_id,
                 '--file',
                 filename)
    file_entity_id = parse(r'Created entity:\s+(syn\d+)\s+', output)
    
    # Verify that we stored the file in Synapse
    f1 = syn.get(file_entity_id)
    fh = syn._getFileHandle(f1.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.S3FileHandle'
    
    # Create an Evaluation to submit to
    eval = Evaluation(name=str(uuid.uuid4()), contentSource=project_id)
    eval = syn.store(eval)
    syn.joinEvaluation(eval)
    
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
    
    # Submit an updated bogus file
    output = run('synapse', 
                 '--skip-checks', 
                 'submit', 
                 '--evaluation', 
                 eval.id, 
                 '--entity',
                 file_entity_id)
    submission_id = parse(r'Submitted \(id: (\d+)\) entity:\s+', output)

    # Tests shouldn't have external dependencies, but here it's required
    ducky_url = 'http://upload.wikimedia.org/wikipedia/commons/9/93/Rubber_Duck.jpg'

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
    exteral_entity_id = parse(r'Created entity:\s+(syn\d+)\s+', output)

    # Verify that we created an external file handle
    f2 = syn.get(exteral_entity_id)
    fh = syn._getFileHandle(f2.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.ExternalFileHandle'

    # Delete project
    output = run('synapse', 
                 '--skip-checks',
                 'delete',
                 project_id)
