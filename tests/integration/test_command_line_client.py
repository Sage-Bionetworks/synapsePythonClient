## test command line client
############################################################
## to run tests: nosetests -vs tests/integration/test_command_line_client.py
import filecmp
import os
import re
import subprocess
import uuid
import json

import synapseclient
import synapseclient.utils as utils

import integration
from integration import schedule_for_cleanup



def setup_module(module):
    module.syn = integration.syn


def run(command, **kwargs):
    print command
    try:
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT, **kwargs)
        print output
        return output
    except Exception as ex:
        print '~' * 60
        print ex
        if hasattr(ex, 'output'):
            print ex.output
        raise

def parse(regex, output):
    m = re.match(regex, output)
    if m:
        if len(m.groups()) > 0:
            return m.group(1).strip()
    else:
        raise Exception('ERROR parsing output: ' + str(output))


def test_command_line_client():
    """Test command line client"""

    ## Note:
    ## In Windows, quoting with single-quotes, as in -name 'My entity name'
    ## seems to cause problems in the call to subprocess.check_output. Make
    ## sure to use double-quotes.

    ## create project
    output = run('synapse create -name "%s" -description "test of command line client" Project' % str(str(uuid.uuid4())))
    project_id = parse(r'Created entity:\s+(syn\d+)\s+', output)
    schedule_for_cleanup(project_id)


    ## create file
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    output = run('synapse add -name "BogusFileEntity" -description "Bogus data to test file upload" -parentid %s %s' % (project_id, filename,))
    file_entity_id = parse(r'Created entity:\s+(syn\d+)\s+', output)

    ## verify that we stored the file in synapse
    f1 = syn.get(file_entity_id)
    fh = syn._getFileHandle(f1.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.S3FileHandle'

    ## get file
    output = run('synapse get %s' % file_entity_id)
    downloaded_filename = parse(r'creating\s+(.*)', output)

    schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)


    ## update file
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    output = run('synapse update -id %s %s' % (file_entity_id, filename,))
    updated_entity_id = parse(r'Updated entity:\s+(syn\d+)', output)


    ## get file
    output = run('synapse get %s' % file_entity_id)
    downloaded_filename = parse(r'creating\s+(.*)', output)

    schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)


    ## create data
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)

    output = run('synapse add -name "BogusData" -description "Bogus data to test file upload" -type Data -parentid %s %s' % (project_id, filename,))
    data_entity_id = parse(r'Created entity:\s+(syn\d+)\s+', output)


    ## get data
    output = run('synapse get %s' % data_entity_id)
    downloaded_filename = parse(r'creating\s+(.*)', output)

    schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)


    ## update data
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)

    output = run('synapse update -id %s %s' % (data_entity_id, filename,))
    updated_entity_id = parse(r'Updated entity:\s+(syn\d+)', output)


    ## get data
    output = run('synapse get %s' % data_entity_id)
    downloaded_filename = parse(r'creating\s+(.*)', output)

    schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)


    # query
    output = run('synapse query "select id, name from entity where parentId==\\"%s\\""' % project_id)
    assert 'BogusData' in output
    assert data_entity_id in output
    assert 'BogusFileEntity' in output
    assert file_entity_id in output


    # provenance
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    output = run('synapse set-provenance -id %s -name TestActivity -description "A very excellent provenance" -used %s -executed "%s"' % (file_entity_id, data_entity_id, repo_url,))
    activity_id = parse(r'Set provenance record (\d+) on entity syn\d+', output)

    output = run('synapse get-provenance -id %s' % (file_entity_id,))
    activity = json.loads(output)
    assert activity['name'] == 'TestActivity'
    assert activity['description'] == 'A very excellent provenance'
    
    used = utils._find_used(activity, lambda used: 'reference' in used)
    assert used['reference']['targetId'] == data_entity_id

    used = utils._find_used(activity, lambda used: 'url' in used)
    assert used['url'] == repo_url
    assert used['wasExecuted'] == True


    ## Tests shouldn't have external dependencies, but this is a pretty picture of Singapore
    singapore_url = 'http://upload.wikimedia.org/wikipedia/commons/thumb/3/3e/1_singapore_city_skyline_dusk_panorama_2011.jpg/1280px-1_singapore_city_skyline_dusk_panorama_2011.jpg'

    ## test external file handle
    output = run('synapse add -name "Singapore" -description "A nice picture of Singapore" -type File -parentid %s %s' % (project_id, singapore_url,))
    exteral_entity_id = parse(r'Created entity:\s+(syn\d+)\s+', output)

    ## verify that we created an external file handle
    f2 = syn.get(exteral_entity_id)
    fh = syn._getFileHandle(f2.dataFileHandleId)
    assert fh['concreteType'] == 'org.sagebionetworks.repo.model.file.ExternalFileHandle'

    output = run('synapse get %s' % exteral_entity_id)
    downloaded_filename = parse(r'creating\s+(.*)', output)

    schedule_for_cleanup(downloaded_filename)
    assert os.path.exists(downloaded_filename)


    ## delete project
    output = run('synapse delete %s' % project_id)
