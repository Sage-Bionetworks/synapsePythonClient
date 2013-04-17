## test command line client
############################################################
import filecmp
import os
import re
import subprocess
import tempfile
import utils
import uuid
import json


# def run_command(command, **kwargs):
#     p = subprocess.Popen(command, shell=True,
#                          stdout=subprocess.PIPE,
#                          stderr=subprocess.STDOUT,
#                          **kwargs)
#     return iter(p.stdout.readline, b'')

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
            return m.group(1)
    else:
        raise Exception("ERROR parsing output: " + str(output))


def setup():
    ## collect stuff to clean up later
    globals()['to_clean_up'] = []

def teardown():
    for item in to_clean_up:
        if utils.is_synapse_id(item):
            try:
                run("synapse delete %s" % item)
            except Exception as ex:
                print ex
        else:
            try:
                os.remove(item)
            except Exception as ex:
                print ex


def test_command_line_client():
    """Test command line client"""

    ## create project
    output = run("synapse create -name '%s' -description 'test of command line client' Project" % str(str(uuid.uuid4())))
    project_id = parse(r'Created entity:\s+(syn\d+)\s+', output)
    to_clean_up.append(project_id)


    ## create file
    filename = utils.make_bogus_data_file()
    to_clean_up.append(filename)
    output = run("synapse add -name 'BogusFileEntity' -description 'Bogus data to test file upload' -parentid %s %s" % (project_id, filename,))
    file_entity_id = parse(r'Created entity:\s+(syn\d+)\s+', output)


    ## get file
    output = run("synapse get %s" % file_entity_id)
    downloaded_filename = parse(r'creating\s+(.*)', output)

    to_clean_up.append(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)


    ## update file
    filename = utils.make_bogus_data_file()
    to_clean_up.append(filename)
    output = run("synapse update -id %s %s" % (file_entity_id, filename,))
    updated_entity_id = parse(r'Updated entity:\s+(syn\d+)', output)


    ## get file
    output = run("synapse get %s" % file_entity_id)
    downloaded_filename = parse(r'creating\s+(.*)', output)

    to_clean_up.append(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)


    ## create data
    filename = utils.make_bogus_data_file()
    to_clean_up.append(filename)

    output = run("synapse add -name 'BogusData' -description 'Bogus data to test file upload' -type Data -parentid %s %s" % (project_id, filename,))
    data_entity_id = parse(r'Created entity:\s+(syn\d+)\s+', output)


    ## get data
    output = run("synapse get %s" % data_entity_id)
    downloaded_filename = parse(r'creating\s+(.*)', output)

    to_clean_up.append(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)


    ## update data
    filename = utils.make_bogus_data_file()
    to_clean_up.append(filename)

    output = run("synapse update -id %s %s" % (data_entity_id, filename,))
    updated_entity_id = parse(r'Updated entity:\s+(syn\d+)', output)


    ## get data
    output = run("synapse get %s" % data_entity_id)
    downloaded_filename = parse(r'creating\s+(.*)', output)

    to_clean_up.append(downloaded_filename)
    assert os.path.exists(downloaded_filename)
    assert filecmp.cmp(filename, downloaded_filename)


    # query
    output = run("synapse query 'select id, name from entity where parentId==\"%s\"'" % project_id)
    assert 'BogusData' in output
    assert data_entity_id in output
    assert 'BogusFileEntity' in output
    assert file_entity_id in output


    # provenance
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    output = run("synapse set-provenance -id %s -name TestActivity -description 'A very excellent provenance' -used %s -executed '%s'" % (file_entity_id, data_entity_id, repo_url,))
    activity_id = parse(r'Set provenance record (\d+) on entity syn\d+', output)

    output = run("synapse get-provenance -id %s" % (file_entity_id,))
    activity = json.loads(output)
    assert activity['name'] == 'TestActivity'
    assert activity['description'] == 'A very excellent provenance'
    
    used = utils._findUsed(activity, lambda used: 'reference' in used)
    assert used['reference']['targetId'] == data_entity_id

    used = utils._findUsed(activity, lambda used: 'url' in used)
    assert used['url'] == repo_url
    assert used['wasExecuted'] == True


    ## delete project
    output = run("synapse delete %s" % project_id)
    to_clean_up.remove(project_id)
