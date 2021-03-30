import filecmp
import os
import tempfile
import shutil
import time

import pytest

from synapseclient import File
from synapseclient.core.exceptions import SynapseMd5MismatchError
import synapseclient.core.utils as utils


def test_download_check_md5(syn, project, schedule_for_cleanup):
    tempfile_path = utils.make_bogus_data_file()
    schedule_for_cleanup(tempfile_path)
    entity_bad_md5 = syn.store(File(path=tempfile_path, parent=project['id'], synapseStore=False))

    pytest.raises(SynapseMd5MismatchError, syn._download_from_URL, entity_bad_md5['externalURL'], tempfile.gettempdir(),
                  entity_bad_md5['dataFileHandleId'], expected_md5="2345a")


def test_resume_partial_download(syn, project, schedule_for_cleanup):
    original_file = utils.make_bogus_data_file(40000)

    entity = File(original_file, parent=project['id'])
    entity = syn.store(entity)

    # stash the original file for comparison later
    shutil.move(original_file, original_file+'.original')
    original_file += '.original'
    schedule_for_cleanup(original_file)

    temp_dir = tempfile.gettempdir()

    url = '%s/entity/%s/file' % (syn.repoEndpoint, entity.id)
    path = syn._download_from_URL(url, destination=temp_dir, fileHandleId=entity.dataFileHandleId,
                                  expected_md5=entity.md5)

    # simulate an imcomplete download by putting the
    # complete file back into its temporary location
    tmp_path = utils.temp_download_filename(temp_dir, entity.dataFileHandleId)
    shutil.move(path, tmp_path)

    # ...and truncating it to some fraction of its original size
    with open(tmp_path, 'r+') as f:
        f.truncate(3*os.path.getsize(original_file)//7)

    # this should complete the partial download
    path = syn._download_from_URL(url, destination=temp_dir, fileHandleId=entity.dataFileHandleId,
                                  expected_md5=entity.md5)

    assert filecmp.cmp(original_file, path), "File comparison failed"


def test_http_download__range_request_error(syn, project):
    # SYNPY-525

    file_path = utils.make_bogus_data_file()
    file_entity = syn.store(File(file_path, parent=project))

    syn.cache.remove(file_entity['_file_handle']['id'])
    # download once and rename to temp file to simulate range exceed
    file_entity = syn.get(file_entity)
    shutil.move(file_entity.path, utils.temp_download_filename(file_entity.path, file_entity.dataFileHandleId))
    file_entity = syn.get(file_entity)

    assert file_path != file_entity.path
    assert filecmp.cmp(file_path, file_entity.path)
