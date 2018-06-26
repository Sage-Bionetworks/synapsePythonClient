import unit
import filecmp
import math
import os
import tempfile
from nose.tools import assert_raises, assert_true, assert_greater_equal
from synapseclient.multipart_upload import find_parts_to_upload, count_completed_parts, calculate_part_size,\
    get_file_chunk, _upload_chunk
from synapseclient.utils import MB, GB, make_bogus_binary_file
from synapseclient.exceptions import SynapseHTTPError
from synapseclient import multipart_upload
from multiprocessing import Value
from multiprocessing.dummy import Pool
from ctypes import c_bool
from mock import patch, MagicMock
import warnings


def setup(module):
    module.syn = unit.syn


def test_find_parts_to_upload():
    assert find_parts_to_upload("") == []
    assert find_parts_to_upload("111111111111111111") == []
    assert find_parts_to_upload("01010101111111110") == [1, 3, 5, 7, 17]
    assert find_parts_to_upload("00000") == [1, 2, 3, 4, 5]


def test_count_completed_parts():
    assert count_completed_parts("") == 0
    assert count_completed_parts("01010101111111110") == 12
    assert count_completed_parts("00000") == 0
    assert count_completed_parts("11111") == 5


def test_calculate_part_size():
    assert 5*MB <= calculate_part_size(fileSize=3*MB,
                                       partSize=None, min_part_size=5*MB, max_parts=10000) == 5*MB
    assert 5*MB <= calculate_part_size(fileSize=6*MB,
                                       partSize=None, min_part_size=5*MB, max_parts=2) == 5*MB
    assert 5*MB <= calculate_part_size(fileSize=11*MB,
                                       partSize=None, min_part_size=5*MB, max_parts=2) == 11*MB / 2.0
    assert 5*MB <= calculate_part_size(fileSize=100*MB,
                                       partSize=None, min_part_size=5*MB, max_parts=2) >= (100*MB) / 2.0
    assert 5*MB <= calculate_part_size(fileSize=11*MB+777,
                                       partSize=None, min_part_size=5*MB, max_parts=2) >= (11*MB+777) / 2.0
    assert 5*MB <= calculate_part_size(fileSize=101*GB+777,
                                       partSize=None, min_part_size=5*MB, max_parts=10000) >= (101*GB+777) / 10000.0

    # return value should always be an integer (SYNPY-372)
    assert type(calculate_part_size(fileSize=3*MB+3391)) is int
    assert type(calculate_part_size(fileSize=50*GB+4999)) is int
    assert type(calculate_part_size(fileSize=101*GB+7717, min_part_size=8*MB)) is int

    # OK
    assert calculate_part_size(6*MB, partSize=10*MB, min_part_size=5*MB, max_parts=10000) == 10*MB

    # partSize too small
    assert_raises(ValueError, calculate_part_size, fileSize=100*MB, partSize=1*MB, min_part_size=5*MB, max_parts=10000)

    # too many parts
    assert_raises(ValueError, calculate_part_size, fileSize=21*MB, partSize=1*MB, min_part_size=1*MB, max_parts=20)


def test_chunks():
    # Read a file in chunks, write the chunks out, and compare to the original
    try:
        file_size = 1*MB
        filepath = make_bogus_binary_file(n=file_size)
        chunksize = 64*1024
        nchunks = int(math.ceil(float(file_size) / chunksize))
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as out:
            for i in range(1, nchunks+1):
                out.write(get_file_chunk(filepath, i, chunksize))
        assert filecmp.cmp(filepath, out.name)
    finally:
        if 'filepath' in locals() and filepath:
            os.remove(filepath)
        if 'out' in locals() and out:
            os.remove(out.name)


def test_upload_chunk__expired_url():
    upload_parts = [{'uploadPresignedUrl': 'https://www.fake.url/fake/news',
                     'partNumber': 420},
                    {'uploadPresignedUrl': 'https://www.google.com',
                     'partNumber': 421},
                    {'uploadPresignedUrl': 'https://rito.pls/',
                     'partNumber': 422},
                    {'uploadPresignedUrl': 'https://never.lucky.gg',
                     'partNumber': 423}
                    ]

    value_doesnt_matter = None
    expired = Value(c_bool, False)
    mocked_get_chunk_function = MagicMock(side_effect=[1, 2, 3, 4])

    with patch.object(multipart_upload, "_put_chunk",
                      side_effect=SynapseHTTPError("useless message",
                                                   response=MagicMock(status_code=403))) as mocked_put_chunk, \
         patch.object(warnings, "warn") as mocked_warn:
        chunk_upload = lambda part: _upload_chunk(part, completed=value_doesnt_matter, status=value_doesnt_matter,
                                                  syn=syn, filename=value_doesnt_matter,
                                                  get_chunk_function=mocked_get_chunk_function,
                                                  fileSize=value_doesnt_matter, partSize=value_doesnt_matter,
                                                  t0=value_doesnt_matter,
                                                  expired=expired, bytes_already_uploaded=value_doesnt_matter)
        # 2 threads both with urls that have expired
        mp = Pool(4)
        mp.map(chunk_upload, upload_parts)
        assert_true(expired.value)

        # assert warnings.warn was only called once
        mocked_warn.assert_called_once_with("The pre-signed upload URL has expired. Restarting upload...\n")

        # assert _put_chunk was called at least once
        assert_greater_equal(len(mocked_put_chunk.call_args_list), 1)
