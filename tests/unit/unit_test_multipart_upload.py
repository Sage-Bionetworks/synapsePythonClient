import filecmp, math, os, tempfile
from nose.tools import assert_raises
from synapseclient.multipart_upload import find_parts_to_upload, count_completed_parts, calculate_part_size, get_file_chunk
from synapseclient.utils import MB, GB, make_bogus_binary_file


def test_find_parts_to_upload():
    assert find_parts_to_upload("") == []
    assert find_parts_to_upload("111111111111111111") == []
    assert find_parts_to_upload("01010101111111110") == [1,3,5,7,17]
    assert find_parts_to_upload("00000") == [1,2,3,4,5]

def test_count_completed_parts():
    assert count_completed_parts("") == 0
    assert count_completed_parts("01010101111111110") == 12
    assert count_completed_parts("00000") == 0
    assert count_completed_parts("11111") == 5


def test_calculate_part_size():
    assert 5*MB <= calculate_part_size(fileSize=3*MB,       partSize=None, min_part_size=5*MB, max_parts=10000) == 5*MB
    assert 5*MB <= calculate_part_size(fileSize=6*MB,       partSize=None, min_part_size=5*MB, max_parts=2) == 5*MB
    assert 5*MB <= calculate_part_size(fileSize=11*MB,      partSize=None, min_part_size=5*MB, max_parts=2) == 11*MB / 2.0
    assert 5*MB <= calculate_part_size(fileSize=100*MB,     partSize=None, min_part_size=5*MB, max_parts=2) >= (100*MB) / 2.0
    assert 5*MB <= calculate_part_size(fileSize=11*MB+777,  partSize=None, min_part_size=5*MB, max_parts=2) >= (11*MB+777) / 2.0
    assert 5*MB <= calculate_part_size(fileSize=101*GB+777, partSize=None, min_part_size=5*MB, max_parts=10000) >= (101*GB+777) / 10000.0

    ## OK
    assert calculate_part_size(6*MB, partSize=10*MB, min_part_size=5*MB, max_parts=10000) == 10*MB

    ## partSize too small
    assert_raises(ValueError, calculate_part_size, fileSize=100*MB, partSize=1*MB, min_part_size=5*MB, max_parts=10000)

    ## too many parts
    assert_raises(ValueError, calculate_part_size, fileSize=21*MB, partSize=1*MB, min_part_size=1*MB, max_parts=20)

def test_chunks():
    # Read a file in chunks, write the chunks out, and compare to the original
    try:
        file_size = 1*MB
        filepath = make_bogus_binary_file(n=file_size)
        chunksize=64*1024
        nchunks = int(math.ceil( float(file_size) / chunksize))
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as out:
            for i in range(1, nchunks+1):
                out.write(get_file_chunk(filepath, i, chunksize))
        assert filecmp.cmp(filepath, out.name)
    finally:
        if 'filepath' in locals() and filepath:
            os.remove(filepath)
        if 'out' in locals() and out:
            os.remove(out.name)
