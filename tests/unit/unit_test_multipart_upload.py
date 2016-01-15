import filecmp, math, os, tempfile
from synapseclient.multipart_upload import find_parts_to_download, count_completed_parts, partition, calculate_part_size, get_file_chunk
from synapseclient.utils import MB, GB, make_bogus_binary_file


def test_find_parts_to_download():
    assert find_parts_to_download("") == []
    assert find_parts_to_download("111111111111111111") == []
    assert find_parts_to_download("01010101111111110") == [1,3,5,7,17]
    assert find_parts_to_download("00000") == [1,2,3,4,5]

def test_count_completed_parts():
    assert count_completed_parts("") == 0
    assert count_completed_parts("01010101111111110") == 12
    assert count_completed_parts("00000") == 0
    assert count_completed_parts("11111") == 5

def test_partition():
    assert list(partition(5, [])) == []
    assert list(partition(3, list(range(10)))) == [[0,1,2], [3,4,5], [6,7,8], [9]]
    assert list(partition(4, list(range(10)))) == [[0,1,2,3], [4,5,6,7], [8,9]]
    assert list(partition(10, [1,2,3])) == [[1,2,3]]

def test_calculate_part_size():
    assert calculate_part_size(3*MB, 10000) == 5*MB
    assert calculate_part_size(6*MB, 2) == 5*MB
    assert calculate_part_size(11*MB, 2) == 11*MB / 2.0
    assert calculate_part_size(100*MB, 2) * 2 >= (100*MB)
    assert calculate_part_size(11*MB+777, 2) * 2 >= (11*MB+777)
    assert calculate_part_size(101*GB+777, 10000) * 10000 >= (101*GB+777)

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
