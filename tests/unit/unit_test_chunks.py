import synapseclient.utils as utils
import filecmp
import tempfile
import os

from synapseclient.utils import GB, MB, KB, nchunks, get_chunk


def setup():
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)


def test_chunks():
    # Read a file in chunks, write the chunks out, and compare to the original
    try:
        filepath = utils.make_bogus_binary_file(n=1*MB)
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as out:
            for i in range(1, nchunks(filepath, chunksize=64*1024)+1):
                out.write(get_chunk(filepath, i, chunksize=64*1024))
        assert filecmp.cmp(filepath, out.name)
    finally:
        if 'filepath' in locals() and filepath:
            os.remove(filepath)
        if 'out' in locals() and out:
            os.remove(out.name)

