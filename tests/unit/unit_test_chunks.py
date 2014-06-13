import synapseclient.utils as utils
import filecmp
import tempfile
import os

KB = 2**10

def setup():
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)

def test_chunks():
    # Read a file in chunks, write the chunks out, and compare to the original
    try:
        filepath = utils.make_bogus_binary_file()
        with open(filepath, 'rb') as f, tempfile.NamedTemporaryFile(mode='wb', delete=False) as out:
            for chunk in utils.chunks(f):
                buff = chunk.read(4*KB)
                while buff:
                    out.write(buff)
                    buff = chunk.read(4*KB)
        assert filecmp.cmp(filepath, out.name)
    finally:
        if 'filepath' in locals() and filepath:
            os.remove(filepath)
        if 'out' in locals() and out:
            os.remove(out.name)
