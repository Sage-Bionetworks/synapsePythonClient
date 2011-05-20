#!/usr/bin/env python2.7

# To debug this, python -m pdb myscript.py

import os, argparse, csv, codecs, cStringIO, hashlib, base64, subprocess, shlex

def createBasicArgParser(description):
    '''
    Make an argument parser with a few default args
    '''
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--debug',
                        help='whether to output verbose information for '
                        + 'debugging purposes, defaults to False',
                        action='store_true', default=False)

    return parser

def computeMd5ForFile(filename, block_size=2**20):
    '''
    lifted this function from
    http://stackoverflow.com/questions/1131220/get-md5-hash-of-a-files-without-open-it-in-python
    '''
    md5 = hashlib.md5()
    f = open(filename,'rb')
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return(md5)

def uploadToS3(localFilepath, s3url, md5):
    '''
    The -f flag to curl will cause this to throw an exception upon failure to upload
    '''
    # Test that we can read the file
    f = open(localFilepath,'rb')
    f.close()

    subprocess.check_output(shlex.split(
            'curl -f -X PUT -H Content-MD5:' + base64.b64encode(md5)
            + ' --data-binary @' + localFilepath
            + ' -H x-amz-acl:bucket-owner-full-control ' + s3url))

def sendEmail():
# TODO sample impl here http://docs.python.org/faq/library.html
    return

#-----[ Classes to deal with latin_1 extended chars] ---------
class UTF8Recoder:
    '''
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    '''
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode('utf-8')

class UnicodeReader:
    '''
    A CSV reader which will iterate over lines in the CSV file 'f',
    which is encoded in the given encoding.
    '''

    def __init__(self, f, dialect=csv.excel, encoding='utf-8', **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, 'utf-8') for s in row]

    def __iter__(self):
        return self

class UnicodeWriter:
    '''
    A CSV writer which will write rows to CSV file 'f',
    which is encoded in the given encoding.
    '''

    def __init__(self, f, dialect=csv.excel, encoding='utf-8', **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode('utf-8') for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode('utf-8')
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

#------- UNIT TESTS -----------------
if __name__ == '__main__':
    import unittest

    class TestUtils(unittest.TestCase):

        #def setUp(self):

        def test_fakeTest(self):
            self.assertEqual("foo", "foo")
    
    unittest.main()
