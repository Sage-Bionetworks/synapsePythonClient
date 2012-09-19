#!/usr/bin/env python2.7

# To debug this, python -m pdb myscript.py

import os, argparse, csv, codecs, cStringIO, urllib, hashlib, base64, subprocess, shlex

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
    

def downloadFile(url, localFilepath):
    dir = os.path.dirname(localFilepath)
    if not os.path.exists(dir):
        os.makedirs(dir)
    urllib.urlretrieve (url, localFilepath)
    
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
