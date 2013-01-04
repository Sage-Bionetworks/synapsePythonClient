#!/usr/bin/env python2.7

# To debug this, python -m pdb myscript.py

import os, urllib, hashlib

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


## download a remote file
## localFilePath can be None, in which case a temporary file is created
## returns a tuple (localFilePath, HTTPmsg), see urlib.urlretrieve
def downloadFile(url, localFilepath=None):
    if (localFilepath):
        dir = os.path.dirname(localFilepath)
        if not os.path.exists(dir):
            os.makedirs(dir)
    return urllib.urlretrieve (url, localFilepath)
