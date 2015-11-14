import synapseclient
from synapseclient import Project, File
import synapseclient.utils as utils
from datetime import datetime
import filecmp
import os, traceback
import argparse
import random
from synapseclient.utils import MB, GB


syn = None


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = synapseclient.Synapse()
    module.syn.login()




def test_upload_speed(uploadSize=60 + 777771, threadCount=5):
    import time
    fh = None
    filepath = utils.make_bogus_binary_file(uploadSize*MB)
    print 'Made bogus file: ', filepath
    try:
        t0 = time.time()
        fh = syn._chunkedUploadFile(filepath, threadCount=threadCount)
        dt =  time.time()-t0
    finally:
        try:
            os.remove(filepath)
        except Exception:
            print traceback.format_exc()
        if fh:
            syn._deleteFileHandle(fh)
    return dt



def main():
    import pandas as pd
    import numpy as np
    global syn
    syn = synapseclient.Synapse()
    syn.login(silent=True)
    sizes = [1,5,10,100,500,1000]
    threads =  [1,2,4,6,8,16]

    results = pd.DataFrame(np.zeros((len(sizes), len(threads))), columns=threads, index=sizes)
    results.index.aname = 'Size (Mb)'
    for size in sizes:
        for thread in threads:
            results.ix[size,thread] = test_upload_speed(size, thread)
            print results
        print 


if __name__ == "__main__":
    main()

