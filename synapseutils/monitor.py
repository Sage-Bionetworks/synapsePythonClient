from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys
import functools
import traceback

def notifyMe(func, syn, messageSubject='', retries=0):
    """Function decorator that notifies you via email whenever an function completes running or 
    there is a failure.

    :param syn:    A synapse object as obtained with syn = synapseclient.login()

    :param messageSubject: A string with subject line for sent out messages.

    :param retries: Number of retries to attempt on failure (default=0)

    Example::
         import synapseutils
         import synapseclient
         syn = synapseclient.login()
    
         my_query = notifyMe(syn.tableQuery, syn, 'Long running query', retries=2)
         results = my_query("select id from syn1223")
    """
    @functools.wraps(func)
    def with_retry_and_messaging(*args, **kwargs):
        attempt = 0
        destination = syn.getUserProfile()['ownerId']
        while attempt<=retries:
            try:
                output = func(*args, **kwargs)
                syn.sendMessage([destination],  messageSubject,
                                'Call to %s completed successfully!' %func.__name__)
                return output
            except Exception as e:
                sys.stderr.write(traceback.format_exc())
                syn.sendMessage([destination], messageSubject, 
                                messageBody = ('Encountered a temporary Failure during upload.  '
                                               'Will retry %i more times. \n\n Error message was:\n%s\n\n%s'
                                               %(retries-attempt, e, traceback.format_exc())))
                attempt +=1
    return with_retry_and_messaging


def with_progress_bar(func, totalCalls, prefix = '', postfix='', isBytes=False):
    """Adds a progress bar to calls to a function

    :param func: Function being wrapped with progress Bar
    :param totalCalls: total number of items/bytes when completed
    :param prefix: String printed before progress bar
    :param prefix: String printed after progress bar
    :param isBytes: A boolean indicating weather to convert bytes to kB, MB, GB etc.
    """
    from multiprocessing import Value, Lock
    completed = Value('d', 0)
    lock = Lock()

    def progress(*args, **kwargs):
        with lock:
            completed.value +=1
        synapseclient.utils.printTransferProgress(completed.value, totalCalls, prefix, postfix, isBytes)
        return func(*args, **kwargs)
    return progress
