
import functools
import traceback
import sys
from multiprocessing import Value, Lock
from synapseclient.core.utils import printTransferProgress


def notifyMe(syn, messageSubject='', retries=0):
    """Function decorator that notifies you via email whenever an function completes running or there is a failure.

    :param syn:             A synapse object as obtained with syn = synapseclient.login()
    :param messageSubject:  A string with subject line for sent out messages.
    :param retries:         Number of retries to attempt on failure (default=0)

    Example::
    
        # to decorate a function that you define
        from synapseutils import notifyMe
        import synapseclient
        syn = synapseclient.login()
        
        @notifyMe(syn, 'Long running function', retries=2)
        def my_function(x):
            doing_something()
            return long_runtime_func(x)
        
        my_function(123)
        
        #############################
        # to wrap a function that already exists
        from synapseutils import notifyMe
        import synapseclient
        syn = synapseclient.login()
        
        notify_decorator = notifyMe(syn, 'Long running query', retries=2)
        my_query = notify_decorator(syn.tableQuery)
        results = my_query("select id from syn1223")
    
        #############################
    """
    def notify_decorator(func):
        @functools.wraps(func)
        def with_retry_and_messaging(*args, **kwargs):
            attempt = 0
            destination = syn.getUserProfile()['ownerId']
            while attempt <= retries:
                try:
                    output = func(*args, **kwargs)
                    syn.sendMessage([destination],  messageSubject,
                                    messageBody='Call to %s completed successfully!' % func.__name__)
                    return output
                except Exception as e:
                    sys.stderr.write(traceback.format_exc())
                    syn.sendMessage([destination], messageSubject,
                                    messageBody=('Encountered a temporary Failure during upload.  '
                                                 'Will retry %i more times. \n\n Error message was:\n%s\n\n%s'
                                                 % (retries-attempt, e, traceback.format_exc())))
                    attempt += 1
        return with_retry_and_messaging
    return notify_decorator


def with_progress_bar(func, totalCalls, prefix='', postfix='', isBytes=False):
    """Wraps a function to add a progress bar based on the number of calls to that function.

    :param func:        Function being wrapped with progress Bar
    :param totalCalls:  total number of items/bytes when completed
    :param prefix:      String printed before progress bar
    :param prefix:      String printed after progress bar
    :param isBytes:     A boolean indicating weather to convert bytes to kB, MB, GB etc.

    :return: a wrapped function that contains a progress bar
    """
    completed = Value('d', 0)
    lock = Lock()

    def progress(*args, **kwargs):
        with lock:
            completed.value += 1
        printTransferProgress(completed.value, totalCalls, prefix, postfix, isBytes)
        return func(*args, **kwargs)
    return progress
