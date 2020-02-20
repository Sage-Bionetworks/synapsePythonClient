import errno
import os
import shutil
import sys
import time
import datetime

from synapseclient.core.exceptions import *
from synapseclient.core.dozer import doze

LOCK_DEFAULT_MAX_AGE = datetime.timedelta(seconds=10)
DEFAULT_BLOCKING_TIMEOUT = datetime.timedelta(seconds=70)
CACHE_UNLOCK_WAIT_TIME = 0.5


class LockedException(Exception):
    pass


class Lock(object):
    """
    Implements a lock by making a directory named [lockname].lock
    """
    SUFFIX = 'lock'

    def __init__(self, name, dir=None, max_age=LOCK_DEFAULT_MAX_AGE, default_blocking_timeout=DEFAULT_BLOCKING_TIMEOUT):
        self.name = name
        self.held = False
        self.dir = dir if dir else os.getcwd()
        self.lock_dir_path = os.path.join(self.dir, ".".join([name, Lock.SUFFIX]))
        self.max_age = max_age
        self.default_blocking_timeout = default_blocking_timeout

    def get_age(self):
        try:
            return time.time() - os.path.getmtime(self.lock_dir_path)
        except OSError as err:
            if err.errno != errno.ENOENT and err.errno != errno.EACCES:
                raise
            return 0

    def acquire(self, break_old_locks=True):
        """Try to acquire lock. Return True on success or False otherwise"""
        if self.held:
            return True
        try:
            os.makedirs(self.lock_dir_path)
            self.held = True
            # Make sure the modification times are correct
            # On some machines, the modification time could be seconds off
            os.utime(self.lock_dir_path, (0, time.time()))
        except OSError as err:
            if err.errno != errno.EEXIST and err.errno != errno.EACCES:
                raise
            # already locked...
            if break_old_locks and self.get_age() > self.max_age.total_seconds():
                sys.stderr.write("Breaking lock whose age is: %s\n" % self.get_age())
                self.held = True
                # Make sure the modification times are correct
                # On some machines, the modification time could be seconds off
                os.utime(self.lock_dir_path, (0, time.time()))
            else:
                self.held = False
        return self.held

    def blocking_acquire(self, timeout=None, break_old_locks=True):
        if self.held:
            return True
        if timeout is None:
            timeout = self.default_blocking_timeout
        lock_acquired = False
        tryLockStartTime = time.time()
        while time.time() - tryLockStartTime < timeout.total_seconds():
            lock_acquired = self.acquire(break_old_locks)
            if lock_acquired:
                break
            else:
                doze(CACHE_UNLOCK_WAIT_TIME)
        if not lock_acquired:
            raise SynapseFileCacheError("Could not obtain a lock on the file cache within timeout: %s  "
                                        "Please try again later" % str(timeout))

    def release(self):
        """Release lock or do nothing if lock is not held"""
        if self.held:
            try:
                shutil.rmtree(self.lock_dir_path)
                self.held = False
            except OSError as err:
                if err.errno != errno.ENOENT:
                    raise

    # Make the lock object a Context Manager
    def __enter__(self):
        self.blocking_acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
