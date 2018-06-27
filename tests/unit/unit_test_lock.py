import random
import time
from threading import Thread
from datetime import timedelta
from synapseclient.lock import Lock


def test_lock():
    user1_lock = Lock("foo", max_age=timedelta(seconds=5))
    user2_lock = Lock("foo", max_age=timedelta(seconds=5))

    assert user1_lock.acquire()
    assert user1_lock.get_age() < 5
    assert not user2_lock.acquire()

    user1_lock.release()

    assert user2_lock.acquire()
    assert not user1_lock.acquire()

    user2_lock.release()


def test_with_lock():
    user1_lock = Lock("foo", max_age=timedelta(seconds=5))
    user2_lock = Lock("foo", max_age=timedelta(seconds=5))

    with user1_lock:
        assert user1_lock.get_age() < 5
        assert not user2_lock.acquire()

    with user2_lock:
        assert user2_lock.acquire()
        assert not user1_lock.acquire()


def test_lock_timeout():
    user1_lock = Lock("foo", max_age=timedelta(seconds=1))
    user2_lock = Lock("foo", max_age=timedelta(seconds=1))

    with user1_lock:
        assert user1_lock.held
        assert user1_lock.get_age() < 1.0
        assert not user2_lock.acquire(break_old_locks=True)
        time.sleep(1.1)
        assert user1_lock.get_age() > 1.0
        assert user2_lock.acquire(break_old_locks=True)


# Try to hammer away at the locking mechanism from multiple threads
NUMBER_OF_TIMES_PER_THREAD = 3


def do_stuff_with_a_locked_resource(name, event_log):
    lock = Lock("foo", max_age=timedelta(seconds=5))
    for i in range(NUMBER_OF_TIMES_PER_THREAD):
        with lock:
            event_log.append((name, i))
        time.sleep(random.betavariate(2, 5))


def test_multithreaded():
    event_log = []

    threads = [Thread(target=do_stuff_with_a_locked_resource, args=("thread %d" % i, event_log)) for i in range(4)]

    for thread in threads:
        thread.start() 

    for thread in threads:
        thread.join()

    counts = {}
    for event in event_log:
        counts.setdefault(event[0], set())
        counts[event[0]].add(event[1])

    for key in counts:
        assert counts[key] == set(range(NUMBER_OF_TIMES_PER_THREAD))
