"""
Created on Sep 21, 2017

@author: bhoff
"""
import synapseclient.core.dozer as doze


def teardown():
    doze.clear_listeners()


def test_doze():
    class CounterClass(object):
        def __init__(self):
            self.val = 0

        def __call__(self):
            self.val = self.val + 1

    counter = CounterClass()

    # register Listener
    doze.add_listener(counter)
    doze.doze(1)  # should call counter_inc() about 10 times
    assert counter.val > 0
