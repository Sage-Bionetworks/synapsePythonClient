"""
Created on Sep 21, 2017

@author: bhoff
"""
import synapseclient.dozer as doze
from nose.tools import assert_greater


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
    assert_greater(counter.val, 0)
