'''
Created on Sep 21, 2017

@author: bhoff
'''
import synapseclient.dozer as doze

def teardown():
    doze.clear_listeners()
    
counter=0
   
def test_doze():
    global counter
    counter=0
    
    def counter_inc():
        global counter
        counter = counter+1
    
    # register Listener
    doze.add_listener(counter_inc)
    doze.doze(1) # should call counter_inc() about 10 times
    assert counter > 0
    