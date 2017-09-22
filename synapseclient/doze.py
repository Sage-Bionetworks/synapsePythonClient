'''
Created on Sep 21, 2017

@author: bhoff

sleep while checking registered listeners
'''
import time

listeners=[]

def add_listener(listener):
    global listeners
    listeners.append(listener)
    
def clear_listeners():
    global listeners
    listeners=[]
    
    
def doze(secs, listener_check_interval_secs=0.1):
    global listeners
    end_time = time.time()+secs
    while time.time()<end_time:
        for listener in listeners:
            listener()
        time.sleep(listener_check_interval_secs)
        
    