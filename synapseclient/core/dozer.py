"""
Created on Sep 21, 2017

@author: bhoff

sleep while checking registered _listeners
"""
import time

from opentelemetry import trace

tracer = trace.get_tracer("synapseclient")

_listeners = []


def add_listener(listener):
    if not callable(listener):
        raise ValueError("listener is not callable")
    _listeners.append(listener)


def clear_listeners():
    del _listeners[:]


def doze(secs, listener_check_interval_secs=0.1, trace_span_name="doze") -> None:
    with tracer.start_as_current_span(name=trace_span_name):
        end_time = time.time() + secs
        while time.time() < end_time:
            for listener in _listeners:
                listener()
            time.sleep(listener_check_interval_secs)
