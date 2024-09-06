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


def doze(
    secs: float,
    listener_check_interval_secs: float = 0.1,
    trace_span_name: str = "doze",
) -> None:
    """Sleep for a given number of seconds while checking registered listeners.

    Arguments:
        secs: the number of seconds to sleep
        listener_check_interval_secs: the interval at which to check the listeners
        trace_span_name: the name of the trace span
    """
    with tracer.start_as_current_span(name=trace_span_name):
        end_time = time.time() + secs
        while time.time() < end_time:
            for listener in _listeners:
                listener()
            time.sleep(listener_check_interval_secs)
