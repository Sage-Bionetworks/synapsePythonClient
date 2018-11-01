from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from nose.tools import assert_equals
import json
import datetime
import time


def test_to_json():
    json_str = '{some json str}'

    class JsonTest:
        def to_json(self):
            return json_str

    assert_equals(json_str, json.loads(json.dumps(JsonTest())))


def test_datetime_json():
    datetime_obj = datetime.datetime.fromtimestamp(round(time.time(), 3))
    datetime_json_str = json.dumps(datetime_obj)
    datetime_from_json = datetime.datetime.strptime(json.loads(datetime_json_str), "%Y-%m-%d %H:%M:%S.%f")
    assert_equals(datetime_obj, datetime_from_json)

