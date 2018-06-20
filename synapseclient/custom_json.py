"""
When imported, monkey-patches the 'json' module's encoder with a custom json encoding function.
"""

from json import JSONEncoder
from datetime import datetime as Datetime
from .utils import datetime_to_iso


# monkey-patching JSONEncoder from
# https://stackoverflow.com/questions/18478287/making-object-json-serializable-with-regular-encoder
def _json_encoder(self, obj):
    if isinstance(obj, Datetime):
        # backend takes date string format of "yy-M-d H:m:s.SSS" with the time zone being UTC
        return datetime_to_iso(obj, sep=" ").replace("Z", '')

    else:
        return getattr(obj.__class__, "to_json", _json_encoder.default)(obj)


_json_encoder.default = JSONEncoder().default  # Save unmodified default.
JSONEncoder.default = _json_encoder  # replacement
