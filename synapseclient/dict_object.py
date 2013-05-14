##
## Represent user-defined annotations on a synapse entity
## chris.bare@sagebase.org
############################################################
import collections
import json

class DictObject(dict):
    def __init__(self, *args, **kwargs):
        self.__dict__ = self
        for arg in args:
            if isinstance(arg, collections.Mapping):
                self.__dict__.update(arg)
        self.__dict__.update(kwargs)


    def __str__(self):
        return json.dumps(self, sort_keys=True, indent=2)
