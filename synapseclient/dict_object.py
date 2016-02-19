##
## Represent user-defined annotations on a synapse entity
## chris.bare@sagebase.org
############################################################
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

import collections
import json

class DictObject(dict):

    @classmethod
    def getByNameURI(cls, name):
        print('%s can\'t be retrieved by name' %cls)
        raise ValueError


    def __init__(self, *args, **kwargs):
        self.__dict__ = self
        for arg in args:
            if isinstance(arg, collections.Mapping):
                self.__dict__.update(arg)
        self.__dict__.update(kwargs)


    def __str__(self):
        return json.dumps(self, sort_keys=True, indent=2)


    def json(self, ensure_ascii=True):
        return json.dumps(self, sort_keys=True, indent=2, ensure_ascii=ensure_ascii)
