"""
******
Tables
******

Table entity


syn.getColumns

syn.getTableColumns

"""
from synapseclient.exceptions import *
from synapseclient.dict_object import DictObject


class ColumnModel(DictObject):
    """
    """

    @classmethod
    def getURI(cls, id):
        return '/column/%s' % id


    def __init__(self, **kwargs):
        super(ColumnModel, self).__init__(kwargs)

    def postURI(self):
        return '/column'


# TableRowSet <-> data.frame / pandas / csv / matrix etc.
# RowSet

class RowSet(DictObject):
    """
    """

    @classmethod
    def getURI(cls, id):
        return '/column/%s' % id


    def __init__(self, **kwargs):
        super(RowSet, self).__init__(kwargs)

    def postURI(self):
        return '/column'

    def putURI(self):
        return None

    def deleteURI(self):
        return None



class Row(DictObject):

    def __init__(self, values, rowId=None, versionNumber=None):
        super(Row, self).__init__()
        self.values = values
        self.rowId = rowId
        self.versionNumber = versionNumber
