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
from synapseclient.utils import id_of



# org.sagebionetworks.repo.model.table.ColumnModel
# id           STRING           The immutable ID issued to new columns
# columnType   STRING           The type of the column must be from this enumeration:
#                               'STRING', 'DOUBLE', 'LONG', 'BOOLEAN', 'FILEHANDLEID'
# maximumSize  INTEGER          A parameter for columnTypes with a maximum size. For example, ColumnType.STRINGs
#                                have a default maximum size of 50 characters, but can be set to a maximumSize of
#                                1 to 1000 characters.
# name         STRING           The display name of the column
# enumValues   ARRAY<STRING>    Columns type of STRING can be constrained to an enumeration values set on this list.
# defaultValue

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


# RowSet
# org.sagebionetworks.repo.model.table.RowSet
# headers       ARRAY<STRING>  The list of ColumnModels ID that describes the rows of this set.
# etag          STRING         Any RowSet returned from Synapse will contain the current etag of the change set. To update any rows from a RowSet the etag must be provided with the POST.
# tableId       STRING         The ID of the TableEntity than owns these rows
# rows          ARRAY<Row>     The Rows of this set. The index of each row value aligns with the index of each header.

class RowSet(DictObject):
    """
    """

    @classmethod
    def getURI(cls, id):
        return '/column/%s' % id

    def __init__(self, columns=None, table=None, **kwargs):
        if columns:
            kwargs.setdefault('headers',[]).extend([id_of(column) for column in columns])
        if table:
            kwargs['tableId'] = id_of(table)
        super(RowSet, self).__init__(kwargs)

    def postURI(self):
        return '/entity/{id}/table'.format(id=self['tableId'])


# Row
# org.sagebionetworks.repo.model.table.Row
# values        ARRAY<STRING>  The values for each column of this row.
# rowId         INTEGER        The immutable ID issued to a new row.
# versionNumber INTEGER        The version number of this row. Each row version is immutable, so when a row is updated a new version is created.

class Row(DictObject):

    def __init__(self, values, rowId=None, versionNumber=None):
        super(Row, self).__init__()
        self.values = values
        self.rowId = rowId
        self.versionNumber = versionNumber
