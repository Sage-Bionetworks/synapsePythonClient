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

#COLUMN_TYPES = ['STRING', 'DOUBLE', 'LONG', 'BOOLEAN', 'DATE', 'FILEHANDLEID']
DTYPE_2_TABLETYPE = {'?':'BOOLEAN',
                     'd': 'DOUBLE', 'g': 'DOUBLE', 'e': 'DOUBLE', 'f': 'DOUBLE',
                     'b': 'LONG', 'B': 'LONG', 'h': 'LONG', 'H': 'LONG',
                     'i': 'LONG', 'I': 'LONG', 'l': 'LONG', 'L': 'LONG',
                     'm': 'LONG', 'q': 'LONG', 'Q': 'LONG',
                     'S': 'STRING', 'U': 'STRING', 'O': 'STRING'}


def df2Table(df, tableName, parentProject):
    """Creates a new table from data in pandas data frame.
    parameters: df, tableName, parentProject
    """

    #Create columns:
    cols = list()
    for col in df:
        columnType = DTYPE_2_TABLETYPE[df[col].dtype.char]
        if columnType == 'STRING':
            size = min(1000, max(30, df[col].str.len().max()*1.5))  #Determine lenght of longest string
            cols.append(ColumnModel(name=col, columnType=columnType, maximumSize=size, defaultValue=''))
        else:
            cols.append(ColumnModel(name=col, columnType=columnType))
    cols = [syn.store(col) for col in cols]

    #Create Table
    table1 = Table(name=tableName, columns=cols, parent=parentProject)
    table1 = syn.store(table1)


    #Add data to Table
    for i in range(12, df.shape[0]/250+1):
        start =  i*250
        end = min((i+1)*250, df.shape[0])
        print start, end
        rowset1 = RowSet(columns=cols, table=table1,
                         rows=[Row(list(df.ix[j,:])) for j in range(start,end)])
        rowset1 = syn.store(rowset1)

    return table1


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
