"""
******
Tables
******

Table entity


~~~~~~
Column
~~~~~~

.. autoclass:: synapseclient.table.Column
   :members: __init__


See:

 - :py:meth:`synapseclient.Synapse.getColumns`
 - :py:meth:`synapseclient.Synapse.getTableColumns`
 - :py:meth:`synapseclient.Synapse.get`
 - :py:meth:`synapseclient.Synapse.store`
 - :py:meth:`synapseclient.Synapse.delete`
"""
import json
import synapseclient
from synapseclient.exceptions import *
from synapseclient.dict_object import DictObject
from synapseclient.utils import id_of, query_limit_and_offset
from synapseclient.entity import Entity, Versionable


#COLUMN_TYPES = ['STRING', 'DOUBLE', 'LONG', 'BOOLEAN', 'DATE', 'FILEHANDLEID']
DTYPE_2_TABLETYPE = {'?':'BOOLEAN',
                     'd': 'DOUBLE', 'g': 'DOUBLE', 'e': 'DOUBLE', 'f': 'DOUBLE',
                     'b': 'LONG', 'B': 'LONG', 'h': 'LONG', 'H': 'LONG',
                     'i': 'LONG', 'I': 'LONG', 'l': 'LONG', 'L': 'LONG',
                     'm': 'LONG', 'q': 'LONG', 'Q': 'LONG',
                     'S': 'STRING', 'U': 'STRING', 'O': 'STRING'}


def df2Table(df, syn,  tableName, parentProject):
    """Creates a new table from data in pandas data frame.
    parameters: df, tableName, parentProject
    """

    #Create columns:
    print df.shape
    cols = list()
    for col in df:
        columnType = DTYPE_2_TABLETYPE[df[col].dtype.char]
        if columnType == 'STRING':
            size = min(1000, max(30, df[col].str.len().max()*1.5))  #Determine lenght of longest string
            cols.append(Column(name=col, columnType=columnType, maximumSize=size, defaultValue=''))
        else:
            cols.append(Column(name=col, columnType=columnType))
    cols = [syn.store(col) for col in cols]

    #Create Table Schema
    schema1 = Schema(name=tableName, columns=cols, parent=parentProject)
    schema1 = syn.store(schema1)


    #Add data to Table
    for i in range(0, df.shape[0]/1200+1):
        start =  i*1200
        end = min((i+1)*1200, df.shape[0])
        print start, end
        rowset1 = RowSet(columns=cols, schema=schema1,
                         rows=[Row(list(df.ix[j,:])) for j in range(start,end)])
        #print len(rowset1.rows)
        rowset1 = syn.store(rowset1)

    return schema1


class Schema(Entity, Versionable):
    """
    A schema defines the set of columns in a table.
    """
    _property_keys = Entity._property_keys + Versionable._property_keys + ['columnIds']
    _local_keys = Entity._local_keys
    _synapse_entity_type = 'org.sagebionetworks.repo.model.table.TableEntity'

    def __init__(self, name=None, columns=None, parent=None, properties=None, annotations=None, local_state=None, **kwargs):
        if name: kwargs['name'] = name
        if columns:
            kwargs.setdefault('columnIds',[]).extend([id_of(column) for column in columns])
        super(Schema, self).__init__(concreteType=Schema._synapse_entity_type, properties=properties, 
                                   annotations=annotations, local_state=local_state, parent=parent, **kwargs)

    def addColumn(self, column):
        self.columnIds.append(id_of(column))

    def removeColumn(self, column):
        self.columnIds.remove(id_of(column))

synapseclient.entity._entity_type_to_class[Schema._synapse_entity_type] = Schema


class Column(DictObject):
    """
    Defines a column to be used in a table :py:class:`synapseclient.table.Schema`.

    :var id:              An immutable ID issued by the platform
    :param columnType:    Can be any of: "STRING", "DOUBLE", "INTEGER", "BOOLEAN", "DATE", "FILEHANDLEID", "ENTITYID"
    :param maximumSize:   A parameter for columnTypes with a maximum size. For example, ColumnType.STRINGs have a default maximum size of 50 characters, but can be set to a maximumSize of 1 to 1000 characters.
    :param name:          The display name of the column
    :param enumValues:    Columns type of STRING can be constrained to an enumeration values set on this list.
    :param defaultValue:  The default value for this column. Columns of type FILEHANDLEID and ENTITYID are not allowed to have default values.

    :type id: string
    :type maximumSize: integer
    :type columnType: string
    :type name: string
    :type enumValues: array of strings
    :type defaultValue: string
    """

    @classmethod
    def getURI(cls, id):
        return '/column/%s' % id

    def __init__(self, **kwargs):
        super(Column, self).__init__(kwargs)

    def postURI(self):
        return '/column'


class RowSet(DictObject):
    """
    A Synapse object of type `org.sagebionetworks.repo.model.table.RowSet <http://rest.synapse.org/org/sagebionetworks/repo/model/table/RowSet.html>`_.

    :param schema:   A :py:class:`synapseclient.table.Schema` object that will be used to set the tableId    
    :param headers:  The list of Column IDs that describe the rows of this set.
    :param tableId:  The ID of the TableEntity than owns these rows
    :param rows:     The :py:class:`synapseclient.table.Row`s of this set. The index of each row value aligns with the index of each header.
    :var etag:       Any RowSet returned from Synapse will contain the current etag of the change set. To update any rows from a RowSet the etag must be provided with the POST.

    :type headers:   array of string
    :type etag:      string
    :type tableId:   string
    :type rows:      array of rows
    """

    def __init__(self, columns=None, schema=None, **kwargs):
        if columns:
            kwargs.setdefault('headers',[]).extend([id_of(column) for column in columns])
        if schema:
            kwargs['tableId'] = id_of(schema)
        super(RowSet, self).__init__(kwargs)

    def postURI(self):
        return '/entity/{id}/table'.format(id=self['tableId'])

    def _synapse_delete(self, syn):
        """
        Delete the rows in the RowSet.
        Example::
            syn.delete(syn.queryTable('select name from %s where no_good = true' % schema1.id))
        """
        uri = '/entity/{id}/table/deleteRows'.format(id=self.tableId)
        return syn.restPOST(uri, body=json.dumps(RowSelection(
            rowIds=[row.rowId for row in self.rows],
            etag=self.etag,
            tableId=self.tableId)))


# Row
# org.sagebionetworks.repo.model.table.Row
# values        ARRAY<STRING>  The values for each column of this row.
# rowId         INTEGER        The immutable ID issued to a new row.
# versionNumber INTEGER        The version number of this row. Each row version is immutable, so when a row is updated a new version is created.

class Row(DictObject):
    """
    A row in a Table.
    """
    def __init__(self, values, rowId=None, versionNumber=None):
        super(Row, self).__init__()
        self.values = values
        self.rowId = rowId
        self.versionNumber = versionNumber


class RowSelection(DictObject):
    """
    A set of rows to be deleted.
    http://rest.synapse.org/POST/entity/id/table/deleteRows.html
    """
    def __init__(self, rowIds, etag, tableId):
        super(RowSelection, self).__init__()
        self.rowIds = rowIds
        self.etag = etag
        self.tableId = tableId

    def _synapse_delete(self, syn):
        """
        Delete the rows.
        Example::
            row_selection = RowSelection(
                rowIds=[1,2,3,4],
                etag="64d265c0-ef5b-4598-a50d-ddcbe71abc61",
                tableId="syn1234567")
            syn.delete(row_selection)
        """
        uri = '/entity/{id}/table/deleteRows'.format(id=self.tableId)
        return syn.restPOST(uri, body=json.dumps(self))


# limit
# pagesize
# does asRowSet return the whole thing? just parts?
# convert columns back to their natural types?
# import from CSV
# download CSV

class TableQueryResult(object):
    """
    An object to wrap rows returned as a result of a table query.

    The TableQueryResult object can be used to iterate over results of a query:

        results = syn.queryTable("select * from syn1234")
        for row in results:
            print row

    """
    def __init__(self, synapse, query, limit=None, offset=None, isConsistent=True, partMask=None):
        self.syn = synapse

        self.query = query
        self.limit = limit
        self.offset = offset

        self.isConsistent = isConsistent
        self.partMask = partMask

        result = self.syn._queryTable(
            query=query,
            limit=limit,
            offset=offset,
            isConsistent=self.isConsistent,
            partMask=partMask)

        self.rowset = result['queryResult']['queryResults']
        self.nextPageToken = result['queryResult'].get('nextPageToken', None)
        self.columns = [Column(**columnModel) for columnModel in result.get('selectColumns', [])]
        self.count = result.get('queryCount', None)
        self.maxRowsPerPage = result.get('maxRowsPerPage', None)
        self.etag = self.rowset.get('etag', None)
        self.tableId = self.rowset.get('tableId', None)
        self.i = -1

    def asDataFrame(self):
        raise NotImplementedError

    def asRowSet(self):
        ## Note that as of stack 60, an empty query will omit the headers field
        ## see PLFM-3014
        return RowSet(headers=self.rowset.get('headers', [col.id for col in self.columns]),
                      tableId=self.rowset['tableId'],
                      etag=self.rowset['etag'],
                      rows=[row for row in self])

    def asInteger(self):
        try:
            return int(self.rowset['rows'][0]['values'][0])
        except:
            raise ValueError("asInteger is only valid for queries such as count queries whose first value is an integer.")

    def _synapse_delete(self, syn):
        """
        Delete the rows that result from a table query.
        Example::
            syn.delete(syn.queryTable('select name from %s where no_good = true' % schema1.id))
        """
        uri = '/entity/{id}/table/deleteRows'.format(id=self.tableId)
        return syn.restPOST(uri, body=json.dumps(RowSelection(
            rowIds=[row['rowId'] for row in self.rowset['rows']],
            etag=self.etag,
            tableId=self.tableId)))

    def __iter__(self):
        return self

    def next(self):
        self.i += 1
        if self.i >= len(self.rowset['rows']):
            if self.nextPageToken:
                result = self.syn._queryTableNext(self.nextPageToken)
                self.rowset = result['queryResults']
                self.nextPageToken = result.get('nextPageToken', None)
                self.i = 0
            else:
                raise StopIteration()
        return self.rowset['rows'][self.i]

