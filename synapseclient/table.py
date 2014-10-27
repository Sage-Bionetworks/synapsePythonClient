"""
******
Tables
******

Synapse Tables enable storage of tabular data in Synapse in a form that can be
queried using a SQL-like query language.

A table has a :py:class:`Schema` defined in terms of :py:class:`Column` objects
that specify types from the following choices: STRING, DOUBLE, INTEGER,
BOOLEAN, DATE, ENTITYID, FILEHANDLEID. Tables hold sets of rows that conform to
the schema.

~~~~~~~
Example
~~~~~~~

Preliminaries::

    import synapseclient
    from synapseclient import Project, File, Folder, Schema
    from synapseclient.table import Column, as_table_columns, create_table

    syn = synapseclient.Synapse()
    syn.login()

    project = syn.get('syn123')

To create a Table, you first need to create a Table :py:class:`Schema`. This
defines the columns of the table::

    cols = [
        Column(name='Name', columnType='STRING', maximumSize=20),
        Column(name='Chromosome', columnType='STRING', maximumSize=20),
        Column(name='Start', columnType='INTEGER'),
        Column(name='End', columnType='INTEGER'),
        Column(name='Strand', columnType='STRING', enumValues=['+', '-'], maximumSize=1),
        Column(name='TranscriptionFactor', columnType='BOOLEAN')]

    schema = Schema(name='My Favorite Genes', columns=cols, parent=project)

Next, let's load some data into the table from a CSV file::

    table = create_table(schema, "/path/to/genes.csv")
    table = syn.store(table)

The :py:func:`create_table` function takes two arguments, a schema object and
data in some form, which can be:

  * a path to a CSV file
  * a `Pandas <http://pandas.pydata.org/>`_ `DataFrame <http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe>`_
  * a :py:class:`RowSet` object

With a bit of luck, we now have a table populated with data. Let's try to query::

    results = syn.queryTable("select * from %s where Chromosome='1' and Start < 900000 and End > 880000" % table.schema.id)
    for row in results:
        print row['values']

------
Pandas
------

`Pandas <http://pandas.pydata.org/>`_ is a popular library for working with
tabular data. If you have Pandas installed, the goal is that Synapse Tables
will play nice with it.

Create a Synapse Table from a `DataFrame <http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe>`_::

    import pandas as pd

    filepath = '/path/to/samples.csv'

    df = pd.DataFrame.from_csv(filepath, header=0, sep='\t', index_col=False)
    schema = Schema(name='Samples', columns=as_table_columns(df), parent=project)
    table = syn.store(create_table(schema, df))

Get query results as a `DataFrame <http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe>`_::

    results = syn.queryTable("select * from %s where age > 90" % table.schema.id)
    df = results.asDataFrame()

--------------
Making changes
--------------

Updates come in two flavors: appending new rows and updating existing ones.

**Appending** new rows is fairly straightforward. To continue the previous
example, we might add some new genes from another file::

    table = syn.store(create_table(table.schema.id, "/path/to/more_genes.csv"))

**Updating** rows requires an etag, which identifies the most recent change
set plus row IDs and version numbers for each row to be modified. We get
those by querying before updating. Minimizing changesets to contain only rows
that actually change will make the process faster.

For example, let's update the names of some of our favorite genes::

    results = syn.queryTable("select * from %s where Chromosome='3'" %table.schema.id)
    df = results.asDataFrame()
    df['Name'] = ['rzing', 'zing1', 'zing2', 'zing3', 'zing4']

Note that we're propagating the etag from the query results. Without it, we'd
get an error saying something about an "Invalid etag"::

    table = syn.store(create_table(schema, df, etag=results.etag))

The etag is used by the server to prevent concurrent users from making
conflicting changes, a technique called optimistic concurrency control. In case
of a conflict, your update may be rejected. You then have to do another query
an try your update again.

-------------
Deleting rows
-------------

Query for the rows you want to delete and call syn.delete on the results::

    results = syn.queryTable("select * from %s where Chromosome='3'" %table.schema.id)
    a = syn.delete(results)

------------------------
Deleting the whole table
------------------------

Deleting the schema deletes the whole table and all rows::

    syn.delete(schema)

~~~~~~~
Queries
~~~~~~~

The query language is quite similar to SQL select statements, except that joins
are not supported. The documentation for the Synapse API has lots of
`query examples <http://rest.synapse.org/org/sagebionetworks/repo/web/controller/TableExamples.html>`_.

~~~~~~
Schema
~~~~~~

.. autoclass:: synapseclient.table.Schema
   :members:

~~~~~~
Column
~~~~~~

.. autoclass:: synapseclient.table.Column
   :members: __init__

~~~~~~
Row
~~~~~~

.. autoclass:: synapseclient.table.Row
   :members: __init__

~~~~~~
Table
~~~~~~

.. autoclass:: synapseclient.table.Table
   :members:

~~~~~~~~~~~~~~~~~~~~
Module level methods
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: as_table_columns

.. autofunction:: create_table

See also:
 - :py:meth:`synapseclient.Synapse.getColumns`
 - :py:meth:`synapseclient.Synapse.getTableColumns`
 - :py:meth:`synapseclient.Synapse.queryTable`
 - :py:meth:`synapseclient.Synapse.get`
 - :py:meth:`synapseclient.Synapse.store`
 - :py:meth:`synapseclient.Synapse.delete`
"""
import csv
import json
import os
import re
import sys
import tempfile
from collections import OrderedDict
from itertools import izip

import synapseclient
from synapseclient.exceptions import *
from synapseclient.dict_object import DictObject
from synapseclient.utils import id_of, query_limit_and_offset, normalize_whitespace
from synapseclient.entity import Entity, Versionable



aggregate_pattern = re.compile(r'(count|max|min|avg|sum)\((.+)\)')

DTYPE_2_TABLETYPE = {'?':'BOOLEAN',
                     'd': 'DOUBLE', 'g': 'DOUBLE', 'e': 'DOUBLE', 'f': 'DOUBLE',
                     'b': 'INTEGER', 'B': 'INTEGER', 'h': 'INTEGER', 'H': 'INTEGER',
                     'i': 'INTEGER', 'I': 'INTEGER', 'l': 'INTEGER', 'L': 'INTEGER',
                     'm': 'INTEGER', 'q': 'INTEGER', 'Q': 'INTEGER',
                     'S': 'STRING', 'U': 'STRING', 'O': 'STRING'}


def test_import_pandas():
    try:
        import pandas as pd
    except ImportError as e1:
        sys.stderr.write("""\n\nPandas not installed!\n
        The synapseclient package recommends but doesn't require the
        installation of Pandas. If you'd like to use Pandas DataFrames,
        refer to the installation instructions at:
          http://pandas.pydata.org/.
        \n\n\n""")
        raise


def as_table_columns(df):
    """
    Return a list of Synapse table :py:class:`Column` objects that correspond to
    the columns in the given `Pandas DataFrame <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html>`_.

    :params df: `Pandas DataFrame <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html>`_
    :returns: A list of Synapse table :py:class:`Column` objects
    """
    # TODO: support Categorical when fully supported in Pandas Data Frames
    cols = list()
    for col in df:
        columnType = DTYPE_2_TABLETYPE[df[col].dtype.char]
        if columnType == 'STRING':
            size = min(1000, max(30, df[col].str.len().max()*1.5))  #Determine lenght of longest string
            cols.append(Column(name=col, columnType=columnType, maximumSize=size, defaultValue=''))
        else:
            cols.append(Column(name=col, columnType=columnType))
    return cols


def df2Table(df, syn, tableName, parentProject):
    """Creates a new table from data in pandas data frame.
    parameters: df, tableName, parentProject
    """

    #Create columns:
    print df.shape
    cols = as_table_columns(df)
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


def to_boolean(value):
    """
    Convert a string to boolean, case insensitively, where true values are:
    true, t, and 1 and false values are: false, f, 0. Raise a ValueError
    for all other values.
    """
    if isinstance(value, bool):
        return value

    if isinstance(value, basestring):
        lower_value = value.lower()
        if lower_value in ['true', 't', '1']:
            return True
        if lower_value in ['false', 'f', '0']:
            return False

    raise ValueError("Can't convert %s to boolean." % value)


def cast_row(row, columns, headers):
    """
    Convert a row of table query results from strings to the correct column type.

    See: http://rest.synapse.org/org/sagebionetworks/repo/model/table/ColumnType.html
    """
    if len(row) != len(headers):
        raise ValueError('Each field in the row must have a matching header.')

    if columns:
        col_map = {col['id']:col for col in columns}
    else:
        col_map = {}

    result = []
    for header, field in izip(headers, row):

        if header in ('ROW_ID', 'ROW_VERSION'):
            result.append(field)
        else:
            ## check for aggregate columns
            m = aggregate_pattern.match(header.lower())
            if m:
                function = m.group(1)
                column_id = m.group(2)
                if function=='count':
                    type = 'INTEGER'
                elif function in ['avg', 'max', 'min', 'sum']:
                    type = 'DOUBLE'
                # else: ## max, min, sum
                #     type = col_map[column_id]['columnType']
                else:
                    type = 'STRING'
            else:
                type = col_map[header]['columnType'] if header in col_map else 'STRING'

            ## convert field to column type
            if type in ['STRING', 'DATE', 'ENTITYID', 'FILEHANDLEID']:
                result.append(field)
            elif type=='DOUBLE':
                result.append(float(field))
            elif type=='INTEGER':
                result.append(int(field))
            elif type=='BOOLEAN':
                result.append(to_boolean(field))
            else:
                raise ValueError("Unknown column type: %s" % type)

    return result


def header_to_column_id(obj):
    """
    Get the column ID referred to by a column header, which might be
    a column ID or an aggregate function such as AVG(C1384)
    """
    m = aggregate_pattern.match(obj.lower())
    return m.group(2) if m else obj


class Schema(Entity, Versionable):
    """
    A Schema is a :py:class:`synapse.entity.Entity` that defines a set of columns in a table.

    :param name: give the Table Schema object a name
    :param description:
    :param columns: a list of :py:class:`Column` objects or their IDs
    :param parent: the project (file a bug if you'd like folders supported) in Synapse to which this table belongs

    ::

        cols = [Column(name='Isotope', columnType='STRING'),
                Column(name='Atomic Mass', columnType='INTEGER'),
                Column(name='Halflife', columnType='DOUBLE'),
                Column(name='Discovered', columnType='DATE')]

        schema = syn.store(Schema(name='MyTable', columns=cols, parent=project))
    """
    _property_keys = Entity._property_keys + Versionable._property_keys + ['columnIds']
    _local_keys = Entity._local_keys + ['columns_to_store']
    _synapse_entity_type = 'org.sagebionetworks.repo.model.table.TableEntity'

    def __init__(self, name=None, columns=None, parent=None, properties=None, annotations=None, local_state=None, **kwargs):
        self.properties.setdefault('columnIds',[])
        if name: kwargs['name'] = name
        if columns:
            for column in columns:
                if isinstance(column, basestring) or isinstance(column, int) or hasattr(column, 'id'):
                    kwargs.setdefault('columnIds',[]).append(id_of(column))
                elif isinstance(column, Column):
                    kwargs.setdefault('columns_to_store',[]).append(column)
                else:
                    raise ValueError("Not a column? %s" % unicode(column))
        super(Schema, self).__init__(concreteType=Schema._synapse_entity_type, properties=properties, 
                                   annotations=annotations, local_state=local_state, parent=parent, **kwargs)

    def addColumn(self, column):
        """
        :param column: a column object or its ID
        """
        if isinstance(column, basestring) or isinstance(column, int) or hasattr(column, 'id'):
            self.properties.columnIds.append(id_of(column))
        elif isinstance(column, Column):
            if not self.__dict__.get('columns_to_store', None):
                self.__dict__['columns_to_store'] = []
            self.__dict__['columns_to_store'].append(column)
        else:
            raise ValueError("Not a column? %s" % unicode(column))

    def addColumns(self, columns):
        """
        :param columns: a list of column objects or their ID
        """
        for column in columns:
            self.addColumn(column)

    def removeColumn(self, column):
        """
        :param column: a column object or its ID
        """
        if isinstance(column, basestring) or isinstance(column, int) or hasattr(column, 'id'):
            self.properties.columnIds.remove(id_of(column))
        elif isinstance(column, Column) and self.columns_to_store:
            self.columns_to_store.remove(column)
        else:
            ValueError("Can't remove column %s" + unicode(column))

    def has_columns(self):
        """Does this schema have columns specified?"""
        return bool(self.properties.get('columnIds',None) or self.__dict__.get('columns_to_store',None))

    def _before_synapse_store(self, syn):
        ## store any columns before storing table
        if self.columns_to_store:
            for column in self.columns_to_store:
                column = syn.store(column)
                self.properties.columnIds.append(column.id)
            self.__dict__['columns_to_store'] = None


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
        elif schema:
            kwargs.setdefault('headers',[]).extend(schema["columnIds"])
        if schema:
            kwargs['tableId'] = id_of(schema)
        if not kwargs.get('tableId',None):
            raise ValueError("Table schema ID must be defined to create a RowSet")
        if not kwargs.get('headers',None):
            raise ValueError("Column headers must be defined to create a RowSet")

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


class Row(DictObject):
    """
    A `row <http://rest.synapse.org/org/sagebionetworks/repo/model/table/Row.html>`_ in a Table.

    :param values:         A list of values
    :param rowId:          The immutable ID issued to a new row
    :param versionNumber:  The version number of this row. Each row version is immutable, so when a row is updated a new version is created.
    """
    def __init__(self, values, rowId=None, versionNumber=None):
        super(Row, self).__init__()
        self.values = values
        self.rowId = rowId
        self.versionNumber = versionNumber


class RowSelection(DictObject):
    """
    A set of rows to be `deleted <http://rest.synapse.org/POST/entity/id/table/deleteRows.html>`_.

    :param rowIds: list of row ids
    :param etag: etag of latest change set
    :param tableId: synapse ID of the table schema
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


def create_table(schema, values, **kwargs):
    """
    Combine a table schema and a set of values into some type of Table object
    depending on what type of values are given.

    :param schema: a table py:class:`Schema` object
    :param value: an object that holds the content of the tables
      - a py:class:`RowSet`
      - a list of lists (or tuples) where each element is a row
      - a string holding the path to a CSV file
      - a Pandas `DataFrame <http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe>`_
    """

    try:
        import pandas as pd
        pandas_available = True
    except ImportError as ex1:
        pandas_available = False

    ## a RowSet
    if isinstance(values, RowSet):
        return RowSetTable(schema, values)

    ## a list of rows
    elif isinstance(values, (list, tuple)):
        return RowSetTable(schema, RowSet(schema=schema, rows=[Row(r) for r in values]))

    ## filename of a csv file
    elif isinstance(values, basestring):
        return CsvFileTable(schema, filepath=values, **kwargs)

    ## pandas DataFrame
    elif pandas_available and isinstance(values, pd.DataFrame):
        return CsvFileTable.from_data_frame(schema, values, **kwargs)


class Table(object):
    """
    Abstract base class for Tables based on different data containers.
    """
    def __init__(self, schema, headers=None, etag=None):
        if isinstance(schema, Schema):
            self.schema = schema
            self.tableId = schema.id if schema and 'id' in schema else None
            self.headers = headers if headers else schema.columnIds
            self.etag = etag
        elif isinstance(schema, basestring):
            self.schema = None
            self.tableId = schema
            self.headers = headers
            self.etag = etag
        else:
            ValueError("Must provide a schema or a synapse ID of a Table Entity")

    def asDataFrame(self):
        raise NotImplementedError()

    def asInteger(self):
        try:
            first_row = self.__iter__().next()
            return int(first_row[0])
        except (KeyError, TypeError) as ex1:
            raise ValueError("asInteger is only valid for queries such as count queries whose first value is an integer.")

    def asRowSet(self):
        return RowSet(headers=self.headers,
                      tableId=self.tableId,
                      etag=self.etag,
                      rows=[row if isinstance(row, Row) else Row(row) for row in self])

    def _synapse_store(self, syn):
        raise NotImplementedError()

    def _synapse_delete(self, syn):
        """
        Delete the rows that result from a table query.

        Example::
            syn.delete(syn.queryTable('select name from %s where no_good = true' % schema1.id))
        """
        uri = '/entity/{id}/table/deleteRows'.format(id=self.tableId)
        return syn.restPOST(uri, body=json.dumps(RowSelection(
            rowIds=[row['rowId'] for row in self],
            etag=self.etag,
            tableId=self.tableId)))

    def _get_headers(self):
        """
        Determine column headers, which are returned as a list of column IDs
        """
        if hasattr(self, "_headers"):
            return self._headers
        elif hasattr(self, "columns") and self.columns:
            return [id_of(column) for column in columns]
        elif self.schema:
            return self.schema.columnIds
        else:
            AttributeError("Unable to determine column headers")

    def __iter__(self):
        raise NotImplementedError()


class RowSetTable(Table):
    """
    A Table object that wraps a RowSet.
    """
    def __init__(self, schema, rowset, columns=None):
        super(RowSetTable, self).__init__(schema, etag=rowset.get('etag', None))
        self.rowset = rowset
        self.columns = columns
        self.i = -1

    def _synapse_store(self, syn):
        syn.store(self.rowset)

    def asDataFrame(self):
        test_import_pandas()
        import pandas as pd

        if not self.columns:
            raise AttributeError('Need to set columns before calling asDataFrame')
        colmap = {column['id']:column for column in self.columns}

        if any([row['rowId'] for row in self.rowset['rows']]):
            rownames = ["%s-%s"%(row['rowId'], row['versionNumber']) for row in self.rowset['rows']]
        else:
            rownames = None

        series = OrderedDict()
        for i, header in enumerate(self.rowset["headers"]):
            column_name = colmap[header]['name']
            series[column_name] = pd.Series(name=column_name, data=[row['values'][i] for row in self.rowset['rows']], index=rownames)

        return pd.DataFrame(data=series, index=rownames)

    def asRowSet(self):
        return self.rowset

    def asInteger(self):
        try:
            return int(self.rowset['rows'][0]['values'][0])
        except (KeyError, TypeError) as ex1:
            raise ValueError("asInteger is only valid for queries such as count queries whose first value is an integer.")

    def __iter__(self):
        def iterate_rows(rows):
            for row in rows:
                yield row
        return iterate_rows(self.rowset['rows'])


class TableQueryResult(Table):
    """
    An object to wrap rows returned as a result of a table query.

    The TableQueryResult object can be used to iterate over results of a query:

        results = syn.queryTable("select * from syn1234")
        for row in results:
            print row
    """
    def __init__(self, synapse, query, limit=None, offset=None, isConsistent=True):
        self.syn = synapse

        self.query = query
        self.limit = limit
        self.offset = offset
        self.isConsistent = isConsistent

        result = self.syn._queryTable(
            query=query,
            limit=limit,
            offset=offset,
            isConsistent=isConsistent)

        self.rowset = result['queryResult']['queryResults']
        self.nextPageToken = result['queryResult'].get('nextPageToken', None)
        self.columns = [Column(**columnModel) for columnModel in result.get('selectColumns', [])]
        self.count = result.get('queryCount', None)
        self.maxRowsPerPage = result.get('maxRowsPerPage', None)
        self.i = -1

        super(TableQueryResult, self).__init__(
            schema=self.rowset.get('tableId', None),
            headers=self.rowset.get("headers", None),
            etag=self.rowset.get('etag', None))

    def _synapse_store(self, syn):
        raise SynapseError("A TableQueryResult is a read only object and can't be stored in Synapse. Convert to a DataFrame or RowSet instead.")

    def asDataFrame(self):
        test_import_pandas()
        import pandas as pd

        colmap = {column['id']:column for column in self.columns}

        ## To turn a TableQueryResult into a data frame, we add a page of rows
        ## at a time on the untested theory that it's more efficient than
        ## adding a single row at a time to the data frame.

        ## first page of rows
        rownames = ["%s-%s"%(row['rowId'], row['versionNumber']) for row in self.rowset['rows']]
        series = OrderedDict()
        for i, header in enumerate(self.rowset["headers"]):
            column_name = colmap[header]['name']
            series[column_name] = pd.Series(name=column_name, data=[row['values'][i] for row in self.rowset['rows']], index=rownames)

        # subsequent pages of rows
        while self.nextPageToken:
            result = self.syn._queryTableNext(self.nextPageToken)
            self.rowset = result['queryResults']
            self.nextPageToken = result.get('nextPageToken', None)
            self.i = 0

            new_rownames = ["%s-%s"%(row['rowId'], row['versionNumber']) for row in self.rowset['rows']]
            rownames.extend(new_rownames)
            for i, header in enumerate(self.rowset["headers"]):
                column_name = colmap[header]['name']
                series[column_name].append(pd.Series(name=column_name, data=[row['values'][i] for row in self.rowset['rows']], index=new_rownames))

        return pd.DataFrame(data=series, index=rownames)

    def asRowSet(self):
        ## Note that as of stack 60, an empty query will omit the headers field
        ## see PLFM-3014
        return RowSet(headers=self.rowset.get('headers', [col.id for col in self.columns]),
                      tableId=self.tableId,
                      etag=self.etag,
                      rows=[row for row in self])

    def asInteger(self):
        try:
            return int(self.rowset['rows'][0]['values'][0])
        except (KeyError, TypeError) as ex1:
            raise ValueError("asInteger is only valid for queries such as count queries whose first value is an integer.")

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


class CsvFileTable(Table):
    """
    An object to wrap a CSV file that may be stored into a Synapse table or
    returned as a result of a table query.
    """

    @classmethod
    def from_table_query(cls, synapse, query, quoteCharacter='"', escapeCharacter="\\", lineEnd=os.linesep, separator=",", header=True, includeRowIdAndRowVersion=True):
        """
        Create a Table object wrapping a CSV file resulting from querying a Synapse table.
        Mostly for internal use.
        """

        download_from_table_result, file_info = synapse._queryTableCsv(
            query=query,
            quoteCharacter=quoteCharacter,
            escapeCharacter=escapeCharacter,
            lineEnd=os.linesep,
            separator=separator,
            header=header,
            includeRowIdAndRowVersion=includeRowIdAndRowVersion)

        self = cls(
            filepath=file_info['path'],
            schema=download_from_table_result.get('tableId', None),
            etag=download_from_table_result.get('etag', None),
            quoteCharacter=quoteCharacter,
            escapeCharacter=escapeCharacter,
            lineEnd=os.linesep,
            separator=separator,
            header=header,
            includeRowIdAndRowVersion=includeRowIdAndRowVersion)

        self.setColumns(
            columns=list(synapse.getColumns(download_from_table_result['headers'])),
            headers=['ROW_ID', 'ROW_VERSION'] + download_from_table_result['headers'] if includeRowIdAndRowVersion else download_from_table_result['headers'])

        return self

    @classmethod
    def from_data_frame(cls, schema, df, filepath=None, etag=None, quoteCharacter='"', escapeCharacter="\\", lineEnd=os.linesep, separator=",", header=True, linesToSkip=0, columns=None):
                ## infer columns from data frame if not specified
        if not schema.has_columns():
            schema.addColumns(as_table_columns(df))

        ## convert row names in the format [row_id]-[version] back to columns
        row_id_version_pattern = re.compile(r'(\d+)\-(\d+)')

        row_id = []
        row_version = []
        for row_name in df.index.values:
            m = row_id_version_pattern.match(unicode(row_name))
            row_id.append(m.group(1) if m else None)
            row_version.append(m.group(2) if m else None)

        df2 = df.copy()
        df2['ROW_ID'] = row_id
        df2['ROW_VERSION'] = row_version

        f = None
        try:
            if filepath:
                f = open(filepath)
            else:
                f = tempfile.NamedTemporaryFile(delete=False)
                filepath = f.name

            df2.to_csv(f,
                index=False,
                sep=separator,
                header=header,
                quotechar=quoteCharacter,
                escapechar=escapeCharacter,
                line_terminator=lineEnd)
        finally:
            if f: f.close()

        return cls(
            schema=schema,
            filepath=filepath,
            etag=etag,
            quoteCharacter=quoteCharacter,
            escapeCharacter=escapeCharacter,
            lineEnd=lineEnd,
            separator=separator,
            header=header,
            columns=columns)


    def __init__(self, schema, filepath, etag=None, quoteCharacter='"', escapeCharacter="\\", lineEnd=os.linesep, separator=",", header=True, linesToSkip=0, includeRowIdAndRowVersion=None, columns=None):
        self.filepath = filepath

        self.includeRowIdAndRowVersion = includeRowIdAndRowVersion
        self.columns = columns

        ## CsvTableDescriptor fields
        self.linesToSkip = linesToSkip
        self.quoteCharacter = quoteCharacter
        self.escapeCharacter = escapeCharacter
        self.lineEnd = lineEnd
        self.separator = separator
        self.header = header

        super(CsvFileTable, self).__init__(schema, headers=[col.id for col in columns] if columns else None, etag=etag)

    def _synapse_store(self, syn):
        if isinstance(self.schema, Schema) and self.schema.get('id', None) is None:
            ## store schema
            self.schema = syn.store(self.schema)

        upload_to_table_result = syn._uploadCsv(
            self.filepath,
            self.schema,
            updateEtag=self.etag,
            quoteCharacter=self.quoteCharacter,
            escapeCharacter=self.escapeCharacter,
            lineEnd=self.lineEnd,
            separator=self.separator,
            header=self.header,
            linesToSkip=self.linesToSkip)

        self.etag = upload_to_table_result['etag']
        return self

    def asDataFrame(self, putRowIdAndVersionInIndex=True):
        test_import_pandas()
        import pandas as pd

        df = pd.DataFrame.from_csv(self.filepath, header=0, sep=self.separator)

        if not putRowIdAndVersionInIndex:
            return df

        ## file might be in three formats:
        ##  1) already indexed by rowid-version
        ##  2) row-id as index and row-version as 1st column
        ##  3) no row-id and version information

        row_id_version_pattern = re.compile(r'\d+\-\d+')

        if df.index.dtype.char in ['S', 'U', 'O'] and all([row_id_version_pattern.match(unicode(row_name)) for row_name in df.index.values]):
            return df
        elif df.index.name=="ROW_ID" and "ROW_VERSION" == df.columns.values[0]:
                ## combine row-ids (in index) and row-versions (in column 0) to
                ## make new row labels consisting of the row id and version
                ## separated by a dash.
                df2 = df.ix[:,1:]
                df2.index = ["%s-%s"%(r,v) for r,v in zip(df.index,df.ix[:,0])]
                return df2
        else:
            return df

    def setColumns(self, columns, headers=None):
        """
        Set the list of :py:class:`synapseclient.table.Column` objects that
        will be used to convert fields to the appropriate data types.

        Columns are automatically set when querying.
        """
        self.columns = columns

        ## if we're given headers (a list of column ids) use those
        ## otherwise, we should be given a list of columns in the
        ## order they appear in the table
        if headers:
            self._headers = headers
        else:
            self._headers = [id_of(column) for column in columns]

    def __iter__(self):
        def iterate_rows(filepath, columns, headers):
            with open(filepath) as f:
                reader = csv.reader(f)
                header = reader.next()
                for row in reader:
                    yield cast_row(row, columns, headers)
        return iterate_rows(self.filepath, self.columns, self._get_headers())
