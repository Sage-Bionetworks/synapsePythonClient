"""
******
Tables
******

Synapse Tables enable storage of tabular data in Synapse in a form that can be
queried using a SQL-like query language.

~~~~~~~~~~~~~~~~~~~~~~~~~~
Tables is an BETA feature
~~~~~~~~~~~~~~~~~~~~~~~~~~
The tables feature is in the beta stage. Please report bugs via
`JIRA <https://sagebionetworks.jira.com/>`_.

A table has a :py:class:`Schema` and holds a set of rows conforming to
that schema.

A :py:class:`Schema` is defined in terms of :py:class:`Column` objects that
specify types from the following choices: STRING, DOUBLE, INTEGER, BOOLEAN,
DATE, ENTITYID, FILEHANDLEID.

~~~~~~~
Example
~~~~~~~

Preliminaries::

    import synapseclient
    from synapseclient import Project, File, Folder
    from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns

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

Next, let's load some data. Let's say we had a file, genes.csv::

    Name,Chromosome,Start,End,Strand,TranscriptionFactor
    foo,1,12345,12600,+,False
    arg,2,20001,20200,+,False
    zap,2,30033,30999,-,False
    bah,1,40444,41444,-,False
    bnk,1,51234,54567,+,True
    xyz,1,61234,68686,+,False

Let's store that in Synapse::

    table = Table(schema, "/path/to/genes.csv")
    table = syn.store(table)

The :py:func:`Table` function takes two arguments, a schema object and
data in some form, which can be:

  * a path to a CSV file
  * a `Pandas <http://pandas.pydata.org/>`_ `DataFrame <http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe>`_
  * a :py:class:`RowSet` object
  * a list of lists where each of the inner lists is a row

With a bit of luck, we now have a table populated with data. Let's try to query::

    results = syn.tableQuery("select * from %s where Chromosome='1' and Start < 41000 and End > 20000" % table.schema.id)
    for row in results:
        print row

------
Pandas
------

`Pandas <http://pandas.pydata.org/>`_ is a popular library for working with
tabular data. If you have Pandas installed, the goal is that Synapse Tables
will play nice with it.

Create a Synapse Table from a `DataFrame <http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe>`_::

    import pandas as pd

    df = pd.read_csv("/path/to/genes.csv", index_col=False)
    schema = Schema(name='My Favorite Genes', columns=as_table_columns(df), parent=project)
    table = syn.store(Table(schema, df))

Get query results as a `DataFrame <http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe>`_::

    results = syn.tableQuery("select * from %s where Chromosome='2'" % table.schema.id)
    df = results.asDataFrame()

--------------
Changing Data
--------------

Once the schema is settled, changes come in two flavors: appending new rows and
updating existing ones.

**Appending** new rows is fairly straightforward. To continue the previous
example, we might add some new genes from another file::

    table = syn.store(Table(table.schema.id, "/path/to/more_genes.csv"))

To quickly add a few rows, use a list of row data::

    new_rows = [["Qux1", "4", 201001, 202001, "+", False],
                ["Qux2", "4", 203001, 204001, "+", False]]
    table = syn.store(Table(schema, new_rows))

**Updating** rows requires an etag, which identifies the most recent change
set plus row IDs and version numbers for each row to be modified. We get
those by querying before updating. Minimizing changesets to contain only rows
that actually change will make processing faster.

For example, let's update the names of some of our favorite genes::

    results = syn.tableQuery("select * from %s where Chromosome='1'" %table.schema.id)
    df = results.asDataFrame()
    df['Name'] = ['rzing', 'zing1', 'zing2', 'zing3']

Note that we're propagating the etag from the query results. Without it, we'd
get an error saying something about an "Invalid etag"::

    table = syn.store(Table(schema, df, etag=results.etag))

The etag is used by the server to prevent concurrent users from making
conflicting changes, a technique called optimistic concurrency. In case
of a conflict, your update may be rejected. You then have to do another query
an try your update again.

------------------------
Changing Table Structure
------------------------

Adding columns can be done using the methods :py:meth:`Schema.addColumn` or :py:meth:`addColumns`
on the :py:class:`Schema` object::

    schema = syn.get("syn000000")
    bday_column = syn.store(Column(name='birthday', columnType='DATE'))
    schema.addColumn(bday_column)
    schema = syn.store(schema)

Renaming or otherwise modifying a column involves removing the column and adding a new column::

    cols = syn.getTableColumns(schema)
    for col in cols:
        if col.name == "birthday":
            schema.removeColumn(col)
    bday_column2 = syn.store(Column(name='birthday2', columnType='DATE'))
    schema.addColumn(bday_column2)
    schema = syn.store(schema)


-------------
Deleting rows
-------------

Query for the rows you want to delete and call syn.delete on the results::

    results = syn.tableQuery("select * from %s where Chromosome='2'" %table.schema.id)
    a = syn.delete(results.asRowSet())

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

.. autofunction:: Table

See also:
 - :py:meth:`synapseclient.Synapse.getColumns`
 - :py:meth:`synapseclient.Synapse.getTableColumns`
 - :py:meth:`synapseclient.Synapse.tableQuery`
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
import synapseclient.utils
from synapseclient.exceptions import *
from synapseclient.dict_object import DictObject
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
    # used to catch ImportError, but other errors can happen (see SYNPY-177)
    except:
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


def column_ids(columns):
    if columns is None:
        return []
    return [col.id for col in columns if 'id' in col]


def row_labels_from_id_and_version(rows):
    return ["%s_%s"%(id, version) for id, version in rows]

def row_labels_from_rows(rows):
    return row_labels_from_id_and_version([(row['rowId'], row['versionNumber']) for row in rows])

def cast_values(values, headers):
    """
    Convert a row of table query results from strings to the correct column type.

    See: http://rest.synapse.org/org/sagebionetworks/repo/model/table/ColumnType.html
    """
    if len(values) != len(headers):
        raise ValueError('Each field in the row must have a matching column header. %d fields, %d headers' % (len(values), len(headers)))

    result = []
    for header, field in izip(headers, values):

        columnType = header.get('columnType', 'STRING')

        ## convert field to column type
        if field is None or field=='':
            result.append(None)
        elif columnType in ['STRING', 'ENTITYID', 'FILEHANDLEID']:
            result.append(field)
        elif columnType=='DOUBLE':
            result.append(float(field))
        elif columnType=='INTEGER':
            result.append(int(field))
        elif columnType=='BOOLEAN':
            result.append(to_boolean(field))
        elif columnType=='DATE':
            result.append(utils.from_unix_epoch_time(field))
        else:
            raise ValueError("Unknown column type: %s" % columnType)

    return result


def cast_row(row, headers):
    row['values'] = cast_values(row['values'], headers)
    return row


def cast_row_set(rowset):
    for i, row in enumerate(rowset['rows']):
        rowset['rows'][i]['values'] = cast_row(row, rowset['headers'])
    return rowset


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
                    kwargs.setdefault('columnIds',[]).append(utils.id_of(column))
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
            self.properties.columnIds.append(utils.id_of(column))
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
            self.properties.columnIds.remove(utils.id_of(column))
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


## add Schema to the map of synapse entity types to their Python representations
synapseclient.entity._entity_type_to_class[Schema._synapse_entity_type] = Schema


## allowed column types
## see http://rest.synpase.org/org/sagebionetworks/repo/model/table/ColumnType.html
ColumnTypes = ['STRING','DOUBLE','INTEGER','BOOLEAN','DATE','FILEHANDLEID','ENTITYID','LINK']


class SelectColumn(DictObject):
    """
    Defines a column to be used in a table :py:class:`synapseclient.table.Schema`.

    :var id:              An immutable ID issued by the platform
    :param columnType:    Can be any of: "STRING", "DOUBLE", "INTEGER", "BOOLEAN", "DATE", "FILEHANDLEID", "ENTITYID"
    :param name:          The display name of the column

    :type id: string
    :type columnType: string
    :type name: string
    """
    def __init__(self, id=None, columnType=None, name=None):
        super(SelectColumn, self).__init__()
        if id:
            self.id = id

        if name:
            self.name = name

        if columnType:
            if columnType not in ColumnTypes:
                raise ValueError('Unrecognized columnType: %s' % columnType)
            self.columnType = columnType

    @classmethod
    def from_column(cls, column):
        return cls(column.get('id', None), column.get('columnType', None), column.get('name', None))


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
    :param headers:  The list of SelectColumn objects that describe the fields in each row.
    :param tableId:  The ID of the TableEntity than owns these rows
    :param rows:     The :py:class:`synapseclient.table.Row`s of this set. The index of each row value aligns with the index of each header.
    :var etag:       Any RowSet returned from Synapse will contain the current etag of the change set. To update any rows from a RowSet the etag must be provided with the POST.

    :type headers:   array of SelectColumns
    :type etag:      string
    :type tableId:   string
    :type rows:      array of rows
    """

    @classmethod
    def from_json(cls, json):
        headers=[SelectColumn(**header) for header in json.get('headers', [])]
        rows=[cast_row(Row(**row), headers) for row in json.get('rows', [])]
        return cls(headers=headers, rows=rows,
            **{ key: json[key] for key in json.keys() if key not in ['headers', 'rows'] })

    def __init__(self, columns=None, schema=None, **kwargs):
        if not 'headers' in kwargs:
            if columns:
                kwargs.setdefault('headers',[]).extend([SelectColumn.from_column(column) for column in columns])
            elif schema and isinstance(schema, Schema):
                kwargs.setdefault('headers',[]).extend([SelectColumn(id=id) for id in schema["columnIds"]])
        if ('tableId' not in kwargs) and schema:
            kwargs['tableId'] = utils.id_of(schema)
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
            syn.delete(syn.tableQuery('select name from %s where no_good = true' % schema1.id))
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
        if rowId is not None:
            self.rowId = rowId
        if versionNumber is not None:
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


def Table(schema, values, **kwargs):
    """
    Combine a table schema and a set of values into some type of Table object
    depending on what type of values are given.

    :param schema: a table py:class:`Schema` object
    :param value: an object that holds the content of the tables
      - a py:class:`RowSet`
      - a list of lists (or tuples) where each element is a row
      - a string holding the path to a CSV file
      - a Pandas `DataFrame <http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe>`_

    Usually, the immediate next step after creating a Table object is to store it::

        table = syn.store(Table(schema, values))

    End users should not need to know the details of these Table subclasses:

      - :py:class:`TableAbstractBaseClass`
      - :py:class:`RowSetTable`
      - :py:class:`TableQueryResult`
      - :py:class:`CsvFileTable`
    """

    try:
        import pandas as pd
        pandas_available = True
    except:
        pandas_available = False

    ## a RowSet
    if isinstance(values, RowSet):
        return RowSetTable(schema, values, **kwargs)

    ## a list of rows
    elif isinstance(values, (list, tuple)):
        return CsvFileTable.from_list_of_rows(schema, values, **kwargs)

    ## filename of a csv file
    elif isinstance(values, basestring):
        return CsvFileTable(schema, filepath=values, **kwargs)

    ## pandas DataFrame
    elif pandas_available and isinstance(values, pd.DataFrame):
        return CsvFileTable.from_data_frame(schema, values, **kwargs)

    else:
        raise ValueError("Don't know how to make tables from values of type %s." % type(values))


class TableAbstractBaseClass(object):
    """
    Abstract base class for Tables based on different data containers.
    """
    def __init__(self, schema, headers=None, etag=None):
        if isinstance(schema, Schema):
            self.schema = schema
            self.tableId = schema.id if schema and 'id' in schema else None
            self.headers = headers if headers else [SelectColumn(id=id) for id in schema.columnIds]
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
            syn.delete(syn.tableQuery('select name from %s where no_good = true' % schema1.id))
        """
        uri = '/entity/{id}/table/deleteRows'.format(id=self.tableId)
        return syn.restPOST(uri, body=json.dumps(RowSelection(
            rowIds=[row['rowId'] for row in self],
            etag=self.etag,
            tableId=self.tableId)))

    def __iter__(self):
        raise NotImplementedError()


class RowSetTable(TableAbstractBaseClass):
    """
    A Table object that wraps a RowSet.
    """
    def __init__(self, schema, rowset):
        super(RowSetTable, self).__init__(schema, etag=rowset.get('etag', None))
        self.rowset = rowset

    def _synapse_store(self, syn):
        row_reference_set = syn.store(self.rowset)
        return self

    def asDataFrame(self):
        test_import_pandas()
        import pandas as pd

        if any([row['rowId'] for row in self.rowset['rows']]):
            rownames = row_labels_from_rows(self.rowset['rows'])
        else:
            rownames = None

        series = OrderedDict()
        for i, header in enumerate(self.rowset["headers"]):
            series[header.name] = pd.Series(name=header.name, data=[row['values'][i] for row in self.rowset['rows']], index=rownames)

        return pd.DataFrame(data=series, index=rownames)

    def asRowSet(self):
        return self.rowset

    def asInteger(self):
        try:
            return int(self.rowset['rows'][0]['values'][0])
        except (KeyError, TypeError) as ex1:
            raise ValueError("asInteger is only valid for queries such as count queries whose first value is an integer.")

    def __iter__(self):
        def iterate_rows(rows, headers):
            for row in rows:
                yield cast_values(row, headers)
        return iterate_rows(self.rowset['rows'], self.rowset['headers'])


class TableQueryResult(TableAbstractBaseClass):
    """
    An object to wrap rows returned as a result of a table query.

    The TableQueryResult object can be used to iterate over results of a query:

        results = syn.tableQuery("select * from syn1234")
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

        self.rowset = RowSet.from_json(result['queryResult']['queryResults'])

        self.columnModels = [Column(**col) for col in result.get('columnModels', [])]
        self.nextPageToken = result['queryResult'].get('nextPageToken', None)
        self.count = result.get('queryCount', None)
        self.maxRowsPerPage = result.get('maxRowsPerPage', None)
        self.i = -1

        super(TableQueryResult, self).__init__(
            schema=self.rowset.get('tableId', None),
            headers=self.rowset.headers,
            etag=self.rowset.get('etag', None))

    def _synapse_store(self, syn):
        raise SynapseError("A TableQueryResult is a read only object and can't be stored in Synapse. Convert to a DataFrame or RowSet instead.")

    def asDataFrame(self):
        """
        Convert query result to a Pandas DataFrame.
        """
        test_import_pandas()
        import pandas as pd

        ## To turn a TableQueryResult into a data frame, we add a page of rows
        ## at a time on the untested theory that it's more efficient than
        ## adding a single row at a time to the data frame.

        def construct_rownames(rowset, offset=0):
            try:
                return row_labels_from_rows(rowset['rows'])
            except KeyError:
                ## if we don't have row id and version, just number the rows
                return range(offset,offset+len(rowset['rows']))

        ## first page of rows
        offset = 0
        rownames = construct_rownames(self.rowset, offset)
        offset += len(self.rowset['rows'])
        series = OrderedDict()
        for i, header in enumerate(self.rowset["headers"]):
            column_name = header.name
            series[column_name] = pd.Series(name=column_name, data=[row['values'][i] for row in self.rowset['rows']], index=rownames)

        # subsequent pages of rows
        while self.nextPageToken:
            result = self.syn._queryTableNext(self.nextPageToken, self.tableId)
            self.rowset = RowSet.from_json(result['queryResults'])
            self.nextPageToken = result.get('nextPageToken', None)
            self.i = 0

            rownames = construct_rownames(self.rowset, offset)
            offset += len(self.rowset['rows'])
            for i, header in enumerate(self.rowset["headers"]):
                column_name = header.name
                series[column_name] = series[column_name].append(pd.Series(name=column_name, data=[row['values'][i] for row in self.rowset['rows']], index=rownames), verify_integrity=True)

        return pd.DataFrame(data=series)

    def asRowSet(self):
        ## Note that as of stack 60, an empty query will omit the headers field
        ## see PLFM-3014
        return RowSet(headers=self.headers,
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
                result = self.syn._queryTableNext(self.nextPageToken, self.tableId)
                self.rowset = RowSet.from_json(result['queryResults'])
                self.nextPageToken = result.get('nextPageToken', None)
                self.i = 0
            else:
                raise StopIteration()
        return self.rowset['rows'][self.i]


class CsvFileTable(TableAbstractBaseClass):
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

        ## A dirty hack to find out if we got back row ID and Version
        ## in particular, we don't get these back from aggregate queries
        with open(file_info['path'], 'r') as f:
            reader = csv.reader(f,
                delimiter=separator,
                escapechar=escapeCharacter,
                lineterminator=lineEnd,
                quotechar=quoteCharacter)
            first_line = reader.next()
        if len(download_from_table_result['headers']) + 2 == len(first_line):
            includeRowIdAndRowVersion = True
        else:
            includeRowIdAndRowVersion = False

        self = cls(
            filepath=file_info['path'],
            schema=download_from_table_result.get('tableId', None),
            etag=download_from_table_result.get('etag', None),
            quoteCharacter=quoteCharacter,
            escapeCharacter=escapeCharacter,
            lineEnd=os.linesep,
            separator=separator,
            header=header,
            includeRowIdAndRowVersion=includeRowIdAndRowVersion,
            headers=[SelectColumn(**header) for header in download_from_table_result['headers']])

        return self

    @classmethod
    def from_data_frame(cls, schema, df, filepath=None, etag=None, quoteCharacter='"', escapeCharacter="\\", lineEnd=os.linesep, separator=",", header=True, linesToSkip=0, includeRowIdAndRowVersion=None, headers=None, **kwargs):
        ## infer columns from data frame if not specified
        if not headers:
            cols = as_table_columns(df)
            headers = [SelectColumn.from_column(col) for col in cols]

        ## if the schema has no columns, use the inferred columns
        if isinstance(schema, Schema) and not schema.has_columns():
            schema.addColumns(cols)

        ## convert row names in the format [row_id]-[version] back to columns
        row_id_version_pattern = re.compile(r'(\d+)_(\d+)')

        row_id = []
        row_version = []
        for row_name in df.index.values:
            m = row_id_version_pattern.match(unicode(row_name))
            row_id.append(m.group(1) if m else None)
            row_version.append(m.group(2) if m else None)

        ## include row ID and version, if we're asked to OR if it's encoded in rownames
        if includeRowIdAndRowVersion==True or (includeRowIdAndRowVersion is None and any(row_id)):
            df2 = df.copy()
            df2.insert(0, 'ROW_ID', row_id)
            df2.insert(1, 'ROW_VERSION', row_version)
            df = df2
            includeRowIdAndRowVersion = True

        f = None
        try:
            if filepath:
                f = open(filepath)
            else:
                f = tempfile.NamedTemporaryFile(delete=False)
                filepath = f.name

            df.to_csv(f,
                index=False,
                sep=separator,
                header=header,
                quotechar=quoteCharacter,
                escapechar=escapeCharacter,
                line_terminator=lineEnd,
                na_rep=kwargs.get('na_rep',''))
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
            includeRowIdAndRowVersion=includeRowIdAndRowVersion,
            headers=headers)

    @classmethod
    def from_list_of_rows(cls, schema, values, filepath=None, etag=None, quoteCharacter='"', escapeCharacter="\\", lineEnd=os.linesep, separator=",", linesToSkip=0, includeRowIdAndRowVersion=None, headers=None):

        ## create CSV file
        f = None
        try:
            if filepath:
                f = open(filepath)
            else:
                f = tempfile.NamedTemporaryFile(delete=False)
                filepath = f.name

            writer = csv.writer(f,
                quoting=csv.QUOTE_NONNUMERIC,
                delimiter=separator,
                escapechar=escapeCharacter,
                lineterminator=lineEnd,
                quotechar=quoteCharacter,
                skipinitialspace=linesToSkip)

            ## if we haven't explicitly set columns, try to grab them from
            ## the schema object
            if not headers and "columns_to_store" in schema and schema.columns_to_store is not None:
                headers = [SelectColumn.from_column(col) for col in schema.columns_to_store]

            ## write headers?
            if headers:
                writer.writerow([header.name for header in headers])
                header = True
            else:
                header = False

            ## write row data
            for row in values:
                writer.writerow(row)

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
            headers=headers,
            includeRowIdAndRowVersion=includeRowIdAndRowVersion)


    def __init__(self, schema, filepath, etag=None, quoteCharacter='"', escapeCharacter="\\", lineEnd=os.linesep, separator=",", header=True, linesToSkip=0, includeRowIdAndRowVersion=None, headers=None):
        self.filepath = filepath

        self.includeRowIdAndRowVersion = includeRowIdAndRowVersion

        ## CsvTableDescriptor fields
        self.linesToSkip = linesToSkip
        self.quoteCharacter = quoteCharacter
        self.escapeCharacter = escapeCharacter
        self.lineEnd = lineEnd
        self.separator = separator
        self.header = header

        super(CsvFileTable, self).__init__(schema, headers=headers, etag=etag)

        self.setColumnHeaders(headers)

    def _synapse_store(self, syn):
        if isinstance(self.schema, Schema) and self.schema.get('id', None) is None:
            ## store schema
            self.schema = syn.store(self.schema)
            self.tableId = self.schema.id

        upload_to_table_result = syn._uploadCsv(
            self.filepath,
            self.schema if self.schema else self.tableId,
            updateEtag=self.etag,
            quoteCharacter=self.quoteCharacter,
            escapeCharacter=self.escapeCharacter,
            lineEnd=self.lineEnd,
            separator=self.separator,
            header=self.header,
            linesToSkip=self.linesToSkip)

        if 'etag' in upload_to_table_result:
            self.etag = upload_to_table_result['etag']
        return self

    def _synapse_delete(self, syn):
        """
        Delete the rows that are the results of this query.

        Example::
            syn.delete(syn.tableQuery('select name from %s where no_good = true' % schema1.id))
        """
        ## Extract row id and version, if present in rows
        row_id_col = None
        for i, header in enumerate(self.headers):
            if header.name=='ROW_ID':
                row_id_col = i
                break

        if row_id_col is None:
            raise SynapseError("Can't Delete. No ROW_IDs found.")

        uri = '/entity/{id}/table/deleteRows'.format(id=self.tableId)
        return syn.restPOST(uri, body=json.dumps(RowSelection(
            rowIds=[row[row_id_col] for row in self],
            etag=self.etag,
            tableId=self.tableId)))

    def asDataFrame(self, rowIdAndVersionInIndex=True):
        test_import_pandas()
        import pandas as pd

        try:
            df = pd.read_csv(self.filepath,
                    sep=self.separator,
                    lineterminator=self.lineEnd,
                    quotechar=self.quoteCharacter,
                    escapechar=self.escapeCharacter,
                    header = 0 if self.header else None,
                    skiprows=self.linesToSkip)
        except pd.parser.CParserError as ex1:
            df = pd.DataFrame()

        if rowIdAndVersionInIndex and "ROW_ID" in df.columns and "ROW_VERSION" in df.columns:
            ## combine row-ids (in index) and row-versions (in column 0) to
            ## make new row labels consisting of the row id and version
            ## separated by a dash.
            df.index = row_labels_from_id_and_version(izip(df["ROW_ID"], df["ROW_VERSION"]))
            del df["ROW_ID"]
            del df["ROW_VERSION"]

        return df

    def asRowSet(self):
        ## Extract row id and version, if present in rows
        row_id_col = None
        row_ver_col = None
        for i, header in enumerate(self.headers):
            if header.name=='ROW_ID':
                row_id_col = i
            elif header.name=='ROW_VERSION':
                row_ver_col = i

        def to_row_object(row, row_id_col=None, row_ver_col=None):
            if isinstance(row, Row):
                return row
            rowId = row[row_id_col] if row_id_col is not None else None
            versionNumber = row[row_ver_col] if row_ver_col is not None else None
            values = [elem for i, elem in enumerate(row) if i not in [row_id_col, row_ver_col]]
            return Row(values, rowId=rowId, versionNumber=versionNumber)

        return RowSet(headers=[elem for i, elem in enumerate(self.headers) if i not in [row_id_col, row_ver_col]],
                      tableId=self.tableId,
                      etag=self.etag,
                      rows=[to_row_object(row, row_id_col, row_ver_col) for row in self])

    def setColumnHeaders(self, headers):
        """
        Set the list of :py:class:`synapseclient.table.SelectColumn` objects that
        will be used to convert fields to the appropriate data types.

        Column headers are automatically set when querying.
        """
        if self.includeRowIdAndRowVersion:
            names = [header.name for header in headers]
            if "ROW_ID" not in names and "ROW_VERSION" not in names:
                headers = [SelectColumn(name="ROW_ID", columnType="STRING"),
                           SelectColumn(name="ROW_VERSION", columnType="STRING")] + headers
        self.headers = headers

    def __iter__(self):
        def iterate_rows(filepath, headers):
            with open(filepath) as f:
                reader = csv.reader(f,
                    delimiter=self.separator,
                    escapechar=self.escapeCharacter,
                    lineterminator=self.lineEnd,
                    quotechar=self.quoteCharacter)
                if self.header:
                    header = reader.next()
                for row in reader:
                    yield cast_values(row, headers)
        return iterate_rows(self.filepath, self.headers)
