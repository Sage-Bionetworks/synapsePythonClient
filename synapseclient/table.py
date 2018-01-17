"""
******
Tables
******

Synapse Tables enable storage of tabular data in Synapse in a form that can be
queried using a SQL-like query language.

A table has a :py:class:`Schema` and holds a set of rows conforming to
that schema.

A :py:class:`Schema` defines a series of :py:class:`Column` of the following
types: STRING, DOUBLE, INTEGER, BOOLEAN, DATE, ENTITYID, FILEHANDLEID, LINK, LARGETEXT, USERID
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
        print(row)

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

--------------------
Table attached files
--------------------

Synapse tables support a special column type called 'File' which contain a file
handle, an identifier of a file stored in Synapse. Here's an example of how
to upload files into Synapse, associate them with a table and read them back
later::

    ## your synapse project
    project = syn.get(...)

    covers_dir = '/path/to/album/covers/'

    ## store the table's schema
    cols = [
        Column(name='artist', columnType='STRING', maximumSize=50),
        Column(name='album', columnType='STRING', maximumSize=50),
        Column(name='year', columnType='INTEGER'),
        Column(name='catalog', columnType='STRING', maximumSize=50),
        Column(name='cover', columnType='FILEHANDLEID')]
    schema = syn.store(Schema(name='Jazz Albums', columns=cols, parent=project))

    ## the actual data
    data = [["John Coltrane",  "Blue Train",   1957, "BLP 1577", "coltraneBlueTrain.jpg"],
            ["Sonny Rollins",  "Vol. 2",       1957, "BLP 1558", "rollinsBN1558.jpg"],
            ["Sonny Rollins",  "Newk's Time",  1958, "BLP 4001", "rollinsBN4001.jpg"],
            ["Kenny Burrel",   "Kenny Burrel", 1956, "BLP 1543", "burrellWarholBN1543.jpg"]]

    ## upload album covers
    for row in data:
        file_handle = syn.uploadSynapseManagedFileHandle(os.path.join(covers_dir, row[4]))
        row[4] = file_handle['id']

    ## store the table data
    row_reference_set = syn.store(RowSet(columns=cols, schema=schema, rows=[Row(r) for r in data]))

    ## Later, we'll want to query the table and download our album covers
    results = syn.tableQuery("select artist, album, year, catalog, cover from %s where artist = 'Sonny Rollins'" % schema.id)
    cover_files = syn.downloadTableColumns(results, ['cover'])

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
`query examples <http://docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html>`_.

~~~~~~
Schema
~~~~~~

.. autoclass:: synapseclient.table.Schema
   :members:
   :noindex:
   
.. autoclass:: synapseclient.table.EntityViewSchema
   :members:
   :noindex:

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
RowSet
~~~~~~

.. autoclass:: synapseclient.table.RowSet
   :members: __init__

~~~~~~
Table
~~~~~~

.. autoclass:: synapseclient.table.TableAbstractBaseClass
   :members:
.. autoclass:: synapseclient.table.RowSetTable
   :members:
.. autoclass:: synapseclient.table.TableQueryResult
   :members:
.. autoclass:: synapseclient.table.CsvFileTable
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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from future.utils import bytes_to_native_str
from builtins import str

from backports import csv
import io
import json
import os
import re
import six
import sys
import tempfile
import copy
import collections
import itertools
from collections import OrderedDict, Sized, Iterable, Mapping, namedtuple
from builtins import zip
from abc import ABCMeta, abstractmethod

from .utils import id_of, from_unix_epoch_time
from .exceptions import *
from .dict_object import DictObject
from .entity import Entity, Versionable, _entity_type_to_class
from . import concrete_types

aggregate_pattern = re.compile(r'(count|max|min|avg|sum)\((.+)\)')

DTYPE_2_TABLETYPE = {'?':'BOOLEAN',
                     'd': 'DOUBLE', 'g': 'DOUBLE', 'e': 'DOUBLE', 'f': 'DOUBLE',
                     'b': 'INTEGER', 'B': 'INTEGER', 'h': 'INTEGER', 'H': 'INTEGER',
                     'i': 'INTEGER', 'I': 'INTEGER', 'l': 'INTEGER', 'L': 'INTEGER',
                     'm': 'INTEGER', 'q': 'INTEGER', 'Q': 'INTEGER',
                     'S': 'STRING', 'U': 'STRING', 'O': 'STRING',
                     'a': 'STRING', 'p': 'INTEGER', 'M': 'DATE'}

MAX_NUM_TABLE_COLUMNS = 152

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


def encode_param_in_python2(a, encoding=None):
    """
    In Python2, the csv module takes parameters that must be encoded byte
    strings - for example: delimiter, escapechar, lineterminator, quotechar.
    But, in Python 3, these have to be unicode strings. Since we're using
    unicode_literals, we'll need to do the conversion in the Python2 case.
    """
    if hasattr(sys.stdout, 'encoding'):
        encoding = sys.stdout.encoding
    if not encoding:
        encoding = 'utf-8'
    if six.PY2 and type(a)==unicode:
        return a.encode(encoding)
    else:
        return a


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
            maxStrLen = df[col].str.len().max()
            if maxStrLen>1000:
                cols.append(Column(name=col, columnType='LARGETEXT', defaultValue=''))
            else:
                size = int(round(min(1000, max(30, maxStrLen*1.5))))  #Determine lenght of longest string
                cols.append(Column(name=col, columnType=columnType, maximumSize=size, defaultValue=''))
        else:
            cols.append(Column(name=col, columnType=columnType))
    return cols


def df2Table(df, syn, tableName, parentProject):
    """Creates a new table from data in pandas data frame.
    parameters: df, tableName, parentProject
    """

    #Create columns:
    cols = as_table_columns(df)
    cols = [syn.store(col) for col in cols]

    #Create Table Schema
    schema1 = Schema(name=tableName, columns=cols, parent=parentProject)
    schema1 = syn.store(schema1)


    #Add data to Table
    for i in range(0, df.shape[0]/1200+1):
        start =  i*1200
        end = min((i+1)*1200, df.shape[0])
        rowset1 = RowSet(columns=cols, schema=schema1,
                         rows=[Row(list(df.ix[j,:])) for j in range(start,end)])
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

    if isinstance(value, six.string_types):
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
    return ["_".join(map(str, row)) for row in rows]

def row_labels_from_rows(rows):
    return row_labels_from_id_and_version([(row['rowId'], row['versionNumber'], row['etag'])
                                           if 'etag' in row else (row['rowId'], row['versionNumber'])
                                           for row in rows])


def cast_values(values, headers):
    """
    Convert a row of table query results from strings to the correct column type.

    See: http://docs.synapse.org/rest/org/sagebionetworks/repo/model/table/ColumnType.html
    """
    if len(values) != len(headers):
        raise ValueError('Each field in the row must have a matching column header. %d fields, %d headers' % (len(values), len(headers)))

    result = []
    for header, field in zip(headers, values):

        columnType = header.get('columnType', 'STRING')

        ## convert field to column type
        if field is None or field=='':
            result.append(None)
        elif columnType in {'STRING', 'ENTITYID', 'FILEHANDLEID', 'LARGETEXT', 'USERID'}:
            result.append(field)
        elif columnType=='DOUBLE':
            result.append(float(field))
        elif columnType=='INTEGER':
            result.append(int(field))
        elif columnType=='BOOLEAN':
            result.append(to_boolean(field))
        elif columnType=='DATE':
            result.append(from_unix_epoch_time(field))
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

@six.add_metaclass(ABCMeta)
class SchemaBase(Entity, Versionable):
    '''
    This is the an Abstract Class for EntityViewSchema and Schema containing the common methods for both
    You can not create an object of this type
    '''

    _property_keys = Entity._property_keys + Versionable._property_keys + ['columnIds']
    _local_keys = Entity._local_keys + ['columns_to_store']

    @property
    @abstractmethod  #forces subclasses to define _synapse_entity_type
    def _synapse_entity_type(self):
        pass

    @abstractmethod
    def __init__(self, name, columns, properties, annotations, local_state, parent, **kwargs):
        self.properties.setdefault('columnIds',[])
        self.__dict__.setdefault('columns_to_store',[])

        if name:
            kwargs['name'] = name
        super(SchemaBase, self).__init__(properties=properties,
                                     annotations=annotations, local_state=local_state, parent=parent, **kwargs)
        if columns:
            self.addColumns(columns)


    def addColumn(self, column):
        """
        :param column: a column object or its ID
        """
        if isinstance(column, six.string_types) or isinstance(column, int) or hasattr(column, 'id'):
            self.properties.columnIds.append(id_of(column))
        elif isinstance(column, Column):
            if not self.__dict__.get('columns_to_store', None):
                self.__dict__['columns_to_store'] = []
            self.__dict__['columns_to_store'].append(column)
        else:
            raise ValueError("Not a column? %s" % str(column))


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
        if isinstance(column, six.string_types) or isinstance(column, int) or hasattr(column, 'id'):
            self.properties.columnIds.remove(id_of(column))
        elif isinstance(column, Column) and self.columns_to_store:
            self.columns_to_store.remove(column)
        else:
            ValueError("Can't remove column %s" + str(column))


    def has_columns(self):
        """Does this schema have columns specified?"""
        return bool(self.properties.get('columnIds',None) or self.__dict__.get('columns_to_store',None))


    def _before_synapse_store(self, syn):
        if len(self.columns_to_store) + len(self.columnIds) > MAX_NUM_TABLE_COLUMNS:
            raise ValueError("Too many columns. The limit is %s columns per table" % MAX_NUM_TABLE_COLUMNS)

        ## store any columns before storing table
        if self.columns_to_store:
            self.properties.columnIds.extend(column.id for column in syn.createColumns(self.columns_to_store))
            self.columns_to_store = []

class Schema(SchemaBase):
    """
    A Schema is an :py:class:`synapseclient.entity.Entity` that defines a set of columns in a table.

    :param name: the name for the Table Schema object
    :param description: User readable description of the schema
    :param columns: a list of :py:class:`Column` objects or their IDs
    :param parent: the project in Synapse to which this table belongs
    :param properties:      A map of Synapse properties
    :param annotations:     A map of user defined annotations
    :param local_state:     Internal use only
                            
    Example::

        cols = [Column(name='Isotope', columnType='STRING'),
                Column(name='Atomic Mass', columnType='INTEGER'),
                Column(name='Halflife', columnType='DOUBLE'),
                Column(name='Discovered', columnType='DATE')]

        schema = syn.store(Schema(name='MyTable', columns=cols, parent=project))
    """
    _synapse_entity_type = 'org.sagebionetworks.repo.model.table.TableEntity'

    def __init__(self, name=None, columns=None, parent=None, properties=None, annotations=None, local_state=None, **kwargs):
        super(Schema, self).__init__(name=name, columns=columns, properties=properties,
                                     annotations=annotations, local_state=local_state, parent=parent, **kwargs)



class EntityViewSchema(SchemaBase):
    """
    A EntityViewSchema is a :py:class:`synapseclient.entity.Entity` that displays all files/projects (depending on user choice) within a given set of scopes

    :param name: the name of the Entity View Table object
    :param columns: a list of :py:class:`Column` objects or their IDs. These are optional.
    :param parent: the project in Synapse to which this table belongs
    :param scopes: a list of Projects/Folders or their ids
    :param type: the type of EntityView to display: either 'file','project' or 'file_and_table'. Defaults to 'file'.
    :param addDefaultViewColumns: If true, adds all default columns (e.g. name, createdOn, modifiedBy etc.) Defaults to True.
     The default columns will be added after a call to :py:meth:`synapseclient.Synapse.store`.
    :param addAnnotationColumns: If true, adds columns for all annotation keys defined across all Entities in the EntityViewSchema's scope. Defaults to True.
     The annotation columns will be added after a call to :py:meth:`synapseclient.Synapse.store`.
    :param ignoredAnnotationColumnNames: A list of strings representing annotation names. When addAnnotationColumns is True,
                                        the names in this list will not be automatically added as columns to the EntityViewSchema
                                        if they exist in any of the defined scopes.
    :param properties:      A map of Synapse properties
    :param annotations:     A map of user defined annotations
    :param local_state:     Internal use only
    
    Example::
    
        project_or_folder = syn.get("syn123")  
        schema = syn.store(EntityViewSchema(name='MyTable', parent=project, scopes=[project_or_folder_id, 'syn123'], view_type='file'))
    """

    _synapse_entity_type = 'org.sagebionetworks.repo.model.table.EntityView'
    _property_keys = SchemaBase._property_keys + ['type', 'scopeIds']
    _local_keys = SchemaBase._local_keys + ['addDefaultViewColumns', 'addAnnotationColumns', 'ignoredAnnotationColumnNames']

    def __init__(self, name=None, columns=None, parent=None, scopes=None, type=None, addDefaultViewColumns=True, addAnnotationColumns=True, ignoredAnnotationColumnNames=[], properties=None, annotations=None, local_state=None, **kwargs):
        if type:
            kwargs['type'] = type

        self.ignoredAnnotationColumnNames =  set(ignoredAnnotationColumnNames)
        super(EntityViewSchema, self).__init__(name=name, columns=columns, properties=properties,
                                               annotations=annotations, local_state=local_state, parent=parent, **kwargs)

        #This is a hacky solution to make sure we don't try to add columns to schemas that we retrieve from synapse
        is_from_normal_constructor = not (properties or local_state)
        self.addDefaultViewColumns = addDefaultViewColumns and is_from_normal_constructor #allowing annotations because user might want to update annotations all at once
        self.addAnnotationColumns = addAnnotationColumns and is_from_normal_constructor

        #set default values after constructor so we don't overwrite the values defined in properties
        #using .get() because properties, unlike local_state, do not have nonexistent keys assigned with a value of None
        if self.get('type') == None:
            self.type = 'file'
        if self.get('scopeIds') == None:
            self.scopeIds = []

        #add the scopes last so that we can append the passed in scopes to those defined in properties
        if scopes is not None:
            self.add_scope(scopes)

    def add_scope(self, entities):
        """
        :param entities: a Project or Folder object or its ID, can also be a list of them
        """
        if isinstance(entities, list):
            temp_list = [id_of(entity) for entity in entities] #add ids to a temp list so that we don't partially modify scopeIds on an exception in id_of()
            self.scopeIds.extend(temp_list)
        else:
            self.scopeIds.append(id_of(entities))

    def _before_synapse_store(self, syn):
        if self.addAnnotationColumns:
            self._add_annotations_as_columns(syn)
            self.addAnnotationColumns = False

        #get the default EntityView columns from Synapse and add them to the columns list
        if self.addDefaultViewColumns:
            self.addColumns(syn._get_default_entity_view_columns(self.type))
            self.addDefaultViewColumns = False

        super(EntityViewSchema, self)._before_synapse_store(syn)


    def _add_annotations_as_columns(self, syn):
        column_type_to_annotation_names = {
            'STRING': set(),
            'INTEGER': set(),
            'DOUBLE': set(),
            'DATE': set()
        }
        all_existing_column_names = set() # set of all existing columns names regardless of type

        # add to existing columns the columns that user has added but not yet created in synapse
        column_generator = itertools.chain(syn.getColumns(self.columnIds), self.columns_to_store) if self.columns_to_store else syn.getColumns(self.columnIds)

        for column in column_generator:
            column_name = column['name']
            column_type = column['columnType']

            all_existing_column_names.add(column_name)
            #add to type specific set
            if column_type in column_type_to_annotation_names:
                column_type_to_annotation_names[column_type].add(column_name)


        #get annotations from each of the scopes and create columns
        columns_to_add = [] #temporarily store all columns so that none are added if any errors occur
        anno_columns = syn._get_annotation_entity_view_columns(self.scopeIds, self.type)
        for column in anno_columns:
            anno_col_name = column['name']
            anno_col_type = column['columnType']
            typed_col_name_set = column_type_to_annotation_names[anno_col_type]
            if (anno_col_name not in self.ignoredAnnotationColumnNames
                 and anno_col_name not in typed_col_name_set):

                if anno_col_name in all_existing_column_names:
                    raise ValueError("The annotation column name [%s] has multiple types in your scopes or in your defined columns.\n"
                                     "Please do one of the following:\n"
                                     "  Turn off the automatic conversion of annotations to column names: entityView.addAnnotationColumns = False\n"
                                     "  Modify your annotations/columns named [%s] to all be of the same type.\n"
                                     "  Add the annotation name to the set of ignored annotation names via entityView.ignoredAnnotations.add(%s).\n" % (anno_col_name, anno_col_name, anno_col_name))
                all_existing_column_names.add(anno_col_name)
                typed_col_name_set.add(anno_col_name)
                columns_to_add.append(column)
        self.addColumns(columns_to_add)



## add Schema to the map of synapse entity types to their Python representations
_entity_type_to_class[Schema._synapse_entity_type] = Schema
_entity_type_to_class[EntityViewSchema._synapse_entity_type] = EntityViewSchema


## allowed column types
## see http://rest.synpase.org/org/sagebionetworks/repo/model/table/ColumnType.html
ColumnTypes = ['STRING','DOUBLE','INTEGER','BOOLEAN','DATE','FILEHANDLEID','ENTITYID','LINK', 'LARGETEXT', 'USERID']


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
        self['concreteType'] = 'org.sagebionetworks.repo.model.table.ColumnModel'


    def postURI(self):
        return '/column'

@six.add_metaclass(ABCMeta)
class AppendableRowset(DictObject):
    """Abstract Base Class for :py:class:`Rowset` and :py:class:`PartialRowset`"""
    @abstractmethod
    def __init__(self, schema, **kwargs):
        if ('tableId' not in kwargs) and schema:
            kwargs['tableId'] = id_of(schema)

        if not kwargs.get('tableId',None):
            raise ValueError("Table schema ID must be defined to create a %s" % type(self).__name__)
        super(AppendableRowset, self).__init__(kwargs)


    def _synapse_store(self, syn):
        """
        Creates and POSTs an AppendableRowSetRequest_

        .. AppendableRowSetRequest: http://docs.synapse.org/rest/org/sagebionetworks/repo/model/table/AppendableRowSetRequest.html
        """
        append_rowset_request ={'concreteType': concrete_types.APPENDABLE_ROWSET_REQUEST,
               'toAppend':self,
               'entityId':self.tableId}

        response = syn._POST_table_transaction(self.tableId, append_rowset_request)
        return response['results'][0]



class PartialRowset(AppendableRowset):
    """A set of Partial Rows used for updating cells of a table.
    PartialRowsets allow you to push only the individual cells you wish to change
    instead of pushing entire rows with many unchanged cells.

    Example::
        #### the following code will change cells in a hypothetical table, syn123:
        #### these same steps will also work for using EntityView tables to change Entity annotations
        #
        # fooCol | barCol             fooCol    |  barCol
        # -----------------  =======> ----------------------
        # foo1   | bar1               foo foo1  |  bar1
        # foo2   | bar2               foo2      |  bar bar 2

        query_results = syn.tableQuery("SELECT * FROM syn123")

        # The easiest way to know the rowId of the row you wish to change
        # is by converting the table to a pandas Dataframe with rowIdAndVersionInIndex=False
        df = query_results.asDataFrame(rowIdAndVersionInIndex=False)

        partial_changes = {df['ROW_ID'][0]: {'fooCol': 'foo foo 1'},
                           df['ROW_ID'][1]: {'barCol': 'bar bar 2'}}

        # you will need to pass in your original query result as an argument
        # so that we can perform column id translation and etag retrieval on your behalf:
        partial_rowset = PartialRowset.from_mapping(partial_changes, query_results)
        syn.store(partial_rowset)

    :param schema: The :py:class:`Schema` of the table to update or its tableId as a string
    :param rows: A list of PartialRows
    """

    @classmethod
    def from_mapping(cls, mapping, originalQueryResult):
        """Creates a PartialRowset
        :param mapping: A mapping of mappings in the structure: {ROW_ID : {COLUMN_NAME: NEW_COL_VALUE}}
        :param originalQueryResult:
        :return: a PartialRowSet that can be syn.store()-ed to apply the changes
        """
        if not isinstance(mapping, Mapping):
            raise ValueError("mapping must be a supported Mapping type such as 'dict'")

        try:
            name_to_column_id = {col.name:col.id for col in originalQueryResult.headers if 'id' in col}
        except AttributeError as e:
            raise ValueError('originalQueryResult must be the result of a syn.tableQuery()')

        row_ids = set(int(id) for id in six.iterkeys(mapping))

        #row_ids in the originalQueryResult are not guaranteed to be in ascending order
        #iterate over all etags but only map the row_ids used for this partial update to their etags
        row_etags = {row_id:etag for row_id, etag in originalQueryResult.iter_etags()
                     if row_id in row_ids and etag is not None}

        partial_rows = [PartialRow(row_changes, row_id, etag=row_etags.get(int(row_id)), nameToColumnId=name_to_column_id)
                        for row_id, row_changes in six.iteritems(mapping)]

        return cls(originalQueryResult.tableId, partial_rows)


    def __init__(self, schema, rows):
        super(PartialRowset, self).__init__(schema)
        self.concreteType = concrete_types.PARTIAL_ROW_SET

        if isinstance(rows, PartialRow):
            self.rows = [rows]
        else:
            try:
                if all(isinstance(row, PartialRow) for row in rows):
                    self.rows = list(rows)
                else:
                    raise ValueError("rows must contain only values of type PartialRow")
            except TypeError:
                raise ValueError("rows must be iterable")



class RowSet(AppendableRowset):
    """
    A Synapse object of type `org.sagebionetworks.repo.model.table.RowSet <http://docs.synapse.org/rest/org/sagebionetworks/repo/model/table/RowSet.html>`_.

    :param schema:   A :py:class:`synapseclient.table.Schema` object that will be used to set the tableId
    :param headers:  The list of SelectColumn objects that describe the fields in each row.
    :param columns:  An alternative to 'headers', a list of column objects that describe the fields in each row.
    :param tableId:  The ID of the TableEntity that owns these rows
    :param rows:     The :py:class:`synapseclient.table.Row` s of this set. The index of each row value aligns with the index of each header.
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
            if columns and schema:
                raise ValueError("Please only user either 'columns' or 'schema' as an argument but not both.")
            if columns:
                kwargs.setdefault('headers',[]).extend([SelectColumn.from_column(column) for column in columns])
            elif schema and isinstance(schema, Schema):
                kwargs.setdefault('headers',[]).extend([SelectColumn(id=id) for id in schema["columnIds"]])

        if not kwargs.get('headers',None):
            raise ValueError("Column headers must be defined to create a RowSet")
        kwargs['concreteType'] = 'org.sagebionetworks.repo.model.table.RowSet'

        super(RowSet, self).__init__(schema, **kwargs)


    def _synapse_store(self, syn):
            response = super(RowSet, self)._synapse_store(syn)
            return response.get('rowReferenceSet', response)


    def _synapse_delete(self, syn):
        """
        Delete the rows in the RowSet.
        Example::
            syn.delete(syn.tableQuery('select name from %s where no_good = true' % schema1.id))
        """
        #TODO: this is deprecated, switch to transaction based async delete
        uri = '/entity/{id}/table/deleteRows'.format(id=self.tableId)
        return syn.restPOST(uri, body=json.dumps(RowSelection(
            rowIds=[row.rowId for row in self.rows],
            etag=self.etag,
            tableId=self.tableId)))


class Row(DictObject):
    """
    A `row <http://docs.synapse.org/rest/org/sagebionetworks/repo/model/table/Row.html>`_ in a Table.

    :param values:         A list of values
    :param rowId:          The immutable ID issued to a new row
    :param versionNumber:  The version number of this row. Each row version is immutable, so when a row is updated a new version is created.
    """
    def __init__(self, values, rowId=None, versionNumber=None, etag=None):
        super(Row, self).__init__()
        self.values = values
        if rowId is not None:
            self.rowId = rowId
        if versionNumber is not None:
            self.versionNumber = versionNumber
        if etag is not None:
            self.etag = etag


class PartialRow(DictObject):
    """This is a lower-level class for use in :py:class::`PartialRowSet` to update individual cells within a table.

    It is recommended you use :py:classmethod::`PartialRowSet.from_mapping`to construct partial changesets to a table.

    If you want to do the tedious parts yourself:

    To change cells in the "foo"(colId:1234) and "bar"(colId:456) columns of a row with rowId=5 ::
        rowId = 5

        #pass in with columnIds as key:
        PartialRow({123: 'fooVal', 456:'barVal'}, rowId)

        #pass in with a nameToColumnId argument

        #manually define:
        nameToColumnId = {'foo':123, 'bar':456}
        #OR if you have the result of a tableQuery() you can generate nameToColumnId using:
        query_result = syn.tableQuery("SELECT * FROM syn123")
        nameToColumnId = {col.name:col.id for col in query_result.headers}

        PartialRow({'foo': 'fooVal', 'bar':'barVal'}, rowId, nameToColumnId=nameToColumnId)

    :param values: A Mapping where:
                - key is name of the column (or its columnId) to change in the desired row
                - value is the new desired value for that column
    :param rowId: The id of the row to be updated
    :param etag: used for updating File/Project Views(::py:class:`EntityViewSchema`). Not necessary for a (::py:class:`Schema`) Table
    :param nameToColumnId: Optional map column names to column Ids. If this is provided, the keys of your `values`
                           Mapping will be replaced with the column ids in the `nameToColumnId` dict. Include this as an argument
                           when you are providing the column names instead of columnIds as the keys to the `values` Mapping

    """

    def __init__(self, values, rowId, etag=None, nameToColumnId=None):
        super(PartialRow, self).__init__()
        if not isinstance(values, Mapping):
            raise ValueError("values must be a Mapping")

        rowId = int(rowId)

        self.values = [{'key': nameToColumnId[x_key] if nameToColumnId is not None else x_key,
                        'value': x_value} for x_key, x_value in six.iteritems(values)]
        self.rowId = rowId
        if etag is not None:
            self.etag = etag


class RowSelection(DictObject):
    """
    A set of rows to be `deleted <http://docs.synapse.org/rest/POST/entity/id/table/deleteRows.html>`_.

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
        #TODO: this is deprecated, switch to transaction based async delete
        uri = '/entity/{id}/table/deleteRows'.format(id=self.tableId)
        return syn.restPOST(uri, body=json.dumps(self))


def Table(schema, values, **kwargs):
    """
    Combine a table schema and a set of values into some type of Table object
    depending on what type of values are given.

    :param schema: a table :py:class:`Schema` object
    :param values: an object that holds the content of the tables
      - a :py:class:`RowSet`
      - a list of lists (or tuples) where each element is a row
      - a string holding the path to a CSV file
      - a Pandas `DataFrame <http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe>`_
      - a dict which will be wrapped by a Pandas `DataFrame <http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe>`_
      
      
    :return: a Table object suitable for storing

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
    elif isinstance(values, six.string_types):
        return CsvFileTable(schema, filepath=values, **kwargs)

    ## pandas DataFrame
    elif pandas_available and isinstance(values, pd.DataFrame):
        return CsvFileTable.from_data_frame(schema, values, **kwargs)

    ## dict
    elif pandas_available and isinstance(values, dict):
        return CsvFileTable.from_data_frame(schema, pd.DataFrame(values), **kwargs)

    else:
        raise ValueError("Don't know how to make tables from values of type %s." % type(values))


class TableAbstractBaseClass(Iterable, Sized):
    """
    Abstract base class for Tables based on different data containers.
    """

    IdEtagTuple = namedtuple('IdEtagTuple', ['row_id', 'row_etag'])

    def __init__(self, schema, headers=None, etag=None):
        if isinstance(schema, Schema):
            self.schema = schema
            self.tableId = schema.id if schema and 'id' in schema else None
            self.headers = headers if headers else [SelectColumn(id=id) for id in schema.columnIds]
            self.etag = etag
        elif isinstance(schema, six.string_types):
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
            first_row = next(iter(self))
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
        #TODO: this is deprecated, switch to transaction based async delete
        uri = '/entity/{id}/table/deleteRows'.format(id=self.tableId)
        return syn.restPOST(uri, body=json.dumps(RowSelection(
            rowIds=[row['rowId'] for row in self],
            etag=self.etag,
            tableId=self.tableId)))


    @abstractmethod
    def iter_etags(self):
        """Iterates the table results to get row_id and row_etag. If an etag does not exist for a row,
        it will generated as (row_id, None)

        :return: a generator that gives :py:class::`collections.namedtuple` with format (row_id, row_etag)
        """
        pass


class RowSetTable(TableAbstractBaseClass):
    """
    A Table object that wraps a RowSet.
    """
    def __init__(self, schema, rowset):
        super(RowSetTable, self).__init__(schema, etag=rowset.get('etag', None))
        self.rowset = rowset

    def _synapse_store(self, syn):
        row_reference_set = syn.store(self.rowset)
        return RowSetTable(self.schema, row_reference_set)

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

    def __len__(self):
        return len(self.rowset['rows'])

    def __len__(self):
        return len(self.rowset['rows'])


    def iter_etags(self):
        raise NotImplementedError("iter_etags is not supported for RowSetTable")


class TableQueryResult(TableAbstractBaseClass):
    """
    An object to wrap rows returned as a result of a table query.
    The TableQueryResult object can be used to iterate over results of a query.

    Example ::
    
        results = syn.tableQuery("select * from syn1234")
        for row in results:
            print(row)
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

    def asDataFrame(self, rowIdAndVersionInIndex=True):
        """
        Convert query result to a Pandas DataFrame.
        :param rowIdAndVersionInIndex: Make the dataframe index consist of the row_id and row_version (and row_etag if it exists)
        """
        test_import_pandas()
        import pandas as pd

        ## To turn a TableQueryResult into a data frame, we add a page of rows
        ## at a time on the untested theory that it's more efficient than
        ## adding a single row at a time to the data frame.

        def construct_rownames(rowset, offset=0):
            try:

                return row_labels_from_rows(rowset['rows']) if rowIdAndVersionInIndex else None
            except KeyError:
                ## if we don't have row id and version, just number the rows
                # python3 cast range to list for safety
                return list(range(offset,offset+len(rowset['rows'])))

        ## first page of rows
        offset = 0
        rownames = construct_rownames(self.rowset, offset)
        offset += len(self.rowset['rows'])
        series = OrderedDict()

        if not rowIdAndVersionInIndex:
            #Since we use an OrderedDict this must happen before we construct the other columns
            #add row id, verison, and etag as rows
            append_etag = False #only useful when (not rowIdAndVersionInIndex), hooray for lazy variables!
            series['ROW_ID']  =  pd.Series(name='ROW_ID', data=[row['rowId'] for row in self.rowset['rows']])
            series['ROW_VERSION'] = pd.Series(name='ROW_VERSION', data=[row['versionNumber'] for row in self.rowset['rows']])

            row_etag = [row.get('etag') for row in self.rowset['rows']]
            if any(row_etag):
                append_etag = True
                series['ROW_ETAG'] = pd.Series(name='ROW_ETAG', data=row_etag)

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

            if not rowIdAndVersionInIndex:
                series['ROW_ID'].append(pd.Series(name='ROW_ID', data=[row['id'] for row in self.rowset['rows']]))
                series['ROW_VERSION'].append(pd.Series(name='ROW_VERSION', data=[row['version'] for row in self.rowset['rows']]))
                if append_etag:
                    series['ROW_ETAG'] = pd.Series(name='ROW_ETAG', data=[row.get('etag') for row in self.rowset['rows']])

            for i, header in enumerate(self.rowset["headers"]):
                column_name = header.name
                series[column_name] = series[column_name].append(pd.Series(name=column_name,
                                                                           data=[row['values'][i] for row in self.rowset['rows']],
                                                                           index=rownames),
                                                                 verify_integrity=rowIdAndVersionInIndex) # can't verify integrity when indices are just numbers instead of 'rowid_rowversion'

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
        """
        Python 2 iterator
        """
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

    def __next__(self):
        """
        Python 3 iterator
        """
        return self.next()

    def __len__(self):
        return len(self.rowset['rows'])


    def __len__(self):
        return len(self.rowset['rows'])


    def iter_etags(self):
        """Iterates the table results to get row_id and row_etag. If an etag does not exist for a row,
        it will generated as (row_id, None)

        :return: a generator that gives :py:class::`collections.namedtuple` with format (row_id, row_etag)
        """
        for row in self:
            yield type(self).IdEtagTuple(int(row['rowId']), row.get('etag'))


class CsvFileTable(TableAbstractBaseClass):
    """
    An object to wrap a CSV file that may be stored into a Synapse table or
    returned as a result of a table query.
    """

    @classmethod
    def from_table_query(cls, synapse, query, quoteCharacter='"', escapeCharacter="\\", lineEnd=str(os.linesep), separator=",", header=True, includeRowIdAndRowVersion=True):
        """
        Create a Table object wrapping a CSV file resulting from querying a Synapse table.
        Mostly for internal use.
        """

        download_from_table_result, path = synapse._queryTableCsv(
            query=query,
            quoteCharacter=quoteCharacter,
            escapeCharacter=escapeCharacter,
            lineEnd=lineEnd,
            separator=separator,
            header=header,
            includeRowIdAndRowVersion=includeRowIdAndRowVersion)

        ## A dirty hack to find out if we got back row ID and Version
        ## in particular, we don't get these back from aggregate queries
        with io.open(path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f,
                delimiter=separator,
                escapechar=escapeCharacter,
                lineterminator=lineEnd,
                quotechar=quoteCharacter)
            first_line = next(reader)
        if len(download_from_table_result['headers']) + 2 == len(first_line):
            includeRowIdAndRowVersion = True
        else:
            includeRowIdAndRowVersion = False

        self = cls(
            filepath=path,
            schema=download_from_table_result.get('tableId', None),
            etag=download_from_table_result.get('etag', None),
            quoteCharacter=quoteCharacter,
            escapeCharacter=escapeCharacter,
            lineEnd=lineEnd,
            separator=separator,
            header=header,
            includeRowIdAndRowVersion=includeRowIdAndRowVersion,
            headers=[SelectColumn(**header) for header in download_from_table_result['headers']])

        return self

    @classmethod
    def from_data_frame(cls, schema, df, filepath=None, etag=None, quoteCharacter='"', escapeCharacter="\\", lineEnd=str(os.linesep), separator=",", header=True, linesToSkip=0, includeRowIdAndRowVersion=None, headers=None, **kwargs):
        ## infer columns from data frame if not specified
        if not headers:
            cols = as_table_columns(df)
            headers = [SelectColumn.from_column(col) for col in cols]

        ## if the schema has no columns, use the inferred columns
        if isinstance(schema, Schema) and not schema.has_columns():
            schema.addColumns(cols)

        ## convert row names in the format [row_id]_[version] or [row_id]_[version]_[etag] back to columns
        etag_pattern = r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}' #etag is essentially a UUID
        row_id_version_pattern = re.compile(r'(\d+)_(\d+)(_(' + etag_pattern + r'))?')

        row_id = []
        row_version = []
        row_etag = []
        for row_name in df.index.values:
            m = row_id_version_pattern.match(str(row_name))
            row_id.append(m.group(1) if m else None)
            row_version.append(m.group(2) if m else None)
            row_etag.append(m.group(4) if m else None)

        ## include row ID and version, if we're asked to OR if it's encoded in rownames
        if includeRowIdAndRowVersion or (includeRowIdAndRowVersion is None and any(row_id)):
            df2 = df.copy()

            cls._insert_dataframe_column_if_not_exist(df2, 0, 'ROW_ID', row_id)
            cls._insert_dataframe_column_if_not_exist(df2, 1, 'ROW_VERSION', row_version)
            if any(row_etag):
                cls._insert_dataframe_column_if_not_exist(df2, 2,'ROW_ETAG', row_etag)

            df = df2
            includeRowIdAndRowVersion = True

        f = None
        try:
            if not filepath:
                temp_dir = tempfile.mkdtemp()
                filepath = os.path.join(temp_dir, 'table.csv')

            if six.PY2:
                ## pandas uses the Python standard library csv module
                ## see: http://stackoverflow.com/a/3348664/199166
                f = open(filepath, 'wb')
            else:
                f = io.open(filepath, mode='w', encoding='utf-8', newline='')

            df.to_csv(f,
                index=False,
                sep=encode_param_in_python2(separator),
                header=encode_param_in_python2(header),
                quotechar=encode_param_in_python2(quoteCharacter),
                escapechar=encode_param_in_python2(escapeCharacter),
                line_terminator=encode_param_in_python2(lineEnd),
                na_rep=encode_param_in_python2(kwargs.get('na_rep','')),
                float_format=encode_param_in_python2("%.12g"))
               # NOTE: reason for flat_format='%.12g':
               # pandas automatically converts int columns into float64 columns when some cells in the column have no value.
               # If we write the whole number back as a decimal (e.g. '3.0'), Synapse complains that we are
               # writing a float into a INTEGER(synapse table type) column.
               # Using the 'g' will strip off '.0' from whole number values.
               # pandas by default (with no float_format parameter) seems to keep 12 values after decimal, so we use '%.12g'.c
               # see SYNPY-267.
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

    @staticmethod
    def _insert_dataframe_column_if_not_exist(dataframe, insert_index, col_name, insert_column_data):
        # if the column already exists verify the column data is same as what we parsed
        if col_name in dataframe.columns:
            if dataframe[col_name].tolist() != insert_column_data:
                raise SynapseError(("A column named '{0}' "
                                   "already exists and does not match the '{0}' "
                                   "values present in the DataFrame's row names. "
                                   "Please refain from using or modifying '{0}' as a column for your data "
                                   "because it is necessary for version tracking in Synapse's tables").format(col_name) )
        else:
            dataframe.insert(insert_index, col_name, insert_column_data)

    @classmethod
    def from_list_of_rows(cls, schema, values, filepath=None, etag=None, quoteCharacter='"', escapeCharacter="\\", lineEnd=str(os.linesep), separator=",", linesToSkip=0, includeRowIdAndRowVersion=None, headers=None):

        ## create CSV file
        f = None
        try:
            if not filepath:
                temp_dir = tempfile.mkdtemp()
                filepath = os.path.join(temp_dir, 'table.csv')

            f = io.open(filepath, 'w', encoding='utf-8', newline='')

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


    def __init__(self, schema, filepath, etag=None, quoteCharacter='"', escapeCharacter="\\", lineEnd=str(os.linesep), separator=",", header=True, linesToSkip=0, includeRowIdAndRowVersion=None, headers=None):
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
        copied_self = copy.copy(self)
        return copied_self._update_self(syn)

    def _update_self(self, syn):
        if isinstance(self.schema, Schema) and self.schema.get('id', None) is None:
            ## store schema
            self.schema = syn.store(self.schema)
            self.tableId = self.schema.id

        result = syn._uploadCsv(
            self.filepath,
            self.schema if self.schema else self.tableId,
            updateEtag=self.etag,
            quoteCharacter=self.quoteCharacter,
            escapeCharacter=self.escapeCharacter,
            lineEnd=self.lineEnd,
            separator=self.separator,
            header=self.header,
            linesToSkip=self.linesToSkip)

        upload_to_table_result = result['results'][0]

        assert upload_to_table_result['concreteType'] in ('org.sagebionetworks.repo.model.table.EntityUpdateResults',
                                                          'org.sagebionetworks.repo.model.table.UploadToTableResult'), "Not an UploadToTableResult or EntityUpdateResults."
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

        #TODO: this is deprecated, switch to transaction based async delete
        uri = '/entity/{id}/table/deleteRows'.format(id=self.tableId)
        return syn.restPOST(uri, body=json.dumps(RowSelection(
            rowIds=[row[row_id_col] for row in self],
            etag=self.etag,
            tableId=self.tableId)))

    def asDataFrame(self, rowIdAndVersionInIndex=True, convert_to_datetime = False):
        """Convert query result to a Pandas DataFrame.
        :param rowIdAndVersionInIndex: Make the dataframe index consist of the row_id and row_version (and row_etag if it exists)
        :param convert_to_datetime: If set to True, will convert all Synapse DATE columns from UNIX timestamp integers into UTC datetime objects
        :return: 
        """
        test_import_pandas()
        import pandas as pd

        try:
            #Handle bug in pandas 0.19 requiring quotechar to be str not unicode or newstr
            quoteChar = bytes_to_native_str(bytes(self.quoteCharacter)) if six.PY2 else self.quoteCharacter

            #determine which columns are DATE columns so we can convert milisecond timestamps into datetime objects
            date_columns = []
            datetime_millisecond_parser = lambda milliseconds: pd.to_datetime(milliseconds, unit='ms', utc=True) #DATEs are stored in csv as unix timestamp in milliseconds
            if convert_to_datetime:
                for select_column in self.headers:
                    if select_column.columnType == "DATE":
                        date_columns.append(select_column.name)


            ## assign line terminator only if for single character
            ## line terminators (e.g. not '\r\n') 'cause pandas doesn't
            ## longer line terminators. See:
            ##    https://github.com/pydata/pandas/issues/3501
            ## "ValueError: Only length-1 line terminators supported"
            df = pd.read_csv(self.filepath,
                    sep=self.separator,
                    lineterminator=self.lineEnd if len(self.lineEnd) == 1 else None,
                    quotechar=quoteChar,
                    escapechar=self.escapeCharacter,
                    header = 0 if self.header else None,
                    skiprows=self.linesToSkip,
                    parse_dates=date_columns,
                    date_parser=datetime_millisecond_parser)
        except pd.parser.CParserError as ex1:
            df = pd.DataFrame()

        if rowIdAndVersionInIndex and "ROW_ID" in df.columns and "ROW_VERSION" in df.columns:
            ## combine row-ids (in index) and row-versions (in column 0) to
            ## make new row labels consisting of the row id and version
            ## separated by a dash.
            zip_args = [df["ROW_ID"], df["ROW_VERSION"]]
            if "ROW_ETAG" in df.columns:
                zip_args.append(df['ROW_ETAG'])

            df.index = row_labels_from_id_and_version(zip(*zip_args))
            del df["ROW_ID"]
            del df["ROW_VERSION"]
            if "ROW_ETAG" in df.columns:
                del df['ROW_ETAG']

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
            with io.open(filepath, encoding='utf-8') as f:
                reader = csv.reader(f,
                    delimiter=self.separator,
                    escapechar=self.escapeCharacter,
                    lineterminator=self.lineEnd,
                    quotechar=self.quoteCharacter)
                if self.header:
                    header = next(reader)

                for row in reader:
                    yield cast_values(row, headers)
        return iterate_rows(self.filepath, self.headers)

    def __len__(self):
        with io.open(self.filepath, encoding='utf-8') as f:
            if self.header:  #ignore the header line
                f.readline()

            return sum(1 for line in f)

    def iter_etags(self):
        """Iterates the table results to get row_id and row_etag. If an etag does not exist for a row,
        it will generated as (row_id, None)

        :return: a generator that gives :py:class::`collections.namedtuple` with format (row_id, row_etag)
        """
        with io.open(self.filepath, encoding='utf-8') as f:
            if not self.header:
                raise ValueError("The csv file must have headers in order to find the etag column")

            reader = csv.reader(f,
                                delimiter=self.separator,
                                escapechar=self.escapeCharacter,
                                lineterminator=self.lineEnd,
                                quotechar=self.quoteCharacter)
            header = next(reader)

            #The ROW_... headers are always in a predefined order
            row_id_index = header.index('ROW_ID')
            try:
                row_etag_index = header.index('ROW_ETAG')
            except ValueError:
                row_etag_index = None

            for row in reader:
                yield type(self).IdEtagTuple(int(row[row_id_index]), row[row_etag_index] if (row_etag_index is not None) else None)
