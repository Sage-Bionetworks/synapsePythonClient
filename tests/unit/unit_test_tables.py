from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

from backports import csv
import io
import math
import os
import sys
import tempfile
from builtins import zip
from mock import MagicMock
from nose.tools import assert_raises, assert_equals, assert_not_equals, raises, assert_false, assert_not_in, assert_sequence_equal
from nose import SkipTest
import unit

try:
    import pandas as pd
    pandas_found = False
except ImportError:
    pandas_found = True

from nose.tools import raises, assert_equals
import unit
import synapseclient
from synapseclient import Entity
from synapseclient.exceptions import SynapseError
from synapseclient.entity import split_entity_namespaces
from synapseclient.table import Column, Schema, CsvFileTable, TableQueryResult, cast_values, \
     as_table_columns, Table, RowSet, SelectColumn, EntityViewSchema, RowSetTable, Row, PartialRow, PartialRowset
from mock import patch
from collections import OrderedDict

def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = unit.syn


def test_cast_values():
    selectColumns = [{'id': '353',
                      'name': 'name',
                      'columnType': 'STRING'},
                     {'id': '354',
                      'name': 'foo',
                      'columnType': 'STRING'},
                     {'id': '355',
                      'name': 'x',
                      'columnType': 'DOUBLE'},
                     {'id': '356',
                      'name': 'n',
                      'columnType': 'INTEGER'},
                     {'id': '357',
                      'name': 'bonk',
                      'columnType': 'BOOLEAN'}]

    row = ('Finklestein', 'bat', '3.14159', '65535', 'true')
    assert cast_values(row, selectColumns)==['Finklestein', 'bat', 3.14159, 65535, True]

    ## group by
    selectColumns = [{'name': 'bonk',
                      'columnType': 'BOOLEAN'},
                     {'name': 'COUNT(name)',
                      'columnType': 'INTEGER'},
                     {'name': 'AVG(x)',
                      'columnType': 'DOUBLE'},
                     {'name': 'SUM(n)',
                      'columnType': 'INTEGER'}]
    row = ('true', '211', '1.61803398875', '1421365')
    assert cast_values(row, selectColumns)==[True, 211, 1.61803398875, 1421365]


def test_schema():
    schema = Schema(name='My Table', parent="syn1000001")

    assert not schema.has_columns()

    schema.addColumn(Column(id='1', name='Name', columnType='STRING'))

    assert schema.has_columns()
    assert schema.properties.columnIds == ['1']

    schema.removeColumn('1')
    assert not schema.has_columns()
    assert schema.properties.columnIds == []

    schema = Schema(name='Another Table', parent="syn1000001")

    schema.addColumns([
        Column(name='Name', columnType='STRING'),
        Column(name='Born', columnType='INTEGER'),
        Column(name='Hipness', columnType='DOUBLE'),
        Column(name='Living', columnType='BOOLEAN')])
    assert schema.has_columns()
    assert len(schema.columns_to_store) == 4
    assert Column(name='Name', columnType='STRING') in schema.columns_to_store
    assert Column(name='Born', columnType='INTEGER') in schema.columns_to_store
    assert Column(name='Hipness', columnType='DOUBLE') in schema.columns_to_store
    assert Column(name='Living', columnType='BOOLEAN') in schema.columns_to_store

    schema.removeColumn(Column(name='Living', columnType='BOOLEAN'))
    assert schema.has_columns()
    assert len(schema.columns_to_store) == 3
    assert Column(name='Living', columnType='BOOLEAN') not in schema.columns_to_store
    assert Column(name='Hipness', columnType='DOUBLE') in schema.columns_to_store


def test_RowSetTable():
    row_set_json = {
        'etag': 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
        'headers': [
         {'columnType': 'STRING', 'id': '353', 'name': 'name'},
         {'columnType': 'DOUBLE', 'id': '355', 'name': 'x'},
         {'columnType': 'DOUBLE', 'id': '3020', 'name': 'y'},
         {'columnType': 'INTEGER', 'id': '891', 'name': 'n'}],
        'rows': [{
          'rowId': 5,
          'values': ['foo', '1.23', '2.2', '101'],
          'versionNumber': 3},
         {'rowId': 6,
          'values': ['bar', '1.34', '2.4', '101'],
          'versionNumber': 3},
         {'rowId': 7,
          'values': ['foo', '1.23', '2.2', '101'],
          'versionNumber': 4},
         {'rowId': 8,
          'values': ['qux', '1.23', '2.2', '102'],
          'versionNumber': 3}],
        'tableId': 'syn2976298'}

    row_set = RowSet.from_json(row_set_json)

    assert row_set.etag == 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
    assert row_set.tableId == 'syn2976298'
    assert len(row_set.headers) == 4
    assert len(row_set.rows) == 4

    schema = Schema(id="syn2976298", name="Bogus Schema", columns=[353,355,3020,891], parent="syn1000001")

    table = Table(schema, row_set)

    assert table.etag == 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
    assert table.tableId == 'syn2976298'
    assert len(table.headers) == 4
    assert len(table.asRowSet().rows) == 4

    try:
        import pandas as pd

        df = table.asDataFrame()
        assert df.shape == (4,4)
        assert all(df['name'] == ['foo', 'bar', 'foo', 'qux'])

    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping part of test_RowSetTable.\n\n')


def test_as_table_columns():
    try:
        import pandas as pd

        df = pd.DataFrame({
            'foobar' : ("foo", "bar", "baz", "qux", "asdf"),
            'x' : tuple(math.pi*i for i in range(5)),
            'n' : (101, 202, 303, 404, 505),
            'really' : (False, True, False, True, False),
            'size' : ('small', 'large', 'medium', 'medium', 'large')})

        cols = as_table_columns(df)

        cols[0]['name'] == 'foobar'
        cols[0]['columnType'] == 'STRING'
        cols[1]['name'] == 'x'
        cols[1]['columnType'] == 'DOUBLE'
        cols[1]['name'] == 'n'
        cols[1]['columnType'] == 'INTEGER'
        cols[1]['name'] == 'really'
        cols[1]['columnType'] == 'BOOLEAN'
        cols[1]['name'] == 'size'
        # TODO: support Categorical when fully supported in Pandas Data Frames
        cols[1]['columnType'] == 'STRING'

    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping test_as_table_columns.\n\n')

def _try_import_pandas(test):
    try:
        import pandas as pd
        return pd
    except ImportError:
        raise SkipTest('Pandas is not installed, skipping '+test+'.\n\n')

def test_dict_to_table():
    pd = _try_import_pandas('test_dict_to_table')

    d = dict(a=[1,2,3], b=["c", "d", "e"])
    df = pd.DataFrame(d)
    schema = Schema(name="Baz", parent="syn12345", columns=as_table_columns(df))

    with patch.object(CsvFileTable, "from_data_frame") as mocked_from_data_frame:
        Table(schema, d)

    # call_agrs is a tuple with values and name
    agrs_list = mocked_from_data_frame.call_args[0]
    # getting the second argument
    df_agr = agrs_list[1]
    assert df_agr.equals(df)

def test_pandas_to_table():
    pd = _try_import_pandas('test_pandas_to_table')

    df = pd.DataFrame(dict(a=[1,2,3], b=["c", "d", "e"]))
    schema = Schema(name="Baz", parent="syn12345", columns=as_table_columns(df))
    print("\n", df, "\n\n")

    ## A dataframe with no row id and version
    table = Table(schema, df)

    for i, row in enumerate(table):
        print(row)
        assert row[0]==(i+1)
        assert row[1]==["c", "d", "e"][i]

    assert len(table)==3

    ## If includeRowIdAndRowVersion=True, include empty row id an versions
    ## ROW_ID,ROW_VERSION,a,b
    ## ,,1,c
    ## ,,2,d
    ## ,,3,e
    table = Table(schema, df, includeRowIdAndRowVersion=True)
    for i, row in enumerate(table):
        print(row)
        assert row[0] is None
        assert row[1] is None
        assert row[2]==(i+1)

    ## A dataframe with no row id and version
    df = pd.DataFrame(index=["1_7","2_7","3_8"], data=dict(a=[100,200,300], b=["c", "d", "e"]))
    print("\n", df, "\n\n")

    table = Table(schema, df)
    for i, row in enumerate(table):
        print(row)
        assert row[0]==["1","2","3"][i]
        assert row[1]==["7","7","8"][i]
        assert row[2]==(i+1)*100
        assert row[3]==["c", "d", "e"][i]

    ## A dataframe with row id and version in columns
    df = pd.DataFrame(dict(ROW_ID=["0","1","2"], ROW_VERSION=["8","9","9"], a=[100,200,300], b=["c", "d", "e"]))
    print("\n", df, "\n\n")

    table = Table(schema, df)
    for i, row in enumerate(table):
        print(row)
        assert row[0]==["0","1","2"][i]
        assert row[1]==["8","9","9"][i]
        assert row[2]==(i+1)*100
        assert row[3]==["c", "d", "e"][i]

def test_csv_table():
    ## Maybe not truly a unit test, but here because it doesn't do
    ## network IO to synapse
    data = [["1", "1", "John Coltrane",  1926, 8.65, False],
            ["2", "1", "Miles Davis",    1926, 9.87, False],
            ["3", "1", "Bill Evans",     1929, 7.65, False],
            ["4", "1", "Paul Chambers",  1935, 5.14, False],
            ["5", "1", "Jimmy Cobb",     1929, 5.78, True],
            ["6", "1", "Scott LaFaro",   1936, 4.21, False],
            ["7", "1", "Sonny Rollins",  1930, 8.99, True],
            ["8", "1", "Kenny Burrel",   1931, 4.37, True]]

    filename = None

    cols = []
    cols.append(Column(id='1', name='Name', columnType='STRING'))
    cols.append(Column(id='2', name='Born', columnType='INTEGER'))
    cols.append(Column(id='3', name='Hipness', columnType='DOUBLE'))
    cols.append(Column(id='4', name='Living', columnType='BOOLEAN'))

    schema1 = Schema(id='syn1234', name='Jazz Guys', columns=cols, parent="syn1000001")

    #TODO: use StringIO.StringIO(data) rather than writing files
    try:
        ## create CSV file
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            filename = temp.name

        with io.open(filename, mode='w', encoding="utf-8", newline='') as temp:
            writer = csv.writer(temp, quoting=csv.QUOTE_NONNUMERIC, lineterminator=str(os.linesep))
            headers = ['ROW_ID', 'ROW_VERSION'] + [col.name for col in cols]
            writer.writerow(headers)
            for row in data:
                print(row)
                writer.writerow(row)

        table = Table(schema1, filename)
        assert isinstance(table, CsvFileTable)

        ## need to set column headers to read a CSV file
        table.setColumnHeaders(
            [SelectColumn(name="ROW_ID", columnType="STRING"),
             SelectColumn(name="ROW_VERSION", columnType="STRING")] +
            [SelectColumn.from_column(col) for col in cols])

        ## test iterator
        # print("\n\nJazz Guys")
        for table_row, expected_row in zip(table, data):
            # print(table_row, expected_row)
            assert table_row==expected_row

        ## test asRowSet
        rowset = table.asRowSet()
        for rowset_row, expected_row in zip(rowset.rows, data):
            #print(rowset_row, expected_row)
            assert rowset_row['values']==expected_row[2:]
            assert rowset_row['rowId']==expected_row[0]
            assert rowset_row['versionNumber']==expected_row[1]

        ## test asDataFrame
        try:
            import pandas as pd

            df = table.asDataFrame()
            assert all(df['Name'] == [row[2] for row in data])
            assert all(df['Born'] == [row[3] for row in data])
            assert all(df['Living'] == [row[5] for row in data])
            assert all(df.index == ['%s_%s'%tuple(row[0:2]) for row in data])
            assert df.shape == (8,4)

        except ImportError as e1:
            sys.stderr.write('Pandas is apparently not installed, skipping asDataFrame portion of test_csv_table.\n\n')

    except Exception as ex1:
        if filename:
            try:
                if os.path.isdir(filename):
                    shutil.rmtree(filename)
                else:
                    os.remove(filename)
            except Exception as ex:
                print(ex)
        raise


def test_list_of_rows_table():
    data = [["John Coltrane",  1926, 8.65, False],
            ["Miles Davis",    1926, 9.87, False],
            ["Bill Evans",     1929, 7.65, False],
            ["Paul Chambers",  1935, 5.14, False],
            ["Jimmy Cobb",     1929, 5.78, True],
            ["Scott LaFaro",   1936, 4.21, False],
            ["Sonny Rollins",  1930, 8.99, True],
            ["Kenny Burrel",   1931, 4.37, True]]

    cols = []
    cols.append(Column(id='1', name='Name', columnType='STRING'))
    cols.append(Column(id='2', name='Born', columnType='INTEGER'))
    cols.append(Column(id='3', name='Hipness', columnType='DOUBLE'))
    cols.append(Column(id='4', name='Living', columnType='BOOLEAN'))

    schema1 = Schema(name='Jazz Guys', columns=cols, id="syn1000002", parent="syn1000001")

    ## need columns to do cast_values w/o storing
    table = Table(schema1, data, headers=[SelectColumn.from_column(col) for col in cols])

    for table_row, expected_row in zip(table, data):
        assert table_row==expected_row

    rowset = table.asRowSet()
    for rowset_row, expected_row in zip(rowset.rows, data):
        assert rowset_row['values']==expected_row

    table.columns = cols

    ## test asDataFrame
    try:
        import pandas as pd

        df = table.asDataFrame()
        assert all(df['Name'] == [r[0] for r in data])

    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping asDataFrame portion of test_list_of_rows_table.\n\n')


def test_aggregate_query_result_to_data_frame():

    try:
        import pandas as pd

        class MockSynapse(object):
            def _queryTable(self, query, limit=None, offset=None, isConsistent=True, partMask=None):
                return {'concreteType': 'org.sagebionetworks.repo.model.table.QueryResultBundle',
                        'maxRowsPerPage': 2,
                        'queryCount': 4,
                        'queryResult': {
                         'concreteType': 'org.sagebionetworks.repo.model.table.QueryResult',
                         'nextPageToken': 'aaaaaaaa',
                         'queryResults': {'etag': 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
                         'headers': [
                          {'columnType': 'STRING',  'name': 'State'},
                          {'columnType': 'INTEGER', 'name': 'MIN(Born)'},
                          {'columnType': 'INTEGER', 'name': 'COUNT(State)'},
                          {'columnType': 'DOUBLE',  'name': 'AVG(Hipness)'}],
                          'rows': [
                           {'values': ['PA', '1935', '2', '1.1']},
                           {'values': ['MO', '1928', '3', '2.38']}],
                          'tableId': 'syn2757980'}},
                        'selectColumns': [{
                         'columnType': 'STRING',
                         'id': '1387',
                         'name': 'State'}]}
            def _queryTableNext(self, nextPageToken, tableId):
                return {'concreteType': 'org.sagebionetworks.repo.model.table.QueryResult',
                        'queryResults': {'etag': 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
                         'headers': [
                          {'columnType': 'STRING',  'name': 'State'},
                          {'columnType': 'INTEGER', 'name': 'MIN(Born)'},
                          {'columnType': 'INTEGER', 'name': 'COUNT(State)'},
                          {'columnType': 'DOUBLE',  'name': 'AVG(Hipness)'}],
                         'rows': [
                          {'values': ['DC', '1929', '1', '3.14']},
                          {'values': ['NC', '1926', '1', '4.38']}],
                         'tableId': 'syn2757980'}}

        result = TableQueryResult(synapse=MockSynapse(), query="select State, min(Born), count(State), avg(Hipness) from syn2757980 group by Living")

        assert result.etag == 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
        assert result.tableId == 'syn2757980'
        assert len(result.headers) == 4

        rs = result.asRowSet()
        assert len(rs.rows) == 4

        result = TableQueryResult(synapse=MockSynapse(), query="select State, min(Born), count(State), avg(Hipness) from syn2757980 group by Living")
        df = result.asDataFrame()

        assert df.shape == (4,4)
        assert all(df['State'].values == ['PA', 'MO', 'DC', 'NC'])

        ## check integer, double and boolean types after PLFM-3073 is fixed
        assert all(df['MIN(Born)'].values == [1935, 1928, 1929, 1926]), "Unexpected values" + str(df['MIN(Born)'].values)
        assert all(df['COUNT(State)'].values == [2,3,1,1])
        assert all(df['AVG(Hipness)'].values == [1.1, 2.38, 3.14, 4.38])

    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping asDataFrame portion of test_aggregate_query_result_to_data_frame.\n\n')


def test_waitForAsync():
    syn = synapseclient.client.Synapse(debug=True, skip_checks=True)
    syn.table_query_timeout = 0.05
    syn.table_query_max_sleep = 0.001
    syn.restPOST = MagicMock(return_value={"token":"1234567"})

    # return a mocked http://docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousJobStatus.html
    syn.restGET  = MagicMock(return_value={
        "jobState": "PROCESSING",
        "progressMessage": "Test progress message",
        "progressCurrent": 10,
        "progressTotal": 100,
        "errorMessage": "Totally fubared error",
        "errorDetails": "Totally fubared error details"})

    assert_raises(synapseclient.exceptions.SynapseTimeoutError, syn._waitForAsync, uri="foo/bar", request={"foo": "bar"})


def _insert_dataframe_column_if_not_exist__setup():
    df = pd.DataFrame()
    column_name = "panda"
    data = ["pandas", "are", "alive", ":)"]
    return df, column_name, data


def test_insert_dataframe_column_if_not_exist__nonexistent_column():
    if pandas_found:
        raise SkipTest("pandas could not be found. please let the pandas into your library.")

    df, column_name, data = _insert_dataframe_column_if_not_exist__setup()

    #method under test
    CsvFileTable._insert_dataframe_column_if_not_exist(df, 0, column_name, data)

    #make sure the data was inserted
    assert_equals(data, df[column_name].tolist())


def test_insert_dataframe_column_if_not_exist__existing_column_matching():
    if pandas_found:
        raise SkipTest("pandas could not be found. please let the pandas into your library.")

    df, column_name, data = _insert_dataframe_column_if_not_exist__setup()

    #add the same data to the DataFrame prior to calling our method
    df.insert(0,column_name, data)

    #method under test
    CsvFileTable._insert_dataframe_column_if_not_exist(df, 0, column_name, data)

    #make sure the data has not changed
    assert_equals(data, df[column_name].tolist())


@raises(SynapseError)
def test_insert_dataframe_column_if_not_exist__existing_column_not_matching():
    if pandas_found:
        raise SkipTest("pandas could not be found. please let the pandas into your library.")
    df, column_name, data = _insert_dataframe_column_if_not_exist__setup()

    #add different data to the DataFrame prior to calling our method
    df.insert(0,column_name, ['mercy', 'main', 'btw'])

    #make sure the data is different
    assert_not_equals(data, df[column_name].tolist())

    #method under test should raise exception
    CsvFileTable._insert_dataframe_column_if_not_exist(df, 0, column_name, data)


def test_build_table_download_file_handle_list__repeated_file_handles():
    syn = synapseclient.client.Synapse(debug=True, skip_checks=True)

    #patch the cache so we don't look there in case FileHandle ids actually exist there
    patch.object(syn.cache, "get", return_value = None)

    cols = [
        Column(name='Name', columnType='STRING', maximumSize=50),
        Column(name='filehandle', columnType='FILEHANDLEID')]

    schema = Schema(name='FileHandleTest', columns=cols, parent='syn420')

    #using some large filehandle numbers so i don
    data = [["ayy lmao", 5318008],
            ["large numberino", 0x5f3759df],
            ["repeated file handle", 5318008],
            ["repeated file handle also", 0x5f3759df]]

    ## need columns to do cast_values w/o storing
    table = Table(schema, data, headers=[SelectColumn.from_column(col) for col in cols])

    file_handle_associations, file_handle_to_path_map = syn._build_table_download_file_handle_list(table, ['filehandle'])

    #verify only 2 file_handles are added (repeats were ignored)
    assert_equals(2, len(file_handle_associations))
    assert_equals(0, len(file_handle_to_path_map)) #might as well check anyways


def test_EntityViewSchema__default_params():
    entity_view = EntityViewSchema(parent="idk")
    assert_equals('file', entity_view.type)
    assert_equals([], entity_view.scopeIds)
    assert_equals(True, entity_view.addDefaultViewColumns)


def test_entityViewSchema__specified_type():
    view_type = 'project'
    entity_view = EntityViewSchema(parent="idk", type=view_type)
    assert_equals(view_type, entity_view.type)


def test_entityViewSchema__sepcified_scopeId():
    scopeId = ["123"]
    entity_view = EntityViewSchema(parent="idk", scopeId=scopeId)
    assert_equals(scopeId, entity_view.scopeId)


def test_entityViewSchema__sepcified_add_default_columns():
    entity_view = EntityViewSchema(parent="idk", addDefaultViewColumns=False)
    assert_false(entity_view.addDefaultViewColumns)


def test_entityViewSchema__add_default_columns_when_from_Synapse():
    properties = {u'concreteType': u'org.sagebionetworks.repo.model.table.EntityView'}
    entity_view = EntityViewSchema(parent="idk", addDefaultViewColumns=True, properties=properties)
    assert_false(entity_view.addDefaultViewColumns)




def test_entityViewSchema__add_scope():
    entity_view = EntityViewSchema(parent="idk")
    entity_view.add_scope(Entity(parent="also idk", id=123))
    entity_view.add_scope(456)
    entity_view.add_scope("789")
    assert_equals([str(x) for x in ["123","456","789"]], entity_view.scopeIds)


def test_Schema__max_column_check():
    table = Schema(name="someName", parent="idk")
    table.addColumns(Column(name="colNum%s"%i, columnType="STRING") for i in range(synapseclient.table.MAX_NUM_TABLE_COLUMNS + 1))
    assert_raises(ValueError, syn.store, table)

    
def test_EntityViewSchema__ignore_column_names_set_info_preserved():
    """
    tests that ignoredAnnotationColumnNames will be preserved after creating a new EntityViewSchema from properties, local_state, and annotations
    """
    ignored_names = {'a','b','c'}
    entity_view = EntityViewSchema("someName", parent="syn123", ignoredAnnotationColumnNames={'a','b','c'})
    properties, annotations, local_state = split_entity_namespaces(entity_view)
    entity_view_copy = Entity.create(properties, annotations, local_state)
    assert_equals( ignored_names, entity_view.ignoredAnnotationColumnNames)
    assert_equals( ignored_names, entity_view_copy.ignoredAnnotationColumnNames)



def test_EntityViewSchema__ignore_column_names():
    syn = synapseclient.client.Synapse(debug=True, skip_checks=True)

    scopeIds = ['123']
    entity_view = EntityViewSchema("someName", scopes = scopeIds ,parent="syn123", ignoredAnnotationColumnNames={'long1'})

    mocked_annotation_result1 = [Column(name='long1', columnType='INTEGER'), Column(name='long2', columnType ='INTEGER')]

    with patch.object(syn, '_get_annotation_entity_view_columns', return_value=mocked_annotation_result1) as mocked_get_annotations,\
         patch.object(syn, 'getColumns') as mocked_get_columns:

        entity_view._add_annotations_as_columns(syn)

        mocked_get_columns.assert_called_once_with([])
        mocked_get_annotations.assert_called_once_with(scopeIds, 'file')

        assert_equals([Column(name='long2', columnType='INTEGER')], entity_view.columns_to_store)


def test_EntityViewSchema__repeated_columnName():
    syn = synapseclient.client.Synapse(debug=True, skip_checks=True)

    scopeIds = ['123']
    entity_view = EntityViewSchema("someName", scopes = scopeIds ,parent="syn123")

    mocked_annotation_result1 = [Column(name='annoName', columnType='INTEGER'), Column(name='annoName', columnType='DOUBLE')]

    with patch.object(syn, '_get_annotation_entity_view_columns', return_value=mocked_annotation_result1) as mocked_get_annotations,\
         patch.object(syn, 'getColumns') as mocked_get_columns:

        assert_raises(ValueError, entity_view._add_annotations_as_columns, syn)

        mocked_get_columns.assert_called_once_with([])
        mocked_get_annotations.assert_called_once_with(scopeIds, 'file')


def test_rowset_asDataFrame__with_ROW_ETAG_column():
    query_result = {
                   'concreteType':'org.sagebionetworks.repo.model.table.QueryResultBundle',
                   'maxRowsPerPage':6990,
                   'selectColumns':[
                      {'id':'61770',  'columnType':'STRING', 'name':'annotationColumn1'},
                      {'id':'61771', 'columnType':'STRING', 'name':'annotationColumn2'}
                   ],
                   'queryCount':1,
                   'queryResult':{
                      'concreteType':'org.sagebionetworks.repo.model.table.QueryResult',
                      'nextPageToken': 'sometoken',
                      'queryResults':{
                         'headers':[
                             {'id': '61770', 'columnType': 'STRING', 'name': 'annotationColumn1'},
                             {'id': '61771', 'columnType': 'STRING', 'name': 'annotationColumn2'}],
                         'concreteType':'org.sagebionetworks.repo.model.table.RowSet',
                         'etag':'DEFAULT',
                         'tableId':'syn11363411',
                         'rows':[{ 'values':[ 'initial_value1', 'initial_value2'],
                               'etag':'7de0f326-9ef7-4fde-9e4a-ac0babca73f6',
                               'rowId':123,
                               'versionNumber':456}]
                      }
                   }
                }
    query_result_next_page = {'concreteType': 'org.sagebionetworks.repo.model.table.QueryResult',
                        'queryResults': {'etag': 'DEFAULT',
                         'headers': [
                             {'id': '61770', 'columnType': 'STRING', 'name': 'annotationColumn1'},
                             {'id': '61771', 'columnType': 'STRING', 'name': 'annotationColumn2'}],
                         'rows':[{ 'values':[ 'initial_value3', 'initial_value4'],
                               'etag':'7de0f326-9ef7-4fde-9e4a-ac0babca73f7',
                               'rowId':789,
                               'versionNumber':101112}],
                         'tableId': 'syn11363411'}}

    with patch.object(syn, "_queryTable", return_value=query_result),\
         patch.object(syn, "_queryTableNext", return_value=query_result_next_page):
        table = syn.tableQuery("select something from syn123", resultsAs='rowset')
        dataframe = table.asDataFrame()
        assert_not_in("ROW_ETAG", dataframe.columns)
        expected_indicies = ['123_456_7de0f326-9ef7-4fde-9e4a-ac0babca73f6', '789_101112_7de0f326-9ef7-4fde-9e4a-ac0babca73f7']
        assert_sequence_equal(expected_indicies, dataframe.index.values.tolist())


def test_RowSetTable_len():
    schema = Schema(parentId="syn123", id='syn456', columns=[Column(name='column_name', id='123')])
    rowset =  RowSet(schema=schema, rows=[Row(['first row']), Row(['second row'])])
    row_set_table = RowSetTable(schema, rowset)
    assert_equals(2, len(row_set_table))


def test_TableQueryResult_len():
    # schema = Schema(parentId="syn123", id='syn456', columns=[Column(name='column_name', id='123')])
    # rowset =  RowSet(schema=schema, rows=[Row(['first row']), Row(['second row'])])

    query_result_dict =  {'queryResult': {
                         'queryResults': {
                         'headers': [
                          {'columnType': 'STRING',  'name': 'col_name'}],
                          'rows': [
                           {'values': ['first_row']},
                           {'values': ['second_row']}],
                          'tableId': 'syn123'}},
                        'selectColumns': [{
                         'columnType': 'STRING',
                         'id': '1337',
                         'name': 'col_name'}]}

    query_string = "SELECT whatever FROM some_table WHERE sky=blue"
    with patch.object(syn, "_queryTable", return_value =  query_result_dict) as mocked_table_query:
        query_result_table = TableQueryResult(syn, query_string)
        args, kwargs = mocked_table_query.call_args
        assert_equals(query_string, kwargs['query'])
        assert_equals(2, len(query_result_table))


class TestPartialRow():
    """
    Testing PartialRow class
    """

    @raises(ValueError)
    def test_constructor__value_not_dict(self):
        PartialRow([], 123)


    @raises(ValueError)
    def test_constructor__row_id_string_not_castable_to_int(self):
        PartialRow({}, "fourty-two")


    def test_constructor__row_id_is_int_castable_string(self):
        partial_row = PartialRow({}, "350")

        assert_equals([], partial_row.values)
        assert_equals(350, partial_row.rowId)
        assert_not_in('etag', partial_row)


    def test_constructor__values_translation(self):
        values = OrderedDict([("12345", "rowValue"),
                              ("09876", "otherValue")])
        partial_row = PartialRow(values, 711)

        expected_values = [{"key":"12345", "value":"rowValue"}, {"key":"09876", "value":"otherValue"}]

        assert_equals(expected_values, partial_row.values)
        assert_equals(711, partial_row.rowId)
        assert_not_in('etag', partial_row)

    def test_constructor__with_etag(self):
        partial_row = PartialRow({}, 420, "my etag")
        assert_equals([], partial_row.values)
        assert_equals(420, partial_row.rowId)
        assert_equals("my etag", partial_row.etag)


    def test_constructor__name_to_col_id(self):
        values = OrderedDict([("row1", "rowValue"),
                              ("row2", "otherValue")])
        names_to_col_id = {"row1": "12345", "row2": "09876"}
        partial_row = PartialRow(values, 711, nameToColumnId=names_to_col_id)

        expected_values = [{"key":"12345", "value":"rowValue"}, {"key":"09876", "value":"otherValue"}]

        assert_equals(expected_values, partial_row.values)
        assert_equals(711, partial_row.rowId)


class TestPartialRowSet():
    @raises(ValueError)
    def test_constructor__not_all_rows_of_type_PartialRow(self):
        rows = [PartialRow({}, 123), "some string instead"]
        PartialRowset("syn123",rows)


    def test_constructor__single_PartialRow(self):
        partial_row = PartialRow({}, 123)
        partial_rowset = PartialRowset("syn123", partial_row)
        assert_equals([partial_row], partial_rowset.rows)

