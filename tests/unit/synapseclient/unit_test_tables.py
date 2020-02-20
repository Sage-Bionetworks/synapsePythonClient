import csv
import shutil
import io
import math
import os
import tempfile

import time
from builtins import zip
from mock import MagicMock
from nose.tools import assert_raises, assert_not_equals, assert_false, assert_not_in, assert_in, assert_sequence_equal,\
    assert_true, assert_is_none, assert_is_instance, raises, assert_equals
import pandas as pd

from tests.unit.test_utils.unit_utils import StringIOContextManager

from synapseclient import *
from synapseclient.core.exceptions import *
from synapseclient.entity import split_entity_namespaces
import synapseclient.table
from synapseclient.table import Column, Schema, CsvFileTable, TableQueryResult, cast_values, \
    as_table_columns, Table, build_table, RowSet, SelectColumn, EntityViewSchema, RowSetTable, Row, PartialRow, \
    PartialRowset, SchemaBase, _get_view_type_mask_for_deprecated_type, EntityViewType, _get_view_type_mask, \
    MAX_NUM_TABLE_COLUMNS
from tests import unit

from synapseclient.core.utils import from_unix_epoch_time
from mock import patch
from collections import OrderedDict

def setup(module):
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
                      'columnType': 'BOOLEAN'},
                     {'id': '358',
                      'name': 'boom',
                      'columnType': 'LINK'}]

    row = ('Finklestein', 'bat', '3.14159', '65535', 'true', 'https://www.synapse.org/')
    assert_equals(cast_values(row, selectColumns),
                  ['Finklestein', 'bat', 3.14159, 65535, True, 'https://www.synapse.org/'])

    # group by
    selectColumns = [{'name': 'bonk',
                      'columnType': 'BOOLEAN'},
                     {'name': 'COUNT(name)',
                      'columnType': 'INTEGER'},
                     {'name': 'AVG(x)',
                      'columnType': 'DOUBLE'},
                     {'name': 'SUM(n)',
                      'columnType': 'INTEGER'}]
    row = ('true', '211', '1.61803398875', '1421365')
    assert_equals(cast_values(row, selectColumns), [True, 211, 1.61803398875, 1421365])

def test_cast_values__unknown_column_type():
    selectColumns = [{'id': '353',
                      'name': 'name',
                      'columnType': 'INTEGER'},
                     {'id': '354',
                      'name': 'foo',
                      'columnType': 'DEFINTELY_NOT_A_EXISTING_TYPE'},
                    ]

    row = ('123', 'othervalue')
    assert_equals(cast_values(row, selectColumns),
                  [123, 'othervalue'])


def test_cast_values__list_type():
    selectColumns = [{'id': '354',
                      'name': 'foo',
                      'columnType': 'STRING_LIST'},
                     {'id': '356',
                      'name': 'n',
                      'columnType': 'INTEGER_LIST'},
                     {'id': '357',
                      'name': 'bonk',
                      'columnType': 'BOOLEAN_LIST'},
                     {'id': '358',
                      'name': 'boom',
                      'columnType': 'DATE_LIST'}]
    now_millis = int(round(time.time() * 1000));
    row = ('["foo", "bar"]', '[1,2,3]', '[true, false]', '['+ str(now_millis) +']')
    assert_equals(cast_values(row, selectColumns),
                  [["foo", "bar"], [1,2,3], [True, False], [from_unix_epoch_time(now_millis)]])



def test_schema():
    schema = Schema(name='My Table', parent="syn1000001")

    assert_false(schema.has_columns())

    schema.addColumn(Column(id='1', name='Name', columnType='STRING'))

    assert_true(schema.has_columns())
    assert_equals(schema.properties.columnIds, ['1'])

    schema.removeColumn('1')
    assert_false(schema.has_columns())
    assert_equals(schema.properties.columnIds, [])

    schema = Schema(name='Another Table', parent="syn1000001")

    schema.addColumns([
        Column(name='Name', columnType='STRING'),
        Column(name='Born', columnType='INTEGER'),
        Column(name='Hipness', columnType='DOUBLE'),
        Column(name='Living', columnType='BOOLEAN')])
    assert_true(schema.has_columns())
    assert_equals(len(schema.columns_to_store), 4)
    assert_in(Column(name='Name', columnType='STRING'), schema.columns_to_store)
    assert_in(Column(name='Born', columnType='INTEGER'), schema.columns_to_store)
    assert_in(Column(name='Hipness', columnType='DOUBLE'), schema.columns_to_store)
    assert_in(Column(name='Living', columnType='BOOLEAN'), schema.columns_to_store)

    schema.removeColumn(Column(name='Living', columnType='BOOLEAN'))
    assert_true(schema.has_columns())
    assert_equals(len(schema.columns_to_store), 3)
    assert_not_in(Column(name='Living', columnType='BOOLEAN'), schema.columns_to_store)
    assert_in(Column(name='Hipness', columnType='DOUBLE'), schema.columns_to_store)


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

    assert_equals(row_set.etag, 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee')
    assert_equals(row_set.tableId, 'syn2976298')
    assert_equals(len(row_set.headers), 4)
    assert_equals(len(row_set.rows), 4)

    schema = Schema(id="syn2976298", name="Bogus Schema", columns=[353, 355, 3020, 891], parent="syn1000001")

    table = Table(schema, row_set)

    assert_equals(table.etag, 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee')
    assert_equals(table.tableId, 'syn2976298')
    assert_equals(len(table.headers), 4)
    assert_equals(len(table.asRowSet().rows), 4)

    df = table.asDataFrame()
    assert_equals(df.shape, (4, 4))
    assert_equals(list(df['name']), ['foo', 'bar', 'foo', 'qux'])


def test_as_table_columns__with_pandas_DataFrame():
    df = pd.DataFrame({
        'foobar': ("foo", "bar", "baz", "qux", "asdf"),
        'x': tuple(math.pi*i for i in range(5)),
        'n': (101, 202, 303, 404, 505),
        'really': (False, True, False, True, False),
        'size': ('small', 'large', 'medium', 'medium', 'large')},
    columns=['foobar', 'x', 'n', 'really', 'size'])

    cols = as_table_columns(df)

    expected_columns = [
        {'defaultValue': '',
         'columnType': 'STRING',
         'name': 'foobar',
         'maximumSize': 30,
         'concreteType': 'org.sagebionetworks.repo.model.table.ColumnModel'},
        {'columnType': 'DOUBLE',
         'name': 'x',
         u'concreteType': 'org.sagebionetworks.repo.model.table.ColumnModel'},
        {'columnType': 'INTEGER',
         'name': 'n',
         'concreteType': 'org.sagebionetworks.repo.model.table.ColumnModel'},
        {'columnType': 'BOOLEAN',
         'name': 'really',
         'concreteType': 'org.sagebionetworks.repo.model.table.ColumnModel'},
        {'defaultValue': '',
         'columnType': 'STRING',
         'name': 'size',
         'maximumSize': 30,
         'concreteType': 'org.sagebionetworks.repo.model.table.ColumnModel'}
    ]
    assert_equals(expected_columns, cols)


def test_as_table_columns__with_non_supported_input_type():
    assert_raises(ValueError, as_table_columns, dict(a=[1, 2, 3], b=["c", "d", "e"]))


def test_as_table_columns__with_csv_file():
    string_io = StringIOContextManager(
        'ROW_ID,ROW_VERSION,Name,Born,Hipness,Living\n'
        '"1", "1", "John Coltrane", 1926, 8.65, False\n'
        '"2", "1", "Miles Davis", 1926, 9.87, False'
    )
    cols = as_table_columns(string_io)

    assert_equals(cols[0]['name'], 'Name')
    assert_equals(cols[0]['columnType'], 'STRING')
    assert_equals(cols[1]['name'], 'Born')
    assert_equals(cols[1]['columnType'], 'INTEGER')
    assert_equals(cols[2]['name'], 'Hipness')
    assert_equals(cols[2]['columnType'], 'DOUBLE')
    assert_equals(cols[3]['name'], 'Living')
    assert_equals(cols[3]['columnType'], 'STRING')


def test_dict_to_table():
    d = dict(a=[1, 2, 3], b=["c", "d", "e"])
    df = pd.DataFrame(d)
    schema = Schema(name="Baz", parent="syn12345", columns=as_table_columns(df))

    with patch.object(CsvFileTable, "from_data_frame") as mocked_from_data_frame:
        Table(schema, d)

    # call_agrs is a tuple with values and name
    agrs_list = mocked_from_data_frame.call_args[0]
    # getting the second argument
    df_agr = agrs_list[1]
    assert_true(df_agr.equals(df))


def test_pandas_to_table():
    df = pd.DataFrame(dict(a=[1, 2, 3], b=["c", "d", "e"]))
    schema = Schema(name="Baz", parent="syn12345", columns=as_table_columns(df))

    # A dataframe with no row id and version
    table = Table(schema, df)

    for i, row in enumerate(table):
        assert_equals(row[0], (i+1))
        assert_equals(row[1], ["c", "d", "e"][i])

        assert_equals(len(table), 3)

    # If includeRowIdAndRowVersion=True, include empty row id an versions
    # ROW_ID,ROW_VERSION,a,b
    # ,,1,c
    # ,,2,d
    # ,,3,e
    table = Table(schema, df, includeRowIdAndRowVersion=True)
    for i, row in enumerate(table):
        assert_is_none(row[0])
        assert_is_none(row[1])
        assert_equals(row[2], (i+1))

    # A dataframe with no row id and version
    df = pd.DataFrame(index=["1_7", "2_7", "3_8"], data=dict(a=[100, 200, 300], b=["c", "d", "e"]))

    table = Table(schema, df)
    for i, row in enumerate(table):
        assert_equals(row[0], ["1", "2", "3"][i])
        assert_equals(row[1], ["7", "7", "8"][i])
        assert_equals(row[2], (i+1)*100)
        assert_equals(row[3], ["c", "d", "e"][i])

    # A dataframe with row id and version in columns
    df = pd.DataFrame(dict(ROW_ID=["0", "1", "2"], ROW_VERSION=["8", "9", "9"], a=[100, 200, 300], b=["c", "d", "e"]))

    table = Table(schema, df)
    for i, row in enumerate(table):
        assert_equals(row[0], ["0", "1", "2"][i])
        assert_equals(row[1], ["8", "9", "9"][i])
        assert_equals(row[2], (i+1)*100)
        assert_equals(row[3], ["c", "d", "e"][i])


def test_csv_table():
    # Maybe not truly a unit test, but here because it doesn't do
    # network IO to synapse
    data = [["1", "1", "John Coltrane",  1926, 8.65, False],
            ["2", "1", "Miles Davis",    1926, 9.87, False],
            ["3", "1", "Bill Evans",     1929, 7.65, False],
            ["4", "1", "Paul Chambers",  1935, 5.14, False],
            ["5", "1", "Jimmy Cobb",     1929, 5.78, True],
            ["6", "1", "Scott LaFaro",   1936, 4.21, False],
            ["7", "1", "Sonny Rollins",  1930, 8.99, True],
            ["8", "1", "Kenny Burrel",   1931, 4.37, True]]

    filename = None

    cols = [Column(id='1', name='Name', columnType='STRING'),
            Column(id='2', name='Born', columnType='INTEGER'),
            Column(id='3', name='Hipness', columnType='DOUBLE'),
            Column(id='4', name='Living', columnType='BOOLEAN')]

    schema1 = Schema(id='syn1234', name='Jazz Guys', columns=cols, parent="syn1000001")

    # TODO: use StringIO.StringIO(data) rather than writing files
    try:
        # create CSV file
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            filename = temp.name

        with io.open(filename, mode='w', encoding="utf-8", newline='') as temp:
            writer = csv.writer(temp, quoting=csv.QUOTE_NONNUMERIC, lineterminator=str(os.linesep))
            headers = ['ROW_ID', 'ROW_VERSION'] + [col.name for col in cols]
            writer.writerow(headers)
            for row in data:
                writer.writerow(row)

        table = Table(schema1, filename)
        assert_is_instance(table, CsvFileTable)

        # need to set column headers to read a CSV file
        table.setColumnHeaders(
            [SelectColumn(name="ROW_ID", columnType="STRING"),
             SelectColumn(name="ROW_VERSION", columnType="STRING")] +
            [SelectColumn.from_column(col) for col in cols])

        # test iterator
        for table_row, expected_row in zip(table, data):
            assert_equals(table_row, expected_row)

        # test asRowSet
        rowset = table.asRowSet()
        for rowset_row, expected_row in zip(rowset.rows, data):
            assert_equals(rowset_row['values'], expected_row[2:])
            assert_equals(rowset_row['rowId'], expected_row[0])
            assert_equals(rowset_row['versionNumber'], expected_row[1])

        df = table.asDataFrame()
        assert_equals(list(df['Name']), [row[2] for row in data])
        assert_equals(list(df['Born']), [row[3] for row in data])
        assert_equals(list(df['Living']), [row[5] for row in data])
        assert_equals(list(df.index), ['%s_%s' % tuple(row[0:2]) for row in data])
        assert_equals(df.shape, (8, 4))

    except Exception:
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

    cols = [Column(id='1', name='Name', columnType='STRING'),
            Column(id='2', name='Born', columnType='INTEGER'),
            Column(id='3', name='Hipness', columnType='DOUBLE'),
            Column(id='4', name='Living', columnType='BOOLEAN')]

    schema1 = Schema(name='Jazz Guys', columns=cols, id="syn1000002", parent="syn1000001")

    # need columns to do cast_values w/o storing
    table = Table(schema1, data, headers=[SelectColumn.from_column(col) for col in cols])

    for table_row, expected_row in zip(table, data):
        assert_equals(table_row, expected_row)

    rowset = table.asRowSet()
    for rowset_row, expected_row in zip(rowset.rows, data):
        assert_equals(rowset_row['values'], expected_row)

    table.columns = cols

    df = table.asDataFrame()
    assert_equals(list(df['Name']), [r[0] for r in data])


def test_aggregate_query_result_to_data_frame():

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

    result = TableQueryResult(synapse=MockSynapse(),
                              query="select State, min(Born), count(State), avg(Hipness) from syn2757980 "
                                    "group by Living")

    assert_equals(result.etag, 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee')
    assert_equals(result.tableId, 'syn2757980')
    assert_equals(len(result.headers), 4)

    rs = result.asRowSet()
    assert_equals(len(rs.rows), 4)

    result = TableQueryResult(synapse=MockSynapse(),
                              query="select State, min(Born), count(State), avg(Hipness) from syn2757980"
                                    " group by Living")
    df = result.asDataFrame()

    assert_equals(df.shape, (4, 4))
    assert_equals(list(df['State'].values), ['PA', 'MO', 'DC', 'NC'])

    # check integer, double and boolean types after PLFM-3073 is fixed
    assert_equals(list(df['MIN(Born)'].values), [1935, 1928, 1929, 1926],
                  "Unexpected values" + str(df['MIN(Born)'].values))
    assert_equals(list(df['COUNT(State)'].values), [2, 3, 1, 1])
    assert_equals(list(df['AVG(Hipness)'].values), [1.1, 2.38, 3.14, 4.38])


def test_waitForAsync():
    syn = Synapse(debug=True, skip_checks=True)
    syn.table_query_timeout = 0.05
    syn.table_query_max_sleep = 0.001
    syn.restPOST = MagicMock(return_value={"token": "1234567"})

    # return a mocked http://docs.synapse.org/rest/org/sagebionetworks/repo/model/asynch/AsynchronousJobStatus.html
    syn.restGET = MagicMock(return_value={
        "jobState": "PROCESSING",
        "progressMessage": "Test progress message",
        "progressCurrent": 10,
        "progressTotal": 100,
        "errorMessage": "Totally fubared error",
        "errorDetails": "Totally fubared error details"})

    assert_raises(SynapseTimeoutError, syn._waitForAsync, uri="foo/bar",
                  request={"foo": "bar"})


def _insert_dataframe_column_if_not_exist__setup():
    df = pd.DataFrame()
    column_name = "panda"
    data = ["pandas", "are", "alive", ":)"]
    return df, column_name, data


def test_insert_dataframe_column_if_not_exist__nonexistent_column():
    df, column_name, data = _insert_dataframe_column_if_not_exist__setup()

    # method under test
    CsvFileTable._insert_dataframe_column_if_not_exist(df, 0, column_name, data)

    # make sure the data was inserted
    assert_equals(data, df[column_name].tolist())


def test_insert_dataframe_column_if_not_exist__existing_column_matching():

    df, column_name, data = _insert_dataframe_column_if_not_exist__setup()

    # add the same data to the DataFrame prior to calling our method
    df.insert(0, column_name, data)

    # method under test
    CsvFileTable._insert_dataframe_column_if_not_exist(df, 0, column_name, data)

    # make sure the data has not changed
    assert_equals(data, df[column_name].tolist())


@raises(SynapseError)
def test_insert_dataframe_column_if_not_exist__existing_column_not_matching():
    df, column_name, data = _insert_dataframe_column_if_not_exist__setup()

    # add different data to the DataFrame prior to calling our method
    df.insert(0, column_name, ['mercy', 'main', 'btw'])

    # make sure the data is different
    assert_not_equals(data, df[column_name].tolist())

    # method under test should raise exception
    CsvFileTable._insert_dataframe_column_if_not_exist(df, 0, column_name, data)


def test_build_table_download_file_handle_list__repeated_file_handles():
    syn = Synapse(debug=True, skip_checks=True)

    # patch the cache so we don't look there in case FileHandle ids actually exist there
    patch.object(syn.cache, "get", return_value=None)

    cols = [
        Column(name='Name', columnType='STRING', maximumSize=50),
        Column(name='filehandle', columnType='FILEHANDLEID')]

    schema = Schema(name='FileHandleTest', columns=cols, parent='syn420')

    # using some large filehandle numbers so i don
    data = [["ayy lmao", 5318008],
            ["large numberino", 0x5f3759df],
            ["repeated file handle", 5318008],
            ["repeated file handle also", 0x5f3759df]]

    # need columns to do cast_values w/o storing
    table = Table(schema, data, headers=[SelectColumn.from_column(col) for col in cols])

    file_handle_associations, file_handle_to_path_map = syn._build_table_download_file_handle_list(table,
                                                                                                   ['filehandle'])

    # verify only 2 file_handles are added (repeats were ignored)
    assert_equals(2, len(file_handle_associations))
    assert_equals(0, len(file_handle_to_path_map))


def test_EntityViewSchema__default_params():
    entity_view = EntityViewSchema(parent="idk")
    assert_equals(EntityViewType.FILE.value, entity_view.viewTypeMask)
    assert_equals([], entity_view.scopeIds)
    assert_equals(True, entity_view.addDefaultViewColumns)


def test_entityViewSchema__specified_deprecated_type():
    view_type = 'project'
    entity_view = EntityViewSchema(parent="idk", type=view_type)
    assert_equals(EntityViewType.PROJECT.value, entity_view.viewTypeMask)
    assert_is_none(entity_view.get('type'))


def test_entityViewSchema__specified_deprecated_type_in_properties():
    view_type = 'project'
    properties = {'type': view_type}
    entity_view = EntityViewSchema(parent="idk", properties=properties)
    assert_equals(EntityViewType.PROJECT.value, entity_view.viewTypeMask)
    assert_is_none(entity_view.get('type'))


def test_entityViewSchema__specified_viewTypeMask():
    entity_view = EntityViewSchema(parent="idk", includeEntityTypes=[EntityViewType.PROJECT])
    assert_equals(EntityViewType.PROJECT.value, entity_view.viewTypeMask)
    assert_is_none(entity_view.get('type'))


def test_entityViewSchema__specified_both_type_and_viewTypeMask():
    entity_view = EntityViewSchema(parent="idk", type='folder', includeEntityTypes=[EntityViewType.PROJECT])
    assert_equals(EntityViewType.PROJECT.value, entity_view.viewTypeMask)
    assert_is_none(entity_view.get('type'))


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
    assert_equals([str(x) for x in ["123", "456", "789"]], entity_view.scopeIds)


def test_Schema__max_column_check():
    table = Schema(name="someName", parent="idk")
    table.addColumns(Column(name="colNum%s" % i, columnType="STRING")
                     for i in range(MAX_NUM_TABLE_COLUMNS + 1))
    assert_raises(ValueError, syn.store, table)

    
def test_EntityViewSchema__ignore_column_names_set_info_preserved():
    """
    tests that ignoredAnnotationColumnNames will be preserved after creating a new EntityViewSchema from properties,
    local_state, and annotations
    """
    ignored_names = {'a', 'b', 'c'}
    entity_view = EntityViewSchema("someName", parent="syn123", ignoredAnnotationColumnNames={'a', 'b', 'c'})
    properties, annotations, local_state = split_entity_namespaces(entity_view)
    entity_view_copy = Entity.create(properties, annotations, local_state)
    assert_equals(ignored_names, entity_view.ignoredAnnotationColumnNames)
    assert_equals(ignored_names, entity_view_copy.ignoredAnnotationColumnNames)


def test_EntityViewSchema__ignore_annotation_column_names():
    syn = Synapse(debug=True, skip_checks=True)

    scopeIds = ['123']
    entity_view = EntityViewSchema("someName", scopes=scopeIds, parent="syn123", ignoredAnnotationColumnNames={'long1'},
                                   addDefaultViewColumns=False, addAnnotationColumns=True)

    mocked_annotation_result1 = [Column(name='long1', columnType='INTEGER'), Column(name='long2',
                                                                                    columnType='INTEGER')]

    with patch.object(syn, '_get_annotation_entity_view_columns', return_value=mocked_annotation_result1)\
            as mocked_get_annotations,\
         patch.object(syn, 'getColumns') as mocked_get_columns,\
         patch.object(SchemaBase, "_before_synapse_store"):

        entity_view._before_synapse_store(syn)

        mocked_get_columns.assert_called_once_with([])
        mocked_get_annotations.assert_called_once_with(scopeIds, EntityViewType.FILE.value)

        assert_equals([Column(name='long2', columnType='INTEGER')], entity_view.columns_to_store)


def test_EntityViewSchema__repeated_columnName_different_type():
    syn = Synapse(debug=True, skip_checks=True)

    scopeIds = ['123']
    entity_view = EntityViewSchema("someName", scopes=scopeIds, parent="syn123")

    columns = [Column(name='annoName', columnType='INTEGER'),
               Column(name='annoName', columnType='DOUBLE')]

    with patch.object(syn, 'getColumns') as mocked_get_columns:

        filtered_results = entity_view._filter_duplicate_columns(syn, columns)

        mocked_get_columns.assert_called_once_with([])
        assert_equals(2, len(filtered_results))
        assert_equals(columns, filtered_results)


def test_EntityViewSchema__repeated_columnName_same_type():
    syn = Synapse(debug=True, skip_checks=True)

    entity_view = EntityViewSchema("someName", parent="syn123")

    columns = [Column(name='annoName', columnType='INTEGER'),
               Column(name='annoName', columnType='INTEGER')]

    with patch.object(syn, 'getColumns') as mocked_get_columns:
        filtered_results = entity_view._filter_duplicate_columns(syn, columns)

        mocked_get_columns.assert_called_once_with([])
        assert_equals(1, len(filtered_results))
        assert_equals(Column(name='annoName', columnType='INTEGER'), filtered_results[0])


def test_rowset_asDataFrame__with_ROW_ETAG_column():
    query_result = {
                   'concreteType': 'org.sagebionetworks.repo.model.table.QueryResultBundle',
                   'maxRowsPerPage': 6990,
                   'selectColumns': [
                       {'id': '61770', 'columnType': 'STRING', 'name': 'annotationColumn1'},
                       {'id': '61771', 'columnType': 'STRING', 'name': 'annotationColumn2'}
                   ],
                   'queryCount': 1,
                   'queryResult': {
                       'concreteType': 'org.sagebionetworks.repo.model.table.QueryResult',
                       'nextPageToken': 'sometoken',
                       'queryResults': {
                           'headers': [
                               {'id': '61770', 'columnType': 'STRING', 'name': 'annotationColumn1'},
                               {'id': '61771', 'columnType': 'STRING', 'name': 'annotationColumn2'}],
                           'concreteType': 'org.sagebionetworks.repo.model.table.RowSet',
                           'etag': 'DEFAULT',
                           'tableId': 'syn11363411',
                           'rows': [{'values': ['initial_value1', 'initial_value2'],
                                     'etag': '7de0f326-9ef7-4fde-9e4a-ac0babca73f6',
                                     'rowId': 123,
                                     'versionNumber':456}]
                       }
                   }
                }
    query_result_next_page = {'concreteType': 'org.sagebionetworks.repo.model.table.QueryResult',
                              'queryResults': {
                                  'etag': 'DEFAULT',
                                  'headers': [
                                      {'id': '61770', 'columnType': 'STRING', 'name': 'annotationColumn1'},
                                      {'id': '61771', 'columnType': 'STRING', 'name': 'annotationColumn2'}],
                                  'rows': [{'values': ['initial_value3', 'initial_value4'],
                                            'etag': '7de0f326-9ef7-4fde-9e4a-ac0babca73f7',
                                            'rowId': 789,
                                            'versionNumber': 101112}],
                                  'tableId': 'syn11363411'}}

    with patch.object(syn, "_queryTable", return_value=query_result),\
         patch.object(syn, "_queryTableNext", return_value=query_result_next_page):
        table = syn.tableQuery("select something from syn123", resultsAs='rowset')
        dataframe = table.asDataFrame()
        assert_not_in("ROW_ETAG", dataframe.columns)
        expected_indicies = ['123_456_7de0f326-9ef7-4fde-9e4a-ac0babca73f6',
                             '789_101112_7de0f326-9ef7-4fde-9e4a-ac0babca73f7']
        assert_sequence_equal(expected_indicies, dataframe.index.values.tolist())


def test_RowSetTable_len():
    schema = Schema(parentId="syn123", id='syn456', columns=[Column(name='column_name', id='123')])
    rowset = RowSet(schema=schema, rows=[Row(['first row']), Row(['second row'])])
    row_set_table = RowSetTable(schema, rowset)
    assert_equals(2, len(row_set_table))


def test_build_table__with_pandas_DataFrame():
    df = pd.DataFrame(dict(a=[1, 2, 3], b=["c", "d", "e"]))
    table = build_table("test", "syn123", df)

    for i, row in enumerate(table):
        assert_equals(row[0], (i+1))
        assert_equals(row[1], ["c", "d", "e"][i])
    assert_equals(len(table), 3)
    headers = [
        {'name': 'a', 'columnType': 'INTEGER'},
        {'name': 'b', 'columnType': 'STRING'}
    ]
    assert_equals(headers, table.headers)


def test_build_table__with_csv():
    string_io = StringIOContextManager('a,b\n'
                                       '1,c\n'
                                       '2,d\n'
                                       '3,e')
    with patch.object(synapseclient.table, "as_table_columns",
                      return_value=[Column(name="a", columnType="INTEGER"),
                                    Column(name="b", columnType="STRING")]),\
         patch.object(io, "open", return_value=string_io):
        table = build_table("test", "syn123", "some_file_name")
        for col, row in enumerate(table):
            assert_equals(row[0], (col + 1))
            assert_equals(row[1], ["c", "d", "e"][col])
        assert_equals(len(table), 3)
        headers = [
            {'name': 'a', 'columnType': 'INTEGER'},
            {'name': 'b', 'columnType': 'STRING'}
        ]
        assert_equals(headers, table.headers)


def test_build_table__with_dict():
    assert_raises(ValueError, build_table, "test", "syn123", dict(a=[1, 2, 3], b=["c", "d", "e"]))


class TestTableQueryResult:
    def setup(self):
        self.rows = [{'rowId': 1, 'versionNumber': 2, 'values': ['first_row']},
                     {'rowId': 5, 'versionNumber': 1, 'values': ['second_row']}]
        self.query_result_dict = {'queryResult': {
            'queryResults': {
                'headers': [
                    {'columnType': 'STRING', 'name': 'col_name'}],
                'rows': self.rows,
                'tableId': 'syn123'}},
            'selectColumns': [{
                'columnType': 'STRING',
                'id': '1337',
                'name': 'col_name'}]}

        self.query_string = "SELECT whatever FROM some_table WHERE sky=blue"

    def test_len(self):
        with patch.object(syn, "_queryTable", return_value=self.query_result_dict) as mocked_table_query:
            query_result_table = TableQueryResult(syn, self.query_string)
            args, kwargs = mocked_table_query.call_args
            assert_equals(self.query_string, kwargs['query'])
            assert_equals(2, len(query_result_table))

    def test_iter_metadata__no_etag(self):
        with patch.object(syn, "_queryTable", return_value=self.query_result_dict):
            query_result_table = TableQueryResult(syn, self.query_string)
            metadata = [x for x in query_result_table.iter_row_metadata()]
            assert_equals(2, len(metadata))
            assert_equals((1, 2, None), metadata[0])
            assert_equals((5, 1, None), metadata[1])

    def test_iter_metadata__has_etag(self):
        self.rows[0].update({'etag': 'etag1'})
        self.rows[1].update({'etag': 'etag2'})
        with patch.object(syn, "_queryTable", return_value=self.query_result_dict):
            query_result_table = TableQueryResult(syn, self.query_string)
            metadata = [x for x in query_result_table.iter_row_metadata()]
            assert_equals(2, len(metadata))
            assert_equals((1, 2, 'etag1'), metadata[0])
            assert_equals((5, 1, 'etag2'), metadata[1])


class TestPartialRow:
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

        expected_values = [{"key": "12345", "value": "rowValue"}, {"key": "09876", "value": "otherValue"}]

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

        expected_values = [{"key": "12345", "value": "rowValue"}, {"key": "09876", "value": "otherValue"}]

        assert_equals(expected_values, partial_row.values)
        assert_equals(711, partial_row.rowId)


class TestPartialRowSet:
    @raises(ValueError)
    def test_constructor__not_all_rows_of_type_PartialRow(self):
        rows = [PartialRow({}, 123), "some string instead"]
        PartialRowset("syn123", rows)

    def test_constructor__single_PartialRow(self):
        partial_row = PartialRow({}, 123)
        partial_rowset = PartialRowset("syn123", partial_row)
        assert_equals([partial_row], partial_rowset.rows)


class TestCsvFileTable:
    def test_iter_metadata__has_etag(self):
        string_io = StringIOContextManager("ROW_ID,ROW_VERSION,ROW_ETAG,asdf\n"
                                           "1,2,etag1,\"I like trains\"\n"
                                           "5,1,etag2,\"weeeeeeeeeeee\"\n")
        with patch.object(io, "open", return_value=string_io):
            csv_file_table = CsvFileTable("syn123", "/fake/file/path")
            metadata = [x for x in csv_file_table.iter_row_metadata()]
            assert_equals(2, len(metadata))
            assert_equals((1, 2, "etag1"), metadata[0])
            assert_equals((5, 1, "etag2"), metadata[1])

    def test_iter_metadata__no_etag(self):
        string_io = StringIOContextManager("ROW_ID,ROW_VERSION,asdf\n"
                                           "1,2,\"I like trains\"\n"
                                           "5,1,\"weeeeeeeeeeee\"\n")
        with patch.object(io, "open", return_value=string_io):
            csv_file_table = CsvFileTable("syn123", "/fake/file/path")
            metadata = [x for x in csv_file_table.iter_row_metadata()]
            assert_equals(2, len(metadata))
            assert_equals((1, 2, None), metadata[0])
            assert_equals((5, 1, None), metadata[1])

    # test __iter__
    def test_iter_with_no_headers(self):
        # self.headers is None
        string_io = StringIOContextManager("ROW_ID,ROW_VERSION,ROW_ETAG,col\n"
                                           "1,2,etag1,\"I like trains\"\n"
                                           "5,1,etag2,\"weeeeeeeeeeee\"\n")
        with patch.object(io, "open", return_value=string_io):
            table = CsvFileTable("syn123", "/fake/file/path")
            iter = table.__iter__()
            assert_raises(ValueError, next, iter)

    def test_iter_with_no_headers_in_csv(self):
        # csv file does not have headers
        string_io = StringIOContextManager("1,2,etag1,\"I like trains\"\n"
                                           "5,1,etag2,\"weeeeeeeeeeee\"\n")
        with patch.object(io, "open", return_value=string_io):
            table = CsvFileTable("syn123", "/fake/file/path", header=False)
            iter = table.__iter__()
            assert_raises(ValueError, next, iter)

    def test_iter_row_metadata_mismatch_in_headers(self):
        # csv file does not contain row metadata, self.headers does
        data = "col1,col2\n" \
               "1,2\n" \
               "2,1\n"
        cols = as_table_columns(StringIOContextManager(data))
        headers = [SelectColumn(name="ROW_ID", columnType="STRING"),
                   SelectColumn(name="ROW_VERSION", columnType="STRING")] + \
                  [SelectColumn.from_column(col) for col in cols]
        with patch.object(io, "open", return_value=StringIOContextManager(data)):
            table = CsvFileTable("syn123", "/fake/file/path", headers=headers)
            iter = table.__iter__()
            assert_raises(ValueError, next, iter)

    def test_iter_with_table_row_metadata(self):
        # csv file has row metadata, self.headers does not
        data = "ROW_ID,ROW_VERSION,col\n" \
               "1,2,\"I like trains\"\n" \
               "5,1,\"weeeeeeeeeeee\"\n"
        cols = as_table_columns(StringIOContextManager(data))
        headers = [SelectColumn.from_column(col) for col in cols]
        with patch.object(io, "open", return_value=StringIOContextManager(data)):
            table = CsvFileTable("syn123", "/fake/file/path", headers=headers)
            expected_rows = [["I like trains"], ["weeeeeeeeeeee"]]
            for expected_row, table_row in zip(expected_rows, table):
                assert_equals(expected_row, table_row)

    def test_iter_with_mismatch_row_metadata(self):
        # self.headers and csv file headers contains mismatch row metadata
        data = "ROW_ID,ROW_VERSION,ROW_ETAG,col\n" \
               "1,2,etag1,\"I like trains\"\n" \
                "5,1,etag2,\"weeeeeeeeeeee\"\n"
        cols = as_table_columns(StringIOContextManager(data))
        headers = [SelectColumn(name="ROW_ID", columnType="STRING"),
                   SelectColumn(name="ROW_VERSION", columnType="STRING")] + \
                  [SelectColumn.from_column(col) for col in cols]
        with patch.object(io, "open", return_value=StringIOContextManager(data)):
            table = CsvFileTable("syn123", "/fake/file/path", headers=headers)
            iter = table.__iter__()
            assert_raises(ValueError, next, iter)

    def test_iter_no_row_metadata(self):
        # both csv headers and self.headers do not contains row metadata
        data = "col1,col2\n" \
               "1,2\n" \
               "2,1\n"
        cols = as_table_columns(StringIOContextManager(data))
        headers = [SelectColumn.from_column(col) for col in cols]
        with patch.object(io, "open", return_value=StringIOContextManager(data)):
            table = CsvFileTable("syn123", "/fake/file/path", headers=headers)
            expected_rows = [[1, 2], [2, 1]]
            for expected_row, table_row in zip(expected_rows, table):
                assert_equals(expected_row, table_row)

    def test_iter_with_file_view_row_metadata(self):
        # csv file and self.headers contain matching row metadata
        data = "ROW_ID,ROW_VERSION,ROW_ETAG,col\n" \
               "1,2,etag1,\"I like trains\"\n" \
               "5,1,etag2,\"weeeeeeeeeeee\"\n"
        cols = as_table_columns(StringIOContextManager(data))
        headers = [SelectColumn(name="ROW_ID", columnType="STRING"),
                   SelectColumn(name="ROW_VERSION", columnType="STRING"),
                   SelectColumn(name="ROW_ETAG", columnType="STRING")] + \
                  [SelectColumn.from_column(col) for col in cols]
        with patch.object(io, "open", return_value=StringIOContextManager(data)):
            table = CsvFileTable("syn123", "/fake/file/path", headers=headers)
            expected_rows = [['1', '2', "etag1", "I like trains"],
                             ['5', '1', "etag2", "weeeeeeeeeeee"]]
            for expected_row, table_row in zip(expected_rows, table):
                assert_equals(expected_row, table_row)

def test_Row_forward_compatibility():
    row = Row("2, 3, 4", rowId=1, versionNumber=1, etag=None, new_field="new")
    assert_equals("2, 3, 4", row.get("values"))
    assert_equals(1, row.get("rowId"))
    assert_equals(1, row.get("versionNumber"))
    assert_is_none(row.get("etag"))
    assert_equals("new", row.get("new_field"))


def test_SelectColumn_forward_compatibility():
    sc = SelectColumn(id=1, columnType="STRING", name="my_col", columnSQL="new")
    assert_equals(1, sc.get("id"))
    assert_equals("STRING", sc.get("columnType"))
    assert_equals("my_col", sc.get("name"))
    assert_equals("new", sc.get("columnSQL"))


def test_get_view_type_mask_for_deprecated_type():
    assert_raises(ValueError, _get_view_type_mask_for_deprecated_type, None)
    assert_raises(ValueError, _get_view_type_mask_for_deprecated_type, 'wiki')
    assert_equals(EntityViewType.FILE.value, _get_view_type_mask_for_deprecated_type('file'))
    assert_equals(EntityViewType.PROJECT.value, _get_view_type_mask_for_deprecated_type('project'))
    assert_equals(EntityViewType.FILE.value | EntityViewType.TABLE.value,
                  _get_view_type_mask_for_deprecated_type('file_and_table'))


def test_get_view_type_mask():
    assert_raises(ValueError, _get_view_type_mask, None)
    assert_raises(ValueError, _get_view_type_mask, [])
    assert_raises(ValueError, _get_view_type_mask, [EntityViewType.DOCKER, 'wiki'])
    # test the map
    assert_equals(EntityViewType.FILE.value, _get_view_type_mask([EntityViewType.FILE]))
    assert_equals(EntityViewType.PROJECT.value, _get_view_type_mask([EntityViewType.PROJECT]))
    assert_equals(EntityViewType.FOLDER.value, _get_view_type_mask([EntityViewType.FOLDER]))
    assert_equals(EntityViewType.TABLE.value, _get_view_type_mask([EntityViewType.TABLE]))
    assert_equals(EntityViewType.VIEW.value, _get_view_type_mask([EntityViewType.VIEW]))
    assert_equals(EntityViewType.DOCKER.value, _get_view_type_mask([EntityViewType.DOCKER]))
    # test combinations
    assert_equals(EntityViewType.PROJECT.value | EntityViewType.FOLDER.value,
                  _get_view_type_mask([EntityViewType.PROJECT, EntityViewType.FOLDER]))
    # test the actual mask value
    assert_equals(0x01, _get_view_type_mask([EntityViewType.FILE]))
    assert_equals(0x02, _get_view_type_mask([EntityViewType.PROJECT]))
    assert_equals(0x04, _get_view_type_mask([EntityViewType.TABLE]))
    assert_equals(0x08, _get_view_type_mask([EntityViewType.FOLDER]))
    assert_equals(0x10, _get_view_type_mask([EntityViewType.VIEW]))
    assert_equals(0x20, _get_view_type_mask([EntityViewType.DOCKER]))
    assert_equals(2**6-1, _get_view_type_mask([EntityViewType.FILE,
                                               EntityViewType.PROJECT,
                                               EntityViewType.FOLDER,
                                               EntityViewType.TABLE,
                                               EntityViewType.VIEW,
                                               EntityViewType.DOCKER
                                               ]))

def test_update_existing_view_type_mask():
    properties = {
        'id': 'syn123',
        'parentId': 'syn456',
        'viewTypeMask': 2
    }
    view = EntityViewSchema(properties=properties)
    assert_equals(view['viewTypeMask'], 2)
    view.set_entity_types([EntityViewType.FILE])
    assert_equals(view['viewTypeMask'], 1)


def test_set_view_types_invalid_input():
    properties = {
        'id': 'syn123',
        'parentId': 'syn456'
    }
    view = EntityViewSchema(type='project', properties=properties)
    assert_equals(view['viewTypeMask'], 2)
    assert_raises(ValueError, view.set_entity_types, None)

