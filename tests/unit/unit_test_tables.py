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
from nose.tools import assert_raises

import synapseclient
from synapseclient.table import Column, Schema, CsvFileTable, TableQueryResult, cast_values, as_table_columns, Table, RowSet, SelectColumn


def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)


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


def test_pandas_to_table():
    try:
        import pandas as pd

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

    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping test_pandas_to_table.\n\n')


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

    # return a mocked http://rest.synapse.org/org/sagebionetworks/repo/model/asynch/AsynchronousJobStatus.html
    syn.restGET  = MagicMock(return_value={
        "jobState": "PROCESSING",
        "progressMessage": "Test progress message",
        "progressCurrent": 10,
        "progressTotal": 100,
        "errorMessage": "Totally fubared error",
        "errorDetails": "Totally fubared error details"})

    assert_raises(synapseclient.exceptions.SynapseTimeoutError, syn._waitForAsync, uri="foo/bar", request={"foo": "bar"})
