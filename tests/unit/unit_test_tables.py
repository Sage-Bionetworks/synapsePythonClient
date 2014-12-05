import math
import csv
import os
import sys
import tempfile
from itertools import izip

from synapseclient.table import Column, Schema, CsvFileTable, TableQueryResult, cast_row, as_table_columns, Table


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60


def test_cast_row():
    columns = [{'id': '353',
                'name': 'name',
                'columnType': 'STRING',
                'maximumSize': 1000},
               {'id': '354',
                'name': 'foo',
                'columnType': 'STRING',
                'enumValues': ['bar', 'bat', 'foo'],
                'maximumSize': 50},
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
    headers = ['353', '354', '355', '356', '357']
    assert cast_row(row, columns, headers)==['Finklestein', 'bat', 3.14159, 65535, True]

    ## group by
    row = ('true', '211', '1.61803398875', '1421365')
    headers = ['357', 'COUNT(C353)', 'AVG(C355)', 'SUM(C356)']
    assert cast_row(row, columns, headers)==[True, 211, 1.61803398875, 1421365]


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
            writer = csv.writer(temp, quoting=csv.QUOTE_NONNUMERIC, lineterminator=os.linesep)
            writer.writerow(['ROW_ID', 'ROW_VERSION'] + [col.name for col in cols])
            filename = temp.name
            for row in data:
                writer.writerow(row)

        table = Table(schema1, filename)
        assert isinstance(table, CsvFileTable)

        ## need to set columns to read a CSV file
        table.setColumns(cols, headers = ['ROW_ID', 'ROW_VERSION'] + [col.id for col in cols])

        ## test iterator
        # print "\n\nJazz Guys"
        for table_row, expected_row in izip(table, data):
            # print table_row, expected_row
            assert table_row==expected_row

        ## test asRowSet
        rowset = table.asRowSet()
        for rowset_row, expected_row in izip(rowset.rows, data):
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
            assert all(df.index == ['%s-%s'%tuple(row[0:2]) for row in data])
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
                print ex
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

    ## need columns to do cast_rows w/o storing
    table = Table(schema1, data, columns=cols)

    for table_row, expected_row in izip(table, data):
        assert table_row==expected_row

    rowset = table.asRowSet()
    for rowset_row, expected_row in izip(rowset.rows, data):
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

    class Synapse(object):
        def _queryTable(self, query, limit=None, offset=None, isConsistent=True, partMask=None):
            return {'concreteType': 'org.sagebionetworks.repo.model.table.QueryResultBundle',
                    'maxRowsPerPage': 2,
                    'queryCount': 4,
                    'queryResult': {
                     'concreteType': 'org.sagebionetworks.repo.model.table.QueryResult',
                     'nextPageToken': 'aaaaaaaa',
                     'queryResults': {'etag': '8c568c77-3827-44a0-8e46-711aeb0fff9b',
                      'headers': ['1387', 'MIN(Born)', 'COUNT(State)', 'AVG(Hipness)'],
                      'rows': [
                       {'values': ['PA', '1935', '2', '1.1']},
                       {'values': ['MO', '1928', '3', '2.38']}],
                      'tableId': 'syn2757980'}},
                    'selectColumns': [{
                     'columnType': 'STRING',
                     'id': '1387',
                     'name': 'State'}]}
        def _queryTableNext(self, nextPageToken):
            return {'concreteType': 'org.sagebionetworks.repo.model.table.QueryResult',
                    'queryResults': {'etag': '8c568c77-3827-44a0-8e46-711aeb0fff9b',
                     'headers': ['1387', 'MIN(Born)', 'COUNT(State)', 'AVG(Hipness)'],
                     'rows': [
                      {'values': ['DC', '1929', '1', '3.14']},
                      {'values': ['NC', '1926', '1', '4.38']}],
                     'tableId': 'syn2757980'}}

    result = TableQueryResult(synapse=Synapse(), query="select State, min(Born), count(State), avg(Hipness) from syn2757980 group by Living")
    df = result.asDataFrame()

    assert df.shape == (4,4)
    assert all(df['State'].values == ['PA', 'MO', 'DC', 'NC'])

    ## check integer, double and boolean types after PLFM-3073 is fixed
    # assert all(df['MIN(Born)'].values == [1935, 1928, 1929, 1926]), "Unexpected values" + unicode(df['MIN(Born)'].values)
    # assert all(df['COUNT(State)'].values == [2,3,1,1])
    # assert all(df['AVG(Hipness)'].values == [1.1, 2.38, 3.14, 4.38])
