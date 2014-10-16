import math
import csv
import os
import sys
import tempfile
from itertools import izip

from synapseclient.table import Column, Schema, CsvFileTable, cast_row, as_table_columns, create_table


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

    schema = Schema(name='Another Table', parent="syn1000001")

    schema.addColumns([
        Column(name='Name', columnType='STRING'),
        Column(name='Born', columnType='INTEGER'),
        Column(name='Hipness', columnType='DOUBLE'),
        Column(name='Living', columnType='BOOLEAN')])

    assert schema.has_columns()


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
    data = [["John Coltrane",  1926, 8.65, False],
            ["Miles Davis",    1926, 9.87, False],
            ["Bill Evans",     1929, 7.65, False],
            ["Paul Chambers",  1935, 5.14, False],
            ["Jimmy Cobb",     1929, 5.78, True],
            ["Scott LaFaro",   1936, 4.21, False],
            ["Sonny Rollins",  1930, 8.99, True],
            ["Kenny Burrel",   1931, 4.37, True]]

    filename = None

    cols = []
    cols.append(Column(id='1', name='Name', columnType='STRING'))
    cols.append(Column(id='2', name='Born', columnType='INTEGER'))
    cols.append(Column(id='3', name='Hipness', columnType='DOUBLE'))
    cols.append(Column(id='4', name='Living', columnType='BOOLEAN'))

    schema1 = Schema(name='Jazz Guys', columns=cols, parent="syn1000001")


    #TODO: use StringIO.StringIO(data) rather than writing files

    try:
        ## create CSV file
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            writer = csv.writer(temp, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow([col.name for col in cols])
            filename = temp.name
            for row in data:
                writer.writerow(row)

        table = create_table(schema1, filename)
        assert isinstance(table, CsvFileTable)

        ## need to set columns to read a CSV file
        table.setColumns(cols)

        print "\n\nJazz Guys"
        for table_row, expected_row in izip(table, data):
            print table_row
            assert table_row==expected_row

        try:
            import pandas as pd

            print "\nasDataFrame"
            df = table.asDataFrame()
            print df

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

    table = create_table(schema1, data)

    for table_row, expected_row in izip(table, data):
        print table_row['values']
        assert table_row['values']==expected_row

