import csv
import json
import math
import os
import random
import tempfile
import time
import uuid
from itertools import izip
from nose.tools import assert_raises
from datetime import datetime

import synapseclient.client as client
import synapseclient.utils as utils
from synapseclient.exceptions import *
from synapseclient import Project, File, Folder, Schema
from synapseclient.table import Column, RowSet, Row, cast_row, as_table_columns, create_table
import synapseclient.exceptions as exceptions

import integration
from integration import schedule_for_cleanup


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = integration.syn
    module.project = integration.project


def test_tables():

    # print "Project ID:", project.id
    # del integration._to_cleanup[:]

    cols = []
    cols.append(Column(name='name', columnType='STRING', maximumSize=1000))
    cols.append(Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']))
    cols.append(Column(name='x', columnType='DOUBLE'))
    cols.append(Column(name='age', columnType='INTEGER'))
    cols.append(Column(name='cartoon', columnType='BOOLEAN'))

    schema1 = syn.store(Schema(name='Foo Table', columns=cols, parent=project))

    print "Table Schema:", schema1.id

    ## Get columns associated with the given table
    retrieved_cols = list(syn.getTableColumns(schema1))

    ## Test that the columns we get are the same as the ones we stored
    assert len(retrieved_cols) == len(cols)
    for retrieved_col, col in izip(retrieved_cols, cols):
        assert retrieved_col.name == col.name
        assert retrieved_col.columnType == col.columnType

    data1 =[('Chris',  'bar', 11.23, 45, False),
            ('Jen',    'bat', 14.56, 40, False),
            ('Jane',   'bat', 17.89,  6, False),
            ('Henry',  'bar', 10.12,  1, False)]
    row_reference_set1 = syn.store(
        RowSet(columns=cols, schema=schema1, rows=[Row(r) for r in data1]))

    assert len(row_reference_set1['rows']) == 4

    ## add more new rows
    ## TODO: use 'NaN', '+Infinity', '-Infinity' when supported by server
    data2 =[('Fred',   'bat', 21.45, 20, True),
            ('Daphne', 'foo', 27.89, 20, True),
            ('Shaggy', 'foo', 23.45, 20, True),
            ('Velma',  'bar', 25.67, 20, True)]
    syn.store(
        RowSet(columns=cols, schema=schema1, rows=[Row(r) for r in data2]))

    results = syn.queryTable("select * from %s order by name" % schema1.id)

    assert results.count==8
    assert results.tableId==schema1.id

    ## Synapse converts all values to strings
    def fields_to_strings(fields):
        return [unicode(a).lower() if isinstance(a, bool) else unicode(a) for a in fields]

    ## test that the values made the round trip
    expected = tuple(fields_to_strings(a) for a in sorted(data1 + data2))
    for expected_values, row in izip(expected, results):
        assert expected_values == row['values'], 'got %s but expected %s' % (row['values'], expected_values)

    ## To modify rows, we have to select then first.
    result2 = syn.queryTable('select * from %s where age>18 and age<30'%schema1.id)

    ## make a change
    rs = result2.asRowSet()
    for row in rs['rows']:
        row['values'][2] = 88.888

    ## store it
    row_reference_set = syn.store(rs)

    ## check if the change sticks
    result3 = syn.queryTable('select name, x, age from %s'%schema1.id)
    rs = result3.asRowSet()

    for row in rs['rows']:
        if int(row['values'][2]) == 20:
            ## don't forget that numeric values come back as strings
            assert row['values'][1] == '88.888'

    ## add a column
    bday_column = syn.store(Column(name='birthday', columnType='DATE'))

    column = syn.getColumn(bday_column.id)
    assert column.name=="birthday"
    assert column.columnType=="DATE"

    schema1.addColumn(bday_column)
    schema1 = syn.store(schema1)

    results = syn.queryTable('select * from %s where cartoon=false order by age'%schema1.id)
    assert results.count==4
    rs = results.asRowSet()

    bdays = ('2013-3-15', '2008-1-3', '1973-12-8', '1969-4-28')
    for bday, row in izip(bdays, rs.rows):
        row['values'][5] = bday
    row_reference_set = syn.store(rs)

    ## query by date and check that we get back two kids
    date_2008_jan_1 = utils.to_unix_epoch_time(datetime(2008,1,1))
    results = syn.queryTable('select name from %s where birthday > %d' % (schema1.id, date_2008_jan_1))
    assert set(["Jane", "Henry"]) == set([row['values'][0] for row in results])

    results = syn.queryTable('select birthday from %s where cartoon=false order by age' % schema1.id)
    for bday, row in izip(bdays, results):
        expected = str(utils.to_unix_epoch_time(datetime.strptime(bday, '%Y-%m-%d')))
        assert row['values'][0] == expected, "got %s but expected %s" % (row['values'][0], expected)

    ## test delete rows by deleting cartoon characters
    syn.delete(syn.queryTable('select name from %s where cartoon = true'%schema1.id))

    result = syn.queryTable('select name from %s' % schema1.id)
    assert set(["Jane", "Henry", "Chris", "Jen"]) == set([row['values'][0] for row in result])


def test_automagically_store_columns():
    print "Project ID:", project.id
    del integration._to_cleanup[:]

    cols = []
    cols.append(Column(name='name', columnType='STRING', maximumSize=1000))
    cols.append(Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']))
    cols.append(Column(name='x', columnType='DOUBLE'))
    cols.append(Column(name='age', columnType='INTEGER'))
    cols.append(Column(name='cartoon', columnType='BOOLEAN'))

    schema1 = syn.store(Schema(name='Foobar Table', columns=cols, parent=project))

    print "Table Schema:", schema1.id

    ## Get columns associated with the given table
    retrieved_cols = list(syn.getTableColumns(schema1))
    print retrieved_cols

    ## Test that the columns we get are the same as the ones we stored
    assert len(retrieved_cols) == len(cols)
    for retrieved_col, col in izip(retrieved_cols, cols):
        assert retrieved_col.name == col.name
        assert retrieved_col.columnType == col.columnType


def test_tables_csv():

    ## Define schema
    cols = []
    cols.append(Column(name='Name', columnType='STRING'))
    cols.append(Column(name='Born', columnType='INTEGER'))
    cols.append(Column(name='Hipness', columnType='DOUBLE'))
    cols.append(Column(name='Living', columnType='BOOLEAN'))

    schema1 = syn.store(Schema(name='Jazz Guys', columns=cols, parent=project))

    data = [["John Coltrane",  1926, 8.65, False],
            ["Miles Davis",    1926, 9.87, False],
            ["Bill Evans",     1929, 7.65, False],
            ["Paul Chambers",  1935, 5.14, False],
            ["Jimmy Cobb",     1929, 5.78, True],
            ["Scott LaFaro",   1936, 4.21, False],
            ["Sonny Rollins",  1930, 8.99, True],
            ["Kenny Burrel",   1931, 4.37, True]]

    ## create CSV file
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        schedule_for_cleanup(temp.name)
        writer = csv.writer(temp, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow([col.name for col in cols])
        for row in data:
            writer.writerow(row)

    ## upload CSV
    UploadToTableResult = syn._uploadCsv(filepath=temp.name, schema=schema1)

    from synapseclient.table import CsvFileTable
    result = CsvFileTable.from_table_query(syn, 'select * from %s' % schema1.id, includeRowIdAndRowVersion=False)
    for expected_row, row in izip(data, result):
        assert expected_row == row, "expected %s but got %s" % (expected_row, row)

    expected = {
         True: [True, 1929, 3, 6.4],
        False: [False, 1926, 5, 7.1]}
    result = CsvFileTable.from_table_query(syn, 'select Living, min(Born), count(Living), avg(Hipness) from %s group by Living' % schema1.id, includeRowIdAndRowVersion=False)
    for row in result:
        assert expected[row[0]][1] == row[1]
        assert expected[row[0]][2] == row[2]
        assert expected[row[0]][3] - row[3] < 0.05


def test_tables_pandas():
    try:
        ## check if we have pandas
        import pandas as pd

        ## create a pandas DataFrame
        df = pd.DataFrame({
            'A' : ("foo", "bar", "baz", "qux", "asdf"),
            'B' : tuple(math.pi*i for i in range(5)),
            'C' : (101, 202, 303, 404, 505),
            'D' : (False, True, False, True, False)})

        cols = as_table_columns(df)
        cols[0].maximumSize = 20
        print "cols=",cols
        schema = Schema(name="Nifty Table", columns=cols, parent=project)

        ## store in Synapse
        table = syn.store(create_table(schema, df))

        ## retrieve the table and verify
        results = syn.queryTable('select * from %s'%table.schema.id)
        df2 = results.asDataFrame()

        ## simulate rowId-version rownames for comparison
        df.index = ['%s-0'%i for i in range(5)]
        assert all(df2 == df)

    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping test_tables_pandas.\n\n')


def test_query_rowset_to_dataframe():
    print "Project ID:", project.id
    del integration._to_cleanup[:]

    cols = []
    cols.append(Column(name='name', columnType='STRING', maximumSize=1000))
    cols.append(Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']))
    cols.append(Column(name='x', columnType='DOUBLE'))
    cols.append(Column(name='n', columnType='INTEGER'))
    cols.append(Column(name='is_bogus', columnType='BOOLEAN'))

    table1 = syn.store(Schema(name='Big Table', columns=cols, parent=project))

    print "Created table:", table1.id
    print "with columns:", table1.columnIds

    n = 3
    m = 100
    for i in range(n):
        rows = []
        for j in range(m):
            foo = cols[1].enumValues[random.randint(0,2)]
            rows.append(Row(('Robot ' + str(i*100 + j), foo, random.random()*200.0, random.randint(0,100), random.random()>=0.5)))
        print "added 100 rows"
        rowset1 = syn.store(RowSet(columns=cols, schema=table1, rows=rows))

    for i in range(10):
        try:
            results = syn.queryTable("select * from %s" % table1.id)
            break
        except SynapseTimeoutError as ex1:
            print "Timed out. Trying again."

    print "number of rows:", results.count
    print "etag:", results.etag
    print "tableId:", results.tableId

    df = results.asDataFrame()

    assert df.shape == (n*m, len(cols))
    assert tuple(df.columns) == ('name', 'foo', 'x', 'n', 'is_bogus')


def dontruntest_big_tables():
    cols = []
    cols.append(Column(name='name', columnType='STRING', maximumSize=1000))
    cols.append(Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']))
    cols.append(Column(name='x', columnType='DOUBLE'))
    cols.append(Column(name='n', columnType='INTEGER'))
    cols.append(Column(name='is_bogus', columnType='BOOLEAN'))

    table1 = syn.store(Schema(name='Big Table', columns=cols, parent=project))

    print "Created table:", table1.id
    print "with columns:", table1.columnIds

    for i in range(10):
        rows = []
        for j in range(100):
            foo = cols[1].enumValues[random.randint(0,2)]
            rows.append(Row(('Robot ' + str(i*100 + j), foo, random.random()*200.0, random.randint(0,100), random.random()>=0.5)))
        print "added 100 rows"
        rowset1 = syn.store(RowSet(columns=cols, schema=table1, rows=rows))

    results = syn.queryTable("select * from %s" % table1.id)
    print "number of rows:", results.count
    print "etag:", results.etag
    print "tableId:", results.tableId

    for row in results:
        print row

    ## should count only queries return just the value?
    # result = syn.restPOST('/table/query?isConsistent=true&countOnly=true', body=json.dumps({'sql':'select * from %s limit 100'%table1.id}), retryPolicy=retryPolicy)
    # result_count = result['rows'][0]['values'][0]

    # rowset3 = syn.restPOST('/table/query?isConsistent=true', body=json.dumps({'sql':'select * from %s where n>50 limit 100'%table1.id}), retryPolicy=retryPolicy)


def dontruntest_big_csvs():
    cols = []
    cols.append(Column(name='name', columnType='STRING', maximumSize=1000))
    cols.append(Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']))
    cols.append(Column(name='x', columnType='DOUBLE'))
    cols.append(Column(name='n', columnType='INTEGER'))
    cols.append(Column(name='is_bogus', columnType='BOOLEAN'))

    schema1 = syn.store(Schema(name='Big Table', columns=cols, parent=project))

    print "Created table:", schema1.id
    print "with columns:", schema1.columnIds

    ## write rows to CSV file
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        schedule_for_cleanup(temp.name)
        writer = csv.writer(temp, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow([col.name for col in cols])

        for i in range(10):
            for j in range(100):
                foo = cols[1].enumValues[random.randint(0,2)]
                writer.writerow(('Robot ' + str(i*100 + j), foo, random.random()*200.0, random.randint(0,100), random.random()>=0.5))
            print "wrote 100 rows to disk"

    ## upload CSV
    UploadToTableResult = syn._uploadCsv(filepath=temp.name, schema=schema1)

    from synapseclient.table import CsvFileTable
    results = CsvFileTable.from_table_query(syn, "select * from %s" % schema1.id)
    print "etag:", results.etag
    print "tableId:", results.tableId

    for row in results:
        print row

