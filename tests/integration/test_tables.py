import csv
import json
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
from synapseclient.table import Column, RowSet, Row, cast_row
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

    print "Project ID:", project.id
    del integration._to_cleanup[:]

    cols = []
    cols.append(syn.store(Column(name='name', columnType='STRING', maximumSize=1000)))
    cols.append(syn.store(Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat'])))
    cols.append(syn.store(Column(name='x', columnType='DOUBLE')))
    cols.append(syn.store(Column(name='age', columnType='INTEGER')))
    cols.append(syn.store(Column(name='cartoon', columnType='BOOLEAN')))

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


def test_tables_csv():

    ## Define schema
    cols = []
    cols.append(syn.store(Column(name='Name', columnType='STRING')))
    cols.append(syn.store(Column(name='Born', columnType='INTEGER')))
    cols.append(syn.store(Column(name='Hipness', columnType='DOUBLE')))
    cols.append(syn.store(Column(name='Living', columnType='BOOLEAN')))

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
    UploadToTableResult = syn._uploadCsv(filename=temp.name, tableId=schema1.id)

    ## download CSV
    download_from_table_result, file_info = syn._queryTableCsv('select * from %s' % schema1.id)

    with open(file_info['path']) as f:
        reader = csv.reader(f)
        header = reader.next()
        assert header == [col.name for col in cols], "expected %s but got %s" % ([col.name for col in cols], header)
        for expected_row, row in izip(data, reader):
            row = cast_row(row, columns=cols, headers=download_from_table_result['headers'])
            print row
            assert expected_row == row, "expected %s but got %s" % (expected_row, row)

    download_from_table_result, file_info = syn._queryTableCsv('select Living, avg(Hipness) from %s group by Living' % schema1.id)
    with open(file_info['path']) as f:
        reader = csv.reader(f)
        header = reader.next()
        print header
        for row in reader:
            row = cast_row(row, columns=cols, headers=download_from_table_result['headers'])
            print row


def dontruntest_big_tables():
    cols = []
    cols.append(syn.store(Column(name='name', columnType='STRING', maximumSize=1000)))
    cols.append(syn.store(Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat'])))
    cols.append(syn.store(Column(name='x', columnType='DOUBLE')))
    cols.append(syn.store(Column(name='n', columnType='INTEGER')))
    cols.append(syn.store(Column(name='is_bogus', columnType='BOOLEAN')))

    table1 = syn.store(Schema(name='Big Table', columns=cols, parent=project))

    print "Created table:", table1.id
    print "with columns:", table1.columnIds

    for i in range(100):
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

