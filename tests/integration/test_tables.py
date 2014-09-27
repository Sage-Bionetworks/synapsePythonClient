import json
import os
import random
import time
import uuid
from nose.tools import assert_raises

import synapseclient.client as client
import synapseclient.utils as utils
from synapseclient.exceptions import *
from synapseclient import Project, File, Folder, Schema
from synapseclient.table import Column, RowSet, Row
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

    print project
    #del integration._to_cleanup[:] 

    cols = []
    cols.append(syn.store(Column(name='name', columnType='STRING', maximumSize=1000)))
    cols.append(syn.store(Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat'])))
    cols.append(syn.store(Column(name='x', columnType='DOUBLE')))
    cols.append(syn.store(Column(name='n', columnType='INTEGER')))
    cols.append(syn.store(Column(name='is_bogus', columnType='BOOLEAN')))

    schema1 = syn.store(Schema(name='Foo Table', columns=cols, parent=project))

    print "Table Schema:", schema1.id
    print "Columns:", schema1.columnIds

    ## Get columns associated with the given table
    retrieved_cols = list(syn.getTableColumns(schema1))

    assert len(retrieved_cols) == len(cols)
    for retrieved_col, col in zip(retrieved_cols, cols):
        assert retrieved_col.name == col.name
        assert retrieved_col.columnType == col.columnType

    rowset1 = RowSet(columns=cols, schema=schema1,
        rows=[Row(('Chris', 'foo', 123.45, 44, True)),
              Row(('Jen',   'bat', 456.78, 40, True)),
              Row(('Jane',  'foo', 52.22, 6, False)),
              Row(('Henry', 'bar', 17.3,  1, False))])
    rowset1 = syn.store(rowset1)

    ## add more new rows
    rowset2 = RowSet(columns=cols, schema=schema1,
        rows=[Row(('Fred', 'bat', 11.45, 7, True)),
              Row(('Daphny', 'foo', 4.378, 1001, False)),
              Row(('Shaggy', 'foo', 5.232, 1002, True)),
              Row(('Velma', 'bar', 7.323,  1003, False))])
    rowset2 = syn.store(rowset2)

    results = syn.queryTable("select * from %s" % schema1.id)
    print "number of rows:", results.count

    assert results.count==8
    assert results.tableId==schema1.id

    for i, row in enumerate(results):
        print row
    assert i==7 ## not 8 'cause it's zero based

    ## to modify rows, we have to select *
    result2 = syn.queryTable('select * from %s where n>1000'%schema1.id)

    ## We'd like an object
    rs = result2.asRowSet()

    ## make a change
    for row in rs['rows']:
        row['values'][2] = 88.888

    row_reference_set = syn.store(rs)

    result3 = syn.queryTable('select name, x, n from %s limit 100'%schema1.id)
    rs = result3.asRowSet()

    ## don't forget that numeric values come back as strings
    for row in rs['rows']:
        print row
        if int(row['values'][2]) > 1000:
            assert row['values'][1] == '88.888'

    ## todo: add a column
    ## todo: GET /column/{columnId}

    ## todo: try lots of rows? lots of cols?
    ## todo: test more of the query syntax

    ## todo: there is not yet a way to delete rows


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

