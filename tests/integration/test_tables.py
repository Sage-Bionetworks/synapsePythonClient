import os, json, random, uuid
from nose.tools import assert_raises

import synapseclient.client as client
import synapseclient.utils as utils
from synapseclient.exceptions import *
from synapseclient import Project, File, Folder, Table
from synapseclient.table import ColumnModel, RowSet, Row
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
    cols = []
    cols.append(syn.store(ColumnModel(name='name', columnType='STRING', maximumSize=1000)))
    cols.append(syn.store(ColumnModel(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat'])))
    cols.append(syn.store(ColumnModel(name='x', columnType='DOUBLE')))
    cols.append(syn.store(ColumnModel(name='n', columnType='LONG')))
    cols.append(syn.store(ColumnModel(name='is_bogus', columnType='BOOLEAN')))

    table1 = Table(name='Foo Table', columns=cols, parent=project)

    table1 = syn.store(table1)

    print table1.id
    print table1.columnIds

    ## Get columns associated with the given table
    retrieved_cols = list(syn.getTableColumns(table1))

    assert len(retrieved_cols) == len(cols)
    for retrieved_col, col in zip(retrieved_cols, cols):
        assert retrieved_col.name == col.name
        assert retrieved_col.columnType == col.columnType

    rowset1 = RowSet(columns=cols, table=table1,
        rows=[Row(('Chris', 'foo', 123.45, 44, True)),
              Row(('Jen',   'bat', 456.78, 40, True)),
              Row(('Jane',  'foo', 52.22, 6, False)),
              Row(('Henry', 'bar', 17.3,  1, False))])
    rowset1 = syn.store(rowset1)

    ## add more new rows
    rowset2 = RowSet(columns=cols, table=table1,
        rows=[Row(('Fred', 'bat', 11.45, 7, True)),
              Row(('Daphny', 'foo', 4.378, 1001, False)),
              Row(('Shaggy', 'foo', 5.232, 1002, True)),
              Row(('Velma', 'bar', 7.323,  1003, False))])
    rowset2 = syn.store(rowset2)

    retryPolicy = syn._build_retry_policy({
        "retry_status_codes": [202, 502, 503],
        "retry_exceptions"  : ['Timeout', 'timeout'],
        "retries"           : 10,
        "wait"              : 1,
        "back_off"          : 2,
        "max_wait"          : 10,
        "verbose"           : True})

    ## should count only queries return just the value?
    result = syn.restPOST('/table/query?isConsistent=true&countOnly=true', body=json.dumps({'sql':'select * from %s limit 100'%table1.id}), retryPolicy=retryPolicy)
    result_count = result['rows'][0]['values'][0]

    ## to modify rows, we have to select *
    rowset3 = syn.restPOST('/table/query?isConsistent=true', body=json.dumps({'sql':'select * from %s where n>1000 limit 100'%table1.id}), retryPolicy=retryPolicy)

    ## We'd like an object
    rs = RowSet(**rowset3)

    ## make a change
    for row in rs['rows']:
        row['values'][2] = 88.888

    row_reference_set = syn.store(rs)

    rowset4 = syn.restPOST('/table/query?isConsistent=True', body=json.dumps({'sql':'select name, x, n from %s limit 100'%table1.id}), retryPolicy=retryPolicy)
    rs = RowSet(**rowset4)

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

