import os
from nose.tools import assert_raises

import synapseclient.client as client
import synapseclient.utils as utils
from synapseclient.exceptions import *
from synapseclient import Project, File, Folder, Table
from synapseclient.table import ColumnModel, RowSet, Row


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

    retrieved_cols = list(syn.getTableColumns(table1))

    assert len(retrieved_cols) == len(cols)
    for retrieved_col, col in zip(retrieved_cols, cols):
        assert retrieved_col.name == col.name
        assert retrieved_col.columnType == col.columnType

