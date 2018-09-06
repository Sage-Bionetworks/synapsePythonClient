# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from backports import csv
import io
import filecmp
import math
import os
import random
import sys
import tempfile
import time
import uuid
import six
from builtins import zip
from nose.tools import assert_equals, assert_less, assert_not_equal, assert_false, assert_true, assert_is_not_none
from pandas.util.testing import assert_frame_equal
from datetime import datetime
from mock import patch
from collections import namedtuple

import synapseclient
from synapseclient.exceptions import *
from synapseclient import File, Folder, Schema, EntityViewSchema
from synapseclient.utils import id_of
from synapseclient.table import Column, RowSet, Row, as_table_columns, Table, PartialRowset

import integration
from integration import schedule_for_cleanup, QUERY_TIMEOUT_SEC

import pandas as pd
import numpy as np


def setup(module):
    module.syn = integration.syn
    module.project = integration.project

    module.syn.table_query_timeout = 423


def test_create_and_update_file_view():

    # Create a folder
    folder = Folder(str(uuid.uuid4()), parent=project, description='creating a file-view')
    folder = syn.store(folder)

    # Create dummy file with annotations in our folder
    path = utils.make_bogus_data_file()
    file_annotations = dict(fileFormat='jpg', dataType='image', artist='Banksy',
                            medium='print', title='Girl With Ballon')
    schedule_for_cleanup(path)
    a_file = File(path, parent=folder, annotations=file_annotations)
    a_file = syn.store(a_file)
    schedule_for_cleanup(a_file)

    # Add new columns for the annotations on this file and get their IDs
    my_added_cols = [syn.store(synapseclient.Column(name=k, columnType="STRING")) for k in file_annotations.keys()]
    my_added_cols_ids = [c['id'] for c in my_added_cols]
    view_default_ids = [c['id'] for c in syn._get_default_entity_view_columns('file')]
    col_ids = my_added_cols_ids + view_default_ids
    scopeIds = [folder['id'].lstrip('syn')]

    # Create an empty entity-view with defined scope as folder

    entity_view = EntityViewSchema(name=str(uuid.uuid4()), scopeIds=scopeIds, addDefaultViewColumns=True,
                                   addAnnotationColumns=False, type='file', columns=my_added_cols, parent=project)

    entity_view = syn.store(entity_view)
    schedule_for_cleanup(entity_view)

    assert_equals(set(scopeIds), set(entity_view.scopeIds))
    assert_equals(set(col_ids), set(entity_view.columnIds))
    assert_equals('file', entity_view.type)

    # get the current view-schema
    view = syn.tableQuery("select * from %s" % entity_view.id)
    schedule_for_cleanup(view.filepath)

    view_dict = list(csv.DictReader(io.open(view.filepath, encoding="utf-8", newline='')))

    # check that all of the annotations were retrieved from the view
    assert_true(set(file_annotations.keys()).issubset(set(view_dict[0].keys())))

    updated_a_file = syn.get(a_file.id, downloadFile=False)

    # Check that the values are the same as what was set
    # Both in the view and on the entity itself
    for k, v in file_annotations.items():
        assert_equals(view_dict[0][k], v)
        assert_equals(updated_a_file.annotations[k][0], v)

    # Make a change to the view and store
    view_dict[0]['fileFormat'] = 'PNG'

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp:
        schedule_for_cleanup(temp.name)
        temp_filename = temp.name

    with io.open(temp_filename, mode='w', encoding="utf-8", newline='') as temp_file:
        dw = csv.DictWriter(temp_file, fieldnames=view_dict[0].keys(),
                            quoting=csv.QUOTE_NONNUMERIC,
                            lineterminator=str(os.linesep))
        dw.writeheader()
        dw.writerows(view_dict)
        temp_file.flush()
    syn.store(synapseclient.Table(entity_view.id, temp_filename))
    new_view_dict = list(csv.DictReader(io.open(temp_filename, encoding="utf-8", newline='')))
    assert_equals(new_view_dict[0]['fileFormat'], 'PNG')

    # query for the change
    start_time = time.time()

    new_view_results = syn.tableQuery("select * from %s" % entity_view.id)
    schedule_for_cleanup(new_view_results.filepath)
    new_view_dict = list(csv.DictReader(io.open(new_view_results.filepath, encoding="utf-8", newline='')))
    # query until change is seen.
    while new_view_dict[0]['fileFormat'] != 'PNG':
        # check timeout
        assert_less(time.time() - start_time, QUERY_TIMEOUT_SEC)
        # query again
        new_view_results = syn.tableQuery("select * from %s" % entity_view.id)
        new_view_dict = list(csv.DictReader(io.open(new_view_results.filepath, encoding="utf-8", newline='')))
    # paranoid check
    assert_equals(new_view_dict[0]['fileFormat'], 'PNG')


def test_entity_view_add_annotation_columns():
    folder1 = syn.store(Folder(name=str(uuid.uuid4()) + 'test_entity_view_add_annotation_columns_proj1', parent=project,
                               annotations={'strAnno': 'str1', 'intAnno': 1, 'floatAnno': 1.1}))
    folder2 = syn.store(Folder(name=str(uuid.uuid4()) + 'test_entity_view_add_annotation_columns_proj2', parent=project,
                               annotations={'dateAnno': datetime.now(), 'strAnno': 'str2', 'intAnno': 2}))
    schedule_for_cleanup(folder1)
    schedule_for_cleanup(folder2)
    scopeIds = [utils.id_of(folder1), utils.id_of(folder2)]

    entity_view = EntityViewSchema(name=str(uuid.uuid4()), scopeIds=scopeIds, addDefaultViewColumns=False,
                                   addAnnotationColumns=True, type='project', parent=project)
    syn.store(entity_view)


def test_rowset_tables():
    cols = [Column(name='name', columnType='STRING', maximumSize=1000),
            Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']),
            Column(name='x', columnType='DOUBLE'),
            Column(name='age', columnType='INTEGER'),
            Column(name='cartoon', columnType='BOOLEAN'),
            Column(name='description', columnType='LARGETEXT')]

    schema1 = syn.store(Schema(name='Foo Table', columns=cols, parent=project))

    data1 = [['Chris',  'bar', 11.23, 45, False, 'a'],
             ['Jen',    'bat', 14.56, 40, False, 'b'],
             ['Jane',   'bat', 17.89,  6, False, 'c'*1002],
             ['Henry',  'bar', 10.12,  1, False, 'd']]
    row_reference_set1 = syn.store(
        RowSet(schema=schema1, rows=[Row(r) for r in data1]))
    assert_equals(len(row_reference_set1['rows']), 4)


def test_tables_csv():

    # Define schema
    cols = [Column(name='Name', columnType='STRING'),
            Column(name='Born', columnType='INTEGER'),
            Column(name='Hipness', columnType='DOUBLE'),
            Column(name='Living', columnType='BOOLEAN')]

    schema = Schema(name='Jazz Guys', columns=cols, parent=project)

    data = [["John Coltrane",  1926, 8.65, False],
            ["Miles Davis",    1926, 9.87, False],
            ["Bill Evans",     1929, 7.65, False],
            ["Paul Chambers",  1935, 5.14, False],
            ["Jimmy Cobb",     1929, 5.78, True],
            ["Scott LaFaro",   1936, 4.21, False],
            ["Sonny Rollins",  1930, 8.99, True],
            ["Kenny Burrel",   1931, 4.37, True]]

    # the following creates a CSV file and uploads it to create a new table
    table = syn.store(Table(schema, data))

    # Query and download an identical CSV
    results = syn.tableQuery("select * from %s" % table.schema.id, resultsAs="csv", includeRowIdAndRowVersion=False)

    # Test that CSV file came back as expected
    for expected_row, row in zip(data, results):
        assert_equals(expected_row, row, "expected %s but got %s" % (expected_row, row))


def test_tables_pandas():
    # create a pandas DataFrame
    df = pd.DataFrame({
        'A': ("foo", "bar", "baz", "qux", "asdf"),
        'B': tuple(0.42*i for i in range(5)),
        'C': (101, 202, 303, 404, 505),
        'D': (False, True, False, True, False),
        # additional data types supported since SYNPY-347
        'int64': tuple(np.int64(range(5))),
        'datetime64': tuple(np.datetime64(d) for d in ['2005-02-01', '2005-02-02', '2005-02-03', '2005-02-04',
                                                       '2005-02-05']),
        'string_': tuple(np.string_(s) for s in ['urgot', 'has', 'dark', 'mysterious', 'past'])})

    cols = as_table_columns(df)
    cols[0].maximumSize = 20
    schema = Schema(name="Nifty Table", columns=cols, parent=project)

    # store in Synapse
    table = syn.store(Table(schema, df))

    # retrieve the table and verify
    results = syn.tableQuery('select * from %s' % table.schema.id, resultsAs='csv')
    df2 = results.asDataFrame(convert_to_datetime=True)

    # simulate rowId-version rownames for comparison
    df.index = ['%s_0' % i for i in range(5)]

    # for python3 we need to convert from numpy.bytes_ to str or the equivalence comparision fails
    if six.PY3:
        df['string_'] = df['string_'].transform(str)

    # SYNPY-717
    df['datetime64'] = df['datetime64'].apply(lambda x: pd.Timestamp(x).tz_localize('UTC'))

    # df2 == df gives Dataframe of boolean values; first .all() gives a Series object of ANDed booleans of each column;
    # second .all() gives a bool that is ANDed value of that Series

    assert_frame_equal(df2, df)


def test_download_table_files():
    cols = [
        Column(name='artist', columnType='STRING', maximumSize=50),
        Column(name='album', columnType='STRING', maximumSize=50),
        Column(name='year', columnType='INTEGER'),
        Column(name='catalog', columnType='STRING', maximumSize=50),
        Column(name='cover', columnType='FILEHANDLEID')]

    schema = syn.store(Schema(name='Jazz Albums', columns=cols, parent=project))
    schedule_for_cleanup(schema)

    data = [["John Coltrane",  "Blue Train",   1957, "BLP 1577", "coltraneBlueTrain.jpg"],
            ["Sonny Rollins",  "Vol. 2",       1957, "BLP 1558", "rollinsBN1558.jpg"],
            ["Sonny Rollins",  "Newk's Time",  1958, "BLP 4001", "rollinsBN4001.jpg"],
            ["Kenny Burrel",   "Kenny Burrel", 1956, "BLP 1543", "burrellWarholBN1543.jpg"]]

    # upload files and store file handle ids
    original_files = []
    for row in data:
        path = utils.make_bogus_data_file()
        original_files.append(path)
        schedule_for_cleanup(path)
        file_handle = syn.uploadFileHandle(path, project)
        row[4] = file_handle['id']

    syn.store(RowSet(schema=schema, rows=[Row(r) for r in data]))

    # retrieve the files for each row and verify that they are identical to the originals
    results = syn.tableQuery("select artist, album, 'year', 'catalog', cover from %s" % schema.id, resultsAs="rowset")
    for i, row in enumerate(results):
        path = syn.downloadTableFile(results, rowId=row.rowId, versionNumber=row.versionNumber, column='cover')
        assert_true(filecmp.cmp(original_files[i], path))
        schedule_for_cleanup(path)

    # test that cached copies are returned for already downloaded files
    original_downloadFile_method = syn._downloadFileHandle
    with patch("synapseclient.Synapse._downloadFileHandle") as _downloadFile_mock:
        _downloadFile_mock.side_effect = original_downloadFile_method

        results = syn.tableQuery("select artist, album, 'year', 'catalog', cover from %s where artist = 'John Coltrane'"
                                 % schema.id, resultsAs="rowset")
        for i, row in enumerate(results):
            file_path = syn.downloadTableFile(results, rowId=row.rowId, versionNumber=row.versionNumber, column='cover')
            assert_true(filecmp.cmp(original_files[i], file_path))

        assert_false(_downloadFile_mock.called, "Should have used cached copy of file and not called _downloadFile")

    # test download table column
    results = syn.tableQuery('select * from %s' % schema.id)
    # uncache 2 out of 4 files
    for i, row in enumerate(results):
        if i % 2 == 0:
            syn.cache.remove(row[6])
    file_map = syn.downloadTableColumns(results, ['cover'])
    assert_equals(len(file_map), 4)
    for row in results:
        filecmp.cmp(original_files[i], file_map[row[6]])


def dontruntest_big_tables():
    cols = [Column(name='name', columnType='STRING', maximumSize=1000),
            Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']),
            Column(name='x', columnType='DOUBLE'),
            Column(name='n', columnType='INTEGER'),
            Column(name='is_bogus', columnType='BOOLEAN')]

    table1 = syn.store(Schema(name='Big Table', columns=cols, parent=project))

    rows_per_append = 10

    for i in range(1000):
        rows = []
        for j in range(rows_per_append):
            foo = cols[1].enumValues[random.randint(0, 2)]
            rows.append(Row(('Robot ' + str(i*rows_per_append + j), foo, random.random()*200.0, random.randint(0, 100),
                             random.random() >= 0.5)))
        syn.store(RowSet(columns=cols, schema=table1, rows=rows))

    syn.tableQuery("select * from %s" % table1.id)

    results = syn.tableQuery("select n, COUNT(n), MIN(x), AVG(x), MAX(x), SUM(x) from %s group by n" % table1.id)
    results.asDataFrame()


def dontruntest_big_csvs():
    cols = [Column(name='name', columnType='STRING', maximumSize=1000),
            Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']),
            Column(name='x', columnType='DOUBLE'),
            Column(name='n', columnType='INTEGER'),
            Column(name='is_bogus', columnType='BOOLEAN')]

    schema1 = syn.store(Schema(name='Big Table', columns=cols, parent=project))

    # write rows to CSV file
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        schedule_for_cleanup(temp.name)
        filename = temp.name

    with io.open(filename, mode='w', encoding="utf-8", newline='') as temp:
        writer = csv.writer(temp, quoting=csv.QUOTE_NONNUMERIC, lineterminator=str(os.linesep))
        writer.writerow([col.name for col in cols])

        for i in range(10):
            for j in range(100):
                foo = cols[1].enumValues[random.randint(0, 2)]
                writer.writerow(('Robot ' + str(i*100 + j), foo, random.random()*200.0, random.randint(0, 100),
                                 random.random() >= 0.5))
    # upload CSV
    syn._uploadCsv(filepath=temp.name, schema=schema1)

    from synapseclient.table import CsvFileTable
    CsvFileTable.from_table_query(syn, "select * from %s" % schema1.id)


def test_synapse_integer_columns_with_missing_values_from_dataframe():
    # SYNPY-267
    cols = [Column(name='x', columnType='STRING'),
            Column(name='y', columnType='INTEGER'),
            Column(name='z', columnType='DOUBLE')]
    schema = syn.store(Schema(name='Big Table', columns=cols, parent=project))

    line_terminator = str(os.linesep)
    # write rows to CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as temp:
        schedule_for_cleanup(temp.name)
        # 2nd row is missing a value in its integer column
        temp.write('x,y,z' + line_terminator
                   + 'a,1,0.9' + line_terminator
                   + 'b,,0.8' + line_terminator
                   + 'c,3,0.7' + line_terminator)
        temp.flush()
        filename = temp.name

    # create a table from csv
    table = Table(schema, filename)
    df = table.asDataFrame()

    table_from_dataframe = Table(schema, df)
    assert_not_equal(table.filepath, table_from_dataframe.filepath)
    df2 = table_from_dataframe.asDataFrame()
    assert_frame_equal(df, df2)

def test_store_table_datetime():
    current_datetime = datetime.fromtimestamp(round(time.time(), 3))
    schema = syn.store(Schema("testTable", [Column(name="testerino", columnType='DATE')], project))
    rowset = RowSet(rows=[Row([current_datetime])], schema=schema)
    syn.store(Table(schema, rowset))

    query_result = syn.tableQuery("select * from %s" % id_of(schema), resultsAs="rowset")
    assert_equals(current_datetime, query_result.rowset['rows'][0]['values'][0])


class TestPartialRowSet(object):

    def test_partial_row_view_csv_query_table(self):
        """
        Test PartialRow updates to tables from cvs queries
        """
        cls = type(self)
        self._test_method(cls.table_schema, "csv", cls.table_changes, cls.expected_table_cells)

    def test_partial_row_view_csv_query_entity_view(self):
        """
        Test PartialRow updates to entity views from cvs queries
        """
        cls = type(self)
        self._test_method(cls.view_schema, "csv", cls.view_changes, cls.expected_view_cells)

    def test_parital_row_rowset_query_table(self):
        """
        Test PartialRow updates to tables from rowset queries
        """
        cls = type(self)
        self._test_method(cls.table_schema, "rowset", cls.table_changes, cls.expected_table_cells)

    def test_parital_row_rowset_query_entity_view(self):
        """
        Test PartialRow updates to entity views from rowset queries
        """
        cls = type(self)
        self._test_method(cls.view_schema, "rowset", cls.view_changes, cls.expected_view_cells)

    def _test_method(self, schema, resultsAs, partial_changes, expected_results):
        # anything starting with "test" will be considered a test case by nosetests so I had to append '_' to it

        query_results = self._query_with_retry("SELECT * FROM %s" % utils.id_of(schema),
                                               resultsAs,
                                               2,
                                               QUERY_TIMEOUT_SEC)
        assert_is_not_none(query_results)
        df = query_results.asDataFrame(rowIdAndVersionInIndex=False)

        partial_changes = {df['ROW_ID'][i]: row_changes for i, row_changes in enumerate(partial_changes)}

        partial_rowset = PartialRowset.from_mapping(partial_changes, query_results)
        syn.store(partial_rowset)

        query_results = self._query_with_retry("SELECT * FROM %s" % utils.id_of(schema),
                                               resultsAs,
                                               2,
                                               QUERY_TIMEOUT_SEC)
        assert_is_not_none(query_results)
        df2 = query_results.asDataFrame()
        assert_true(self._rows_match(df2, expected_results))

    def _query_with_retry(self, query, resultsAs, expected_result_len, timeout):
        start_time = time.time()
        while time.time() - start_time < timeout:
            query_results = syn.tableQuery(query, resultsAs=resultsAs)
            if len(query_results) == expected_result_len:
                return query_results
        return None

    def _rows_match(self, df2, expected_results):
        if df2 is None:
            return False

        for expected_row, df_row in zip(expected_results, df2.iterrows()):
            df_idx, actual_row = df_row
            for expected_cell in expected_row:
                if expected_cell.value != actual_row[expected_cell.col_index]:
                    return False
            return True

    @classmethod
    def setup_class(cls):
        cls.table_schema = cls._table_setup()
        cls.view_schema = cls._view_setup()

        cls.table_changes = [{'foo': 4}, {'bar': 5}]
        cls.view_changes = [{'bar': 6}, {'foo': 7}]

        # class used to in asserts for cell values
        ExpectedTableCell = namedtuple('ExpectedTableCell', ['col_index', 'value'])

        cls.expected_table_cells = [[ExpectedTableCell(0, 4)],
                                    [ExpectedTableCell(1, 5)]]
        cls.expected_view_cells = [[ExpectedTableCell(1, 6)],
                                   [ExpectedTableCell(0, 7)]]

    @classmethod
    def _table_setup(cls):
        # set up a table
        cols = [Column(name='foo', columnType='INTEGER'), Column(name='bar', columnType='INTEGER')]
        schema = syn.store(Schema(name='PartialRowTest' + str(uuid.uuid4()), columns=cols, parent=project))
        data = [[1, None], [None, 2]]
        syn.store(RowSet(schema=schema, rows=[Row(r) for r in data]))
        return schema

    @classmethod
    def _view_setup(cls):
        # set up a file view
        folder = syn.store(Folder(name="PartialRowTestFolder" + str(uuid.uuid4()), parent=project))
        syn.store(File("~/path/doesnt/matter", name="f1", parent=folder, synapseStore=False))
        syn.store(File("~/path/doesnt/matter/again", name="f2", parent=folder, synapseStore=False))

        cols = [Column(name='foo', columnType='INTEGER'), Column(name='bar', columnType='INTEGER')]
        return syn.store(
            EntityViewSchema(name='PartialRowTestViews' + str(uuid.uuid4()), columns=cols, addDefaultViewColumns=False,
                             parent=project, scopes=[folder]))

