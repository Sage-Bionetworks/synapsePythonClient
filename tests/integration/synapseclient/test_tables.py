import csv
import io
import os
import random
import tempfile
import time
import uuid
from datetime import datetime

from pandas.testing import assert_frame_equal
import pytest

import pandas as pd
import numpy as np

from synapseclient import (
    as_table_columns,
    Column,
    EntityViewSchema,
    EntityViewType,
    File,
    Folder,
    PartialRowset,
    Row,
    RowSet,
    Schema,
    Table,
)
import synapseclient.core.utils as utils

from tests.integration import QUERY_TIMEOUT_SEC


@pytest.fixture(scope='module', autouse=True)
def _init_query_timeout(request, syn):
    existing_timeout = syn.table_query_timeout
    syn.table_query_timeout = 423

    def revert_timeout():
        syn.table_query_timeout = existing_timeout
    request.addfinalizer(revert_timeout)


def test_create_and_update_file_view(syn, project, schedule_for_cleanup):

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
    my_added_cols = [syn.store(Column(name=k, columnType="STRING")) for k in file_annotations.keys()]
    my_added_cols_ids = [c['id'] for c in my_added_cols]
    view_default_ids = [c['id'] for c in syn._get_default_view_columns("entityview", EntityViewType.FILE.value)]
    col_ids = my_added_cols_ids + view_default_ids
    scopeIds = [folder['id'].lstrip('syn')]

    # Create an empty entity-view with defined scope as folder

    entity_view = EntityViewSchema(name=str(uuid.uuid4()), scopeIds=scopeIds, addDefaultViewColumns=True,
                                   addAnnotationColumns=False, type='file', columns=my_added_cols, parent=project)

    entity_view = syn.store(entity_view)
    schedule_for_cleanup(entity_view)

    assert set(scopeIds) == set(entity_view.scopeIds)
    assert set(col_ids) == set(entity_view.columnIds)
    assert EntityViewType.FILE.value == entity_view.viewTypeMask

    # get the current view-schema
    view = syn.tableQuery("select * from %s" % entity_view.id)
    schedule_for_cleanup(view.filepath)

    view_dict = list(csv.DictReader(io.open(view.filepath, encoding="utf-8", newline='')))

    # check that all of the annotations were retrieved from the view
    assert set(file_annotations.keys()).issubset(set(view_dict[0].keys()))

    updated_a_file = syn.get(a_file.id, downloadFile=False)

    # Check that the values are the same as what was set
    # Both in the view and on the entity itself
    for k, v in file_annotations.items():
        assert view_dict[0][k] == v
        assert updated_a_file.annotations[k][0] == v

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
    syn.store(Table(entity_view.id, temp_filename))
    new_view_dict = list(csv.DictReader(io.open(temp_filename, encoding="utf-8", newline='')))
    assert new_view_dict[0]['fileFormat'] == 'PNG'

    # query for the change
    start_time = time.time()

    new_view_results = syn.tableQuery("select * from %s" % entity_view.id)
    schedule_for_cleanup(new_view_results.filepath)
    new_view_dict = list(csv.DictReader(io.open(new_view_results.filepath, encoding="utf-8", newline='')))
    # query until change is seen.
    while new_view_dict[0]['fileFormat'] != 'PNG':
        # check timeout
        assert time.time() - start_time < QUERY_TIMEOUT_SEC
        # query again
        new_view_results = syn.tableQuery("select * from %s" % entity_view.id)
        new_view_dict = list(csv.DictReader(io.open(new_view_results.filepath, encoding="utf-8", newline='')))
    # paranoid check
    assert new_view_dict[0]['fileFormat'] == 'PNG'


def test_entity_view_add_annotation_columns(syn, project, schedule_for_cleanup):
    folder1 = syn.store(Folder(name=str(uuid.uuid4()) + 'test_entity_view_add_annotation_columns_proj1', parent=project,
                               annotations={'strAnno': 'str1', 'intAnno': 1, 'floatAnno': 1.1}))
    folder2 = syn.store(Folder(name=str(uuid.uuid4()) + 'test_entity_view_add_annotation_columns_proj2', parent=project,
                               annotations={'dateAnno': datetime.now(), 'strAnno': 'str2', 'intAnno': 2}))
    schedule_for_cleanup(folder1)
    schedule_for_cleanup(folder2)
    scopeIds = [utils.id_of(folder1), utils.id_of(folder2)]

    # This test is to ensure that user code which use the deprecated field `type` continue to work
    # TODO: remove this test case in Synapse Python client 2.0
    entity_view = EntityViewSchema(name=str(uuid.uuid4()), scopeIds=scopeIds, addDefaultViewColumns=False,
                                   addAnnotationColumns=True, type='project', parent=project)
    syn.store(entity_view)
    # This test is to ensure that user code which use the deprecated field `type` continue to work
    # TODO: remove this test case in Synapse Python client 2.0
    entity_view = EntityViewSchema(name=str(uuid.uuid4()), scopeIds=scopeIds, addDefaultViewColumns=False,
                                   addAnnotationColumns=True, type='file', includeEntityTypes=[EntityViewType.PROJECT],
                                   parent=project)
    syn.store(entity_view)

    entity_view = EntityViewSchema(name=str(uuid.uuid4()), scopeIds=scopeIds, addDefaultViewColumns=False,
                                   addAnnotationColumns=True, includeEntityTypes=[EntityViewType.PROJECT],
                                   parent=project)
    syn.store(entity_view)


def test_rowset_tables(syn, project):
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
    assert len(row_reference_set1['rows']) == 4


def test_tables_csv(syn, project):

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
        assert expected_row == row, "expected %s but got %s" % (expected_row, row)


def test_tables_pandas(syn, project):
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
    df.index = ['%s_1' % i for i in range(1, 6)]

    df['string_'] = df['string_'].transform(str)

    # SYNPY-717
    # This is a check for windows
    if os.name == 'nt':
        df['datetime64'] = pd.to_datetime(df['datetime64'], utc=True)
    else:
        df['datetime64'] = pd.to_datetime(df['datetime64'], unit='ms', utc=True)

    # df2 == df gives Dataframe of boolean values; first .all() gives a Series object of ANDed booleans of each column;
    # second .all() gives a bool that is ANDed value of that Series

    assert_frame_equal(df2, df)


def dontruntest_big_tables(syn, project):
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


def dontruntest_big_csvs(syn, project, schedule_for_cleanup):
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


def test_synapse_integer_columns_with_missing_values_from_dataframe(syn, project, schedule_for_cleanup):
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
    assert table.filepath != table_from_dataframe.filepath
    df2 = table_from_dataframe.asDataFrame()
    assert_frame_equal(df, df2)


def test_store_table_datetime(syn, project):
    current_datetime = datetime.fromtimestamp(round(time.time(), 3))
    schema = syn.store(Schema("testTable", [Column(name="testerino", columnType='DATE')], project))
    rowset = RowSet(rows=[Row([current_datetime])], schema=schema)
    syn.store(Table(schema, rowset))

    query_result = syn.tableQuery("select * from %s" % utils.id_of(schema), resultsAs="rowset")
    assert current_datetime == query_result.rowset['rows'][0]['values'][0]


@pytest.fixture(scope='class')
def partial_rowset_test_state(syn, project):
    cols = [Column(name='foo', columnType='INTEGER'), Column(name='bar', columnType='INTEGER')]
    table_schema = syn.store(Schema(name='PartialRowTest' + str(uuid.uuid4()), columns=cols, parent=project))
    data = [[1, None], [None, 2]]
    syn.store(RowSet(schema=table_schema, rows=[Row(r) for r in data]))

    # set up a file view
    folder = syn.store(Folder(name="PartialRowTestFolder" + str(uuid.uuid4()), parent=project))
    syn.store(File("~/path/doesnt/matter", name="f1", parent=folder, synapseStore=False))
    syn.store(File("~/path/doesnt/matter/again", name="f2", parent=folder, synapseStore=False))

    cols = [Column(name='foo', columnType='INTEGER'), Column(name='bar', columnType='INTEGER')]
    view_schema = syn.store(
        EntityViewSchema(name='PartialRowTestViews' + str(uuid.uuid4()), columns=cols, addDefaultViewColumns=False,
                         parent=project, scopes=[folder]))

    table_changes = [{'foo': 4}, {'bar': 5}]
    view_changes = [{'bar': 6}, {'foo': 7}]

    expected_table_cells = pd.DataFrame({'foo': [4.0, float('NaN')], 'bar': [float('NaN'), 5.0]})
    expected_view_cells = pd.DataFrame({'foo': [float('NaN'), 7.0], 'bar': [6.0, float('NaN')]})

    class TestState:
        def __init__(self):
            self.syn = syn
            self.project = project
            self.table_schema = table_schema
            self.view_schema = view_schema
            self.table_changes = table_changes
            self.view_changes = view_changes
            self.expected_table_cells = expected_table_cells
            self.expected_view_cells = expected_view_cells

    return TestState()


class TestPartialRowSet:

    def test_partial_row_view_csv_query_table(self, partial_rowset_test_state):
        """
        Test PartialRow updates to tables from cvs queries
        """
        test_state = partial_rowset_test_state
        self._test_method(
            test_state.syn,
            test_state.table_schema,
            "csv",
            test_state.table_changes,
            test_state.expected_table_cells
        )

    def test_partial_row_view_csv_query_entity_view(self, partial_rowset_test_state):
        """
        Test PartialRow updates to entity views from cvs queries
        """
        test_state = partial_rowset_test_state
        self._test_method(
            test_state.syn,
            test_state.view_schema,
            "csv",
            test_state.view_changes,
            test_state.expected_view_cells
        )

    def test_parital_row_rowset_query_table(self, partial_rowset_test_state):
        """
        Test PartialRow updates to tables from rowset queries
        """
        test_state = partial_rowset_test_state
        self._test_method(
            test_state.syn,
            test_state.table_schema,
            "rowset",
            test_state.table_changes,
            test_state.expected_table_cells
        )

    def test_parital_row_rowset_query_entity_view(self, partial_rowset_test_state):
        """
        Test PartialRow updates to entity views from rowset queries
        """
        test_state = partial_rowset_test_state
        self._test_method(
            test_state.syn,
            test_state.view_schema,
            "rowset",
            test_state.view_changes,
            test_state.expected_view_cells
        )

    def _test_method(self, syn, schema, resultsAs, partial_changes, expected_results):
        query_results = self._query_with_retry(
            syn,
            "SELECT * FROM %s" % utils.id_of(schema),
            resultsAs,
            2,
            None,
            QUERY_TIMEOUT_SEC
        )
        assert query_results is not None
        df = query_results.asDataFrame(rowIdAndVersionInIndex=False)

        partial_changes = {df['ROW_ID'][i]: row_changes for i, row_changes in enumerate(partial_changes)}

        partial_rowset = PartialRowset.from_mapping(partial_changes, query_results)
        syn.store(partial_rowset)

        assert self._query_with_retry(
            syn,
            "SELECT * FROM %s" % utils.id_of(schema),
            resultsAs,
            None,
            expected_results,
            QUERY_TIMEOUT_SEC
        ) is not None

    def _query_with_retry(self, syn, query, resultsAs, expected_result_len, expected_frame, timeout):
        # query and look for the expected_frame
        # if the expected_frame is not specified, look for the number of lines returned
        start_time = time.time()
        while time.time() - start_time < timeout:
            query_results = syn.tableQuery(query, resultsAs=resultsAs)
            if expected_frame is not None:
                df2 = query_results.asDataFrame()
                # remove the column index which cannot be set to expected_results
                df2 = df2.reset_index(drop=True)
                try:
                    assert_frame_equal(df2, expected_frame, check_like=True, check_dtype=False)
                    return query_results
                except AssertionError:
                    # hasn't found the result yet
                    pass
            elif expected_result_len and len(query_results) == expected_result_len:
                return query_results
        return None
