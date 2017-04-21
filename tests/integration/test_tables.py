# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str

from backports import csv
import io
import json
import filecmp
import math
import os
import random
import sys
import tempfile
import time
import uuid
from builtins import zip
from nose.tools import assert_raises
from datetime import datetime
from mock import patch

import synapseclient
import synapseclient.client as client
import synapseclient.utils as utils
from synapseclient.exceptions import *
from synapseclient import Project, File, Folder, Schema
from synapseclient.table import Column, RowSet, Row, cast_values, as_table_columns, Table
import synapseclient.exceptions as exceptions

import integration
from integration import schedule_for_cleanup


def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn
    module.project = integration.project

    print("Crank up timeout on async calls")
    module.syn.table_query_timeout = 423

def test_create_and_update_file_view():

    ## Create a folder
    folder = Folder(str(uuid.uuid4()), parent=project, description='creating a file-view')
    folder = syn.store(folder)

    ## Create dummy file with annotations in our folder
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

    # Required IDs for Synapse properties
    minimal_view_schema_column_ids = [x['id'] for x in syn.restGET("/column/tableview/defaults/file")['list']]
    
    scopeIds = [folder['id'].lstrip('syn')]

    column_ids = minimal_view_schema_column_ids + my_added_cols_ids
    ## Create an empty entity-view with defined scope as folder
    body = {'columnIds': column_ids,
            'concreteType': 'org.sagebionetworks.repo.model.table.EntityView',
            'entityType': 'org.sagebionetworks.repo.model.table.EntityView',
            'name': str(uuid.uuid4()),
            'parentId': project.id,
            'scopeIds': scopeIds,
            'type': 'file'}

    entity_view = syn.restPOST(uri='/entity', body=json.dumps(body))
    entity_view = syn.get(entity_view)
    schedule_for_cleanup(entity_view)

    assert set(scopeIds) == set(entity_view.scopeIds)
    assert set(column_ids) == set(entity_view.columnIds)

    ## get the current view-schema
    view = syn.tableQuery("select * from %s" % entity_view.id)
    schedule_for_cleanup(view.filepath)

    view_dict = list(csv.DictReader(io.open(view.filepath, encoding="utf-8", newline='')))

    # check that all of the annotations were retrieved from the view
    assert set(file_annotations.keys()).issubset(set(view_dict[0].keys()))

    updated_a_file = syn.get(a_file.id, downloadFile=False)

    # Check that the values are the same as what was set
    # Both in the view and on the entity itself
    for k, v in file_annotations.items():
        assert view_dict[0][k] == v, "%s != %s" % (view_dict[0][k], v)
        assert updated_a_file.annotations[k][0] == v, "%s != %s" % (updated_a_file.annotations[k][0], v)

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

    new_view = syn.store(synapseclient.Table(entity_view.id, temp_filename))
    new_view_results = syn.tableQuery("select * from %s" % entity_view.id)
    schedule_for_cleanup(new_view_results.filepath)

    new_view_dict = list(csv.DictReader(io.open(new_view_results.filepath, encoding="utf-8", newline='')))
    assert new_view_dict[0]['fileFormat'] == 'PNG', "%s != PNG" % (new_view_dict[0]['fileFormat'], )

def test_rowset_tables():
    # print("Project ID:", project.id)
    # del integration._to_cleanup[:]

    cols = []
    cols.append(Column(name='name', columnType='STRING', maximumSize=1000))
    cols.append(Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']))
    cols.append(Column(name='x', columnType='DOUBLE'))
    cols.append(Column(name='age', columnType='INTEGER'))
    cols.append(Column(name='cartoon', columnType='BOOLEAN'))
    cols.append(Column(name='description', columnType='LARGETEXT'))

    schema1 = syn.store(Schema(name='Foo Table', columns=cols, parent=project))

    print("Table Schema:", schema1.id)

    ## Get columns associated with the given table
    retrieved_cols = list(syn.getTableColumns(schema1))

    ## Test that the columns we get are the same as the ones we stored
    assert len(retrieved_cols) == len(cols)
    for retrieved_col, col in zip(retrieved_cols, cols):
        assert retrieved_col.name == col.name
        assert retrieved_col.columnType == col.columnType

    data1 = [['Chris', 'bar', 11.23, 45, False, 'a'],
             ['Jen', 'bat', 14.56, 40, False, 'b'],
             ['Jane', 'bat', 17.89, 6, False, 'c' * 1002],
             ['Henry', 'bar', 10.12, 1, False, 'd']]
    row_reference_set1 = syn.store(
        RowSet(columns=cols, schema=schema1, rows=[Row(r) for r in data1]))

    assert len(row_reference_set1['rows']) == 4

    ## add more new rows
    data2 = [['Fred', 'bat', 21.45, 20, True, 'e'],
             ['Daphne', 'foo', 27.89, 20, True, 'f'],
             ['Shaggy', 'foo', 23.45, 20, True, 'g'],
             ['Velma', 'bar', 25.67, 20, True, 'h']]
    syn.store(
        RowSet(columns=cols, schema=schema1, rows=[Row(r) for r in data2]))

    results = syn.tableQuery("select * from %s order by name" % schema1.id, resultsAs="rowset")

    assert results.count == 8
    assert results.tableId == schema1.id

    ## test that the values made the round trip
    expected = sorted(data1 + data2)
    for expected_values, row in zip(expected, results):
        assert expected_values == row['values'], 'got %s but expected %s' % (row['values'], expected_values)

    ## To modify rows, we have to select then first.
    result2 = syn.tableQuery('select * from %s where age>18 and age<30' % schema1.id, resultsAs="rowset")

    ## make a change
    rs = result2.asRowSet()
    for row in rs['rows']:
        row['values'][2] = 88.888

    ## store it
    row_reference_set = syn.store(rs)

    ## check if the change sticks
    result3 = syn.tableQuery('select name, x, age from %s' % schema1.id, resultsAs="rowset")
    for row in result3:
        if int(row['values'][2]) == 20:
            assert row['values'][1] == 88.888

    ## Add a column
    bday_column = syn.store(Column(name='birthday', columnType='DATE'))

    column = syn.getColumn(bday_column.id)
    assert column.name == "birthday"
    assert column.columnType == "DATE"

    schema1.addColumn(bday_column)
    schema1 = syn.store(schema1)

    results = syn.tableQuery('select * from %s where cartoon=false order by age' % schema1.id, resultsAs="rowset")
    rs = results.asRowSet()

    ## put data in new column
    bdays = ('2013-3-15', '2008-1-3', '1973-12-8', '1969-4-28')
    for bday, row in zip(bdays, rs.rows):
        row['values'][6] = bday
    row_reference_set = syn.store(rs)

    ## query by date and check that we get back two kids
    date_2008_jan_1 = utils.to_unix_epoch_time(datetime(2008, 1, 1))
    results = syn.tableQuery(
        'select name from %s where birthday > %d order by birthday' % (schema1.id, date_2008_jan_1), resultsAs="rowset")
    assert ["Jane", "Henry"] == [row['values'][0] for row in results]

    try:
        import pandas as pd
        df = results.asDataFrame()
        assert all(df.ix[:, "name"] == ["Jane", "Henry"])
    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping part of test_rowset_tables.\n\n')

    results = syn.tableQuery('select birthday from %s where cartoon=false order by age' % schema1.id,
                             resultsAs="rowset")
    for bday, row in zip(bdays, results):
        assert row['values'][0] == datetime.strptime(bday, "%Y-%m-%d"), "got %s but expected %s" % (
            row['values'][0], bday)

    try:
        import pandas as pd
        results = syn.tableQuery(
            "select foo, MAX(x), COUNT(foo), MIN(age) from %s group by foo order by foo" % schema1.id,
            resultsAs="rowset")
        df = results.asDataFrame()
        print(df)
        assert df.shape == (3, 4)
        assert all(df.iloc[:, 0] == ["bar", "bat", "foo"])
        assert all(df.iloc[:, 1] == [88.888, 88.888, 88.888])
        assert all(df.iloc[:, 2] == [3, 3, 2])
    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping part of test_rowset_tables.\n\n')

    ## test delete rows by deleting cartoon characters
    syn.delete(syn.tableQuery('select name from %s where cartoon = true' % schema1.id, resultsAs="rowset"))

    results = syn.tableQuery('select name from %s order by birthday' % schema1.id, resultsAs="rowset")
    assert ["Chris", "Jen", "Jane", "Henry"] == [row['values'][0] for row in results]

    ## check what happens when query result is empty
    results = syn.tableQuery('select * from %s where age > 1000' % schema1.id, resultsAs="rowset")
    assert len(list(results)) == 0

    try:
        import pandas as pd
        results = syn.tableQuery('select * from %s where age > 1000' % schema1.id, resultsAs="rowset")
        df = results.asDataFrame()
        assert df.shape[0] == 0
    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping part of test_rowset_tables.\n\n')


def test_view_and_update_csv():
    try:
        import pandas as pd
    except ImportError as e1:
        sys.stderr.write(
            'Pandas is not installed, skipping testing entity-view update via user-defined manifest CSV.\n\n')

    ## Update the project
    project_name = str(uuid.uuid4())
    project = Project(name=project_name)
    project = syn.store(project)
    schedule_for_cleanup(project)
    project = syn.getEntity(project)
    # print(project.id)
    assert project.name == project_name

    ## Create and get a de-novo folder
    folder = Folder('De-Novo Folder', parent=project, description='creating a file-view')
    folder = syn.createEntity(folder)
    folder = syn.getEntity(folder)
    assert folder.name == 'De-Novo Folder'
    assert folder.parentId == project.id
    # print(folder)
    assert folder.description == 'creating a file-view'

    ## Create dummy files with annotations in our de-novo folder
    path = utils.make_bogus_data_file()
    schedule_for_cleanup(path)
    a_file = File(path, parent=folder, description=u'',
                  contentType='text/flapdoodle',
                  fileFormat='jpg',
                  dataType='image',
                  artist='Banksy',
                  medium='print',
                  title='Girl With Ballon')
    a_file = syn.store(a_file)
    file_info = syn.getEntity(a_file)
    # print(a_file)
    # print(file_info)
    assert a_file.path == path

    ## How-to get scopeIds, not required yet: The list of container IDs (projects or folders) that define the scope of this view
    '''body = {u'concreteType': u'org.sagebionetworks.repo.model.table.ViewScope', u'scope': [file_info.id]}
    entity_scope = syn.restPOST(uri='/column/view/scope', body=json.dumps(body))
    print(entity_scope)'''

    # minimal_schema_columnIds = [u'2510', u'23543', u'23544', u'23545', u'31783', u'26823', u'31784', u'23550',
    #                            u'30514', u'31785', u'31782', u'31786']

    ## Create an empty entity-view with defined scope as folder 
    body = {u'columnIds': [],
            u'concreteType': u'org.sagebionetworks.repo.model.table.EntityView',
            u'entityType': u'org.sagebionetworks.repo.model.table.EntityView',
            u'name': u'empty file view',
            u'parentId': project.id,
            u'scopeIds': [folder['id'][3:]],
            u'type': u'file'}

    entity_view = syn.restPOST(uri='/entity', body=json.dumps(body))
    # print(entity_view)
    entity_info = syn.getEntity(entity_view)
    # print(entity_info)

    ## add folder to entity-view (stuck)
    # view_1 = syn.store(synapseclient.Table(entity_info['id'], path))

    ## do we need to define the view-schema columns? 
    cols = [
        Column(name='fileFormat', columnType='STRING', maximumSize=20),
        Column(name='dataType', columnType='STRING', maximumSize=20),
        Column(name='artist', columnType='STRING', maximumSize=20),
        Column(name='medium', columnType='STRING', maximumSize=20),
        Column(name='title', columnType='STRING', maximumSize=20)]

    schema = Schema(name='my view schema', columns=cols, parent=entity_info.id)
    # print(schema) # columnIds are not defined

    ## None de-novo test 
    '''view_id = "syn8529621"
    query = "select * from %s" % view_id

    ## the format of this data will be used to create a de-novo entity-view (when creation of entity-view is implemented)
    df = pd.DataFrame({
            'id': ("syn8528274", "syn8528280", "syn8528283", "syn8528288"),
            'fileFormat': ("jpg", "jpg", "jpg", "jpg"),
            'dataType': ("image", "image", "image", "image"),
            'artist': ("Banksy", "Banksy", "Banksy", "Banksy"),
            'medium': ("Print", "Print", "Print", "Print"),
            'title': ("Gangsta Rat", "Girl With Ballon", "Pulp Fiction", "Radar Rat"),
            })

    ## get the current view-schema
    view = syn.tableQuery(query)
    view_df = view.asDataFrame()

    ## check if the user-defined schema is a subset of view-schema
    assert df.columns.isin(view_df.columns).all()

    ## update the view-schema by user-defined data-frame manifest
    view = syn.store(synapseclient.Table(view_id, df))

    ## get the updated view-schema
    updated_view = syn.tableQuery(query)
    updated_df = updated_view.asDataFrame()

    ## check if syn.store() updated the view-schema
    assert 'Banksy' in list(df.ix[:, 'artist'])
    assert 'jpg' in list(df.ix[:, 'fileFormat'])
    assert 'Pulp Fiction' in list(df.ix[:, 'title'])

    ## revert view-schema changes for next test
    view = syn.store(synapseclient.Table(view_id, view_df))'''


def test_tables_csv():
    ## Define schema
    cols = []
    cols.append(Column(name='Name', columnType='STRING'))
    cols.append(Column(name='Born', columnType='INTEGER'))
    cols.append(Column(name='Hipness', columnType='DOUBLE'))
    cols.append(Column(name='Living', columnType='BOOLEAN'))

    schema = Schema(name='Jazz Guys', columns=cols, parent=project)

    data = [["John Coltrane", 1926, 8.65, False],
            ["Miles Davis", 1926, 9.87, False],
            ["Bill Evans", 1929, 7.65, False],
            ["Paul Chambers", 1935, 5.14, False],
            ["Jimmy Cobb", 1929, 5.78, True],
            ["Scott LaFaro", 1936, 4.21, False],
            ["Sonny Rollins", 1930, 8.99, True],
            ["Kenny Burrel", 1931, 4.37, True]]

    ## the following creates a CSV file and uploads it to create a new table
    table = syn.store(Table(schema, data))

    ## Query and download an identical CSV
    results = syn.tableQuery("select * from %s" % table.schema.id, resultsAs="csv", includeRowIdAndRowVersion=False)

    ## Test that CSV file came back as expected
    for expected_row, row in zip(data, results):
        assert expected_row == row, "expected %s but got %s" % (expected_row, row)

    try:
        ## check if we have pandas
        import pandas as pd

        df = results.asDataFrame()
        assert all(df.columns.values == ['Name', 'Born', 'Hipness', 'Living'])
        assert list(df.iloc[1, [0, 1, 3]]) == ['Miles Davis', 1926, False]
        assert df.iloc[1, 2] - 9.87 < 0.0001
    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping test of .asDataFrame for CSV tables.\n\n')

    ## Aggregate query
    expected = {
        True: [True, 1929, 3, 6.38],
        False: [False, 1926, 5, 7.104]}

    results = syn.tableQuery(
        'select Living, min(Born), count(Living), avg(Hipness) from %s group by Living' % table.schema.id,
        resultsAs="csv", includeRowIdAndRowVersion=False)
    for row in results:
        living = row[0]
        assert expected[living][1] == row[1]
        assert expected[living][2] == row[2]
        assert abs(expected[living][3] - row[3]) < 0.0001

    ## Aggregate query results to DataFrame
    try:
        ## check if we have pandas
        import pandas as pd

        df = results.asDataFrame()
        assert all(expected[df.iloc[0, 0]][0:3] == df.iloc[0, 0:3])
        assert abs(expected[df.iloc[1, 0]][3] - df.iloc[1, 3]) < 0.0001
    except ImportError as e1:
        sys.stderr.write(
            'Pandas is apparently not installed, skipping test of .asDataFrame for aggregate queries as CSV tables.\n\n')

    ## Append rows
    more_jazz_guys = [["Sonny Clark", 1931, 8.43, False],
                      ["Hank Mobley", 1930, 5.67, False],
                      ["Freddie Hubbard", 1938, float('nan'), False],
                      ["Thelonious Monk", 1917, float('inf'), False]]
    table = syn.store(Table(table.schema, more_jazz_guys))

    ## test that CSV file now has more jazz guys
    results = syn.tableQuery("select * from %s" % table.schema.id, resultsAs="csv")
    for expected_row, row in zip(data + more_jazz_guys, results):
        for field, expected_field in zip(row[2:], expected_row):
            if type(field) is float and math.isnan(field):
                assert type(expected_field) is float and math.isnan(expected_field)
            elif type(expected_field) is float and math.isnan(expected_field):
                assert type(field) is float and math.isnan(field)
            else:
                assert expected_field == field

    ## Update as a RowSet
    rowset = results.asRowSet()
    for row in rowset['rows']:
        if row['values'][1] == 1930:
            row['values'][2] = 8.5
    row_reference_set = syn.store(rowset)

    ## aggregate queries won't return row id and version, so we need to
    ## handle this correctly
    results = syn.tableQuery('select Born, COUNT(*) from %s group by Born order by Born' % table.schema.id,
                             resultsAs="csv")
    assert results.includeRowIdAndRowVersion == False
    for i, row in enumerate(results):
        assert row[0] == [1917, 1926, 1929, 1930, 1931, 1935, 1936, 1938][i]
        assert row[1] == [1, 2, 2, 2, 2, 1, 1, 1][i]

    try:
        import pandas as pd
        results = syn.tableQuery("select * from %s where Born=1930" % table.schema.id, resultsAs="csv")
        df = results.asDataFrame()
        print("\nUpdated hipness to 8.5", df)
        all(df['Born'].values == 1930)
        all(df['Hipness'].values == 8.5)

        ## Update via a Data Frame
        df['Hipness'] = 9.75
        table = syn.store(Table(table.tableId, df, etag=results.etag))

        results = syn.tableQuery("select * from %s where Born=1930" % table.tableId, resultsAs="csv")
        for row in results:
            assert row[4] == 9.75
    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping part of test_tables_csv.\n\n')

    ## check what happens when query result is empty
    results = syn.tableQuery('select * from %s where Born=2013' % table.tableId, resultsAs="csv")
    assert len(list(results)) == 0

    try:
        import pandas as pd
        results = syn.tableQuery('select * from %s where Born=2013' % table.tableId, resultsAs="csv")
        df = results.asDataFrame()
        assert df.shape[0] == 0
    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping part of test_tables_csv.\n\n')

    ## delete some rows
    results = syn.tableQuery('select * from %s where Hipness < 7' % table.tableId, resultsAs="csv")
    syn.delete(results)


def test_tables_pandas():
    try:
        ## check if we have pandas
        import pandas as pd

        ## create a pandas DataFrame
        df = pd.DataFrame({
            'A': ("foo", "bar", "baz", "qux", "asdf"),
            'B': tuple(math.pi * i for i in range(5)),
            'C': (101, 202, 303, 404, 505),
            'D': (False, True, False, True, False)})

        cols = as_table_columns(df)
        cols[0].maximumSize = 20
        schema = Schema(name="Nifty Table", columns=cols, parent=project)

        ## store in Synapse
        table = syn.store(Table(schema, df))

        ## retrieve the table and verify
        results = syn.tableQuery('select * from %s' % table.schema.id)
        df2 = results.asDataFrame()

        ## simulate rowId-version rownames for comparison
        df.index = ['%s_0' % i for i in range(5)]
        assert all(df2 == df)

    except ImportError as e1:
        sys.stderr.write('Pandas is apparently not installed, skipping test_tables_pandas.\n\n')


def test_download_table_files():
    cols = [
        Column(name='artist', columnType='STRING', maximumSize=50),
        Column(name='album', columnType='STRING', maximumSize=50),
        Column(name='year', columnType='INTEGER'),
        Column(name='catalog', columnType='STRING', maximumSize=50),
        Column(name='cover', columnType='FILEHANDLEID')]

    schema = syn.store(Schema(name='Jazz Albums', columns=cols, parent=project))
    schedule_for_cleanup(schema)

    data = [["John Coltrane", "Blue Train", 1957, "BLP 1577", "coltraneBlueTrain.jpg"],
            ["Sonny Rollins", "Vol. 2", 1957, "BLP 1558", "rollinsBN1558.jpg"],
            ["Sonny Rollins", "Newk's Time", 1958, "BLP 4001", "rollinsBN4001.jpg"],
            ["Kenny Burrel", "Kenny Burrel", 1956, "BLP 1543", "burrellWarholBN1543.jpg"]]

    ## upload files and store file handle ids
    original_files = []
    for row in data:
        path = utils.make_bogus_data_file()
        original_files.append(path)
        schedule_for_cleanup(path)
        file_handle = syn._uploadToFileHandleService(path)
        row[4] = file_handle['id']

    row_reference_set = syn.store(RowSet(columns=cols, schema=schema, rows=[Row(r) for r in data]))

    ## retrieve the files for each row and verify that they are identical to the originals
    results = syn.tableQuery('select artist, album, year, catalog, cover from %s' % schema.id, resultsAs="rowset")
    for i, row in enumerate(results):
        print("%s_%s" % (row.rowId, row.versionNumber), row.values)
        path = syn.downloadTableFile(results, rowId=row.rowId, versionNumber=row.versionNumber, column='cover')
        assert filecmp.cmp(original_files[i], path)
        schedule_for_cleanup(path)

    ## test that cached copies are returned for already downloaded files
    original_downloadFile_method = syn._downloadFileHandle
    with patch("synapseclient.Synapse._downloadFileHandle") as _downloadFile_mock:
        _downloadFile_mock.side_effect = original_downloadFile_method

        results = syn.tableQuery(
            "select artist, album, year, catalog, cover from %s where artist = 'John Coltrane'" % schema.id,
            resultsAs="rowset")
        for i, row in enumerate(results):
            print("%s_%s" % (row.rowId, row.versionNumber), row.values)
            file_path = syn.downloadTableFile(results, rowId=row.rowId, versionNumber=row.versionNumber, column='cover')
            assert filecmp.cmp(original_files[i], file_path)

        assert not _downloadFile_mock.called, "Should have used cached copy of file and not called _downloadFile"

    ## test download table column
    results = syn.tableQuery('select * from %s' % schema.id)
    ## uncache 2 out of 4 files
    for i, row in enumerate(results):
        if i % 2 == 0:
            syn.cache.remove(row[6])
    file_map = syn.downloadTableColumns(results, ['cover'])
    assert len(file_map) == 4
    for row in results:
        filecmp.cmp(original_files[i], file_map[row[6]])


def dontruntest_big_tables():
    cols = []
    cols.append(Column(name='name', columnType='STRING', maximumSize=1000))
    cols.append(Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']))
    cols.append(Column(name='x', columnType='DOUBLE'))
    cols.append(Column(name='n', columnType='INTEGER'))
    cols.append(Column(name='is_bogus', columnType='BOOLEAN'))

    table1 = syn.store(Schema(name='Big Table', columns=cols, parent=project))

    print("Created table:", table1.id)
    print("with columns:", table1.columnIds)

    rows_per_append = 10

    for i in range(1000):
        rows = []
        for j in range(rows_per_append):
            foo = cols[1].enumValues[random.randint(0, 2)]
            rows.append(Row(('Robot ' + str(i * rows_per_append + j), foo, random.random() * 200.0,
                             random.randint(0, 100), random.random() >= 0.5)))
        print("added %d rows" % rows_per_append)
        rowset1 = syn.store(RowSet(columns=cols, schema=table1, rows=rows))

    results = syn.tableQuery("select * from %s" % table1.id)
    print("etag:", results.etag)
    print("tableId:", results.tableId)

    for row in results:
        print(row)

    results = syn.tableQuery("select n, COUNT(n), MIN(x), AVG(x), MAX(x), SUM(x) from %s group by n" % table1.id)
    df = results.asDataFrame()

    print(df.shape)
    print(df)


def dontruntest_big_csvs():
    cols = []
    cols.append(Column(name='name', columnType='STRING', maximumSize=1000))
    cols.append(Column(name='foo', columnType='STRING', enumValues=['foo', 'bar', 'bat']))
    cols.append(Column(name='x', columnType='DOUBLE'))
    cols.append(Column(name='n', columnType='INTEGER'))
    cols.append(Column(name='is_bogus', columnType='BOOLEAN'))

    schema1 = syn.store(Schema(name='Big Table', columns=cols, parent=project))

    print("Created table:", schema1.id)
    print("with columns:", schema1.columnIds)

    ## write rows to CSV file
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        schedule_for_cleanup(temp.name)
        filename = temp.name

    with io.open(filename, mode='w', encoding="utf-8", newline='') as temp:
        writer = csv.writer(temp, quoting=csv.QUOTE_NONNUMERIC, lineterminator=str(os.linesep))
        writer.writerow([col.name for col in cols])

        for i in range(10):
            for j in range(100):
                foo = cols[1].enumValues[random.randint(0, 2)]
                writer.writerow(('Robot ' + str(i * 100 + j), foo, random.random() * 200.0, random.randint(0, 100),
                                 random.random() >= 0.5))
            print("wrote 100 rows to disk")

    ## upload CSV
    UploadToTableResult = syn._uploadCsv(filepath=temp.name, schema=schema1)

    from synapseclient.table import CsvFileTable
    results = CsvFileTable.from_table_query(syn, "select * from %s" % schema1.id)
    print("etag:", results.etag)
    print("tableId:", results.tableId)

    for row in results:
        print(row)
