from synapseclient.table import cast_row

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
