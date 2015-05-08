import os

import synapseclient
from synapseclient.entity import Project, Folder, File

import integration
from integration import schedule_for_cleanup


def setup(module):
    print '\n'
    print '~' * 60
    print os.path.basename(__file__)
    print '~' * 60
    module.syn = integration.syn
    module.project = integration.project


def test_query():
    ## TODO: replace this test with the one below when query() is replaced
    
    # Remove all the Entities that are in the project
    qry = syn.query("select id from entity where entity.parentId=='%s'" % project['id'])
    for res in qry['results']:
        syn.delete(res['entity.id'])
    
    # Add entities and verify that I can find them with a query
    for i in range(2):
        syn.store(Folder(parent=project['id']))
        qry = syn.query("select id, name from entity where entity.parentId=='%s'" % project['id'])
        assert qry['totalNumberOfResults'] == i + 1

        
def test_chunked_query():
    oldLimit = synapseclient.client.QUERY_LIMIT
    try:
        synapseclient.client.QUERY_LIMIT = 3
        
        # Remove all the Entities that are in the project
        iter = syn.chunkedQuery("select id from entity where entity.parentId=='%s'" % project['id'])
        for res in iter:
            syn.delete(res['entity.id'])
        
        # Dump a bunch of Entities into the project
        for i in range(synapseclient.client.QUERY_LIMIT * 5):
            syn.store(Folder(parent=project['id']))
                
        # Give a bunch of limits/offsets to be ignored (except for the first ones)
        queryString = "select * from entity where entity.parentId=='%s' offset  1 limit 9999999999999    offset 2345   limit 6789 offset 3456    limit 5689" % project['id']
        iter = syn.chunkedQuery(queryString)
        count = 0
        for res in iter:
            count += 1
        assert count == (synapseclient.client.QUERY_LIMIT * 5)
    finally:
        synapseclient.client.QUERY_LIMIT = oldLimit


def test_chunked_query_giant_row():
    import synapseclient.utils as utils

    absurdly_long_desription = 'This is an absurdly long description!' + '~'*512000

    normal = syn.store(Folder('normal', description='A file with a normal length description', parentId=project['id']))
    absurd = syn.store(Folder('absurd', description=absurdly_long_desription, parentId=project['id']))

    resultgen = syn.chunkedQuery('select * from entity where parentId=="%s"' % project['id'])
    ids = [result['entity.id'] for result in resultgen]

    ## the expected behavior is that the absurdly long row will be skipped
    ## but the normal row will be returned

    assert normal.id in ids

