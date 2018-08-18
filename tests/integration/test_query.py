# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import time

from nose.tools import assert_less, assert_equals
import unittest

import deprecation

import synapseclient
from synapseclient.entity import Folder

import integration
from integration import schedule_for_cleanup, QUERY_TIMEOUT_SEC


def setup(module):

    module.syn = integration.syn
    module.project = integration.project


@deprecation.fail_if_not_removed
@unittest.skip("Deprecated")
def test_query():
    # TODO: replace this test with the one below when query() is replaced
    query_str = "select id from entity where entity.parentId=='%s'" % project['id']
    # Remove all the Entities that are in the project
    qry = syn.query(query_str)
    while len(qry['results']) > 0:
        for res in qry['results']:
            syn.delete(res['entity.id'])
        qry = syn.query(query_str)

    # Add entities and verify that I can find them with a query
    for i in range(2):
        syn.store(Folder(parent=project['id']))

        start_time = time.time()
        qry = syn.query(query_str)
        while len(qry['results']) != i + 1:
            assert_less(time.time() - start_time, QUERY_TIMEOUT_SEC)
            time.sleep(2)
            qry = syn.query(query_str)

        assert_equals(len(qry['results']), i + 1)


@deprecation.fail_if_not_removed
@unittest.skip("Deprecated")
def test_chunked_query():
    oldLimit = synapseclient.client.QUERY_LIMIT
    try:
        synapseclient.client.QUERY_LIMIT = 3
        time.sleep(3)
        # Remove all the Entities that are in the project
        iter = syn.chunkedQuery("select id from entity where entity.parentId=='%s'" % project['id'])
        for res in iter:
            syn.delete(res['entity.id'])
        
        # Dump a bunch of Entities into the project
        for i in range(synapseclient.client.QUERY_LIMIT * 5):
            syn.store(Folder(parent=project['id']))

        time.sleep(3)

        # Give a bunch of limits/offsets to be ignored (except for the first ones)
        queryString = "select * from entity where entity.parentId=='%s' offset  1 limit 9999999999999" \
                      "    offset 2345   limit 6789 offset 3456    limit 5689" % project['id']
        count = 0
        start_time = time.time()
        while count != (synapseclient.client.QUERY_LIMIT * 5):
            assert_less(time.time() - start_time, QUERY_TIMEOUT_SEC)
            time.sleep(2)
            iter = syn.chunkedQuery(queryString)
            count = 0
            for res in iter:
                count += 1
        assert_equals(count, (synapseclient.client.QUERY_LIMIT * 5))
    finally:
        synapseclient.client.QUERY_LIMIT = oldLimit


@deprecation.fail_if_not_removed
@unittest.skip("Deprecated")
def test_chunked_query_giant_row():
    absurdly_long_desription = 'This is an absurdly long description!' + '~'*512000

    normal = syn.store(Folder('normal', description='A file with a normal length description', parentId=project['id']))
    absurd = syn.store(Folder('absurd', description=absurdly_long_desription, parentId=project['id']))

    # the expected behavior is that the absurdly long row will be skipped
    # but the normal row will be returned
    start_time = time.time()
    ids = []
    while normal.id not in ids:
        assert_less(time.time() - start_time, QUERY_TIMEOUT_SEC)
        time.sleep(2)
        resultgen = syn.chunkedQuery('select * from entity where parentId=="%s"' % project['id'])
        ids = [result['entity.id'] for result in resultgen]


