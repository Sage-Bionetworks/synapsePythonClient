# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import mock

from nose.tools import assert_raises, assert_is_none, assert_is_not_none

import synapseclient
from synapseclient.exceptions import SynapseUnmetAccessRestrictions
import integration


def setup(module):
    module.syn = integration.syn
    module.project = integration.project


def test_access_restrictions():
    # Bruce gives this test a 'B'. The 'A' solution would be to
    # construct the mock value from the schemas. -jcb
    with mock.patch('synapseclient.Synapse._getEntityBundle') as _getEntityBundle_mock:
        _getEntityBundle_mock.return_value = {
            'annotations': {
              'etag': 'cbda8e02-a83e-4435-96d0-0af4d3684a90',
              'id': 'syn1000002',
              'stringAnnotations': {}},
            'entity': {
              'concreteType': 'org.sagebionetworks.repo.model.FileEntity',
              'createdBy': 'Miles Dewey Davis',
              'entityType': 'org.sagebionetworks.repo.model.FileEntity',
              'etag': 'cbda8e02-a83e-4435-96d0-0af4d3684a90',
              'id': 'syn1000002',
              'name': 'so_what.mp3',
              'parentId': 'syn1000001',
              'versionLabel': '1',
              'versionNumber': 1,
              'dataFileHandleId': '42'},

            'entityType': 'org.sagebionetworks.repo.model.FileEntity',
            'fileHandles': [
                {
                    'id': '42'
                }
            ],
            'restrictionInformation': {'hasUnmetAccessRequirement': True}
        }

        entity = syn.get('syn1000002', downloadFile=False)
        assert_is_not_none(entity)
        assert_is_none(entity.path)

        # Downloading the file is the default, but is an error if we have unmet access requirements
        assert_raises(synapseclient.exceptions.SynapseUnmetAccessRestrictions, syn.get, 'syn1000002', downloadFile=True)
