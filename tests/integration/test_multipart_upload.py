# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import filecmp
import os, sys, traceback

import synapseclient
import synapseclient.utils as utils
from synapseclient.utils import MB, GB
from synapseclient import Activity, Entity, Project, Folder, File
import tempfile

import integration
from integration import schedule_for_cleanup


def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn
    module.project = integration.project

def test_round_trip():
    fhid = None
    filepath = utils.make_bogus_binary_file(6*MB + 777771)
    print('Made bogus file: ', filepath)
    try:
        fhid = syn.multipartUpload(filepath)
        print('FileHandle: {fhid}'.format(fhid=fhid))

        # Download the file and compare it with the original
        junk = File(filepath, parent=project, dataFileHandleId=fhid)
        junk.properties.update(syn._createEntity(junk.properties))
        junk.update(syn._downloadFileEntity(junk, filepath))
        assert filecmp.cmp(filepath, junk.path)

    finally:
        try:
            if 'junk' in locals():
                syn.delete(junk)
        except Exception:
            print(traceback.format_exc())
        try:
            os.remove(filepath)
        except Exception:
            print(traceback.format_exc())
        try:
            if fhid:
                print('Deleting fileHandle', fhid)
                syn._deleteFileHandle(fhid)
        except Exception:
            print(traceback.format_exc())
