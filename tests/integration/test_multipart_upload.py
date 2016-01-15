# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import filecmp
import os, random, sys, traceback
from io import open

import synapseclient
import synapseclient.utils as utils
from synapseclient.utils import MB, GB
from synapseclient import Activity, Entity, Project, Folder, File
from synapseclient.multipart_upload import multipart_upload, multipart_upload_string
import synapseclient.multipart_upload as multipart_upload_module
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
        fhid = multipart_upload(syn, filepath)
        print('FileHandle: {fhid}'.format(fhid=fhid))

        # Download the file and compare it with the original
        junk = File(filepath, parent=project, dataFileHandleId=fhid)
        junk.properties.update(syn._createEntity(junk.properties))
        (tmp_f, tmp_path) = tempfile.mkstemp()
        schedule_for_cleanup(tmp_path)
        junk.update(syn._downloadFileEntity(junk, tmp_path))
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


def test_randomly_failing_parts():
    FAILURE_RATE = 1.0/3.0
    fhid = None
    filepath = utils.make_bogus_binary_file(6*MB + 777771)
    print('Made bogus file: ', filepath)

    normal_put_chunk = None

    def _put_chunk_or_fail_randomly(url, chunk, verbose=False):
        if random.random() < FAILURE_RATE:
            raise IOError("Ooops! Artificial upload failure for testing.")
        else:
            return normal_put_chunk(url, chunk, verbose)

    ## Mock _put_chunk to fail randomly
    normal_put_chunk = multipart_upload_module._put_chunk
    multipart_upload_module._put_chunk = _put_chunk_or_fail_randomly

    try:
        fhid = multipart_upload(syn, filepath, retries=25)
        print('FileHandle: {fhid}'.format(fhid=fhid))

        # Download the file and compare it with the original
        junk = File(filepath, parent=project, dataFileHandleId=fhid)
        junk.properties.update(syn._createEntity(junk.properties))
        (tmp_f, tmp_path) = tempfile.mkstemp()
        schedule_for_cleanup(tmp_path)
        junk.update(syn._downloadFileEntity(junk, tmp_path))
        assert filecmp.cmp(filepath, junk.path)

    finally:
        ## Un-mock _put_chunk
        if normal_put_chunk:
            multipart_upload_module._put_chunk = normal_put_chunk

        try:
            if 'junk' in locals():
                syn.delete(junk)
        except Exception:
            print(traceback.format_exc())
        try:
            os.remove(filepath)
        except Exception:
            print(traceback.format_exc())


def test_multipart_upload_string():
    text = "\n".join((
        "In Xanadu did Kubla Khan",
        "A stately pleasure-dome decree:",
        "Where Alph, the sacred river, ran",
        "Through caverns measureless to man",
        "Down to a sunless sea.",
        "Zürich, 金沢市, 서울, แม่ฮ่องสอน, Москва"))
    fhid = multipart_upload_string(syn, text)
    print('FileHandle: {fhid}'.format(fhid=fhid))

    # Download the file and compare it with the original
    junk = File("message.txt", parent=project, dataFileHandleId=fhid)
    junk.properties.update(syn._createEntity(junk.properties))
    (tmp_f, tmp_path) = tempfile.mkstemp()
    schedule_for_cleanup(tmp_path)
    junk.update(syn._downloadFileEntity(junk, tmp_path))

    with open(junk.path, encoding='utf-8') as f:
        retrieved_text = f.read()

    assert retrieved_text == text



