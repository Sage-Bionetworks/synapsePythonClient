# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import filecmp
import os
import random
import traceback
from io import open

import synapseclient.utils as utils
from synapseclient.utils import MB
from synapseclient import File
from synapseclient.multipart_upload import multipart_upload, multipart_upload_string
import synapseclient.multipart_upload as multipart_upload_module
import tempfile

from nose.tools import assert_equals, assert_true

import integration
from integration import schedule_for_cleanup


def setup(module):
    module.syn = integration.syn
    module.project = integration.project


def test_round_trip():
    fhid = None
    filepath = utils.make_bogus_binary_file(multipart_upload_module.MIN_PART_SIZE + 777771)
    try:
        fhid = multipart_upload(syn, filepath)

        # Download the file and compare it with the original
        junk = File(parent=project, dataFileHandleId=fhid)
        junk.properties.update(syn._createEntity(junk.properties))
        (tmp_f, tmp_path) = tempfile.mkstemp()
        schedule_for_cleanup(tmp_path)

        junk['path'] = syn._downloadFileHandle(fhid, junk['id'], 'FileEntity', tmp_path)
        assert_true(filecmp.cmp(filepath, junk.path))

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
    multipart_upload_module.MIN_PART_SIZE = 5*MB
    multipart_upload_module.MAX_RETRIES = 20

    filepath = utils.make_bogus_binary_file(multipart_upload_module.MIN_PART_SIZE*2 + 777771)

    normal_put_chunk = None

    def _put_chunk_or_fail_randomly(url, chunk, verbose=False):
        if random.random() < FAILURE_RATE:
            raise IOError("Ooops! Artificial upload failure for testing.")
        else:
            return normal_put_chunk(url, chunk, verbose)

    # Mock _put_chunk to fail randomly
    normal_put_chunk = multipart_upload_module._put_chunk
    multipart_upload_module._put_chunk = _put_chunk_or_fail_randomly

    try:
        fhid = multipart_upload(syn, filepath)

        # Download the file and compare it with the original
        junk = File(parent=project, dataFileHandleId=fhid)
        junk.properties.update(syn._createEntity(junk.properties))
        (tmp_f, tmp_path) = tempfile.mkstemp()
        schedule_for_cleanup(tmp_path)

        junk['path'] = syn._downloadFileHandle(fhid, junk['id'], 'FileEntity', tmp_path)
        assert_true(filecmp.cmp(filepath, junk.path))

    finally:
        # Un-mock _put_chunk
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


def test_multipart_upload_big_string():
    cities = ["Seattle", "Portland", "Vancouver", "Victoria",
              "San Francisco", "Los Angeles", "New York",
              "Oaxaca", "Cancún", "Curaçao", "जोधपुर",
              "অসম", "ལྷ་ས།", "ཐིམ་ཕུ་", "دبي", "አዲስ አበባ",
              "São Paulo", "Buenos Aires", "Cartagena",
              "Amsterdam", "Venice", "Rome", "Dubrovnik",
              "Sarajevo", "Madrid", "Barcelona", "Paris",
              "Αθήνα", "Ρόδος", "København", "Zürich",
              "金沢市", "서울", "แม่ฮ่องสอน", "Москва"]

    text = "Places I wanna go:\n"
    while len(text.encode('utf-8')) < multipart_upload_module.MIN_PART_SIZE:
        text += ", ".join(random.choice(cities) for i in range(5000)) + "\n"

    fhid = multipart_upload_string(syn, text)

    # Download the file and compare it with the original
    junk = File(parent=project, dataFileHandleId=fhid)
    junk.properties.update(syn._createEntity(junk.properties))
    (tmp_f, tmp_path) = tempfile.mkstemp()
    schedule_for_cleanup(tmp_path)

    junk['path'] = syn._downloadFileHandle(fhid, junk['id'], "FileEntity", tmp_path)

    with open(junk.path, encoding='utf-8') as f:
        retrieved_text = f.read()

    assert_equals(retrieved_text, text)

