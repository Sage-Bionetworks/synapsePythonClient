import unittest
from unittest import mock
from queue import Queue
from synapseclient.core.download.producer_consumer_download_threads import CloseableQueue
from nose.tools import assert_equal

class TestClosableQueueu(unittest.TestCase):
    def test_close(self):
        normal_queue = CloseableQueue(5)
        normal_queue.put('a')
        normal_queue.put('b')
        normal_queue.close()
        normal_queue.put('c')

        queue_iteration = [item for item in normal_queue]
        assert_equal(['a','b'], queue_iteration)



if __name__ == '__main__':
    unittest.main()
