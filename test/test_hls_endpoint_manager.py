import unittest
from unittest.mock import Mock, patch
from kinesis_video_labeller.hls_endpoint_manager import HLSEndpointManager


def mock_get_hls_endpoints(pending, processing):
    return [{'stream_id': 'foo_stream', 'url': 'http://example.com'},
            {'stream_id': 'bar_stream', 'url': 'http://example.com'},
            {'stream_id': 'baz_stream', 'url': 'http://example.com'}]


class TestHLSEndpointManager(unittest.TestCase):
    def test_processing_complete(self):
        manager = HLSEndpointManager(Mock())
        manager.processing_endpoints.add('a_test_stream')
        manager.pending_endpoints['a_pending_stream'] = "http://example.com"
        manager.processing_complete('a_test_stream')
        manager.processing_complete('a_pending_stream')

        self.assertEqual(0, len(manager.processing_endpoints))
        self.assertEqual(0, len(manager.pending_endpoints))

    def test_processing_started(self):
        manager = HLSEndpointManager(Mock())
        manager.pending_endpoints['a_pending_stream'] = "http://example.com"
        manager.processing_started('a_pending_stream')

        self.assertEqual(1, len(manager.processing_endpoints))
        self.assertEqual(0, len(manager.pending_endpoints))

    def test_endpoint_finder_populates_manager_lists(self):
        manager = HLSEndpointManager(Mock())

        manager.get_hls_urls(mock_get_hls_endpoints)
        self.assertEqual(len(manager.pending_endpoints), 3)
