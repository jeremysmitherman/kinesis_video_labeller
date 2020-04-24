import time
import unittest
from kinesis_video_labeller.etl import process_labeling_result


class TestETL(unittest.TestCase):
    def test_empty_frame_event_generates_etl(self):
        result = process_labeling_result('test_stream', {
            "faces": [],
            "labels": []
        }, int(time.time()))

        self.assertEqual(result['data']['attributes']['event_type'], 'empty_frame')

    def test_multiple_in_frame_event_generates_etl(self):
        result = process_labeling_result('test_stream', {
            "faces": ["a_face", "another_face"],
            "labels": []
        }, int(time.time()))

        self.assertEqual(result['data']['attributes']['event_type'], 'multiple_in_frame')

    def test_populated_frame_doesnt_generate_etl(self):
        result = process_labeling_result('test_stream', {
            "faces": ["mockface"],
            "labels": []
        }, int(time.time()))

        self.assertIsNone(result)
