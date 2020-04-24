"""Manages HLS endpoints found by HLS endpoint finder actors
"""
import time
from kinesis_video_labeller import logger
from kinesis_video_labeller.complete_notifications import get_completed_streams


class HLSEndpointManager:
    """Actor for operating a concrete endpoint_finder instance"""
    def __init__(self, frame_extractor):
        self.pending_endpoints = {}
        self.processing_endpoints = set()
        self.frame_extractor = frame_extractor

    def start(self, endpoint_finder_function, label_function, delivery_function):
        while True:
            for stream in get_completed_streams():
                logger.info("Marking stream {0} as complete".format(stream))
                self.processing_complete(stream)

            self.get_hls_urls(endpoint_finder_function)
            processing_endpoints = []
            logger.debug("There are {0} endpoints pending.".format(len(self.pending_endpoints)))
            for stream_id, stream_url in self.pending_endpoints.items():
                try:
                    self.frame_extractor.extract_frames_from_hls.remote(stream_id,
                                                                        stream_url,
                                                                        label_function,
                                                                        delivery_function)
                    processing_endpoints.append(stream_id)
                except Exception as e:
                    logger.error(e)
                    pass
            for id in processing_endpoints:
                self.processing_started(id)
            time.sleep(5)

    def get_hls_urls(self, endpoint_finder_function):
        """Directs the endpoint finder to gather a list of HLS endpoints

        The finder should return a list of dictionaries with the keys 'stream_id' and 'url'.
        The manager will then insert those results into the finder's pending_endpoints so that they
        aren't re-processed when the list is refreshed.
        """
        for endpoint in endpoint_finder_function(self.pending_endpoints, self.processing_endpoints):
            self.pending_endpoints[endpoint['stream_id']] = endpoint['url']

    def processing_started(self, stream_id):
        """Moves a stream from pending to processing in the endpoint finder

        Pending streams are found by get_hls_urls, and this should be called once a frame extractor is operating on
        a stream to move it from pending to processing.
        """
        try:
            del (self.pending_endpoints[stream_id])
            self.processing_endpoints.add(stream_id)
        except KeyError:
            logger.error("No pending endpoint with id: " + stream_id)

    def processing_complete(self, stream_id):
        """Removes a stream ID from the pending/processing pipeline.

        Should be called after a frame_extractor has finished with an HLS endpoint so that if this ID needs to be
        processed later get_hls_endpoints can find it again.
        """
        try:
            del(self.pending_endpoints[stream_id])
        except KeyError:
            logger.debug("Stream {0} wasn't found in processing endpoints.".format(stream_id))

        try:
            self.processing_endpoints.remove(stream_id)
        except KeyError:
            logger.debug("Stream {0} wasn't found in processing endpoints.".format(stream_id))
        logger.info("Stream {0} has been marked complete".format(stream_id))
