"""AWS Kinesis Video Streams HLS endpoint finder implementation"""

import boto3
import os
from kinesis_video_labeller import logger


def get_hls_streaming_session_urls(pending_streams, processing_streams, settings=None):
    """Returns list of HLS endpoints from a list of Kinesis Video Streams.

    Needs AWS_DEFAULT_REGION somewhere in the environment. Will only return HLS url's for video streams that
    have fragments available when this function is called (i.e. the stream must be created AND currently
    have video being streamed to it before AWS will cough up the URL for it).
    """

    if not settings:
        settings = {}

    region_name = settings.setdefault('kvs_region_name', os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))

    # A little weird with the two clients created in this method, kinesisvideo gets the data endpoint
    # needed by kinesis-video-archived-media later in the function to actually get the HLS url.
    video_client = boto3.client('kinesisvideo', region_name=region_name)
    logger.debug("Started boto3 KVS client in {0}".format(region_name))
    streams = video_client.list_streams()

    # Discard streams that aren't ready to be checked or have been marked for deletion.
    active_steams = [stream for stream in streams['StreamInfoList'] if stream['Status'] == 'ACTIVE']

    endpoints = []
    for stream in active_steams:
        if stream['StreamName'] in pending_streams or stream['StreamName'] in processing_streams:
            continue
        try:
            # Get the data endpoint for this stream ID, need this later to get the HLS endpoint.
            endpoint = video_client.get_data_endpoint(
                APIName="GET_HLS_STREAMING_SESSION_URL",
                StreamName=stream['StreamName']
            )['DataEndpoint']
        except Exception as e:
            logger.error("Failed to get data endpoint: {0}\n{1}".format(stream['StreamName'], e))
            continue

        # Build a client using the data endpoint retrieved by kinesisvideo
        streaming_client = boto3.client("kinesis-video-archived-media", endpoint_url=endpoint, region_name=region_name)
        try:
            # Get the HLS endpoint URL and append it to our endpoints list.
            endpoints.append({"stream_id": stream['StreamName'],
                              "url": streaming_client.get_hls_streaming_session_url(
                                  StreamName=stream['StreamName'],
                                  PlaybackMode="LIVE",
                                  Expires=3600
                              )['HLSStreamingSessionURL']
                              })
        except streaming_client.exceptions.ResourceNotFoundException:
            continue

    logger.debug("Found {0} endpoints with streaming data.".format(len(endpoints)))
    return endpoints
