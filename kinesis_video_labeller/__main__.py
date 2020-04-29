import ray
import os
from kinesis_video_labeller.hls_endpoint_manager import HLSEndpointManager
from kinesis_video_labeller.hls_endpoint_finders.kvs import get_hls_streaming_session_urls
from kinesis_video_labeller.frame_extractors.opencv import OpenCVExtractor
from kinesis_video_labeller.image_labellers.rekognition import label_image
from kinesis_video_labeller.etl_deliverers.rabbit import deliver_etl

if __name__ == '__main__':
    ray.init(address=os.environ.get('RAY_ADDRESS'))
    extractor = OpenCVExtractor.remote()
    try:
        manager = HLSEndpointManager(extractor)
        manager.start(get_hls_streaming_session_urls, label_image, deliver_etl)
    except KeyboardInterrupt:
        pass
