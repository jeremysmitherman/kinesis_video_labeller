import cv2
import ray
import time
from kinesis_video_labeller import logger
from kinesis_video_labeller.complete_notifications import set_completed_stream


@ray.remote
class OpenCVExtractor:
    def extract_frames_from_hls(self, stream_id, hls_url, label_function, delivery_function, settings=None):
        if not settings:
            settings = {}

        label_delay = settings.setdefault('label_delay', 5)
        last_frame_time = 0

        capture = cv2.VideoCapture(hls_url)
        try:
            while True:
                success, frame = capture.read()
                if not success:
                    break

                if int(time.time()) > last_frame_time + label_delay:
                    did_encode, numpy_image = cv2.imencode('.png', frame)
                    logger.debug("Sending image to labeler.  Size: {:.2f} MB".format(len(numpy_image.tobytes()) / 1024))
                    label_function.remote(stream_id, numpy_image.tobytes(), delivery_function, settings)
                    last_frame_time = int(time.time())
        finally:
            logger.info("Informing HLS manager that stream {0} is finished.".format(stream_id))
            set_completed_stream(stream_id)
