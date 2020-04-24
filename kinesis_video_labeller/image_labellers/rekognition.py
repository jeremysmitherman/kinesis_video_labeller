import boto3
import ray
import os
import time
from kinesis_video_labeller import logger
from kinesis_video_labeller.etl import process_labeling_result


@ray.remote
def label_image(stream_id, image_bytes, delivery_function, settings=None):
    if not settings:
        settings = {}

    # Get region in order from settings, environment, default to us-east-1
    region_name = settings.setdefault('rekognition_region_name', os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))
    client = boto3.client('rekognition', region_name=region_name)
    faces = client.detect_faces(
        Image={
            'Bytes': image_bytes
        },
        Attributes=["ALL"]
    )

    labels = client.detect_labels(
        Image={
            'Bytes': image_bytes
        }
    )

    result = process_labeling_result(stream_id, {
        "faces": faces['FaceDetails'],
        "labels": labels['Labels']
    }, int(time.time()), settings)

    logger.debug("Found {0} faces and {1} labels in image data".format(
        len(faces['FaceDetails']), len(labels['Labels'])))

    if result:
        logger.debug("Sending JSON API formatted document regarding ML event")
        delivery_function.remote(result)
