import json
import rabbitpy
import ray
from kinesis_video_labeller import logger


@ray.remote
def deliver_etl(result):
    queue_name = 'ml_events'

    with rabbitpy.Connection() as conn:
        logger.debug("Connected to RabbitMQ")
        with conn.channel() as channel:
            channel.enable_publisher_confirms()
            queue = rabbitpy.Queue(channel, queue_name)
            queue.durable = True
            queue.declare()

            message = rabbitpy.Message(channel, json.dumps(result))
            logger.info("ML Event message pushed to RabbitMQ")
            if not message.publish('', queue_name, mandatory=True):
                logger.error("Message failed to publish data for stream id: {0}")
