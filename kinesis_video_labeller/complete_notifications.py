import rabbitpy
from kinesis_video_labeller import logger


def get_completed_streams():
    queue_name = 'ml_completed_streams'
    completed_streams = []

    with rabbitpy.Connection() as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, queue_name)
            queue.durable = True
            queue.declare()

            logger.debug("Connected to RabbitMQ (GET COMPLETED)")
            while len(queue) > 0:
                message = queue.get()
                completed_streams.append(message.body.decode())
                message.ack()

    return completed_streams


def set_completed_stream(stream):
    queue_name = 'ml_completed_streams'

    with rabbitpy.Connection() as conn:
        with conn.channel() as channel:
            queue = rabbitpy.Queue(channel, queue_name)
            queue.durable = True
            queue.declare()

            logger.debug("Connected to RabbitMQ (SET COMPLETED)")

            message = rabbitpy.Message(channel, stream)
            try:
                message.publish('', queue_name, mandatory=True)
            except rabbitpy.exceptions.MessageReturnedException:
                logger.error("Message failed to publish data for stream id: {0}".format(stream))
