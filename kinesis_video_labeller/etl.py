from uuid import uuid4


def process_labeling_result(stream_id, recognition_result, timestamp, settings=None):
    event_type = None
    if len(recognition_result['faces']) != 1:
        if len(recognition_result['faces']) == 0:
            event_type = 'empty_frame'
        if len(recognition_result['faces']) > 1:
            event_type = 'multiple_in_frame'

    if event_type:
        return {
            "data": {
                "type": "ml_event",
                "id": str(uuid4()),
                "attributes": {
                    "event_type": event_type,
                    "timestamp": timestamp
                },
                "relationships": {
                    "stream": {
                        "data": {
                            "id": stream_id,
                            "type": "video_stream"
                        }
                    }
                }
            }
        }
