import os
import json
from google.cloud import pubsub_v1

with open("configs/geocodes.txt") as f:
    GEOS = f.read().splitlines()

# GEOS = ["US", "VN", "DE"]

def broadcast():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        os.getenv("PROJECT_ID"),
        os.getenv("TOPIC_ID"),
    )
    for geo in GEOS:
        data = {
            "geo": geo,
        }
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()

    return {
        "message_sent": len(geo),
    }
