import json
import os
import time

from kafka import KafkaProducer
from requests_sse import EventSource


def save_checkpoint(event_id):
    tmp = "checkpoint.tmp"
    final = "checkpoint.json"

    try:
        with open(tmp, "w") as f:
            json.dump(event_id, f)
            f.flush()
            os.fsync(f.fileno())
        for _ in range(5):
            try:
                os.replace(tmp, final)
                break
            except PermissionError:
                time.sleep(0.5)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


url = "https://stream.wikimedia.org/v2/stream/recentchange"
headers = {"User-Agent": "my-wikimedia-stream-test/1.0"}

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


last_event_id = None
# read the last event id
if os.path.exists("checkpoint.json"):
    with open("checkpoint.json") as f:
        last_event_id = json.load(f)


with EventSource(url, headers=headers, latest_event_id=last_event_id) as stream:
    for event in stream:
        if event.type == "message":
            try:
                change = json.loads(event.data)
            except ValueError:
                pass
            else:
                # discard canary events
                if change["meta"]["domain"] == "canary":
                    continue

                event_id = event.last_event_id
                # wait for kafka ack
                producer.send("wiki_events", value=change).get()

                # save checkpoint
                save_checkpoint(event_id)
