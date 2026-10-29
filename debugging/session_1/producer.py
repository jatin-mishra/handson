# /// script
# requires-python = ">=3.10"
# dependencies = ["confluent-kafka>=2.5"]
# ///
"""Press Enter to publish one event to `inventory`. Type q + Enter (or Ctrl+C) to quit."""
import json
from datetime import datetime
from itertools import count
from zoneinfo import ZoneInfo

from confluent_kafka import Producer

TOPIC = "inventory"
IST = ZoneInfo("Asia/Kolkata")
producer = Producer({"bootstrap.servers": "localhost:9092"})
sequence = count(1)  # resets on every run


def send_event():
    # Constant payload; only event_id and updated_at change per event.
    event = {
        "event_id": next(sequence),
        "product_id": "P-1001",
        "remaining_inventory": 42,
        "updated_at": datetime.now(IST).isoformat(),
    }
    body = json.dumps(event)
    producer.produce(TOPIC, key=event["product_id"], value=body)
    producer.flush()
    print(f"Produced: {body}")


if __name__ == "__main__":
    print(f"Producer up (topic={TOPIC}). Press Enter to send an event, q + Enter to quit.")
    try:
        while input().strip().lower() != "q":
            send_event()
    except (KeyboardInterrupt, EOFError):
        pass
    print("Bye")
