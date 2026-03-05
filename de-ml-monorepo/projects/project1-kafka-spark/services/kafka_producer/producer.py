"""Kafka event producer – sends synthetic events at a configurable rate."""

import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# Read config from environment
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
PRODUCER_RATE = float(os.getenv("PRODUCER_RATE", "1"))  # events per second


def build_producer() -> KafkaProducer:
    """Create a KafkaProducer with JSON serialisation."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def make_event(event_id: int) -> dict:
    """Generate a single synthetic event record."""
    return {
        "id": event_id,
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "value": random.choice(["click", "view", "purchase", "scroll"]),
        "user_id": random.randint(1, 1000),
    }


def run(producer: KafkaProducer) -> None:
    """Produce events indefinitely at PRODUCER_RATE events/second."""
    event_id = 0
    delay = 1.0 / PRODUCER_RATE
    print(f"Producing to {KAFKA_BROKER}/{KAFKA_TOPIC} at {PRODUCER_RATE} eps")
    while True:
        event = make_event(event_id)
        producer.send(KAFKA_TOPIC, value=event)
        event_id += 1
        time.sleep(delay)


if __name__ == "__main__":
    p = build_producer()
    try:
        run(p)
    finally:
        p.flush()
        p.close()
