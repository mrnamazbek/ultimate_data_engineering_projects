"""
Integration smoke test: producer sends events and consumer receives them.

Requires running Kafka at KAFKA_BROKER.
Skipped automatically unless INTEGRATION=1 env var is set.
"""

import json
import os
import sys

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "services", "kafka_producer")
)

INTEGRATION = os.getenv("INTEGRATION", "0") == "1"
pytestmark = pytest.mark.skipif(not INTEGRATION, reason="integration tests disabled")


def test_producer_sends_to_kafka():
    """Producer should send ≥10 events to Kafka and consumer receives them."""
    from producer import build_producer, make_event, KAFKA_TOPIC  # noqa: PLC0415

    from kafka import KafkaConsumer  # noqa: PLC0415

    received: list = []

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
        auto_offset_reset="earliest",
        consumer_timeout_ms=12000,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    producer = build_producer()

    for i in range(10):
        producer.send(KAFKA_TOPIC, value=make_event(i))
    producer.flush()

    for msg in consumer:
        received.append(msg.value)
        if len(received) >= 10:
            break

    consumer.close()
    assert len(received) >= 10
