"""Unit tests for the Kafka producer (no real Kafka needed)."""

from unittest.mock import MagicMock, patch

# Adjust path so we can import the producer module
import sys
import os

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "services", "kafka_producer")
)

from producer import make_event


def test_make_event_fields():
    """make_event must return all required fields."""
    event = make_event(42)
    assert event["id"] == 42
    assert "ts" in event
    assert event["value"] in ("click", "view", "purchase", "scroll")
    assert 1 <= event["user_id"] <= 1000


def test_make_event_unique_ids():
    """Consecutive events should have sequential ids."""
    events = [make_event(i) for i in range(10)]
    ids = [e["id"] for e in events]
    assert ids == list(range(10))


def test_run_sends_events():
    """run() should call producer.send for each iteration."""
    mock_producer = MagicMock()

    # Patch time.sleep to avoid actual delay and stop after 3 iterations
    call_count = {"n": 0}

    def fake_sleep(_delay):
        call_count["n"] += 1
        if call_count["n"] >= 3:
            raise KeyboardInterrupt

    with patch("producer.time.sleep", fake_sleep):
        with patch("producer.PRODUCER_RATE", 1):
            from producer import run

            try:
                run(mock_producer)
            except KeyboardInterrupt:
                pass

    assert mock_producer.send.call_count >= 3
