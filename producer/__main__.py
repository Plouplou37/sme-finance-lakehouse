"""Publish the generator's events to Kafka (M3).

Run from the repository root, after the generator:
    uv run --env-file .env python -m producer

Reads data/generated/<topic>.jsonl (one event per line, in arrival order) and sends each line,
unchanged, to its topic, with the key set by the event contract. Every delivery is checked, and the
script ends with exit code 1 if a single message is missing.

Run it once per generated dataset: running it again publishes every event a second time. Kafka keeps
both copies (same event_id), and silver will have to remove them (M5). `make reset` starts over.
"""

import json
import os
from collections import Counter
from pathlib import Path

from confluent_kafka import KafkaError, Message, Producer

from generator.events import KEY_FIELD_BY_TOPIC, TOPICS  # reused: same topics and keys as the generator

DATA_DIR = Path("data/generated")
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")

PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    # librdkafka disables idempotence by default (the Java client enables it): a resend after a lost
    # acknowledgement would then be written twice. With it, the broker drops the resend. It also
    # forces acks=all: a message counts as delivered only once the broker has written it.
    "enable.idempotence": True,
    # librdkafka hashes keys with CRC32 by default, while Java clients use murmur2. Same algorithm,
    # same partition for a given key, whatever the language of the producer.
    "partitioner": "murmur2_random",
}


def main() -> None:
    manifest_path = DATA_DIR / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"{manifest_path} not found: run the generator first (uv run python -m generator)")
    expected = json.loads(manifest_path.read_text(encoding="utf-8"))["events"]["by_topic"]

    producer = Producer(PRODUCER_CONFIG)
    delivered: Counter[str] = Counter()
    failed: list[str] = []

    def on_delivery(error: KafkaError | None, message: Message) -> None:
        """Called once the broker has answered for one message (from poll() or flush())."""
        topic = message.topic() or "?"  # typed as optional by the library, always set for a produced message
        if error is not None:
            failed.append(f"{topic} key={message.key()}: {error}")
        else:
            delivered[topic] += 1

    for topic in TOPICS:
        key_field = KEY_FIELD_BY_TOPIC[topic]  # customer_id or invoice_id (event contract)
        with (DATA_DIR / f"{topic}.jsonl").open(encoding="utf-8") as file:
            for line in file:
                value = line.rstrip("\n")  # the event exactly as the generator wrote it
                key = json.loads(value)[key_field]
                # produce() only puts the message in a local queue: it is sent later, in batches.
                # If the queue is full, wait for earlier deliveries to free some space, then retry.
                while True:
                    try:
                        producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                        break
                    except BufferError:
                        producer.poll(1)
                producer.poll(0)  # run the delivery callbacks that are ready, without waiting
        print(f"Queued: {topic}")

    # Without flush(), the last batches could still be in the local queue when the script ends.
    remaining = producer.flush(30)  # waits up to 30 s for every answer; returns how many are left

    problems = []
    for topic in TOPICS:
        status = "OK" if delivered[topic] == expected[topic] else "MISMATCH"
        print(f"{topic}: {delivered[topic]} delivered, {expected[topic]} in the manifest -> {status}")
        if status != "OK":
            problems.append(f"{topic}: {delivered[topic]} delivered instead of {expected[topic]}")
    if remaining:
        problems.append(f"{remaining} messages still unanswered after flush")
    if failed:
        problems.append(f"{len(failed)} deliveries failed, first one: {failed[0]}")
    if problems:
        raise SystemExit(f"Production incomplete: {problems}")  # exit code 1
    print(f"OK: {sum(delivered.values())} events published")


if __name__ == "__main__":
    main()
