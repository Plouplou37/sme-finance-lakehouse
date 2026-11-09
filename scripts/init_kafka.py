"""Create the Kafka topics of the event contract in Redpanda. Safe to run several times (idempotent).

Run from the repository root:
    uv run --env-file .env scripts/init_kafka.py

The broker address comes from KAFKA_BOOTSTRAP_SERVERS (in .env): from the Windows host it is
localhost:19092, the external listener Redpanda advertises to the host.
"""

import os
import time

from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")

# Must match docs/event-contract.md.
TOPICS = ["finance.customers.v1", "finance.invoices.v1", "finance.payments.v1"]
PARTITIONS = 3  # enough to see ordering per key and consumers sharing partitions; small for our volume
REPLICATION_FACTOR = 1  # a single broker: no replica to copy to
# Keep every message: the volume is tiny and the topics must survive breaks between sessions.
# In production, retention would be limited: the long-term history is the bronze layer's job.
TOPIC_CONFIG = {"retention.ms": "-1"}


def check(admin: AdminClient) -> list[str]:
    """Return what differs from the expected topics (an empty list means everything is right)."""
    existing = admin.list_topics(timeout=10).topics  # topic name -> metadata, including its partitions
    problems = []
    for topic in TOPICS:
        if topic not in existing:
            problems.append(f"{topic}: missing")
        elif len(existing[topic].partitions) != PARTITIONS:
            problems.append(f"{topic}: {len(existing[topic].partitions)} partitions instead of {PARTITIONS}")
    return problems


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    # Create only the missing topics: creating an existing topic fails with TOPIC_ALREADY_EXISTS.
    existing = admin.list_topics(timeout=10).topics
    missing = [topic for topic in TOPICS if topic not in existing]
    for topic in TOPICS:
        if topic not in missing:
            print(f"Topic already exists: {topic}")
    if missing:
        new_topics = [
            NewTopic(topic, num_partitions=PARTITIONS, replication_factor=REPLICATION_FACTOR, config=TOPIC_CONFIG)
            for topic in missing
        ]
        for topic, future in admin.create_topics(new_topics).items():
            future.result()  # waits for the broker's answer; raises an exception if the creation failed
            print(f"Topic created: {topic}")

    # Check the result instead of trusting the calls above. A new topic can take a moment
    # to appear in the metadata, so retry for a few seconds before giving up.
    for _ in range(10):
        problems = check(admin)
        if not problems:
            break
        time.sleep(1)
    if problems:
        raise SystemExit(f"Kafka topics are not as expected: {problems}")  # exit code 1: make up stops
    print(f"OK: {len(TOPICS)} topics with {PARTITIONS} partitions each")


if __name__ == "__main__":
    main()
