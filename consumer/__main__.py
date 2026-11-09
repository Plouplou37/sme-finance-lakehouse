"""Read the three topics back as a consumer group, and check what the producer published (M3).

Run from the repository root:
    uv run --env-file .env python -m consumer                                      # new group: full check
    uv run --env-file .env python -m consumer --group resume-test --stop-after 1000  # read part, then stop
    uv run --env-file .env python -m consumer --group resume-test                    # resume where it stopped

The group starts where it last committed (from the beginning for a new group), and stops after
--stop-after messages, or after a few seconds without any new message. When a run reads every
message since the beginning, it also checks:
- the counts per topic against the generator's manifest;
- that each key always lands in the same partition;
- that the events of each key come out in the order the producer sent them (the file order).
"""

import argparse
import json
import os
import time
from collections import Counter, defaultdict
from pathlib import Path

from confluent_kafka import Consumer

from generator.events import KEY_FIELD_BY_TOPIC, TOPICS

DATA_DIR = Path("data/generated")
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
FIRST_MESSAGE_WAIT_S = 20  # joining a group takes a few seconds before the first partitions are assigned
IDLE_STOP_S = 5  # once messages flow, stop after this long without a new one
COMMIT_EVERY = 100


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m consumer", description="Read the topics back and check them.")
    parser.add_argument("--group", default=f"verify-{int(time.time())}", help="consumer group (default: a new one)")
    parser.add_argument("--stop-after", type=int, help="stop after this many messages (simulates a stopped consumer)")
    return parser.parse_args()


def expected_order() -> dict[tuple[str, str], list[str]]:
    """(topic, key) -> event_ids in the order the producer sent them, read from the generator's files."""
    order: dict[tuple[str, str], list[str]] = defaultdict(list)
    for topic in TOPICS:
        with (DATA_DIR / f"{topic}.jsonl").open(encoding="utf-8") as file:
            for line in file:
                event = json.loads(line)
                order[(topic, event[KEY_FIELD_BY_TOPIC[topic]])].append(event["event_id"])
    return order


def main() -> None:
    args = parse_args()
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": args.group,
            # librdkafka's default is "largest": a new group would start at the END of each partition
            # and see none of the messages already written.
            "auto.offset.reset": "earliest",
            # Commit ourselves, AFTER processing: if we crash in between, the messages are read again
            # (at least once), never lost.
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(TOPICS)

    read: Counter[str] = Counter()
    order: dict[tuple[str, str], list[str]] = defaultdict(list)
    partitions_of_key: dict[tuple[str, str], set[int]] = defaultdict(set)
    last_message_at = None
    started_at = time.monotonic()
    total = 0
    uncommitted = 0  # messages processed since the last commit
    try:
        try:
            while args.stop_after is None or total < args.stop_after:
                message = consumer.poll(1.0)
                now = time.monotonic()
                if message is None:
                    waited = now - (last_message_at or started_at)
                    if waited > (IDLE_STOP_S if last_message_at else FIRST_MESSAGE_WAIT_S):
                        break  # nothing new: everything available has been read
                    continue
                if message.error():
                    raise SystemExit(f"Kafka error: {message.error()}")
                last_message_at = now
                topic, key = message.topic() or "?", (message.key() or b"").decode()
                event = json.loads(message.value() or b"{}")
                read[topic] += 1
                order[(topic, key)].append(event["event_id"])
                partitions_of_key[(topic, key)].add(message.partition() or 0)
                total += 1
                uncommitted += 1
                if uncommitted == COMMIT_EVERY:
                    consumer.commit(asynchronous=False)  # "processed up to here", stored in __consumer_offsets
                    uncommitted = 0
        except KeyboardInterrupt:
            print("Stopped with Ctrl+C: committing what was processed, then leaving the group")
        # A commit with nothing new to commit fails (_NO_OFFSET), hence the check.
        if uncommitted:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()  # leaves the group cleanly: its partitions can be reassigned at once

    print(f"Group {args.group}: {total} messages read")
    for topic in TOPICS:
        print(f"  {topic}: {read[topic]}")

    manifest = json.loads((DATA_DIR / "manifest.json").read_text(encoding="utf-8"))
    if total != manifest["events"]["total"]:
        print(
            f"This run read {total} of the {manifest['events']['total']} events: counts and order are only checked "
            f"on a full read. Run again with --group {args.group} to resume where it stopped."
        )
        return

    # Full read: check everything the producer published.
    problems = []
    for topic in TOPICS:
        if read[topic] != manifest["events"]["by_topic"][topic]:
            problems.append(f"{topic}: {read[topic]} read, {manifest['events']['by_topic'][topic]} in the manifest")
    split_keys = [key for key, partitions in partitions_of_key.items() if len(partitions) > 1]
    if split_keys:
        problems.append(f"{len(split_keys)} keys found in several partitions, e.g. {split_keys[0]}")
    expected = expected_order()
    out_of_order = [key for key, event_ids in expected.items() if order.get(key) != event_ids]
    if out_of_order:
        problems.append(f"{len(out_of_order)} keys out of order, e.g. {out_of_order[0]}")
    if problems:
        raise SystemExit(f"Check failed: {problems}")
    print(f"OK: counts match the manifest, every key stays in one partition, {len(expected)} keys in order")


if __name__ == "__main__":
    main()
