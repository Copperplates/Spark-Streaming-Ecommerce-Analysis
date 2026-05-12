import argparse
import csv
import json
import os
import random
import shutil
import time
from datetime import datetime


DEFAULT_SOURCE_FILE = "/opt/spark-data/E-commerceCustomerBehavior-Sheet1.csv"
DEFAULT_STREAM_INPUT_DIR = "/opt/spark-data/realtime_input"


def parse_args():
    parser = argparse.ArgumentParser(description="Produce e-commerce events to Kafka.")
    parser.add_argument(
        "--mode",
        choices=["files", "kafka"],
        default="files",
        help="Write streaming events to files or Kafka.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka-broker:29092",
        help="Kafka bootstrap servers inside Docker network.",
    )
    parser.add_argument(
        "--topic",
        default="ecommerce-transactions",
        help="Kafka topic name.",
    )
    parser.add_argument(
        "--source-file",
        default=DEFAULT_SOURCE_FILE,
        help="CSV seed file mounted into the Spark container.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="How many Kafka batches to publish before exiting.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Number of events published per batch.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=5.0,
        help="Pause between batches.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic demo output.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_STREAM_INPUT_DIR,
        help="Directory used by file-stream mode.",
    )
    parser.add_argument(
        "--reset-output",
        action="store_true",
        help="Delete existing file-stream input before publishing.",
    )
    return parser.parse_args()


def parse_bool(value):
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def load_seed_rows(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    if not rows:
        raise ValueError(f"No rows found in source file: {path}")

    return rows


def derive_satisfaction(average_rating, days_since_last_purchase):
    if average_rating >= 4.3 and days_since_last_purchase <= 28:
        return "Satisfied"
    if average_rating < 3.7 or days_since_last_purchase > 38:
        return "Unsatisfied"
    return "Neutral"


def mutate_row(base_row, event_index, rng):
    total_spend = max(
        120.0,
        round(float(base_row["Total Spend"]) + rng.uniform(-90.0, 140.0), 2),
    )
    items_purchased = max(1, int(base_row["Items Purchased"]) + rng.randint(-2, 3))
    average_rating = min(
        5.0,
        max(2.8, round(float(base_row["Average Rating"]) + rng.uniform(-0.4, 0.5), 1)),
    )
    days_since_last_purchase = max(
        1,
        int(base_row["Days Since Last Purchase"]) + rng.randint(-6, 8),
    )
    discount_applied = parse_bool(base_row["Discount Applied"])
    if rng.random() < 0.25:
        discount_applied = not discount_applied

    payload = {
        "event_id": f"evt-{event_index:06d}",
        "event_time": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "customer_id": int(base_row["Customer ID"]),
        "gender": base_row["Gender"],
        "age": max(18, min(70, int(base_row["Age"]) + rng.randint(-1, 1))),
        "city": base_row["City"],
        "membership_type": base_row["Membership Type"],
        "total_spend": total_spend,
        "items_purchased": items_purchased,
        "average_rating": average_rating,
        "discount_applied": discount_applied,
        "days_since_last_purchase": days_since_last_purchase,
    }
    payload["satisfaction_level"] = derive_satisfaction(
        average_rating, days_since_last_purchase
    )
    return payload


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    seed_rows = load_seed_rows(args.source_file)
    event_index = 1
    producer = None

    if args.mode == "files":
        if args.reset_output and os.path.exists(args.output_dir):
            shutil.rmtree(args.output_dir)
        os.makedirs(args.output_dir, exist_ok=True)
    else:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
        )

    try:
        for batch_number in range(1, args.iterations + 1):
            batch_events = []
            for _ in range(args.batch_size):
                source_row = rng.choice(seed_rows)
                batch_events.append(mutate_row(source_row, event_index, rng))
                event_index += 1

            if args.mode == "files":
                output_path = os.path.join(
                    args.output_dir, f"batch_{batch_number:03d}.json"
                )
                with open(output_path, "w", encoding="utf-8") as handle:
                    for payload in batch_events:
                        handle.write(json.dumps(payload) + "\n")
                print(
                    f"Published batch {batch_number}/{args.iterations} "
                    f"with {len(batch_events)} events to {output_path}."
                )
            else:
                for payload in batch_events:
                    producer.send(args.topic, value=payload)
                producer.flush()
                print(
                    f"Published batch {batch_number}/{args.iterations} "
                    f"with {len(batch_events)} events to topic {args.topic}."
                )

            if batch_number < args.iterations:
                time.sleep(args.sleep_seconds)
    finally:
        if producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
