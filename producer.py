import os
import sys
import time
import json
import argparse
from datetime import datetime, timezone
import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def ensure_single_partition_topic(bootstrap_servers, topic_name):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin.create_topics([topic])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()

def get_current_rate(elapsed_sec, base_rate, mode):
    if mode == "burst":
        if elapsed_sec < 40:
            return 20.0
        elif elapsed_sec < 80:
            return 150.0
        else:
            return 20.0
    return float(base_rate)

def main():
    parser = argparse.ArgumentParser(description="Kafka Replay Producer for Lambda Benchmark")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--input", default="data/cleaned_events.parquet")
    parser.add_argument("--rate", type=float, default=20.0, help="Events per second (constant mode)")
    parser.add_argument("--duration", type=int, default=120, help="Duration in seconds")
    parser.add_argument("--mode", choices=["constant", "burst"], default="constant")
    parser.add_argument("--gt-out", default="logs/produced_ground_truth.jsonl")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.gt_out) or ".", exist_ok=True)
    ensure_single_partition_topic(args.bootstrap_servers, args.topic)

    df = pd.read_parquet(args.input).sort_values("seq").reset_index(drop=True)
    records = df.to_dict(orient="records")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        linger_ms=5
    )

    sent_count = 0
    error_count = 0
    start_time = time.monotonic()
    next_send_time = start_time

    def on_error(excp):
        nonlocal error_count
        error_count += 1

    print(f"[Producer] Starting replay: mode={args.mode}, base_rate={args.rate}/s, duration={args.duration}s")

    with open(args.gt_out, "w", encoding="utf-8") as gt_file:
        for row in records:
            now_mono = time.monotonic()
            elapsed = now_mono - start_time
            if elapsed >= args.duration:
                break

            current_rate = get_current_rate(elapsed, args.rate, args.mode)
            interval = 1.0 / current_rate

            if now_mono < next_send_time:
                time.sleep(next_send_time - now_mono)

            produced_at_iso = datetime.now(timezone.utc).isoformat()
            event_payload = {
                "event_id": str(row["event_id"]),
                "seq": int(row["seq"]),
                "event_time": str(row["event_time"]),
                "produced_at": produced_at_iso,
                "category": str(row["category"]),
                "amount_cents": int(row["amount_cents"])
            }

            producer.send(args.topic, value=event_payload, partition=0).add_errback(on_error)
            gt_file.write(json.dumps(event_payload) + "\n")
            sent_count += 1

            if sent_count % 20 == 0:
                gt_file.flush()

            next_send_time += interval
            if time.monotonic() - next_send_time > 1.0:
                next_send_time = time.monotonic()

    producer.flush()
    producer.close()
    total_time = time.monotonic() - start_time
    print(f"[Producer] Finished in {total_time:.2f}s | Sent: {sent_count} | Errors: {error_count}")
    if error_count > 0:
        sys.exit(1)
    with open(f"{args.gt_out}.done", "w", encoding="utf-8") as done_f:
        done_f.write(str(sent_count))

if __name__ == "__main__":
    main()

