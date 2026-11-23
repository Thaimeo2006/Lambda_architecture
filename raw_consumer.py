import os
import time
import json
import signal
import argparse
import pandas as pd
from kafka import KafkaConsumer

running = True

def handle_sigterm(signum, frame):
    global running
    running = False

signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)

def flush_buffer(buffer, tmp_dir, out_dir):
    if not buffer:
        return
    df = pd.DataFrame(buffer)
    min_seq = int(df["seq"].min())
    max_seq = int(df["seq"].max())
    filename = f"part_{min_seq:08d}_{max_seq:08d}_{int(time.time()*1000)}.parquet"
    tmp_path = os.path.join(tmp_dir, filename + ".tmp")
    final_path = os.path.join(out_dir, filename)

    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, final_path)

def main():
    parser = argparse.ArgumentParser(description="Kafka to Immutable Parquet Raw Consumer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--group-id", default="raw_parquet_archiver")
    parser.add_argument("--out-dir", default="raw_parquet")
    parser.add_argument("--tmp-dir", default="raw_tmp")
    parser.add_argument("--flush-interval", type=float, default=3.0)
    parser.add_argument("--max-batch-size", type=int, default=500)
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(args.tmp_dir, exist_ok=True)

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=500
    )

    buffer = []
    last_flush = time.monotonic()
    total_archived = 0

    def commit_batch():
        nonlocal total_archived, last_flush
        if not buffer:
            return
        flush_buffer(buffer, args.tmp_dir, args.out_dir)
        consumer.commit()
        total_archived += len(buffer)
        buffer.clear()
        last_flush = time.monotonic()

    print("[RawConsumer] Started listening for events...")
    while running:
        try:
            for msg in consumer:
                buffer.append(msg.value)
                if len(buffer) >= args.max_batch_size or (time.monotonic() - last_flush) >= args.flush_interval:
                    commit_batch()
                if not running:
                    break
            if buffer and (time.monotonic() - last_flush) >= args.flush_interval:
                commit_batch()
        except Exception as e:
            print(f"[RawConsumer] Error: {e}")

    commit_batch()

    consumer.close()
    print(f"[RawConsumer] Stopped cleanly. Total archived to Parquet: {total_archived}")

if __name__ == "__main__":
    main()

