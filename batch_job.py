import os
import re
import sys
import time
import signal
import argparse
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

running = True

def handle_sigterm(signum, frame):
    global running
    running = False

signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)

FILE_PATTERN = re.compile(r"^part_(\d+)_(\d+)_\d+\.parquet$")

def find_contiguous_watermark(raw_dir):
    if not os.path.exists(raw_dir):
        return 0, []

    intervals = []
    for fname in os.listdir(raw_dir):
        match = FILE_PATTERN.match(fname)
        if match:
            min_s = int(match.group(1))
            max_s = int(match.group(2))
            intervals.append((min_s, max_s, os.path.join(raw_dir, fname)))

    if not intervals:
        return 0, []

    intervals.sort(key=lambda x: (x[0], x[1]))
    h_seq = 0
    valid_files = []

    for min_s, max_s, fpath in intervals:
        if min_s <= h_seq + 1:
            if max_s > h_seq:
                h_seq = max_s
            valid_files.append(fpath)
        else:
            break

    return h_seq, valid_files

def run_single_batch(spark, raw_dir, pg_conn_str, last_h, metrics_log_path):
    t0 = time.monotonic()
    new_h, valid_files = find_contiguous_watermark(raw_dir)

    if new_h <= last_h or not valid_files:
        return last_h

    df = spark.read.parquet(*valid_files)
    df_valid = df.filter(F.col("seq") <= new_h).dropDuplicates(["event_id"]).cache()
    try:
        agg_rows = (
            df_valid.groupBy("category")
            .agg(
                F.count("*").alias("cnt"),
                F.coalesce(F.sum("amount_cents"), F.lit(0)).alias("sum_cents")
            )
            .collect()
        )

        new_events = (
            df_valid.filter(F.col("seq") > last_h)
            .select("event_id", "seq", "produced_at")
            .collect()
        )
    finally:
        df_valid.unpersist()

    now_utc = datetime.now(timezone.utc)
    conn = psycopg2.connect(pg_conn_str)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM batch_views;")
                if agg_rows:
                    view_tuples = [
                        (r["category"], int(r["cnt"]), int(r["sum_cents"]), int(new_h), now_utc)
                        for r in agg_rows
                    ]
                    execute_values(
                        cur,
                        """
                        INSERT INTO batch_views (category, cnt, sum_cents, h_seq, updated_at)
                        VALUES %s
                        """,
                        view_tuples
                    )

                cur.execute(
                    "UPDATE watermark_meta SET h_seq = %s, updated_at = %s WHERE id = 1;",
                    (int(new_h), now_utc)
                )

                if new_events:
                    seen_tuples = [
                        (str(r["event_id"]), int(r["seq"]), str(r["produced_at"]), now_utc)
                        for r in new_events
                    ]
                    execute_values(
                        cur,
                        """
                        INSERT INTO batch_seen_events (event_id, seq, produced_at, visible_at)
                        VALUES %s
                        ON CONFLICT (event_id) DO NOTHING
                        """,
                        seen_tuples,
                        page_size=1000
                    )
    finally:
        conn.close()

    duration_ms = (time.monotonic() - t0) * 1000.0
    print(f"[BatchJob] Published snapshot H={new_h} (prev={last_h}) | Categories={len(agg_rows)} | Duration={duration_ms:.1f}ms")

    with open(metrics_log_path, "a", encoding="utf-8") as f:
        f.write(f"{now_utc.isoformat()},{last_h},{new_h},{len(valid_files)},{duration_ms:.2f}\n")

    return new_h

def main():
    parser = argparse.ArgumentParser(description="Spark Batch Layer Job")
    parser.add_argument("--raw-dir", default="raw_parquet")
    parser.add_argument("--pg-conn", default="dbname=lambda_serving user=lambda_user password=lambda_password host=localhost port=5432")
    parser.add_argument("--interval", type=int, default=30, help="Periodic run interval (seconds)")
    parser.add_argument("--cores", type=int, default=2)
    parser.add_argument("--memory", default="1g")
    parser.add_argument("--opt", action="store_true", help="Enable Spark configuration optimizations for E4")
    parser.add_argument("--metrics-out", default="logs/batch_metrics.csv")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.metrics_out) or ".", exist_ok=True)
    if not os.path.exists(args.metrics_out):
        with open(args.metrics_out, "w", encoding="utf-8") as f:
            f.write("timestamp_utc,prev_h,new_h,num_parquet_files,batch_duration_ms\n")

    shuffle_parts = "4" if args.opt else "200"
    spark = (
        SparkSession.builder
        .appName("IT4931_Lambda_Batch_Layer")
        .master(f"local[{args.cores}]")
        .config("spark.driver.memory", args.memory)
        .config("spark.sql.shuffle.partitions", shuffle_parts)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    last_h = 0
    print(f"[BatchJob] Daemon started: interval={args.interval}s, cores={args.cores}, mem={args.memory}, opt={args.opt}")
    next_run = time.monotonic() + args.interval

    while running:
        now = time.monotonic()
        if now >= next_run:
            try:
                last_h = run_single_batch(spark, args.raw_dir, args.pg_conn, last_h, args.metrics_out)
            except Exception as e:
                print(f"[BatchJob] Error during batch execution: {e}")
            next_run = time.monotonic() + args.interval
        time.sleep(0.5)

    final_ok = True
    try:
        run_single_batch(spark, args.raw_dir, args.pg_conn, last_h, args.metrics_out)
    except Exception as e:
        final_ok = False
        print(f"[BatchJob] Final flush failed: {e}")

    spark.stop()
    if not final_ok:
        sys.exit(1)
    print("[BatchJob] Stopped cleanly.")

if __name__ == "__main__":
    main()

