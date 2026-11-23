import os
import time
import signal
import argparse
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

def main():
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Speed Layer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--checkpoint-dir", default="checkpoints/speed")
    parser.add_argument("--pg-conn", default="dbname=lambda_serving user=lambda_user password=lambda_password host=localhost port=5432")
    parser.add_argument("--cores", type=int, default=2)
    parser.add_argument("--memory", default="2g")
    parser.add_argument("--opt", action="store_true", help="Enable Batch Upsert & Trigger optimizations for E4")
    parser.add_argument("--metrics-out", default="logs/speed_metrics.csv")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.metrics_out) or ".", exist_ok=True)
    if not os.path.exists(args.metrics_out):
        with open(args.metrics_out, "w", encoding="utf-8") as f:
            f.write("timestamp_utc,batch_id,num_records,write_duration_ms,mode\n")

    spark_version = pyspark.__version__
    scala_version = "2.12" if spark_version.startswith("3.") else "2.13"
    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"

    shuffle_parts = "4" if args.opt else "200"
    trigger_interval = "200 milliseconds" if args.opt else "1 second"
    max_offsets = "5000" if args.opt else "500"

    spark = (
        SparkSession.builder
        .appName("IT4931_Lambda_Speed_Layer")
        .master(f"local[{args.cores}]")
        .config("spark.driver.memory", args.memory)
        .config("spark.jars.packages", kafka_pkg)
        .config("spark.sql.shuffle.partitions", shuffle_parts)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("seq", LongType(), False),
        StructField("event_time", StringType(), True),
        StructField("produced_at", StringType(), False),
        StructField("category", StringType(), False),
        StructField("amount_cents", LongType(), False)
    ])

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    parsed_stream = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .filter(F.col("event_id").isNotNull())
    )

    pg_conn = None

    def get_pg_conn():
        nonlocal pg_conn
        if pg_conn is None or pg_conn.closed:
            pg_conn = psycopg2.connect(args.pg_conn)
        return pg_conn

    def process_micro_batch(batch_df, batch_id):
        t0 = time.monotonic()
        rows = batch_df.dropDuplicates(["event_id"]).collect()
        if not rows:
            return

        now_utc = datetime.now(timezone.utc)
        conn = get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                if args.opt:
                    tuples = [
                        (r["event_id"], int(r["seq"]), r["category"], int(r["amount_cents"]), r["produced_at"], now_utc)
                        for r in rows
                    ]
                    execute_values(
                        cur,
                        """
                        INSERT INTO speed_events (event_id, seq, category, amount_cents, produced_at, visible_at)
                        VALUES %s
                        ON CONFLICT (event_id) DO NOTHING
                        """,
                        tuples,
                        page_size=1000
                    )
                else:
                    for r in rows:
                        cur.execute(
                            """
                            INSERT INTO speed_events (event_id, seq, category, amount_cents, produced_at, visible_at)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (event_id) DO NOTHING
                            """,
                            (r["event_id"], int(r["seq"]), r["category"], int(r["amount_cents"]), r["produced_at"], now_utc)
                        )
                        conn.commit()

        dur_ms = (time.monotonic() - t0) * 1000.0
        mode_str = "OPT_BATCH_UPSERT" if args.opt else "BASE_ROW_BY_ROW"
        print(f"[SpeedJob] Batch={batch_id} | Records={len(rows)} | WriteTime={dur_ms:.1f}ms | Mode={mode_str}")
        with open(args.metrics_out, "a", encoding="utf-8") as f:
            f.write(f"{now_utc.isoformat()},{batch_id},{len(rows)},{dur_ms:.2f},{mode_str}\n")

    query = (
        parsed_stream.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", args.checkpoint_dir)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    running = True

    def stop_query(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop_query)
    signal.signal(signal.SIGTERM, stop_query)

    print(f"[SpeedJob] Streaming started: cores={args.cores}, mem={args.memory}, opt={args.opt}, trigger='{trigger_interval}'")
    try:
        while running and query.isActive:
            query.awaitTermination(timeout=0.5)

        if query.isActive:
            print("[SpeedJob] Stopping streaming query...")
            query.stop()
    finally:
        if pg_conn is not None and not pg_conn.closed:
            pg_conn.close()
        spark.stop()
    print("[SpeedJob] Stopped cleanly.")

if __name__ == "__main__":
    main()

