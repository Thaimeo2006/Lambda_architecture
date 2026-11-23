import os
import sys
import time
import shutil
import signal
import argparse
import subprocess
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError

def reset_environment(exp_id, bootstrap_servers="localhost:9092", topic="events"):
    print("[Runner] Resetting environment (Directories, Kafka topic, PostgreSQL schema)...")
    for d in ["raw_parquet", "raw_tmp", "checkpoints", "logs"]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    for suffix in ("batch_cost.csv", "speed_cost.csv"):
        stale = os.path.join("metrics", "runs", f"{exp_id}_{suffix}")
        if os.path.exists(stale):
            os.remove(stale)

    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        try:
            admin.delete_topics([topic])
        except UnknownTopicOrPartitionError:
            pass

        deadline = time.monotonic() + 10.0
        while topic in admin.list_topics():
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Kafka topic '{topic}' was not deleted within 10s")
            time.sleep(0.25)

        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
    finally:
        admin.close()

    with open("sql/schema.sql", "rb") as f:
        subprocess.run(
            ["docker", "exec", "-i", "lambda_postgres", "psql", "-U", "lambda_user", "-d", "lambda_serving"],
            input=f.read(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
    time.sleep(1.0)

def terminate_proc(proc, name, timeout=15):
    if proc and proc.poll() is None:
        print(f"[Runner] Stopping {name} (PID={proc.pid})...")
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()

def main():
    parser = argparse.ArgumentParser(description="Single Experiment Run Orchestrator")
    parser.add_argument("--exp-id", required=True, help="Example: E1_A_run1")
    parser.add_argument("--arch", choices=["A", "B"], required=True, help="A: Batch-only, B: Lambda")
    parser.add_argument("--rate", type=float, default=20.0)
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--mode", choices=["constant", "burst"], default="constant")
    parser.add_argument("--batch-interval", type=int, default=30)
    parser.add_argument("--cores", type=int, default=2)
    parser.add_argument("--memory", default="2g")
    parser.add_argument("--opt", action="store_true", help="Enable E4 optimizations")
    args = parser.parse_args()

    py_exec = sys.executable
    reset_environment(args.exp_id)

    procs = {}
    try:
        procs["raw"] = subprocess.Popen([py_exec, "raw_consumer.py"])

        batch_cmd = [
            py_exec, "batch_job.py",
            "--interval", str(args.batch_interval),
            "--cores", str(args.cores),
            "--memory", args.memory,
            "--metrics-out", f"metrics/runs/{args.exp_id}_batch_cost.csv"
        ]
        if args.opt:
            batch_cmd.append("--opt")
        procs["batch"] = subprocess.Popen(batch_cmd)

        if args.arch == "B":
            speed_cmd = [
                py_exec, "speed_job.py",
                "--cores", str(args.cores),
                "--memory", args.memory,
                "--metrics-out", f"metrics/runs/{args.exp_id}_speed_cost.csv"
            ]
            if args.opt:
                speed_cmd.append("--opt")
            procs["speed"] = subprocess.Popen(speed_cmd)
            print("[Runner] Waiting 12s for Spark Structured Streaming to initialize...")
            time.sleep(12)
        else:
            time.sleep(3)

        procs["monitor"] = subprocess.Popen([
            py_exec, "monitor.py",
            "--exp-id", args.exp_id,
            "--duration", str(args.duration),
            "--drain-timeout", "70",
            "--out-dir", "metrics/runs"
        ])

        procs["producer"] = subprocess.Popen([
            py_exec, "producer.py",
            "--rate", str(args.rate),
            "--duration", str(args.duration),
            "--mode", args.mode
        ])

        procs["producer"].wait()
        if procs["producer"].returncode != 0:
            raise RuntimeError(f"Producer failed with exit code {procs['producer'].returncode}")

        for dep in ("raw", "speed"):
            p = procs.get(dep)
            if p and p.poll() is not None and p.returncode != 0:
                raise RuntimeError(f"{dep} exited unexpectedly with code {p.returncode}")

        print("[Runner] Producer finished. Waiting 4s for RawConsumer to flush final Parquet...")
        time.sleep(4)

        terminate_proc(procs.get("batch"), "batch", timeout=60)
        if procs["batch"].returncode not in (0, None):
            raise RuntimeError(f"BatchJob failed during shutdown with exit code {procs['batch'].returncode}")

        procs["monitor"].wait(timeout=80)
        if procs["monitor"].returncode != 0:
            raise RuntimeError(f"Monitor failed with exit code {procs['monitor'].returncode}")

    finally:
        for name in ["producer", "monitor", "speed", "batch", "raw"]:
            terminate_proc(procs.get(name), name)

    print(f"[Runner] Experiment {args.exp_id} finished successfully!\n")

if __name__ == "__main__":
    main()

