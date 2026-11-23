import os
import sys
import json
import argparse
import subprocess
import pandas as pd

EXPERIMENTS = [
    {"exp_id": "E1_A_run1", "group": "E1_A", "arch": "A", "rate": 20, "mode": "constant", "cores": 2, "mem": "2g", "opt": False},
    {"exp_id": "E1_A_run2", "group": "E1_A", "arch": "A", "rate": 20, "mode": "constant", "cores": 2, "mem": "2g", "opt": False},
    {"exp_id": "E1_B_run1", "group": "E1_B", "arch": "B", "rate": 20, "mode": "constant", "cores": 2, "mem": "2g", "opt": False},
    {"exp_id": "E1_B_run2", "group": "E1_B", "arch": "B", "rate": 20, "mode": "constant", "cores": 2, "mem": "2g", "opt": False},

    {"exp_id": "E2_A_run1", "group": "E2_A", "arch": "A", "rate": 100, "mode": "constant", "cores": 2, "mem": "2g", "opt": False},
    {"exp_id": "E2_A_run2", "group": "E2_A", "arch": "A", "rate": 100, "mode": "constant", "cores": 2, "mem": "2g", "opt": False},
    {"exp_id": "E2_B_run1", "group": "E2_B", "arch": "B", "rate": 100, "mode": "constant", "cores": 2, "mem": "2g", "opt": False},
    {"exp_id": "E2_B_run2", "group": "E2_B", "arch": "B", "rate": 100, "mode": "constant", "cores": 2, "mem": "2g", "opt": False},

    {"exp_id": "E3_B_burst_run1", "group": "E3_B", "arch": "B", "rate": 20, "mode": "burst", "cores": 2, "mem": "2g", "opt": False},
    {"exp_id": "E3_B_burst_run2", "group": "E3_B", "arch": "B", "rate": 20, "mode": "burst", "cores": 2, "mem": "2g", "opt": False},

    {"exp_id": "E4_B_opt_run1", "group": "E4_B_opt", "arch": "B", "rate": 100, "mode": "constant", "cores": 2, "mem": "2g", "opt": True},
    {"exp_id": "E4_B_opt_run2", "group": "E4_B_opt", "arch": "B", "rate": 100, "mode": "constant", "cores": 2, "mem": "2g", "opt": True},

    {"exp_id": "E5_C1_2c2g_run1", "group": "E5_C1", "arch": "B", "rate": 100, "mode": "constant", "cores": 2, "mem": "2g", "opt": False},
    {"exp_id": "E5_C1_2c2g_run2", "group": "E5_C1", "arch": "B", "rate": 100, "mode": "constant", "cores": 2, "mem": "2g", "opt": False},
    {"exp_id": "E5_C2_4c4g_run1", "group": "E5_C2", "arch": "B", "rate": 100, "mode": "constant", "cores": 4, "mem": "4g", "opt": False},
    {"exp_id": "E5_C2_4c4g_run2", "group": "E5_C2", "arch": "B", "rate": 100, "mode": "constant", "cores": 4, "mem": "4g", "opt": False},
]

def aggregate_summaries(metrics_dir="metrics"):
    runs_dir = os.path.join(metrics_dir, "runs")
    if not os.path.isdir(runs_dir):
        runs_dir = metrics_dir

    rows = []
    for cfg in EXPERIMENTS:
        exp_id = cfg["exp_id"]
        summary_path = os.path.join(runs_dir, f"{exp_id}_summary.json")
        if not os.path.exists(summary_path):
            continue
        with open(summary_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        batch_cost_path = os.path.join(runs_dir, f"{exp_id}_batch_cost.csv")
        avg_batch_ms = 0.0
        if os.path.exists(batch_cost_path):
            bdf = pd.read_csv(batch_cost_path)
            if not bdf.empty:
                avg_batch_ms = float(bdf["batch_duration_ms"].mean())

        row = {
            "group": cfg["group"],
            "arch": cfg["arch"],
            "rate": cfg["rate"],
            "mode": cfg["mode"],
            "cores": cfg["cores"],
            "mem": cfg["mem"],
            "opt": cfg["opt"],
            "avg_batch_duration_ms": round(avg_batch_ms, 2),
            **data
        }
        rows.append(row)

    if not rows:
        print("[Aggregator] No summary files found to aggregate.")
        return

    df_all = pd.DataFrame(rows)
    all_csv = os.path.join(metrics_dir, "summary_all_runs.csv")
    df_all.to_csv(all_csv, index=False)
    print(f"[Aggregator] Saved raw run summaries to {all_csv}")

    mean_cols = [
        "freshness_p50_ms", "freshness_p95_ms",
        "avg_throughput_eps", "max_throughput_eps",
        "avg_backlog", "max_backlog",
        "cpu_avg_percent", "cpu_max_percent",
        "ram_avg_mb", "ram_max_mb",
        "q1_p95_latency_ms", "q2_p95_latency_ms",
        "avg_batch_duration_ms",
    ]
    agg_spec = {col: "mean" for col in mean_cols}
    agg_spec["count_diff"] = lambda s: s.abs().max()
    agg_spec["sum_diff_cents"] = lambda s: s.abs().max()
    agg_spec["is_correct"] = "all"

    df_agg = df_all.groupby(["group", "arch", "rate", "mode", "cores", "mem", "opt"], as_index=False).agg(agg_spec)
    df_agg = df_agg.round(2)
    agg_csv = os.path.join(metrics_dir, "summary_aggregated.csv")
    df_agg.to_csv(agg_csv, index=False)
    print(f"[Aggregator] Saved aggregated table to {agg_csv}")

def is_valid_completed_run(summary_file):
    if not os.path.exists(summary_file):
        return False
    try:
        with open(summary_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("is_correct") is True and data.get("sampled_events_unseen", 1) == 0
    except (OSError, ValueError):
        return False

def main():
    parser = argparse.ArgumentParser(description="Run full E1-E5 benchmark suite")
    parser.add_argument("--duration", type=int, default=120, help="Duration of each run (default 120s)")
    parser.add_argument("--only", help="Run only a specific group (e.g., E1, E2, E3, E4, E5)")
    parser.add_argument("--skip-existing", action="store_true", help="Skip runs that already have a summary.json file")
    parser.add_argument("--aggregate-only", action="store_true", help="Only aggregate CSV from completed runs")
    args = parser.parse_args()

    os.makedirs(os.path.join("metrics", "runs"), exist_ok=True)
    if args.aggregate_only:
        aggregate_summaries("metrics")
        return

    py_exec = sys.executable
    for cfg in EXPERIMENTS:
        if args.only and not cfg["exp_id"].startswith(args.only):
            continue

        summary_file = os.path.join("metrics", "runs", f"{cfg['exp_id']}_summary.json")
        if args.skip_existing and is_valid_completed_run(summary_file):
            print(f"[Suite] Skipping {cfg['exp_id']} (already exists)")
            continue

        print("=" * 72)
        print(f"[Suite] Starting {cfg['exp_id']} | Arch={cfg['arch']} | Rate={cfg['rate']} | Cores={cfg['cores']} | Mem={cfg['mem']} | Opt={cfg['opt']}")
        print("=" * 72)

        cmd = [
            py_exec, "run_experiment.py",
            "--exp-id", cfg["exp_id"],
            "--arch", cfg["arch"],
            "--rate", str(cfg["rate"]),
            "--duration", str(args.duration),
            "--mode", cfg["mode"],
            "--cores", str(cfg["cores"]),
            "--memory", cfg["mem"]
        ]
        if cfg["opt"]:
            cmd.append("--opt")

        subprocess.run(cmd, check=True)

    aggregate_summaries("metrics")

if __name__ == "__main__":
    main()

