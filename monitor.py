import os
import time
import json
import argparse
from datetime import datetime, timezone
import numpy as np
import psutil
import psycopg2

def parse_iso(ts_str):
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))

def read_new_ground_truth(gt_file, gt_offset, gt_events):
    if not os.path.exists(gt_file):
        return gt_offset, []

    new_events = []
    with open(gt_file, "r", encoding="utf-8") as gf:
        gf.seek(gt_offset)
        while True:
            pos = gf.tell()
            line = gf.readline()
            if not line:
                break
            if not line.endswith("\n"):
                gf.seek(pos)
                break
            gt_offset = gf.tell()
            stripped = line.strip()
            if not stripped:
                continue
            ev = json.loads(stripped)
            eid = ev["event_id"]
            if eid not in gt_events:
                gt_events[eid] = ev
                new_events.append(ev)

    return gt_offset, new_events

def sample_active_events(new_events, elapsed, warmup, next_sample_elapsed, sample_step_sec, sampled_ids, pending_samples):
    if elapsed < warmup or not new_events or elapsed < next_sample_elapsed:
        return next_sample_elapsed

    slots = int((elapsed - next_sample_elapsed) // sample_step_sec) + 1
    next_sample_elapsed += slots * sample_step_sec

    pick_count = min(slots, len(new_events))
    stride = len(new_events) / pick_count
    for i in range(pick_count):
        ev = new_events[int(i * stride)]
        eid = ev["event_id"]
        if eid not in sampled_ids:
            sampled_ids.add(eid)
            pending_samples[eid] = (ev["seq"], ev["produced_at"], parse_iso(ev["produced_at"]))

    return next_sample_elapsed

def poll_freshness(conn, pending_samples, resolved_samples):
    if not pending_samples:
        return

    pending_list = list(pending_samples.keys())
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.event_id, s.visible_at
            FROM speed_events s
            WHERE s.event_id = ANY(%s)
            UNION ALL
            SELECT b.event_id, b.visible_at
            FROM batch_seen_events b
            WHERE b.event_id = ANY(%s)
            """,
            (pending_list, pending_list)
        )
        rows = cur.fetchall()

    earliest_vis = {}
    for eid, vis_dt in rows:
        if eid not in earliest_vis or vis_dt < earliest_vis[eid]:
            earliest_vis[eid] = vis_dt

    poll_now_utc = datetime.now(timezone.utc)
    for eid, vis_dt in earliest_vis.items():
        seq_no, prod_iso, prod_dt = pending_samples.pop(eid)
        effective_vis_dt = max(vis_dt, prod_dt)
        freshness_ms = (effective_vis_dt - prod_dt).total_seconds() * 1000.0
        poll_detected_ms = (poll_now_utc - prod_dt).total_seconds() * 1000.0
        resolved_samples[eid] = (seq_no, prod_iso, effective_vis_dt.isoformat(), freshness_ms, poll_detected_ms)

def build_summary(exp_id, poll_interval, warmup, timeseries_rows, q1_latencies_ms, q2_latencies_ms,
                  sampled_ids, resolved_samples, pending_samples, gt_events, final_q2):
    gt_total_count = len(gt_events)
    gt_total_sum = sum(int(ev["amount_cents"]) for ev in gt_events.values())

    srv_count = int(final_q2[0]) if final_q2 else 0
    srv_sum = int(final_q2[1]) if final_q2 else 0
    final_h = int(final_q2[2]) if final_q2 else 0

    active_rows = [r for r in timeseries_rows if r["phase"] == "active"] or timeseries_rows
    fresh_vals = [v[3] for v in resolved_samples.values()]
    p50_fresh = float(np.percentile(fresh_vals, 50)) if fresh_vals else -1.0
    p95_fresh = float(np.percentile(fresh_vals, 95)) if fresh_vals else -1.0

    return {
        "exp_id": exp_id,
        "polling_interval_sec": poll_interval,
        "warmup_sec": warmup,
        "sampled_events_total": len(sampled_ids),
        "sampled_events_visible": len(resolved_samples),
        "sampled_events_unseen": len(pending_samples),
        "freshness_p50_ms": round(p50_fresh, 2),
        "freshness_p95_ms": round(p95_fresh, 2),
        "avg_throughput_eps": round(float(np.mean([r["throughput_eps"] for r in active_rows])), 2) if active_rows else 0.0,
        "max_throughput_eps": round(float(np.max([r["throughput_eps"] for r in active_rows])), 2) if active_rows else 0.0,
        "avg_backlog": round(float(np.mean([r["backlog"] for r in active_rows])), 2) if active_rows else 0.0,
        "max_backlog": int(np.max([r["backlog"] for r in active_rows])) if active_rows else 0,
        "cpu_avg_percent": round(float(np.mean([r["cpu_percent"] for r in active_rows])), 2) if active_rows else 0.0,
        "cpu_max_percent": round(float(np.max([r["cpu_percent"] for r in active_rows])), 2) if active_rows else 0.0,
        "ram_avg_mb": round(float(np.mean([r["ram_used_mb"] for r in active_rows])), 1) if active_rows else 0.0,
        "ram_max_mb": round(float(np.max([r["ram_used_mb"] for r in active_rows])), 1) if active_rows else 0.0,
        "q1_p95_latency_ms": round(float(np.percentile(q1_latencies_ms, 95)), 2) if q1_latencies_ms else 0.0,
        "q2_p95_latency_ms": round(float(np.percentile(q2_latencies_ms, 95)), 2) if q2_latencies_ms else 0.0,
        "ground_truth_count": gt_total_count,
        "serving_count": srv_count,
        "count_diff": srv_count - gt_total_count,
        "ground_truth_sum_cents": gt_total_sum,
        "serving_sum_cents": srv_sum,
        "sum_diff_cents": srv_sum - gt_total_sum,
        "final_batch_h": final_h,
        "is_correct": (srv_count == gt_total_count) and (srv_sum == gt_total_sum)
    }

def main():
    parser = argparse.ArgumentParser(description="End-to-End Performance & Correctness Monitor")
    parser.add_argument("--exp-id", required=True, help="Example: E1_A_run1, E2_B_run1")
    parser.add_argument("--pg-conn", default="dbname=lambda_serving user=lambda_user password=lambda_password host=localhost port=5432")
    parser.add_argument("--gt-file", default="logs/produced_ground_truth.jsonl")
    parser.add_argument("--duration", type=int, default=120, help="Event emission duration (seconds)")
    parser.add_argument("--drain-timeout", type=int, default=40, help="Max wait time after emission stops for system to drain")
    parser.add_argument("--warmup", type=int, default=20, help="Initial warm-up duration (seconds)")
    parser.add_argument("--poll-interval", type=float, default=0.25, help="Polling cycle to measure freshness (seconds)")
    parser.add_argument("--sample-target", type=int, default=80, help="Target sample events (>= 50)")
    parser.add_argument("--out-dir", default="metrics/runs")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    ts_csv_path = os.path.join(args.out_dir, f"{args.exp_id}_timeseries.csv")
    fresh_csv_path = os.path.join(args.out_dir, f"{args.exp_id}_freshness_samples.csv")
    summary_json_path = os.path.join(args.out_dir, f"{args.exp_id}_summary.json")

    conn = psycopg2.connect(args.pg_conn)
    conn.autocommit = True

    start_mono = time.monotonic()
    last_ts_record = start_mono
    prev_visible_count = 0
    prev_ts_mono = start_mono

    gt_offset = 0
    gt_events = {}
    sampled_ids = set()
    pending_samples = {}
    resolved_samples = {}

    active_window = max(1, args.duration - args.warmup)
    sample_step_sec = active_window / max(1, args.sample_target)
    next_sample_elapsed = float(args.warmup)

    psutil.cpu_percent(interval=None)
    timeseries_rows = []
    q1_latencies_ms = []
    q2_latencies_ms = []

    print(f"[Monitor] Started tracking {args.exp_id} (poll={args.poll_interval}s, warmup={args.warmup}s)")

    with open(ts_csv_path, "w", encoding="utf-8") as ts_f:
        ts_f.write("elapsed_sec,phase,produced_count,visible_count,backlog,throughput_eps,batch_h,q1_latency_ms,q2_latency_ms,cpu_percent,ram_used_mb,ram_percent\n")

        while True:
            now_mono = time.monotonic()
            elapsed = now_mono - start_mono
            phase = "warmup" if elapsed < args.warmup else ("active" if elapsed <= args.duration else "draining")

            gt_done = os.path.exists(f"{args.gt_file}.done")
            gt_offset, new_events = read_new_ground_truth(args.gt_file, gt_offset, gt_events)
            total_produced = len(gt_events)

            next_sample_elapsed = sample_active_events(
                new_events, elapsed, args.warmup, next_sample_elapsed, sample_step_sec, sampled_ids, pending_samples
            )
            poll_freshness(conn, pending_samples, resolved_samples)

            if (now_mono - last_ts_record) >= 1.0:
                dt = now_mono - prev_ts_mono
                with conn.cursor() as cur:
                    t_q1 = time.monotonic()
                    cur.execute("SELECT category, total_count, total_sum_cents FROM v_q1_category;")
                    _ = cur.fetchall()
                    q1_ms = (time.monotonic() - t_q1) * 1000.0

                    t_q2 = time.monotonic()
                    cur.execute("SELECT system_total_count, system_total_sum_cents, current_batch_h FROM v_q2_total;")
                    q2_row = cur.fetchone()
                    q2_ms = (time.monotonic() - t_q2) * 1000.0

                vis_count = int(q2_row[0]) if q2_row else 0
                batch_h = int(q2_row[2]) if q2_row else 0
                backlog = max(0, total_produced - vis_count)
                tput = max(0.0, (vis_count - prev_visible_count) / dt) if dt > 0 else 0.0

                cpu_pct = psutil.cpu_percent(interval=None)
                vm = psutil.virtual_memory()
                ram_mb = vm.used / (1024.0 * 1024.0)
                ram_pct = vm.percent

                row_data = {
                    "elapsed_sec": round(elapsed, 2),
                    "phase": phase,
                    "produced_count": total_produced,
                    "visible_count": vis_count,
                    "backlog": backlog,
                    "throughput_eps": round(tput, 2),
                    "batch_h": batch_h,
                    "q1_latency_ms": round(q1_ms, 2),
                    "q2_latency_ms": round(q2_ms, 2),
                    "cpu_percent": round(cpu_pct, 1),
                    "ram_used_mb": round(ram_mb, 1),
                    "ram_percent": round(ram_pct, 1)
                }
                timeseries_rows.append(row_data)
                if phase == "active":
                    q1_latencies_ms.append(q1_ms)
                    q2_latencies_ms.append(q2_ms)

                ts_f.write(
                    f"{row_data['elapsed_sec']},{phase},{total_produced},{vis_count},{backlog},"
                    f"{row_data['throughput_eps']},{batch_h},{row_data['q1_latency_ms']},{row_data['q2_latency_ms']},"
                    f"{row_data['cpu_percent']},{row_data['ram_used_mb']},{row_data['ram_percent']}\n"
                )
                ts_f.flush()

                prev_visible_count = vis_count
                prev_ts_mono = now_mono
                last_ts_record = now_mono

                if elapsed > args.duration:
                    if (gt_done and total_produced > 0 and vis_count >= total_produced and not pending_samples) or (elapsed >= args.duration + args.drain_timeout):
                        break

            time.sleep(args.poll_interval)

    with open(fresh_csv_path, "w", encoding="utf-8") as ff:
        ff.write("event_id,seq,produced_at,visible_at,freshness_ms,poll_detected_ms,status\n")
        for eid, (seq_no, prod_iso, vis_iso, f_ms, p_ms) in sorted(resolved_samples.items(), key=lambda x: x[1][0]):
            ff.write(f"{eid},{seq_no},{prod_iso},{vis_iso},{f_ms:.2f},{p_ms:.2f},VISIBLE\n")
        for eid, (seq_no, prod_iso, _) in sorted(pending_samples.items(), key=lambda x: x[1][0]):
            ff.write(f"{eid},{seq_no},{prod_iso},NA,-1,-1,UNSEEN\n")

    with conn.cursor() as cur:
        cur.execute("SELECT system_total_count, system_total_sum_cents, current_batch_h FROM v_q2_total;")
        final_q2 = cur.fetchone()
    conn.close()

    summary = build_summary(
        args.exp_id, args.poll_interval, args.warmup,
        timeseries_rows, q1_latencies_ms, q2_latencies_ms,
        sampled_ids, resolved_samples, pending_samples, gt_events, final_q2
    )

    with open(summary_json_path, "w", encoding="utf-8") as sf:
        json.dump(summary, sf, indent=2, ensure_ascii=False)

    print(f"[Monitor] Completed {args.exp_id}: p50={summary['freshness_p50_ms']}ms, p95={summary['freshness_p95_ms']}ms, Correct={summary['is_correct']} (Diff Count={summary['count_diff']}, Diff Sum={summary['sum_diff_cents']})")

if __name__ == "__main__":
    main()
