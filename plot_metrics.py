import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams["figure.dpi"] = 150
plt.rcParams["font.size"] = 10

def get_group_metric(df_agg, group_name, col_name, default=0.0):
    sub = df_agg[df_agg["group"] == group_name]
    if sub.empty:
        return default
    return float(sub[col_name].iloc[0])

def has_groups(df_agg, required_groups, chart_name="chart"):
    present = set(df_agg["group"])
    missing = [g for g in required_groups if g not in present]
    if missing:
        print(f"[Plotter] Skipping {chart_name} (missing groups: {', '.join(missing)})")
        return False
    return True

def draw_bar_panel(ax, labels, values, colors, ylabel, title, fmt):
    bars = ax.bar(labels, values, color=colors, width=0.5)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    for b in bars:
        ax.annotate(fmt.format(b.get_height()), xy=(b.get_x() + b.get_width() / 2, b.get_height()),
                    xytext=(0, 3), textcoords="offset points", ha="center", fontweight="bold")
    ax.set_ylim(0, max(max(values), 10) * 1.25)

def plot_chart1(df_agg, out_dir):
    if not has_groups(df_agg, ["E1_A", "E1_B", "E2_A", "E2_B"], "chart1"):
        return
    labels = ["E1: Low Load (20 eps)", "E2: High Load (100 eps)"]
    batch_vals = [
        get_group_metric(df_agg, "E1_A", "freshness_p95_ms") / 1000.0,
        get_group_metric(df_agg, "E2_A", "freshness_p95_ms") / 1000.0,
    ]
    lambda_vals = [
        get_group_metric(df_agg, "E1_B", "freshness_p95_ms") / 1000.0,
        get_group_metric(df_agg, "E2_B", "freshness_p95_ms") / 1000.0,
    ]

    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    rects1 = ax.bar(x - width/2, batch_vals, width, label="A: Batch-only (30s cycle)", color="#4C72B0")
    rects2 = ax.bar(x + width/2, lambda_vals, width, label="B: Lambda (Batch + Speed)", color="#DD8452")

    ax.set_ylabel("p95 End-to-End Freshness (seconds)")
    ax.set_title("Chart 1: p95 Freshness across Workloads (Batch-only vs. Lambda)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    for rect in rects1:
        h = rect.get_height()
        ax.annotate(f"{h:.2f} s", xy=(rect.get_x() + rect.get_width()/2, h),
                    xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9, fontweight="bold")
    for rect in rects2:
        h = rect.get_height()
        ax.annotate(f"{h:.2f} s\n({h*1000:.0f} ms)", xy=(rect.get_x() + rect.get_width()/2, h),
                    xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=8.5, fontweight="bold")

    ax.set_ylim(0, max(max(batch_vals, default=1.0), max(lambda_vals, default=1.0)) * 1.25 + 1)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "chart1_freshness_comparison.png"), bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)

def plot_chart2(runs_dir, charts_dir):
    candidates = sorted(glob.glob(os.path.join(runs_dir, "E3_B*_timeseries.csv")))
    title_suffix = "E3 Burst Workload (20 -> 150 -> 20 eps)"
    if not candidates:
        candidates = sorted(glob.glob(os.path.join(runs_dir, "E2_B*_timeseries.csv")))
        title_suffix = "E2 High Load (100 eps)"
    if not candidates:
        return

    df_ts = pd.read_csv(candidates[0])
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9.0, 5.5), sharex=True)

    ax1.plot(df_ts["elapsed_sec"], df_ts["throughput_eps"], color="#2B5C8F", linewidth=1.8, label="Serving Throughput (eps)")
    if "E3" in candidates[0]:
        ax1.axvspan(40, 80, color="#F8D7DA", alpha=0.45, label="Burst Phase (150 eps)")
    ax1.set_ylabel("Throughput (events/sec)")
    ax1.set_title(f"Chart 2: Throughput & Backlog over Time — {title_suffix}", fontsize=11)
    ax1.legend(loc="upper right")
    ax1.grid(True, linestyle="--", alpha=0.5)

    ax2.plot(df_ts["elapsed_sec"], df_ts["backlog"], color="#C44E52", linewidth=1.8, label="Unserved Backlog (events)")
    ax2.fill_between(df_ts["elapsed_sec"], df_ts["backlog"], color="#C44E52", alpha=0.2)
    if "E3" in candidates[0]:
        ax2.axvspan(40, 80, color="#F8D7DA", alpha=0.45, label="Overload / Accumulation Phase")
        ax2.axvspan(80, 120, color="#D4EDDA", alpha=0.35, label="Recovery / Drain Phase (20 eps)")
    ax2.set_xlabel("Elapsed Time (seconds)")
    ax2.set_ylabel("Backlog (events)")
    ax2.legend(loc="upper right")
    ax2.grid(True, linestyle="--", alpha=0.5)

    fig.tight_layout()
    fig.savefig(os.path.join(charts_dir, "chart2_throughput_backlog_timeseries.png"), bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)

def plot_chart3(df_agg, out_dir):
    if not has_groups(df_agg, ["E2_B", "E4_B_opt"], "chart3"):
        return
    e2_fresh = get_group_metric(df_agg, "E2_B", "freshness_p95_ms")
    e4_fresh = get_group_metric(df_agg, "E4_B_opt", "freshness_p95_ms")
    e2_backlog = get_group_metric(df_agg, "E2_B", "max_backlog")
    e4_backlog = get_group_metric(df_agg, "E4_B_opt", "max_backlog")
    e2_cpu = get_group_metric(df_agg, "E2_B", "cpu_avg_percent")
    e4_cpu = get_group_metric(df_agg, "E4_B_opt", "cpu_avg_percent")

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(10.5, 4.2))
    configs = ["E2 (Baseline)", "E4 (Optimized)"]
    colors = ["#C44E52", "#55A868"]

    draw_bar_panel(ax1, configs, [e2_fresh, e4_fresh], colors,
                   "Milliseconds (ms)", "p95 Freshness (Lower is better)", "{:.0f} ms")
    draw_bar_panel(ax2, configs, [e2_backlog, e4_backlog], colors,
                   "Events", "Max Backlog (Lower is better)", "{:.0f}")
    draw_bar_panel(ax3, configs, [e2_cpu, e4_cpu], ["#4C72B0", "#8172B3"],
                   "CPU Utilization (%)", "Avg CPU Usage (%)", "{:.1f}%")

    fig.suptitle("Chart 3: Before vs. After Bottleneck Optimization (E2 Baseline vs. E4 Batch Upsert)", fontweight="bold")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "chart3_e2_vs_e4_optimization.png"), bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)

def plot_chart4(df_agg, out_dir):
    present = set(df_agg["group"])
    if not (("E5_C1" in present or "E2_B" in present) and "E5_C2" in present):
        print("[Plotter] Skipping chart4 (missing groups: E5_C1/E2_B or E5_C2)")
        return
    c1_tput = get_group_metric(df_agg, "E5_C1", "avg_throughput_eps", get_group_metric(df_agg, "E2_B", "avg_throughput_eps"))
    c2_tput = get_group_metric(df_agg, "E5_C2", "avg_throughput_eps")
    c1_fresh = get_group_metric(df_agg, "E5_C1", "freshness_p95_ms", get_group_metric(df_agg, "E2_B", "freshness_p95_ms"))
    c2_fresh = get_group_metric(df_agg, "E5_C2", "freshness_p95_ms")
    c1_cpu = get_group_metric(df_agg, "E5_C1", "cpu_avg_percent", get_group_metric(df_agg, "E2_B", "cpu_avg_percent"))
    c2_cpu = get_group_metric(df_agg, "E5_C2", "cpu_avg_percent")

    speedup_obs = (c2_tput / c1_tput) if c1_tput > 0 else 0.0
    eff_obs = (speedup_obs / (4.0 / 2.0)) * 100.0

    configs = ["C1\n(2 Cores / 2 GB)", "C2\n(4 Cores / 4 GB)"]
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(10.5, 4.5))

    draw_bar_panel(ax1, configs, [c1_tput, c2_tput], ["#4C72B0", "#2B5C8F"],
                   "Throughput (events/sec)", f"Throughput\n(Obs S={speedup_obs:.2f}x, E={eff_obs:.1f}%)", "{:.1f} eps")
    draw_bar_panel(ax2, configs, [c1_fresh, c2_fresh], ["#DD8452", "#55A868"],
                   "p95 Freshness (ms)", "p95 Freshness (ms)", "{:.0f} ms")
    draw_bar_panel(ax3, configs, [c1_cpu, c2_cpu], ["#8172B3", "#64B5CD"],
                   "Avg CPU (%)", "System CPU Utilization (%)", "{:.1f}%")

    fig.suptitle("Chart 4: E5 Scale-up Comparison (C1: 2 Cores/2GB vs. C2: 4 Cores/4GB on Workload E2)", fontweight="bold")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "chart4_e5_scaleup.png"), bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)

def main():
    metrics_dir = "metrics"
    runs_dir = os.path.join(metrics_dir, "runs")
    if not os.path.isdir(runs_dir):
        runs_dir = metrics_dir
    charts_dir = os.path.join(metrics_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)

    agg_path = os.path.join(metrics_dir, "summary_aggregated.csv")
    if not os.path.exists(agg_path):
        print(f"[Plotter] Cannot find {agg_path}. Run run_all_experiments.py first.")
        return

    df_agg = pd.read_csv(agg_path)
    plot_chart1(df_agg, charts_dir)
    plot_chart2(runs_dir, charts_dir)
    plot_chart3(df_agg, charts_dir)
    plot_chart4(df_agg, charts_dir)
    print(f"[Plotter] Successfully generated charts in {charts_dir}/:")
    for f in sorted(glob.glob(os.path.join(charts_dir, "chart*.png"))):
        print(f"  - {f}")

if __name__ == "__main__":
    main()

