import os
import json
import tempfile
import unittest
import pandas as pd
import psycopg2

from batch_job import find_contiguous_watermark
from monitor import read_new_ground_truth, sample_active_events
from raw_consumer import flush_buffer
from run_all_experiments import aggregate_summaries, is_valid_completed_run


class TestWatermark(unittest.TestCase):
    def test_empty_and_missing_dir(self):
        self.assertEqual(find_contiguous_watermark("/tmp/nonexistent_lambda_dir_xyz"), (0, []))
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(find_contiguous_watermark(d), (0, []))

    def test_contiguous_gap_overlap_and_ignored_files(self):
        with tempfile.TemporaryDirectory() as d:
            names = [
                "part_00000001_00000010_1000.parquet",
                "part_00000005_00000020_1001.parquet",  # overlaps
                "part_00000021_00000030_1002.parquet",  # contiguous
                "part_00000032_00000040_1003.parquet",  # gap at 31
                "ignore_me.tmp",
            ]
            for name in names:
                open(os.path.join(d, name), "w").close()

            h, files = find_contiguous_watermark(d)
            self.assertEqual(h, 30)
            self.assertEqual(len(files), 3)

    def test_raw_consumer_flush_matches_watermark_parser(self):
        with tempfile.TemporaryDirectory() as d:
            tmp_dir = os.path.join(d, "tmp")
            out_dir = os.path.join(d, "out")
            os.makedirs(tmp_dir)
            os.makedirs(out_dir)

            batch1 = [
                {"event_id": "e1", "seq": 1, "event_time": "2024-01-01T00:00:00Z", "produced_at": "2026-01-01T00:00:00+00:00", "category": "ZONE_1", "amount_cents": 100},
                {"event_id": "e2", "seq": 2, "event_time": "2024-01-01T00:00:01Z", "produced_at": "2026-01-01T00:00:01+00:00", "category": "ZONE_2", "amount_cents": 200},
            ]
            flush_buffer(batch1, tmp_dir, out_dir)
            h, files = find_contiguous_watermark(out_dir)
            self.assertEqual(h, 2)
            self.assertEqual(len(files), 1)
            self.assertEqual(os.listdir(tmp_dir), [])


class TestMonitorHelpers(unittest.TestCase):
    def test_partial_json_line_is_not_lost_or_crashed(self):
        with tempfile.TemporaryDirectory() as d:
            gt_path = os.path.join(d, "gt.jsonl")
            ev1 = {"event_id": "e1", "seq": 1, "produced_at": "2026-01-01T00:00:00+00:00", "amount_cents": 100}
            ev2 = {"event_id": "e2", "seq": 2, "produced_at": "2026-01-01T00:00:01+00:00", "amount_cents": 200}
            line2 = json.dumps(ev2)

            with open(gt_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(ev1) + "\n")
                f.write(line2[:15])  # truncated mid-line

            gt_events = {}
            offset, added = read_new_ground_truth(gt_path, 0, gt_events)
            self.assertEqual([e["event_id"] for e in added], ["e1"])

            with open(gt_path, "a", encoding="utf-8") as f:
                f.write(line2[15:] + "\n")

            offset, added2 = read_new_ground_truth(gt_path, offset, gt_events)
            self.assertEqual([e["event_id"] for e in added2], ["e2"])
            self.assertEqual(len(gt_events), 2)

    def test_sampling_skips_warmup_and_spreads_across_active_window(self):
        sampled = set()
        pending = {}
        warmup_evs = [
            {"event_id": f"w{i}", "seq": i, "produced_at": "2026-01-01T00:00:00+00:00"}
            for i in range(1, 50)
        ]
        next_t = sample_active_events(warmup_evs, elapsed=19.9, warmup=20, next_sample_elapsed=20.0,
                                      sample_step_sec=1.25, sampled_ids=sampled, pending_samples=pending)
        self.assertEqual(len(sampled), 0)
        self.assertEqual(next_t, 20.0)

        active_evs = [
            {"event_id": f"a{i}", "seq": 50 + i, "produced_at": "2026-01-01T00:00:20+00:00"}
            for i in range(10)
        ]
        next_t = sample_active_events(active_evs, elapsed=20.1, warmup=20, next_sample_elapsed=next_t,
                                      sample_step_sec=1.25, sampled_ids=sampled, pending_samples=pending)
        self.assertEqual(len(sampled), 1)
        self.assertAlmostEqual(next_t, 21.25)


class TestServingViews(unittest.TestCase):
    def test_views_deduplicate_at_watermark(self):
        conn_str = "dbname=lambda_serving user=lambda_user password=lambda_password host=localhost port=5432"
        try:
            conn = psycopg2.connect(conn_str, connect_timeout=2)
        except psycopg2.OperationalError:
            self.skipTest("PostgreSQL container not running")

        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM batch_views; DELETE FROM speed_events;")
                cur.execute("UPDATE watermark_meta SET h_seq = 2 WHERE id = 1;")
                cur.execute("INSERT INTO batch_views (category, cnt, sum_cents, h_seq) VALUES ('ZONE_1', 2, 300, 2);")
                cur.execute("""
                    INSERT INTO speed_events (event_id, seq, category, amount_cents, produced_at) VALUES
                    ('e1', 1, 'ZONE_1', 100, NOW()),
                    ('e2', 2, 'ZONE_1', 200, NOW()),
                    ('e3', 3, 'ZONE_1', 400, NOW()),
                    ('e4', 4, 'ZONE_2', 500, NOW());
                """)
                cur.execute("SELECT system_total_count, system_total_sum_cents, current_batch_h FROM v_q2_total;")
                total_cnt, total_sum, h_seq = cur.fetchone()
                self.assertEqual((total_cnt, total_sum, h_seq), (4, 1200, 2))
        finally:
            conn.rollback()
            conn.close()


class TestSuiteAggregation(unittest.TestCase):
    def _write_summary(self, directory, exp_id, count_diff, sum_diff_cents, is_correct):
        summary = {
            "exp_id": exp_id,
            "freshness_p50_ms": 100.0,
            "freshness_p95_ms": 200.0,
            "avg_throughput_eps": 20.0,
            "max_throughput_eps": 25.0,
            "avg_backlog": 1.0,
            "max_backlog": 5,
            "cpu_avg_percent": 30.0,
            "cpu_max_percent": 50.0,
            "ram_avg_mb": 2000.0,
            "ram_max_mb": 2500.0,
            "q1_p95_latency_ms": 2.0,
            "q2_p95_latency_ms": 3.0,
            "count_diff": count_diff,
            "sum_diff_cents": sum_diff_cents,
            "is_correct": is_correct,
        }
        with open(os.path.join(directory, f"{exp_id}_summary.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f)

    def test_aggregate_preserves_opposite_sign_errors_and_batch_avg(self):
        with tempfile.TemporaryDirectory() as d:
            self._write_summary(d, "E1_A_run1", count_diff=5, sum_diff_cents=100, is_correct=False)
            self._write_summary(d, "E1_A_run2", count_diff=-5, sum_diff_cents=-100, is_correct=False)

            with open(os.path.join(d, "E1_A_run1_batch_cost.csv"), "w", encoding="utf-8") as f:
                f.write("batch_duration_ms\n1000\n3000\n")
            with open(os.path.join(d, "E1_A_run2_batch_cost.csv"), "w", encoding="utf-8") as f:
                f.write("batch_duration_ms\n4000\n")

            aggregate_summaries(d)
            df_agg = pd.read_csv(os.path.join(d, "summary_aggregated.csv"))
            self.assertEqual(len(df_agg), 1)
            row = df_agg.iloc[0]
            self.assertEqual(int(row["count_diff"]), 5)
            self.assertEqual(int(row["sum_diff_cents"]), 100)
            self.assertFalse(bool(row["is_correct"]))
            self.assertEqual(float(row["avg_batch_duration_ms"]), 3000.0)

    def test_is_valid_completed_run_rejects_failed_or_unseen(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertFalse(is_valid_completed_run(os.path.join(d, "missing.json")))

            bad_correct = os.path.join(d, "bad_correct.json")
            with open(bad_correct, "w", encoding="utf-8") as f:
                json.dump({"is_correct": False, "sampled_events_unseen": 0}, f)
            self.assertFalse(is_valid_completed_run(bad_correct))

            bad_unseen = os.path.join(d, "bad_unseen.json")
            with open(bad_unseen, "w", encoding="utf-8") as f:
                json.dump({"is_correct": True, "sampled_events_unseen": 1}, f)
            self.assertFalse(is_valid_completed_run(bad_unseen))

            good = os.path.join(d, "good.json")
            with open(good, "w", encoding="utf-8") as f:
                json.dump({"is_correct": True, "sampled_events_unseen": 0}, f)
            self.assertTrue(is_valid_completed_run(good))


if __name__ == "__main__":
    unittest.main()
