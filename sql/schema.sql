DROP VIEW IF EXISTS v_q2_total CASCADE;
DROP VIEW IF EXISTS v_q1_category CASCADE;
DROP TABLE IF EXISTS batch_seen_events CASCADE;
DROP TABLE IF EXISTS speed_events CASCADE;
DROP TABLE IF EXISTS batch_views CASCADE;
DROP TABLE IF EXISTS watermark_meta CASCADE;

CREATE TABLE watermark_meta (
    id INT PRIMARY KEY CHECK (id = 1),
    h_seq BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO watermark_meta (id, h_seq, updated_at) VALUES (1, 0, NOW());

CREATE TABLE batch_views (
    category TEXT PRIMARY KEY,
    cnt BIGINT NOT NULL DEFAULT 0,
    sum_cents BIGINT NOT NULL DEFAULT 0,
    h_seq BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE speed_events (
    event_id TEXT PRIMARY KEY,
    seq BIGINT NOT NULL,
    category TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_speed_events_seq ON speed_events(seq);
CREATE INDEX idx_speed_events_cat_seq ON speed_events(category, seq);

CREATE TABLE batch_seen_events (
    event_id TEXT PRIMARY KEY,
    seq BIGINT NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_batch_seen_seq ON batch_seen_events(seq);

CREATE VIEW v_q1_category AS
WITH current_h AS (
    SELECT h_seq FROM watermark_meta WHERE id = 1
),
speed_agg AS (
    SELECT 
        s.category,
        COUNT(*) AS cnt,
        COALESCE(SUM(s.amount_cents), 0) AS sum_cents
    FROM speed_events s
    CROSS JOIN current_h m
    WHERE s.seq > m.h_seq
    GROUP BY s.category
),
combined AS (
    SELECT category, cnt, sum_cents FROM batch_views
    UNION ALL
    SELECT category, cnt, sum_cents FROM speed_agg
)
SELECT 
    category,
    CAST(SUM(cnt) AS BIGINT) AS total_count,
    CAST(SUM(sum_cents) AS BIGINT) AS total_sum_cents
FROM combined
GROUP BY category
ORDER BY category;

CREATE VIEW v_q2_total AS
SELECT 
    COALESCE(CAST(SUM(total_count) AS BIGINT), 0) AS system_total_count,
    COALESCE(CAST(SUM(total_sum_cents) AS BIGINT), 0) AS system_total_sum_cents,
    (SELECT h_seq FROM watermark_meta WHERE id = 1) AS current_batch_h
FROM v_q1_category;
