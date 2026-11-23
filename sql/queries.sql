SELECT 
    category, 
    total_count, 
    total_sum_cents 
FROM v_q1_category;

SELECT 
    system_total_count, 
    system_total_sum_cents, 
    current_batch_h 
FROM v_q2_total;

SELECT 
    COALESCE(s.event_id, b.event_id) AS event_id,
    COALESCE(s.seq, b.seq) AS seq,
    COALESCE(s.produced_at, b.produced_at) AS produced_at,
    LEAST(s.visible_at, b.visible_at) AS first_visible_at,
    EXTRACT(EPOCH FROM (LEAST(s.visible_at, b.visible_at) - COALESCE(s.produced_at, b.produced_at))) * 1000.0 AS freshness_ms
FROM speed_events s
FULL OUTER JOIN batch_seen_events b ON s.event_id = b.event_id
ORDER BY seq ASC
LIMIT 10;
