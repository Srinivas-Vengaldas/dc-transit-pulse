-- Validation: gold's observed delay vs WMATA's own trip-update delay for the same trip and stop.
-- Two independent sources for the same departures; close agreement means the departure logic is right.
-- Window: service dates 2026-10-02 to 2026-10-08 (pipelines.metrics.MEASUREMENT_WINDOW). Read-only.
WITH g AS (
  SELECT trip_id, service_date, stop_id, observed_ts, delay_s
  FROM workspace.transit.gold_timepoint_departures
  WHERE service_date BETWEEN DATE'2026-10-02' AND DATE'2026-10-08'
),
tu AS (
  SELECT trip_id, trip_start_date, event_ts, delay_s AS tu_delay_s
  FROM workspace.transit.silver_trip_updates
  WHERE delay_s IS NOT NULL
    AND trip_start_date BETWEEN DATE'2026-10-02' AND DATE'2026-10-08'
),
pairs AS (
  SELECT g.*, tu.tu_delay_s,
         abs(unix_timestamp(tu.event_ts) - unix_timestamp(g.observed_ts)) AS gap_s,
         row_number() OVER (PARTITION BY g.trip_id, g.service_date, g.stop_id
                            ORDER BY abs(unix_timestamp(tu.event_ts) - unix_timestamp(g.observed_ts))) AS rn
  FROM g JOIN tu ON g.trip_id = tu.trip_id AND g.service_date = tu.trip_start_date
)
SELECT count(*)                                    AS pairs,
       count(DISTINCT service_date)                AS days,
       round(corr(delay_s, tu_delay_s), 3)         AS correlation,
       percentile(abs(delay_s - tu_delay_s), 0.5)  AS median_abs_diff_s,
       percentile(abs(delay_s - tu_delay_s), 0.9)  AS p90_abs_diff_s,
       percentile(gap_s, 0.5)                      AS median_match_gap_s
FROM pairs
WHERE rn = 1;
