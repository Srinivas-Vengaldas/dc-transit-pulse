# Benchmarks and measured metrics

Every number in the main README's results table comes from this folder's method, run once
on a fixed window chosen before the data existed.

## Measurement window

**2026-10-02 to 2026-10-08, Eastern time: 7 full days of scheduled runs.**

- The schedule was unpaused on 2026-10-01 and ran only that evening, so 2026-10-01 is a partial
  day and is left out.
- The job starts 9 runs a day (06:00 to 22:00 ET, every 2 hours). No run crosses midnight, so a
  calendar day is a clean unit.
- The window is set in code (`pipelines.metrics.MEASUREMENT_WINDOW`) and was committed before
  measuring. Changing it means changing that line, which shows up in git history.

## Pipeline metrics

`benchmarks/queries.sql` holds every query with the window filled in. It is generated from
`pipelines/metrics.py` (`python -m pipelines.metrics > benchmarks/queries.sql`), and a test fails
if the two drift apart.

| Metric | Source | Reported as |
|---|---|---|
| Events per day | `ops_run_log`, bronze step, rows added | mean, min and max over the 7 days |
| Freshness (newest departure to gold build) | `ops_run_log`, gold step, scheduled runs only | median, p95, mean, min, max |
| Rows removed (duplicates and late rows) | bronze vs silver + quarantine row counts, by ingest time | count and % per feed |
| Data quality | `dq_results` for runs in the window | % of checks passed, runs where all passed |
| Job reliability | `ops_run_log`, last attempt of each step | runs succeeded / runs started |
| Run coverage | `ops_run_log` vs 9 scheduled runs a day | missing and failed runs per day |
| Step runtime | `ops_run_log` | median and max seconds per step |

Run it in a Databricks notebook:

```python
from pipelines.metrics import MEASUREMENT_WINDOW, compute_metrics, save_metrics
m = compute_metrics(spark, *MEASUREMENT_WINDOW)
save_metrics(spark, m)
```

`save_metrics` appends the result, with the window and a timestamp, to `ops_metric_results`.

Gaps are reported, not hidden. If a run is missing or failed, the run-coverage table lists the day
and hour, and the results section says what happened and whether it changes any number.

## Validation against WMATA

`crosscheck.sql` pairs each graded departure in the window with WMATA's own trip-update delay for
the same trip, taken at the nearest moment, and reports the pair count, correlation, and the median
and p90 absolute difference. An early 2-minute sample (124 pairs) is not quoted; only the full-window
run is.

## Optimization before and after

`pipelines/optimize.py` measures both states under the same conditions:

1. `run_benchmark(spark, "before")` records file count and size per table, which is deterministic,
   and times each benchmark query 5 times. The first run is reported separately, and the median
   of all 5 is the headline.
2. `optimize_tables(spark)` adds liquid clustering on `(service_date, route_id)` and runs `OPTIMIZE`.
3. `run_benchmark(spark, "after")` runs the same queries on the same serverless compute, in the same
   session, right after.
4. `compare_sql("before", "after")` prints the side-by-side table from `ops_benchmarks`.

The gold step's runtime in scheduled runs before and after the change is a second, independent check.

## Results

Filled in after the window closes. See the main README.
