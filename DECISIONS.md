# Design decisions

Each entry: what was chosen, why, and the alternative rejected.

## Phase 1: Setup, schema, profiling

### D1. Pandas, not PySpark
- **Chosen:** Pandas (+ Postgres for set-based work).
- **Why:** The full file is 34 MB gzipped / 2.3M rows. Profiling the entire file in pandas took ~46 s on a laptop, and it fits in memory many times over. Spark adds a JVM, cluster config and startup cost and solves no problem we have.
- **Rejected:** PySpark. It would become justified at roughly 100× this size (see README "100× scale" section in Phase 6).

### D2. Profile first, write rules later
- **Chosen:** `scripts/profile_raw.py` generates `docs/data_profile.md` from the actual file; rules in Phase 3 cite it.
- **Why:** Several "expected" problems turned out different from the brief. (moteid, epoch) duplicates are mostly *not* duplicates (epoch counter restart), and the extreme values are a few sentinel values tied to low battery rather than random outliers.
- **Rejected:** Writing generic range/null checks up front. That would have produced a dedup rule deleting ~113K valid readings.

### D3. Parse on a single space, not whitespace runs
- **Why:** The file encodes empty fields as consecutive spaces (findings #1). `sep=" "` gives 8 positions on every line.
- **Rejected:** `sep=r"\s+"` / `str.split()`. That shifts values left and silently mislabels columns.

### D4. Raw layer stores each line as-is (`raw_line TEXT`)
- **Why:** Raw must never fail to load and must preserve exactly what the source said. Even *splitting* the line is a parsing decision here (D3), so it happens in staging, where it is unit-tested and failures go to `staging.rejects` with a reason. Only the line terminator is stripped; trailing spaces are kept because they encode empty trailing fields.
- **Rejected:** Typed raw columns (one bad token fails the load, or gets coerced before we've recorded it). Pre-split text columns (my first Phase 1 draft): this hides the split logic in the loader and can't represent a line with the wrong number of fields. Changed in Phase 2.

### D5. Natural key `(source_file, source_line_no)` for raw and staging
- **Why:** The file has no reliable natural key (#7: moteid+epoch isn't unique). Line number is stable for a given file, so it's the basis of idempotency: a rerun of a batch can't insert a line twice, and every staged or clean row traces back to one source line.
- **Rejected:** Surrogate `BIGSERIAL` only. Reruns would create new ids for the same line, so duplicates couldn't be detected.

### D6. Batches = line ranges of the file, with deterministic batch ids
- **Why:** The file isn't time-ordered (#9), so "load everything after timestamp X" would miss late lines. A line range is exact and repeatable; `batch_id = '<file>:<start>-<end>'` means the same batch always gets the same id.
- **Rejected:** Time-window batches. Random UUID batch ids would make reruns indistinguishable from new loads.

### D7. Postgres schemas per layer + an `audit` schema
- **Why:** The layer is visible in every qualified name (`clean.sensor_readings`). `dq_results` spans layers, so it goes in `audit` rather than in one of the four data layers.
- **Rejected:** Separate databases per layer (cross-database queries are awkward in Postgres). Putting `dq_results` in `clean` (it also records staging/raw checks).

### D8. `TIMESTAMP` without time zone
- **Why:** The source has no offset. Storing `TIMESTAMPTZ` would require inventing one.
- **Rejected:** `TIMESTAMPTZ` with an assumed US/Pacific zone.

### D9. Clean and marts tables are defined in their own phases
- **Why:** Their columns (flag names, mart metrics) depend on the rules derived from the profile. The schemas exist now; the tables are added to `sql/init/` in Phases 3–4.

### D10. DDL via Postgres `docker-entrypoint-initdb.d`
- **Why:** No extra tool is needed; it runs automatically on `docker compose up` with an empty volume.
- **Trade-off:** Scripts only run on first init. Changing the DDL later requires `docker compose down -v`.
- **Rejected:** A migration tool (Alembic/Flyway). That's more machinery than a portfolio project with a fixed schema needs.

## Phase 2: Raw load and staging

### D11. Raw idempotency via `ON CONFLICT DO NOTHING`, staging via delete-and-rebuild per batch
- **Raw:** COPY the batch into a temp table, then `INSERT … SELECT … ON CONFLICT (source_file, source_line_no) DO NOTHING`. COPY is the fastest bulk path into Postgres but can't handle conflicts itself, hence the temp table. Raw rows are never updated or deleted.
- **Staging:** Staging is fully derived from raw, so `DELETE WHERE batch_id = X` then reinsert, in one transaction. Rerunning a batch replaces its rows, and a crash mid-way rolls back to the previous state.
- **Verified:** A second backfill inserted 0 raw rows. Restaging a batch left counts unchanged (2,313,682 raw = 2,313,156 staged + 526 rejected).
- **Rejected:** `TRUNCATE` + full reload every run (not incremental). Upserts (`ON CONFLICT DO UPDATE`) into staging (more complex, and if the parser changes, rows that become rejects would linger in staging).

### D12. Fixed-grid batches; incremental resumes from the last partial batch
- Batch k always covers lines `[(k-1)·N+1, k·N]`. Incremental mode finds `max(line_end)`, restarts at the start of that grid batch and relies on `ON CONFLICT` to skip already-loaded lines. Only batches that gained rows are passed downstream.
- **Verified:** Loading a 1,200,000-line copy of the file and then the full file gave batch 3 a top-up of 300,000 rows, created batches 4–5, and ended identical to a single backfill.
- **Assumption:** The source file is append-only (existing lines never change). If a line *did* change, raw would keep the original; detecting that would need a hash per line (not implemented; not needed for this static dataset).
- **Rejected:** Batch boundaries based on "what's new since last run" (e.g. lines 1,200,001–1,700,000). The same lines would get different batch ids depending on run history.
- Default N = 500,000 → 5 batches. gzip can't seek by line, so a later batch still streams past earlier lines (about 2 s per 500K lines). That's acceptable at this size.

### D13. What is a reject (agreed after Phase 1)
- A row is rejected only if it can't be attributed to a mote at a point in time, or has a present-but-malformed token: `wrong_field_count`, `missing_/invalid_timestamp`, `missing_/invalid_epoch`, `missing_/invalid_moteid`, `*_out_of_int_range`, `invalid_<sensor>`. All reasons are recorded, joined with `;`.
- An empty sensor value → NULL, row kept. Implausible values (122 °C, unknown motes) are *not* rejects; they're flagged in the clean layer.
- **Result on the real file:** 526 rejects, all `missing_moteid`.
- **Rejected:** Rejecting any row with a NULL (would discard 92,975 rows that are fine apart from light).

### D14. Strict token regexes before type conversion
- `pd.to_numeric` accepts `"nan"`/`"inf"`, and ISO-8601 datetime parsing accepts forms not in this file. A regex validates each token's shape first, so anything unexpected becomes a visible reject instead of a silent NaN.

### D15. Staging in pandas (Python), not SQL
- **Why:** The parsing rules are the part most worth unit-testing, and pure pandas functions (`pipeline/parse.py`) test without a database. 500K-row batches stage in about 15 s each.
- **Rejected:** Parsing in SQL with `split_part`/regex casts. That's faster at scale, but harder to unit-test and to collect multiple reject reasons per row.

## Phase 3: Data quality checks and clean layer

### D16. Flag, don't delete; per-value usability columns
- Every staged reading reaches clean (except true duplicates). Each row carries `dq_flags TEXT[]` (all rules it tripped) plus `temperature_ok / humidity_ok / light_ok / voltage_ok`.
- **Why per-value booleans:** Failures are per sensor. A reading with the stuck 122.153 °C temperature still has a usable light value. Marts aggregate each metric over rows where its `_ok` is true.
- **Rejected:** One row-level `is_valid` flag. It would throw away good light/voltage values along with bad temperatures. A severity enum per row has the same problem.

### D17. Temperature range 0–50 °C, humidity 0–100 %, voltage 1.8–3.3 V, light ≥ 0
- Every temperature reading above 50 °C (and 99.9 % of 40–50 °C) comes from a mote below 2.4 V. Readings < 0 °C (539) aren't physical for an indoor lab. 30–35 °C is kept: it comes from healthy motes with low humidity.
- **Rejected:** Statistical bounds (mean ± 3σ, IQR). Bad data is ~17 % of rows and concentrated on one value, so it distorts mean and σ. The physical bounds come straight from the data and are easy to explain.
- **Rejected:** Upper bound 35 or 40 °C. That would discard plausible warm readings from healthy motes. The residual drift between 35 and 50 °C at 2.30–2.40 V is accepted (see D18).

### D18. `low_battery` (< 2.30 V) invalidates temperature and humidity
- Below 2.30 V, only 5.9 % of readings even look plausible, and the median is the stuck value.
- **Cost (measured):** 33,112 readings with an in-range temperature are marked unusable only because of this rule. **Benefit:** values that are in range but drifting from failing batteries don't reach the marts.
- The two rules complement each other: 37,497 out-of-range temperatures come from motes at ≥ 2.30 V (drift in the 2.30–2.40 V band), which the battery rule alone would miss.
- **Rejected:** 2.35 V (also discards 2.30–2.35 V, where 66 % look fine). 2.4 V (discards more). No battery rule (lets drifted-but-in-range values into averages).
- Light is *not* invalidated by low battery. There's no evidence it degrades the same way, and I'm not inventing a rule without evidence.

### D19. Spike rule: isolated blip > 5 °C vs both neighbours within 120 s
- Evaluated only on readings that already passed range and battery rules, so dying-battery garbage doesn't create false spikes around it. "Both neighbours, same direction" distinguishes a one-reading blip from a genuine step change.
- **Result:** 2 rows. That's honestly small: the range and battery rules already remove the bad data, and normal readings change by a median of 0.01 °C between samples.
- **Rejected:** Rolling z-score / rolling median. Postgres has no windowed median, and it would be harder to explain for 2 rows of benefit.

### D20. Epoch check against the network's hourly consensus
- Epoch is a shared, wrapping 16-bit counter (findings, Phase 3). Each reading's epoch is compared with the median epoch of its hour, using wrap-aware distance; > 240 ticks (2 h) → `epoch_inconsistent` (18,114 rows).
- The flag is informational. It doesn't invalidate sensor values, because `reading_ts` is the time of record.
- **Rejected:** A global `T0 + 31 s × epoch` model (wrong period, ignores wrap; wrongly put 471K rows > 1 week off in Phase 1). Per-mote linear fits (break on every reboot).

### D21. Deduplication: same mote, epoch and values, within 60 s
- **Result:** 5 rows removed, matching the 5 same-packet groups found in Phase 1.
- `PARTITION BY` treats NULLs as equal, so copies missing the same field still match. The first copy (earliest `reading_ts`, then line number) is kept.
- **Rejected:** Dedup on (moteid, epoch), which would remove ~113K real readings.

### D22. Clean is rebuilt in full, in one transaction with its post-condition checks
- Every batch spans 34–38 of the 38 days, so dedup, spikes and the epoch consensus need data from all batches anyway. The full rebuild + checks takes 21–28 s.
- `TRUNCATE` + `INSERT` + `no_rows_lost` + `no_duplicates_remaining` run in one transaction. If an error-severity check fails, the transaction rolls back and clean keeps its previous contents.
- **Verified:** I deleted mote 1's rows inside the rebuild (simulated bug). `no_rows_lost` reported 43,047 failures, the transaction rolled back, and clean's row count and `cleaned_at` were unchanged.
- **Rejected:** Per-batch incremental clean (wrong: misses cross-batch duplicates and neighbours). Per-day partition rebuild (equivalent to full here, because every batch touches every day; the right answer at larger scale; see README "100×").

### D23. Two severities and policy thresholds in `audit.dq_results`
- `error`: the pipeline's own output is wrong (reconciliation, reject rate > 1 %, missing mote registry, rows lost, duplicates remaining). The task fails.
- `warn`: the source data is worse than our tolerance. It's recorded and reported, and the pipeline continues because the values are flagged and excluded from marts.
- Thresholds are policy (5 % value checks, 1 % structural, 0.1 % duplicates/spikes/voltage, 10 % of motes silent), **not fitted to the data**. Four warn checks fail on this dataset (temperature/humidity out of range, low battery, silent motes), and that's an accurate description of it.
- Results are upserted on (run_id, check_name, batch_id), so an Airflow retry overwrites rather than duplicates. They're written in their own transaction so failures survive the rollback in D22.
- Every registered rule is recorded even when it finds nothing; a zero is a result.

### D24. "Silent mote" is measured against when the network stopped, not the last timestamp
- After 2004-04-01 23:00 only a trickle of readings remains, mostly from unregistered motes 56 and 58. Measuring against `max(reading_ts)` called all 54 motes silent (first run). `clean.v_network_end` = end of the last hour with ≥ 10 % of registered motes reporting. **Result:** 29 of 54 silent > 24 h before it. That's stable for 5–30 % cutoffs.
- Defined once as a view so the Phase 4 health mart uses the same definition.

### D25. Validate = gate on staging; rules live only in the clean SQL
- The DAG order is ingest → stage → **validate** → clean. Validate checks that staging is complete and parseable (per batch) and that the mote registry exists. Clean applies the rules and records the flag counts.
- **Why:** Each rule is defined exactly once (`clean.sql`). Running the rules in validate too would mean either two definitions or computing the expensive window functions twice.

## Phase 4: Marts and quality report

### D26. Expected readings = 1 per 30 s while the network was running
- Measured, not taken from the docs: a regression of hourly median epoch on time gives 30.001 s/tick (r² = 0.9999999), whereas the dataset page says 31 s. Using 31 s would understate expected readings by ~3 % and inflate every delivery rate.
- The "network running" window runs from the first registered-mote reading to `clean.v_network_end` (D24). Partial first days are prorated (2004-02-28: 2,762.5 expected). Days after the end get 0 expected and a NULL rate, not a fake 0 %.
- **Rejected:** Expected = 2,880 for every calendar day (penalises motes for the days after the experiment ended). Expected per mote from its own first/last reading only (hides a mote dying early). That figure is still provided as `delivery_rate_while_alive` in the health mart.

### D27. `mote_daily` and `mote_health` cover the 54 registered motes; `anomaly_counts` covers all ids
- Registered motes have locations and are the deployment. Motes 55/56/58 send plausible data but aren't in `mote_locs.txt`, and 6485/33117/65407 are corrupt ids with one reading each. Including them would add "motes" to the health mart that have no location and, for three of them, no real existence.
- They aren't lost: they stay in `clean`, appear in `anomaly_counts` under `unknown_mote`, and are counted in the report (9,866 rows, 0.43 %).
- **Rejected:** Including every id in all marts (pollutes health stats). Rejecting unknown ids in staging (would lose ~10K plausible readings; agreed in Phase 1).

### D28. Dense day grid in `mote_daily`
- Every registered mote × every day (54 × 36 = 1,944 rows), with `reading_count = 0` on days without data. A mote that died appears as rows with delivery 0, instead of disappearing from the table (which makes "which motes reported nothing on day X?" an anti-join every time).
- **Rejected:** Sparse (only days with readings). It's smaller, but silence becomes invisible.

### D29. Marts are rebuilt in full, in one transaction with reconciliation checks
- Same pattern as clean (D22): TRUNCATE + INSERT + 4 error-severity checks (`mote_daily` sums to clean, grid complete, `anomaly_counts` sums to the flags in clean, one health row per mote). A failure rolls back to the previous marts. The build takes ~8.5 s.
- Marts are small (1,944 + 54 + 2,028 rows) and derived entirely from clean, so incremental maintenance would add complexity for no gain at this size.

### D30. Health metrics count any *received* reading, flagged or not
- Gaps and delivery measure "did the radio deliver a packet?". A stuck 122.153 °C reading still proves the mote was alive. Value quality is reported separately (`temperature_ok_pct`, `low_battery_since`).
- Network-wide outages (2 outages, 41 hours with no registered mote reporting) are *not* subtracted from mote gaps or expected readings. Mote metrics describe what was actually received. Outages are reported separately (`network_outage_hours` check, report §6) so a reader can tell them apart.
- **Rejected:** Excluding outage hours from expected readings. It's arguably fairer per mote, but it adds a second definition of "expected" that a reader must know about. Worth revisiting if the marts were used to rank motes.

### D31. Battery trend = least-squares slope of daily average voltage
- `regr_slope(voltage_avg, day)` over days with usable voltage gives V/day. `low_battery_since` is the first day the daily average fell below the same 2.30 V used by the clean rule, which keeps the rules consistent. It separates battery deaths from other failures: mote 50 died at 2.558 V.
- **Rejected:** First single reading < 2.30 V (one noisy reading would set it). Fitting voltage curves / projecting time-to-death (more than was asked for).

### D32. Report generated from the database, one file per run
- `reports/dq_report_<run_id>.md` is built only from this run's `audit.dq_results` rows and queries on the tables, so it can't drift from what the pipeline actually measured. Rerunning for a run id overwrites the file.
- Beyond the check table it shows the *values* behind the flags (top values reveal the stuck 122.153 / −3.91901 readings) and the most common flag per worst mote, because a bare "100 % flagged" for mote 28 turned out to be only "no light sensor data".
- **Rejected:** HTML or a dashboard (Markdown was asked for, and it renders on GitHub). Storing the report text in the database (the file is the artifact; the underlying numbers are already in `dq_results`).

## Phase 5: Airflow DAG and tests

### D33. Airflow 2.10 (LocalExecutor) with a thin DAG over the `pipeline` package
- The DAG file only wires tasks. All logic lives in `pipeline/`, which also runs from the CLI (`python -m pipeline …`) and from tests. Logic can be tested and debugged without Airflow, and the DAG reads as the dependency graph.
- LocalExecutor runs tasks as subprocesses of the scheduler. The pipeline is one linear chain on one machine, so Celery/Kubernetes executors would add Redis/workers for no benefit.
- TaskFlow (`@task`) passes the batch-id list via XCom (ingest → stage → validate). The rest is ordered with `>>` because those tasks rebuild whole tables and need no input.
- Imports happen inside tasks so the scheduler's DAG parsing (every 30 s) doesn't import pandas.
- **Rejected:** Airflow 3.x (newer, but 2.10 is what most teams run and what most docs/tutorials describe). BashOperator calling the CLI (works, but loses XCom and typed parameters).

### D34. Retries for transient failures, immediate failure for data-quality failures
- `retries=2`, 30 s delay with exponential backoff, 30 min `execution_timeout` per task, `on_failure_callback` logging one clear line.
- A failed error-severity check raises `DataQualityError`, which the DAG converts to `AirflowFailException`. Airflow then fails the task **without** retrying, since rerunning a deterministic check gives the same answer.
- **Verified:**
  - Hiding `data.txt.gz` made `ingest` fail attempt 1 with the README hint; it went `up_for_retry` and succeeded on attempt 2 once the file was back.
  - Emptying the mote registry made `validate` fail on attempt 1 only ("Immediate failure requested"). Downstream tasks were `upstream_failed`, the failure was recorded in `dq_results`, clean was untouched (unchanged `cleaned_at`), and the next run recovered.
- A first attempt at that test (deleting only `raw.mote_locs`) didn't fail at all: `ingest` reloads reference files every run and restored the rows. That's useful self-healing, but a bad test, so I fixed the test.

### D35. `@daily` incremental schedule, `catchup=False`, `max_active_runs=1`, unpaused at creation
- `docker compose up` must run the whole thing, so the DAG is unpaused at creation and the scheduler immediately runs the latest interval. On an empty database, incremental = load everything.
- Daily incremental runs pick up lines appended to the file. For this static file they find none and just rebuild clean/marts (~1 min). Backfill is a manual trigger with `{"mode": "backfill"}`.
- `max_active_runs=1`: clean and marts are rebuilt with TRUNCATE + INSERT, so two concurrent runs would contend. One run at a time makes that impossible.
- **Rejected:** `schedule=None` (manual only, so `up` wouldn't run anything). `catchup=True` (one run per day since `start_date` for a static file).
- Batch size is deliberately *not* a DAG parameter. Changing it would create overlapping batch grids. `ingest_readings` now refuses a batch size that differs from the one a file was loaded with.

### D36. One Postgres server, separate `airflow` database
- `sql/init/00_airflow_db.sql` creates Airflow's metadata DB next to the warehouse DB. That's one container, and the two never share tables.
- **Trade-off:** Restarting Postgres interrupts both Airflow and the pipeline. At scale they'd be separate instances (README "100×").

### D37. Pin pandas 2.1.4 / numpy 1.26.4 to match the Airflow image
- The official Airflow 2.10.5 image ships them. Installing pandas 2.2.3 on top would fight Airflow's dependency constraints. The local venv was downgraded and all tests pass on both. The custom image only adds pytest.

### D38. Two kinds of tests
- `tests/test_parse.py`, `tests/test_ingest_and_quality.py`: pure unit tests (parsing rules incl. the real double-space and trailing-space lines, rejects, batch grid, check thresholds).
- `tests/test_clean_rules.py`: the validation rules are SQL, so they're tested against a throwaway `wsn_test` database built from the same DDL, with one hand-made scenario per rule. These include near-misses that must *not* be flagged: a step change that isn't a spike, the same epoch with different values that isn't a duplicate, identical values 61 s apart, and an epoch just after the 16-bit wrap. The tests are skipped if Postgres isn't reachable.
- **Verified the tests can fail:** Raising the spike threshold to 15 °C and removing the wrap-aware epoch distance in `clean.sql` made exactly `test_isolated_blip_is_a_spike` and `test_epoch_check_is_wrap_aware` fail. The file was then restored.
- 61 tests pass locally and inside the Airflow container.
- **Rejected:** Mocking the database for rule tests (it would test the mock, not the SQL). testcontainers (an extra dependency; the compose Postgres is already there).

## Phase 6: README

### D39. README metrics come from one documented fresh run
- Every number in the README comes from one `docker compose down -v && up` run (`scheduled__2026-10-09T00:00:00+00:00`) and one backfill run after it (`manual_backfill`). The README cites both run ids, and their reports are committed in `reports/`. The 7 reports from Phase 4–5 test runs (retry/failure tests) were moved out of the repo so the committed reports match the README.
- The 100× section labels its timings as linear projections, not measurements.
- **Rejected:** Mixing timings from different runs. Quoting the faster backfill ingest (12.5 s) as "load time" (it inserted nothing).

### D40. Architecture diagram in Mermaid
- It renders natively on GitHub and is text, so it's diffable and stays in sync with code reviews. I rendered it with Mermaid 10.9.1 before committing: the first left-to-right layout was too wide to read, so it's now top-down with numbered steps.
- **Rejected:** An exported image (goes stale silently).
