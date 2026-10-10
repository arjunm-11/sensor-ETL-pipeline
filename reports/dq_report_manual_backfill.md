# Data quality report: run `manual_backfill`

Generated 2026-10-10 00:16:21 UTC from `audit.dq_results` and the pipeline tables.

## 1. Rows through the layers

| layer | rows | note |
|---|---|---|
| raw | 2,313,682 | 5 batches |
| staging | 2,313,156 | 526 rejected (0.02% of raw) |
| clean | 2,313,151 | 5 duplicates removed |
| clean, no flags | 1,771,105 | 76.57% of clean |
| clean, registered motes (feed marts) | 2,303,285 | 99.57% of clean |

## 2. Check results

32 checks recorded for this run: **0 error-severity failures**, **5 warnings**. Error failures stop the pipeline; warnings describe the source data (flagged values are excluded from marts, not deleted).

| result | layer | check | batch | severity | checked | failed | failed % | threshold | what it checks |
|---|---|---|---|---|---|---|---|---|---|
| PASS | staging | `mote_locs_loaded` | all | error | 1 | 0 | 0.00% | 0.0% | mote location reference table is populated (54 motes) |
| PASS | staging | `reject_rate` | `000000001-000500000` | error | 500,000 | 0 | 0.00% | 1.0% | lines that could not be parsed |
| PASS | staging | `reject_rate` | `000500001-001000000` | error | 500,000 | 0 | 0.00% | 1.0% | lines that could not be parsed |
| PASS | staging | `reject_rate` | `001000001-001500000` | error | 500,000 | 0 | 0.00% | 1.0% | lines that could not be parsed |
| PASS | staging | `reject_rate` | `001500001-002000000` | error | 500,000 | 0 | 0.00% | 1.0% | lines that could not be parsed |
| PASS | staging | `reject_rate` | `002000001-002500000` | error | 313,682 | 526 | 0.17% | 1.0% | lines that could not be parsed |
| PASS | staging | `stage_reconciliation` | `000000001-000500000` | error | 500,000 | 0 | 0.00% | 0.0% | every raw line is either staged or rejected |
| PASS | staging | `stage_reconciliation` | `000500001-001000000` | error | 500,000 | 0 | 0.00% | 0.0% | every raw line is either staged or rejected |
| PASS | staging | `stage_reconciliation` | `001000001-001500000` | error | 500,000 | 0 | 0.00% | 0.0% | every raw line is either staged or rejected |
| PASS | staging | `stage_reconciliation` | `001500001-002000000` | error | 500,000 | 0 | 0.00% | 0.0% | every raw line is either staged or rejected |
| PASS | staging | `stage_reconciliation` | `002000001-002500000` | error | 313,682 | 0 | 0.00% | 0.0% | every raw line is either staged or rejected |
| PASS | clean | `duplicates_removed` | all | warn | 2,313,156 | 5 | 0.00% | 0.1% | same packet received more than once |
| PASS | clean | `epoch_inconsistent` | all | warn | 2,313,151 | 18,114 | 0.78% | 1.0% | epoch > 2 h (240 ticks) from the network's hourly consensus |
| PASS | clean | `humidity_missing` | all | warn | 2,313,151 | 376 | 0.02% | 1.0% | humidity field empty |
| **FAIL** | clean | `humidity_out_of_range` | all | warn | 2,313,151 | 299,084 | 12.93% | 5.0% | humidity outside 0-100 % |
| PASS | clean | `light_missing` | all | warn | 2,313,151 | 93,352 | 4.04% | 5.0% | light field empty |
| PASS | clean | `light_out_of_range` | all | warn | 2,313,151 | 0 | 0.00% | 0.0% | light below 0 lux |
| **FAIL** | clean | `low_battery` | all | warn | 2,313,151 | 403,751 | 17.45% | 5.0% | voltage below 2.30 V: temperature/humidity unreliable |
| **FAIL** | clean | `network_outage_hours` | all | warn | 816 | 41 | 5.02% | 1.0% | hours while the network was running with zero readings from any registered mote (units: hours) |
| PASS | clean | `no_duplicates_remaining` | all | error | 2,313,151 | 0 | 0.00% | 0.0% | no two clean rows are the same packet (mote, epoch, values, within 60 s) |
| PASS | clean | `no_rows_lost` | all | error | 2,313,156 | 0 | 0.00% | 0.0% | staged rows absent from clean are all duplicates of a kept row |
| **FAIL** | clean | `silent_motes` | all | warn | 54 | 29 | 53.70% | 10.0% | known motes silent for more than 24 hours before the network stopped (units: motes) |
| PASS | clean | `temperature_missing` | all | warn | 2,313,151 | 375 | 0.02% | 1.0% | temperature field empty |
| **FAIL** | clean | `temperature_out_of_range` | all | warn | 2,313,151 | 408,136 | 17.64% | 5.0% | temperature outside 0-50 C (indoor lab) |
| PASS | clean | `temperature_spike` | all | warn | 2,313,151 | 2 | 0.00% | 0.1% | isolated temperature blip > 5 C vs both neighbours within 120 s |
| PASS | clean | `unknown_mote` | all | warn | 2,313,151 | 9,866 | 0.43% | 1.0% | mote id not in mote_locs.txt (ids 1-54) |
| PASS | clean | `voltage_missing` | all | warn | 2,313,151 | 0 | 0.00% | 1.0% | voltage field empty |
| PASS | clean | `voltage_out_of_range` | all | warn | 2,313,151 | 8 | 0.00% | 0.1% | voltage outside 1.8-3.3 V |
| PASS | marts | `anomaly_counts_reconcile` | all | error | 1,233,064 | 0 | 0.00% | 0.0% | sum of anomaly_counts equals total flags in clean |
| PASS | marts | `mote_daily_grid_complete` | all | error | 1,944 | 0 | 0.00% | 0.0% | one mote_daily row per registered mote per day, including empty days |
| PASS | marts | `mote_daily_reconciles` | all | error | 2,303,285 | 0 | 0.00% | 0.0% | sum of mote_daily.reading_count equals registered-mote rows in clean |
| PASS | marts | `mote_health_complete` | all | error | 54 | 0 | 0.00% | 0.0% | one mote_health row per registered mote |

## 3. Rejected lines (staging)

| reason | lines |
|---|---|
| missing_moteid | 526 |

## 4. What the flagged values are

Most frequent values among flagged readings:

- `temperature_out_of_range` (408,136 rows): `122.153` × 370,196 (90.70%), `122.134` × 2,399 (0.59%), `122.114` × 644 (0.16%)
- `humidity_out_of_range` (299,084 rows): `-3.91901` × 212,232 (70.96%), `-4.0` × 63,356 (21.18%), `-3.83804` × 12,081 (4.04%)
- `voltage_out_of_range` (8 rows): `0.0130561` × 1 (12.50%), `0.0180369` × 1 (12.50%), `0.00910083` × 1 (12.50%)

Usable values per metric (what the marts aggregate):

| metric | usable | share of clean |
|---|---|---|
| temperature | 1,871,843 | 80.92% |
| humidity | 1,906,274 | 82.41% |
| light | 2,219,799 | 95.96% |
| voltage | 2,313,143 | 100.00% |

## 5. Mote health (registered motes)

Network considered stopped at **2004-04-02 00:00:00** (last hour with ≥ 10% of registered motes reporting). A mote is `dead` if silent for more than 24 h before that.

| status | motes |
|---|---|
| alive_at_end | 25 |
| dead | 29 |

Dead motes. *Low battery since* is empty when the daily average voltage never fell below 2.30 V, meaning the mote stopped for a reason other than a flat battery:

| mote | last seen | hours silent before end | delivery while alive | last-day avg V | low battery since |
|---|---|---|---|---|---|
| 15 | 2004-03-03 | 699.0 | 0.1681 | 2.165 | 2004-03-03 |
| 5 | 2004-03-03 | 702.8 | 0.0026 | 2.208 | 2004-03-03 |
| 50 | 2004-03-11 | 509.5 | 0.4296 | 2.558 |  |
| 8 | 2004-03-14 | 438.1 | 0.3710 | 2.116 | 2004-03-13 |
| 18 | 2004-03-15 | 416.0 | 0.7374 | 2.024 | 2004-03-10 |
| 20 | 2004-03-15 | 429.1 | 0.6227 | 1.967 | 2004-03-14 |
| 17 | 2004-03-21 | 285.6 | 0.5320 | 2.061 | 2004-03-16 |
| 16 | 2004-03-21 | 269.1 | 0.5282 | 2.145 | 2004-03-17 |
| 45 | 2004-03-24 | 210.0 | 0.7336 | 2.288 | 2004-03-24 |
| 19 | 2004-03-25 | 180.5 | 0.5183 | 2.090 | 2004-03-19 |
| 35 | 2004-03-26 | 164.3 | 0.6575 | 2.065 | 2004-03-19 |
| 6 | 2004-03-29 | 78.9 | 0.4038 | 1.987 | 2004-03-20 |
| 12 | 2004-03-29 | 89.5 | 0.2651 | 2.137 | 2004-03-23 |
| 32 | 2004-03-29 | 90.9 | 0.4959 | 2.154 | 2004-03-23 |
| 28 | 2004-03-30 | 50.2 | 0.5060 | 2.210 | 2004-03-26 |
| 46 | 2004-03-30 | 49.8 | 0.5771 | 1.981 | 2004-03-20 |
| 4 | 2004-03-30 | 64.1 | 0.4860 | 2.095 | 2004-03-22 |
| 21 | 2004-03-30 | 65.3 | 0.6505 | 2.082 | 2004-03-22 |
| 22 | 2004-03-30 | 62.8 | 0.6665 | 2.062 | 2004-03-21 |
| 31 | 2004-03-30 | 48.0 | 0.7138 | 2.051 | 2004-03-24 |
| 54 | 2004-03-31 | 36.7 | 0.3789 | 2.029 | 2004-03-23 |
| 3 | 2004-03-31 | 44.7 | 0.5045 | 2.099 | 2004-03-24 |
| 11 | 2004-03-31 | 43.1 | 0.4517 | 2.019 | 2004-03-22 |
| 13 | 2004-03-31 | 47.6 | 0.3422 | 1.986 | 2004-03-21 |
| 14 | 2004-03-31 | 41.9 | 0.3429 | 2.113 | 2004-03-25 |
| 27 | 2004-03-31 | 42.8 | 0.4060 | 2.074 | 2004-03-23 |
| 51 | 2004-03-31 | 45.5 | 0.4576 | 2.042 | 2004-03-23 |
| 53 | 2004-03-31 | 41.0 | 0.3029 | 2.048 | 2004-03-23 |
| 1 | 2004-03-31 | 43.0 | 0.4647 | 2.061 | 2004-03-23 |

## 6. Network-wide outages

Hours in which no registered mote reported at all (base station / logging outage, not individual mote failures). These hours also count as gaps in each mote's health metrics.

| from | to | hours |
|---|---|---|
| 2004-03-10 10:00:00 | 2004-03-11 09:00:00 | 23 |
| 2004-03-15 17:00:00 | 2004-03-16 11:00:00 | 18 |

## 7. Motes with the highest share of flagged readings

| mote | readings | flagged | flagged % | most common flag | rows with it |
|---|---|---|---|---|---|
| 5 | 35 | 35 | 100.0 | `humidity_missing` | 35 |
| 28 | 46,441 | 46,441 | 100.0 | `light_missing` | 46,441 |
| 26 | 61,521 | 26,970 | 43.8 | `temperature_out_of_range` | 17,816 |
| 18 | 35,312 | 14,268 | 40.4 | `humidity_out_of_range` | 11,396 |
| 30 | 41,129 | 16,490 | 40.1 | `temperature_out_of_range` | 14,237 |
