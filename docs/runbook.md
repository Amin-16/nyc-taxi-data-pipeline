# Runbook

## 1. How to run the pipeline end-to-end

**Batch & incremental (trips, weather, zone lookup, Silver, Gold):**

1. Confirm resources are live: ADLS (`stdatalakenyctaxi`), Databricks cluster attached, ADF linked services show green on Test Connection.
2. In ADF Studio → **Manage → Triggers**, confirm `tr_master_monthly_run` is **Started**.
3. It fires automatically once per month (Tumbling Window). To run manually/out-of-cycle: open `pl_master_orchestrator` → **Trigger now** → supply `p_window_start` (e.g. `"2026-01"`).
4. Watch progress in **ADF → Monitor**. Expected sequence: `pipeline_start` → `Check_Zone_Lookup` → `Ingest_Trips` ‖ `Ingest_Weather` → `Silver_Processing` → `Gold_Build` → `pipeline_end`.
5. Full run time: ingestion ~2-5 min, Silver ~2-3 min, Gold ~17-20 min (full Silver reprocess by design — see design-decisions doc).

**Streaming (must be started manually — not part of the master pipeline):**

1. Databricks → **Workflows → Jobs** → confirm/start the Continuous jobs for `03_stream_consume_trips` and `12_clean_stream_trips`.
2. Run `scripts/trip_event_producer.py` locally to generate simulated live events.
3. To refresh the streaming Gold mart: manually trigger `25_build_fact_trips_streaming.py` (its ADF schedule trigger is **deliberately kept Stopped** between sessions — start it manually, or trigger the notebook directly in Databricks).

**Verify a successful run:**

```python
spark.read.format("delta").load(".../bronze/_control/pipeline_run_log").orderBy(col("started_at").desc()).show(5)
```

Latest row should show `status = SUCCEEDED`.

---

## 2. How to recover from a failure

| Failed stage                      | Recovery                                                                                                                                                                                                                                                                                                          |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Ingest_Trips / Ingest_Weather** | Check the alert email (names the failed activity). Fix the root cause, then re-run `pl_master_orchestrator` for the same `p_window_start` — Get Metadata/If Condition (trips) or the watermark (weather) will safely resume without duplicating already-landed data.                                              |
| **Silver_Processing**             | If it's a **breaking schema change** (logged in `dq_results` as `breaking_schema_change_detected`), a human must review the source schema and update the notebook before re-running — this is intentional, not a bug. Otherwise, just re-run; Auto Loader checkpoints mean only new/unprocessed files are reread. |
| **Gold_Build**                    | Safe to simply re-run — all Gold writes are idempotent (hash-based keys + MERGE). No cleanup needed first.                                                                                                                                                                                                        |
| **Streaming jobs stop/crash**     | Restart the Continuous Job in Databricks Workflows — checkpointed state resumes automatically, no data loss, no duplicate processing (bounded by the event-time watermark).                                                                                                                                       |
| **Any stage, general**            | Never manually edit Gold/Silver data to "fix" a bad run — always fix the source/notebook and re-run. Every layer is designed to be safely re-runnable.                                                                                                                                                            |

---

## 3. What to check first when something breaks

1. **The alert email** — per-stage alerts (via Logic App `la-nyc-taxi-alerts`) name the specific failed activity and error message.
2. **ADF → Monitor** — click the failed pipeline run → the specific failed activity → **Output/Error** tab for the raw error.
3. **`pipeline_run_log`** — confirms which stage got a `FAILED` status and when.
4. **`dq_results`** — check for `CRITICAL` severity rows, especially `breaking_schema_change_detected`, before assuming it's an infra issue.
5. **`ingestion_log` / `watermark_log`** — confirm whether state actually advanced (it shouldn't have, if the run failed) — if it did advance on a failed run, that's a bug, not expected behavior.
6. **Databricks → Compute** — for cluster-related failures, check for quota errors (`AZURE_QUOTA_EXCEEDED_EXCEPTION`) — a known issue on this free-trial subscription if an interactive cluster and a job cluster try to run simultaneously. Fix: ensure only one cluster is active at a time, or confirm both are sized at `Standard_DS2_v2` (2-core) so they fit within the 4-core regional quota together.
7. **Databricks → Workflows → Jobs** — for streaming-specific issues, check the job's run history and `query.exception()` / query progress, separate from ADF Monitor entirely.
