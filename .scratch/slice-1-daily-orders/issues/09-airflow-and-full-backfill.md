# 09: Airflow orchestration and full Olist backfill

**What to build:** Airflow runs the pipeline. A daily DAG runs bronze → silver → facts → dimensions for each Business Date. The dimensions step runs one at a time across parallel runs (a 1-slot pool), and each task retries once. The real Olist data is seeded, one date is run first so every table exists, then every date from 2016-09-04 to 2018-10-17 is backfilled. Gold Revenue for the whole range matches the Shop Database, and rerunning a date leaves Gold identical. A README runbook explains setup, running, backfill and tests. This ticket completes all five done criteria in the spec.

**Blocked by:** 08: Quarantine and Silver Payments

**Status:** ready-for-human

This ticket needs you for the Kaggle download and for watching a backfill that takes hours.

**Reference:** `../plan.md` Task 10. Spec "Done when" 1–5.

- [ ] `docker compose up` starts Airflow alongside the databases, and the DAG loads with no import errors
- [ ] One Business Date (`2017-11-24`) runs green in Airflow
- [ ] The full backfill completes
- [ ] Reconcile over the full range prints MATCH, and the quarantined row count is reported (and explained if not 0)
- [ ] Rerunning `2017-11-24` produces the same Gold hash as before
- [ ] The README runbook is written

**How to check it yourself:**
- The Airflow UI at http://localhost:8080 shows green runs
- `.venv/Scripts/python -m pipeline.reconcile --from 2016-09-04 --to 2018-10-17` prints MATCH
