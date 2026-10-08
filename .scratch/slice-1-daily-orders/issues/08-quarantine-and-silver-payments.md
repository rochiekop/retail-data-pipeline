# 08: Quarantine and Silver Payments

**What to build:** Bad rows are no longer silently left out. They're recorded in `silver.quarantine` with the entity, the record key, the failure reason and the Bronze load they came from, so anyone can find the original record. Payments also reach Silver, typed, with their own checks: invalid sequence, duplicate, unknown or quarantined parent Order, unreadable or negative value. Rerunning a date after the source is fixed clears that date's Quarantine rows, even when the new run quarantines nothing. Gold Revenue doesn't change.

**Blocked by:** 07: Gold star schema, daily Revenue by Product Category, reconcile

**Status:** ready-for-agent

**Reference:** `../plan.md` Task 6 (Quarantine model and Orders Quarantine tests) and Task 7 (Payments checks, `silver.payments`, Order Item and Payment Quarantine tests). Review Focus item 4 applies.

- [ ] Each failure kind for Orders, Order Items and Payments produces one Quarantine row with the expected reason
- [ ] `silver.payments` holds the date's valid Payments with proper types
- [ ] After a fixed source is reloaded and Silver rerun, that date has no Quarantine rows
- [ ] Gold Revenue for the date is the same as before this ticket

**How to check it yourself:**
- Tests: `.venv/Scripts/python -m pytest -v` all pass
- Insert a bad row by hand (e.g. an Order Item with price `-5.00`) into the test warehouse's Bronze, run the silver step, and see it in `silver.quarantine` with reason `negative price`
