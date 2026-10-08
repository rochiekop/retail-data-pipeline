# 06: Silver — Orders and Order Items, typed

**What to build:** One command, `pipeline.run --business-date D --step bronze|silver`, takes a Business Date from the sources through Bronze into Silver. Silver holds that day's Orders and Order Items with real types: timestamps, integers and two-decimal prices. It reads only the latest Bronze load and keeps one row per key. Rows that fail a check are left out of Silver. The checks are a bad timestamp or status, a duplicate, a negative or unreadable price, an unknown Order, an unknown Product or a quarantined parent Order. Recording those rows comes in ticket 08. Rerunning a date replaces that date in Silver and never duplicates it. dbt refuses to run without a Business Date.

**Blocked by:** 05: Bronze — Merchandising Catalog

**Status:** ready-for-agent

**Reference:** `../plan.md` Task 6 (run command, dbt project, macros, Orders) and Task 7 (catalog lookups, Order Items). In this ticket, build the checks models but **not** the Quarantine table. Leave the Quarantine tests for ticket 08. Global Constraints apply, especially `join_use_nulls`, the ClickHouse alias rule and unique CTE names.

- [ ] `silver.orders` and `silver.order_items` hold the Business Date's valid rows with proper types
- [ ] Silver reads only the latest Bronze load for the date
- [ ] Invalid rows (each check above) don't reach Silver
- [ ] Running the silver step twice for the same date leaves the same rows
- [ ] A malformed date on the command line is rejected, and dbt without a Business Date fails with a message naming `business_date`

**How to check it yourself:**
- Tests: `.venv/Scripts/python -m pytest -v` all pass
- Query `silver.order_items` for `2017-11-24` in the test warehouse: o1/1 has price `100.00` as a number, and there are 4 rows
