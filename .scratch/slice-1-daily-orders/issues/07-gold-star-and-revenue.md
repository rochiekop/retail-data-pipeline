# 07: Gold star schema, daily Revenue by Product Category, reconcile

**What to build:** The Gold star schema and the first business answer. `fact_order_items` holds one row per Order Item for the Business Date, flagged with whether it counts as Revenue. `dim_product` resolves each Product's category: the English name, else the original name, else Uncategorized. `dim_date` is a calendar. The view `daily_revenue_by_category` gives Revenue (price excluding Freight, without canceled or unavailable Orders), Order count and Order Item count per day and category. The pipeline now has four steps: bronze, silver, facts, dimensions. A reconcile command compares Gold Revenue with Revenue computed directly from the Shop Database and prints MATCH or MISMATCH.

**Blocked by:** 06: Silver — Orders and Order Items, typed

**Status:** ready-for-agent

**Reference:** `../plan.md` Task 8 (star schema) and Task 9 (reconcile). ADR 0004 explains star vs snowflake. Review Focus item 5 applies.

- [ ] `fact_order_items` has one row per Order Item, with canceled and unavailable Orders flagged as not counting
- [ ] `dim_product` maps: has translation → English name; has no translation (`pc_gamer`) → original name; no category → `Uncategorized`
- [ ] `dim_date` covers 2016-01-01 to 2018-12-31
- [ ] The Revenue view excludes Freight and canceled or unavailable Orders
- [ ] Rerunning all steps for a date leaves Gold unchanged, and different dates don't affect each other
- [ ] Reconcile returns MATCH when Gold is correct and MISMATCH (exit code 1) when Gold rows are missing

**How to check it yourself:**
- Tests: `.venv/Scripts/python -m pytest -v` all pass
- For the fixture date `2017-11-24`, the view shows toys `100.00`, Uncategorized `50.00`, pc_gamer `20.00` (total 170.00)
- With real data seeded: `pipeline.run --business-date 2017-11-24 --step all`, then `pipeline.reconcile --from 2017-11-24 --to 2017-11-24` prints MATCH
