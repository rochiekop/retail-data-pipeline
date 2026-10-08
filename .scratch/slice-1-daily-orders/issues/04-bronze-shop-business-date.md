# 04: Bronze — one Business Date from the Shop Database

**What to build:** For a given Business Date, the Orders purchased that day, with their Order Items and Payments, are extracted from the Shop Database and loaded unchanged into the warehouse's Bronze layer. Every source column is stored as text, and each row is tagged with its Business Date and the time of the load. Loading the same date again adds a new load and keeps the old one for auditing. Orders at the edge of the day land on the correct date: 23:59:59 belongs to that day, and 00:00:00 belongs to the next.

**Blocked by:** 03: Merchandising Catalog built

**Status:** ready-for-agent

**Reference:** `../plan.md` Task 4. Review Focus item 3 applies. ADR 0003 explains why ClickHouse and why loads are kept.

- [ ] Only Orders purchased on the Business Date are loaded, together with their Order Items and Payments
- [ ] All source columns in Bronze are text (`Nullable(String)`)
- [ ] Each row carries its Business Date and load time
- [ ] A second load of the same date is added alongside the first, not over it
- [ ] A day with no Orders loads nothing and doesn't fail
- [ ] Every load is recorded in `bronze.shop_loads`, even when it finds nothing, so a rerun that now finds no rows replaces the older load

**How to check it yourself:**
- Tests: `.venv/Scripts/python -m pytest tests/test_bronze_shop.py -v` shows 6 passed
- Look at the data: `docker compose exec warehouse-test clickhouse-client --user warehouse --password warehouse -q "select * from bronze.orders limit 5"` right after the tests, or against `warehouse` once real data is loaded
