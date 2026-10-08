# 02: Shop Database seeded from Olist

**What to build:** The Olist CSVs can be loaded into the Shop Database with one command. This creates Orders, Order Items, Payments, Customers, Sellers and Reviews with proper types. Before loading, every file's columns are checked against the source contract. A changed or missing column fails loudly and names the file. Quoted empty values (Olist writes `""` for a missing timestamp) become NULL, and zip codes keep their leading zeros. Running the seed twice gives the same result.

**Blocked by:** 01: Local stack running

**Status:** ready-for-agent

**Reference:** `../plan.md` Task 2 (includes the tiny Olist-shaped test fixture). Review Focus item 1 applies.

- [ ] The seed loads all six Shop Database tables from a folder of Olist CSVs and reports a row count per table
- [ ] A missing quoted value loads as NULL, not an empty string
- [ ] Zip codes such as `04195` keep their leading zero
- [ ] A file with renamed columns fails with an error naming that file
- [ ] Seeding twice produces the same counts

**How to check it yourself:**
- Tests: `.venv/Scripts/python -m pytest tests/test_seed_shop_db.py -v` shows 5 passed
- Real data, optional at this point: download Olist into `data/olist/`, then run `.venv/Scripts/python -m pipeline.seed shop-db`. Expect about `orders: 99441, order_items: 112650, order_payments: 103886`.
