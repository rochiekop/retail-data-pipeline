# Slice 1: Daily Orders batch pipeline — design

Vocabulary: see `CONTEXT.md`. Decisions with lasting weight: `docs/adr/` (in particular 0003 ClickHouse and 0004 star schema).

## Goal

The first end-to-end batch ELT path: for one Business Date, move Orders, Order Items and Payments from the Shop Database, plus the Merchandising Catalog, through Bronze → Silver → Gold in a ClickHouse warehouse. It ends in a Gold star schema and a report view of **daily Revenue by Product Category**.

## Architecture

```
Shop Database (Postgres) ─┐
                          ├─ extract + load (Python) ─▶ Bronze ─ transform (dbt) ─▶ Silver ─▶ Gold star ─▶ report views
Merchandising Catalog ────┘                            └──────────── ClickHouse warehouse ────────────┘
(Excel)                                              (later: Kafka ─▶ ClickHouse Kafka engine ─▶ Bronze)
```

## Decisions

- **Process:** ELT. Data lands unchanged in Bronze, and every transformation runs as SQL in the warehouse (dbt).
- **Dataset:** Olist Brazilian E-commerce (Kaggle `olistbr/brazilian-ecommerce`), downloaded manually to `data/olist/raw/`.
- **Shop Database:** Postgres, seeded from the Olist CSVs (orders, order items, payments, customers, sellers, reviews) into schema `shop` with proper types.
- **Merchandising Catalog:** one Excel workbook generated from the Olist products and category-translation CSVs, with sheets `products` and `category_translation`.
- **Warehouse:** ClickHouse with databases `bronze`, `silver`, `gold`. MergeTree tables.
- **Stack:** Docker Compose (Postgres for the shop and Airflow metadata, ClickHouse for the warehouse plus a separate ClickHouse for tests), Python for extract and load, dbt (dbt-clickhouse) for Silver and Gold, Airflow 3 for scheduling and backfill.
- **Business Date:** the date part of `order_purchase_timestamp` (stored in São Paulo local time). A run for D covers Orders purchased on D.
- **Bronze:** append-only. Every source column is stored as `Nullable(String)`, unmodified. Shop rows carry `_business_date` and `_loaded_at`; catalog rows carry `_source_checksum` and `_loaded_at`. An unchanged catalog file (same checksum) is not reloaded.
- **Silver:** reads the latest Bronze load for the Business Date (latest catalog load for products), then casts, deduplicates and checks the rows. Rows that fail go to `silver.quarantine` with a reason and their Bronze `loaded_at`. The full record stays in Bronze.
- **Gold (star schema):**
  - `fact_order_items`: Order Item grain. Columns: business_date, order_id, order_item_id, product_id, seller_id, order_status, price, freight, counts_as_revenue.
  - `dim_product`: product_id, its category name, and the resolved Product Category.
  - `dim_date`: a calendar covering 2016 to 2018.
  - `daily_revenue_by_category`: a view over the fact and `dim_product`.
- **Failure policy:** structural problems (missing file, missing sheet, changed columns) fail the run. Bad rows are quarantined and the run continues.
- **Reruns:** Silver and Gold facts are partitioned by Business Date. A run drops its date's partition and then inserts, so running a date twice changes nothing. Bronze keeps every load for auditing. Dimensions and views are rebuilt in a step that runs one at a time across parallel runs (an Airflow pool with 1 slot).
- **Revenue:** sum of Order Item price, excluding Freight, for Orders whose status is not `canceled` or `unavailable`.
- **Product Category in Gold:** English translation, else the original category name, else `Uncategorized`.

## Done when

1. `docker compose up` brings up the Shop Database, ClickHouse and Airflow.
2. The Shop Database is seeded and the Merchandising Catalog is generated from Olist.
3. An Airflow run for one Business Date goes bronze → silver → facts → dimensions.
4. A backfill over all Olist dates completes, and the Gold Revenue total matches a SQL check against the Shop Database.
5. Rerunning a Business Date leaves Gold unchanged.

## Deferred

- **Streaming:** Kafka topics for Orders and Reviews (replayed from the Shop Database), consumed by a ClickHouse Kafka engine table into Bronze. Silver will need to switch from the latest load to the latest row per key (ADR 0003).
- Lookback re-extraction for late status changes.
- Category history (SCD2).
- `dim_customer` and `dim_seller`, and Reviews.
- Late Delivery features and Average Order Value.
