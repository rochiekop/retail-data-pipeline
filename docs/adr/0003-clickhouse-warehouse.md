# ClickHouse as the warehouse, ready for Kafka

The warehouse (Bronze, Silver, Gold) is ClickHouse, a columnar OLAP database. The Shop Database stays Postgres, because it plays the transactional system. We considered a second Postgres (familiar, enforces uniqueness, has transactions) and DuckDB (columnar, but a single file allows only one writer at a time, which conflicts with parallel Airflow backfill runs). ClickHouse gives the standard industry shape for this project: a transactional source → Kafka → a columnar warehouse. Its built-in Kafka engine can later read topics directly into the same Bronze tables.

## Consequences

- **Reruns replace partitions.** Silver and Gold tables are partitioned by Business Date, and a rerun drops that day's partition before inserting. ClickHouse handles row-level updates and deletes poorly, so this is the idiomatic replacement.
- **Uniqueness is not enforced.** ClickHouse has no primary-key constraints and no multi-table transactions. dbt tests guard uniqueness, and each Bronze table's load is atomic on its own.
- **LEFT JOINs fill defaults, not NULLs**, unless `join_use_nulls = 1`. The dbt profile sets it, and Silver checks rely on it.
- **Streaming will change Silver's read rule.** Batch Silver reads the latest Bronze *load* per Business Date. A Kafka stream lands many small loads, so the streaming slice must switch Silver to the latest *row per key*.
