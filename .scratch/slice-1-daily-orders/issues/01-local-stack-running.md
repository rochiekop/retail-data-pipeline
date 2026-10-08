# 01: Local stack running

**What to build:** A developer can start the project's local infrastructure with one command and run the test suite against it. The Shop Database (Postgres), the ClickHouse warehouse and a separate throwaway ClickHouse for tests all start in Docker. The Python project installs, and connection settings come from environment variables with local defaults. Tests never touch dev data: they use a `shop_test` database and the test ClickHouse.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

**Reference:** spec `../spec.md`; exact code in `../plan.md` Task 1. Read `CONTEXT.md` and ADR 0003 first.

- [x] The repo is a git repository with the scaffold committed (including `CONTEXT.md`, `CLAUDE.md`, `docs/` and `.scratch/`)
- [x] Docker Compose starts the Shop Database, the warehouse and the test warehouse
- [x] Connection settings default to the local Compose ports and can be overridden by environment variables
- [x] The tests confirm both databases are reachable

**How to check it yourself:**
- `docker compose ps` shows `shop-db`, `warehouse` and `warehouse-test` running
- `.venv/Scripts/python -m pytest -v` shows 4 passed
