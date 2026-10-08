"""Run one pipeline step for one Business Date."""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

from pipeline import config
from pipeline.bronze import ensure_bronze_tables, extract_catalog, extract_shop
from pipeline.warehouse import connect

STEPS = ("bronze", "silver", "facts", "dimensions")
DBT_SELECTORS = {
    "silver": "path:models/silver",
    "facts": "path:models/gold/facts",
    # Dimensions and report views are rebuilt whole, so only one run may do this at a time.
    "dimensions": "path:models/gold/dimensions path:models/gold/reports",
}
DBT_DIR = Path(os.getenv("DBT_DIR", Path(__file__).resolve().parents[2] / "dbt"))


def dbt_executable() -> Path:
    """dbt is installed next to the Python running this code (venv Scripts/ or bin/)."""
    return Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")


def _dbt_build(selector: str, business_date: date) -> None:
    # A private target/log dir per run lets several Business Dates run in parallel.
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(
            [
                str(dbt_executable()), "build",
                "--select", selector,
                "--vars", json.dumps({"business_date": business_date.isoformat()}),
                "--project-dir", str(DBT_DIR),
                "--profiles-dir", str(DBT_DIR),
                "--target-path", str(Path(tmp) / "target"),
                "--log-path", str(Path(tmp) / "logs"),
            ],
            check=True,
        )


def _require_bronze_load(business_date: date) -> None:
    # ClickHouse's max() over no rows is 1970, not NULL, so without this check a dbt step would
    # quietly build an empty Business Date when the bronze step never ran for it.
    client = connect()
    try:
        ensure_bronze_tables(client)
        shop_loads = client.query(
            "select count() from bronze.shop_loads where _business_date = {d:Date}",
            parameters={"d": business_date},
        ).result_rows[0][0]
        catalog_loads = client.query("select count() from bronze.catalog_loads").result_rows[0][0]
    finally:
        client.close()
    if not shop_loads or not catalog_loads:
        raise RuntimeError(f"No Bronze load for {business_date}: run the bronze step first")


def run_step(step: str, business_date: date) -> None:
    if step == "bronze":
        extract_catalog(config.catalog_path())
        extract_shop(business_date)
    elif step in DBT_SELECTORS:
        _require_bronze_load(business_date)
        _dbt_build(DBT_SELECTORS[step], business_date)
    else:
        raise ValueError(f"unknown step {step!r}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m pipeline.run")
    parser.add_argument("--business-date", required=True, type=date.fromisoformat)
    parser.add_argument("--step", required=True, choices=[*STEPS, "all"])
    args = parser.parse_args(argv)
    for step in STEPS if args.step == "all" else (args.step,):
        run_step(step, args.business_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
