"""Explicitly clear only this project's local Bronze/control rows."""

from __future__ import annotations

import argparse
import shutil

from sqlalchemy import text

from load_bronze import ensure_database
from pipeline_utils import INCOMING_DIR, create_db_engine


def reset_dev_data(confirm: bool) -> None:
    if not confirm:
        raise SystemExit("Refusing to reset data without --yes")
    engine = create_db_engine()
    ensure_database(engine)
    with engine.begin() as connection:
        connection.execute(text("""
            truncate table
                bronze.customers_raw,
                bronze.promotions_raw,
                bronze.orders_raw,
                bronze.order_items_raw,
                control.ingested_files,
                control.ingestion_batches,
                control.pipeline_watermarks
            restart identity
        """))
    if INCOMING_DIR.exists():
        shutil.rmtree(INCOMING_DIR)
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", action="store_true", help="confirm the local dummy-data reset")
    args = parser.parse_args()
    reset_dev_data(args.yes)
    print("Cleared only Bronze and ingestion-control rows in the configured database.")


if __name__ == "__main__":
    main()
