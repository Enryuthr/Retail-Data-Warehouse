"""Mark an ingested date complete after dbt succeeds and advance its watermark."""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timezone

from sqlalchemy import text

from load_bronze import batch_id, ensure_database
from pipeline_utils import create_db_engine

LOG = logging.getLogger(__name__)


def parse_logical_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from exc


def complete_pipeline(logical_date: date, pipeline_name: str) -> dict[str, object]:
    engine = create_db_engine()
    ensure_database(engine)
    identifier = batch_id(pipeline_name, logical_date)
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        batch = connection.execute(text("""
            select batch_id, status, rows_received, last_source_updated_at
            from control.ingestion_batches
            where batch_id = :batch_id
            for update
        """), {"batch_id": identifier}).mappings().first()
        if not batch:
            raise ValueError(f"No ingestion batch exists for {identifier}")
        if batch["status"] == "completed":
            return {"batch_id": identifier, "status": "completed", "idempotent_noop": True}
        if batch["status"] != "ingested":
            raise ValueError(f"Batch {identifier} is {batch['status']}; dbt completion is not allowed")

        file_statuses = connection.execute(text("""
            select count(*) as file_count,
                   count(*) filter (where status = 'completed') as completed_count
            from control.ingested_files
            where batch_id = :batch_id
        """), {"batch_id": identifier}).mappings().one()
        if file_statuses["file_count"] != 4 or file_statuses["completed_count"] != 4:
            raise ValueError(f"Batch {identifier} does not have four completed source files")

        connection.execute(text("""
            update control.ingestion_batches
            set status = 'completed', completed_at = :completed_at
            where batch_id = :batch_id
        """), {"completed_at": now, "batch_id": identifier})
        connection.execute(text("""
            insert into control.pipeline_watermarks (
                pipeline_name, last_logical_date, batch_id, last_source_updated_at, updated_at
            ) values (
                :pipeline_name, :logical_date, :batch_id, :last_source_updated_at, :updated_at
            )
            on conflict (pipeline_name) do update set
                last_logical_date = excluded.last_logical_date,
                batch_id = excluded.batch_id,
                last_source_updated_at = excluded.last_source_updated_at,
                updated_at = excluded.updated_at
            where control.pipeline_watermarks.last_logical_date is null
               or control.pipeline_watermarks.last_logical_date <= excluded.last_logical_date
        """), {
            "pipeline_name": pipeline_name,
            "logical_date": logical_date,
            "batch_id": identifier,
            "last_source_updated_at": batch["last_source_updated_at"],
            "updated_at": now,
        })
    result = {"batch_id": identifier, "status": "completed", "idempotent_noop": False}
    LOG.info("Pipeline marked complete: %s", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=parse_logical_date)
    parser.add_argument("--pipeline-name", default="retail_daily")
    args = parser.parse_args()
    complete_pipeline(args.date, args.pipeline_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
