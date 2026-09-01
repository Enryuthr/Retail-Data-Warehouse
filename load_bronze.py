"""Load one date-partitioned source batch into append-only Bronze."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import sys
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import Boolean, Date, DateTime, Float, Integer, Numeric, inspect, text

from pipeline_utils import (
    BASE_DIR,
    ENTITY_SPECS,
    MIGRATION_FILE,
    TRACKING_COLUMNS,
    create_db_engine,
)

LOG = logging.getLogger(__name__)
BRONZE_SCHEMA = "bronze"
META_COLUMNS = [
    "batch_id", "logical_date", "source_file", "file_checksum", "source_updated_at",
    "ingested_at", "operation", "business_date", "source_row_number", "source_record_hash",
]
STATUS_VALUES = {"I", "U", "D"}


def quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def parse_logical_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from exc


def parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid source_updated_at: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"source_updated_at must include a UTC offset: {value!r}")
    return parsed.astimezone(timezone.utc)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_file_name(logical_date: date, entity: str) -> str:
    return f"data/incoming/{logical_date.isoformat()}/{entity}.csv"


def source_record_hash(entity: str, row: dict[str, str]) -> str:
    values = [
        entity,
        *(row.get(column, "") for column in ENTITY_SPECS[entity]["columns"]),
        row.get("operation", ""),
        row.get("source_updated_at", ""),
        row.get("business_date", ""),
    ]
    return hashlib.sha256("\x1f".join(values).encode("utf-8")).hexdigest()


def read_batch(logical_date: date, source_dir: Path) -> tuple[dict[str, list[dict[str, str]]], dict[str, str]]:
    rows_by_entity: dict[str, list[dict[str, str]]] = {}
    checksums: dict[str, str] = {}
    for entity, spec in ENTITY_SPECS.items():
        path = source_dir / f"{entity}.csv"
        if not path.exists():
            raise FileNotFoundError(f"Missing source file: {path}")
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            expected = spec["columns"] + TRACKING_COLUMNS
            if reader.fieldnames != expected:
                raise ValueError(
                    f"Unexpected header in {path}. Expected {expected}, got {reader.fieldnames}"
                )
            rows = []
            for row_number, row in enumerate(reader, start=2):
                normalized = {column: (row.get(column) or "") for column in expected}
                normalized["operation"] = normalized["operation"].strip().upper()
                normalized["_source_row_number"] = str(row_number)
                rows.append(normalized)
        rows_by_entity[entity] = rows
        checksums[source_file_name(logical_date, entity)] = sha256_file(path)
    return rows_by_entity, checksums


def migration_sql() -> str:
    if not MIGRATION_FILE.exists():
        raise FileNotFoundError(f"Missing database migration: {MIGRATION_FILE}")
    return MIGRATION_FILE.read_text(encoding="utf-8")


def ensure_database(engine) -> None:
    with engine.begin() as connection:
        raw_connection = connection.connection
        with raw_connection.cursor() as cursor:
            cursor.execute(migration_sql())
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS bronze")
        for entity, spec in ENTITY_SPECS.items():
            table = f"{BRONZE_SCHEMA}.{quote(spec['table'])}"
            columns = [f"{quote(column)} text" for column in spec["columns"]]
            columns.extend([
                '"batch_id" text',
                '"logical_date" date',
                '"source_file" text',
                '"file_checksum" text',
                '"source_updated_at" timestamptz',
                '"ingested_at" timestamptz',
                '"operation" text',
                '"business_date" date',
                '"source_row_number" integer',
                '"source_record_hash" text',
            ])
            connection.exec_driver_sql(
                f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)})"
            )
            for column in spec["columns"]:
                connection.exec_driver_sql(
                    f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {quote(column)} text"
                )
            connection.exec_driver_sql(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {quote('uq_' + spec['table'] + '_source_record_hash')} "
                f"ON {table} ({quote('source_record_hash')}) "
                f"WHERE {quote('source_record_hash')} IS NOT NULL"
            )


def existing_column_types(engine, entity: str) -> dict[str, object]:
    spec = ENTITY_SPECS[entity]
    return {
        column["name"]: column["type"]
        for column in inspect(engine).get_columns(spec["table"], schema=BRONZE_SCHEMA)
    }


def convert_value(value: str, column_type: object) -> object:
    if value == "":
        return None
    if isinstance(column_type, Boolean):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "yes", "y", "1"}:
            return True
        if normalized in {"false", "f", "no", "n", "0"}:
            return False
        raise ValueError(f"invalid boolean value: {value!r}")
    if isinstance(column_type, Integer):
        return int(value)
    if isinstance(column_type, (Float, Numeric)):
        return Decimal(value) if isinstance(column_type, Numeric) else float(value)
    if isinstance(column_type, Date):
        return date.fromisoformat(value)
    if isinstance(column_type, DateTime):
        return parse_timestamp(value)
    return value


def validate_batch(
    engine,
    logical_date: date,
    rows_by_entity: dict[str, list[dict[str, str]]],
) -> None:
    errors: list[str] = []
    for entity, rows in rows_by_entity.items():
        key_column = ENTITY_SPECS[entity]["key"]
        seen_keys = Counter()
        seen_hashes = Counter()
        for row in rows:
            key = row.get(key_column, "").strip()
            operation = row.get("operation", "").strip().upper()
            business_date = row.get("business_date", "").strip()
            source_updated_at = row.get("source_updated_at", "").strip()
            seen_keys[key] += 1
            seen_hashes[source_record_hash(entity, row)] += 1
            if not key:
                errors.append(f"{entity}: blank {key_column}")
            if operation not in STATUS_VALUES:
                errors.append(f"{entity} {key}: invalid operation {operation!r}")
            try:
                source_timestamp = parse_timestamp(source_updated_at)
                if source_timestamp.date() != logical_date:
                    errors.append(f"{entity} {key}: source_updated_at is not on logical date")
            except ValueError as exc:
                errors.append(f"{entity} {key}: {exc}")
            try:
                if date.fromisoformat(business_date) > logical_date:
                    errors.append(f"{entity} {key}: business_date is in the future")
            except ValueError:
                errors.append(f"{entity} {key}: invalid business_date {business_date!r}")
        errors.extend(
            f"{entity}: duplicate business key {key!r} in one file"
            for key, count in seen_keys.items()
            if key and count > 1
        )
        errors.extend(
            f"{entity}: duplicate source version {source_hash} in one file"
            for source_hash, count in seen_hashes.items()
            if count > 1
        )

    with engine.connect() as connection:
        current_keys: dict[str, set[str]] = {}
        # ponytail: full latest-key scan per batch; add maintained current-key tables only when volume makes it measurable.
        for entity, spec in ENTITY_SPECS.items():
            table = f"{BRONZE_SCHEMA}.{quote(spec['table'])}"
            key = quote(spec["key"])
            rows = connection.execute(text(f"""
                select {key}::text as business_key
                from (
                    select
                        {key},
                        operation,
                        row_number() over (
                            partition by {key}
                            order by source_updated_at desc nulls last,
                                     ingested_at desc nulls last,
                                     batch_id desc nulls last,
                                     source_record_hash desc nulls last
                        ) as row_num
                    from {table}
                    where {key} is not null
                ) latest
                where row_num = 1
                  and coalesce(operation, 'I') <> 'D'
            """)).scalars()
            current_keys[entity] = {str(value) for value in rows}

    available = {entity: set(keys) for entity, keys in current_keys.items()}
    for entity, rows in rows_by_entity.items():
        key_column = ENTITY_SPECS[entity]["key"]
        for row in rows:
            if row["operation"] in {"I", "U"}:
                available[entity].add(row[key_column])
            elif row["operation"] == "D":
                available[entity].discard(row[key_column])

    for entity, rows in rows_by_entity.items():
        key_column = ENTITY_SPECS[entity]["key"]
        for row in rows:
            operation = row["operation"]
            key = row[key_column]
            if operation in {"U", "D"} and key not in current_keys[entity]:
                errors.append(f"{entity} {key}: {operation} requires an existing business key")

    for row in rows_by_entity["orders"]:
        if row["operation"] == "D":
            continue
        if row["customer_id"] not in available["customers"]:
            errors.append(f"orders {row['order_id']}: unknown customer_id {row['customer_id']}")
        if row["promotion_id"] and row["promotion_id"] not in available["promotions"]:
            errors.append(f"orders {row['order_id']}: unknown promotion_id {row['promotion_id']}")
    for row in rows_by_entity["order_items"]:
        if row["operation"] == "D":
            continue
        if row["order_id"] not in available["orders"]:
            errors.append(f"order_items {row['order_item_id']}: unknown order_id {row['order_id']}")
        if row["product_id"] == "":
            errors.append(f"order_items {row['order_item_id']}: blank product_id")
        if row["promotion_id"] and row["promotion_id"] not in available["promotions"]:
            errors.append(f"order_items {row['order_item_id']}: unknown promotion_id {row['promotion_id']}")

    if errors:
        raise ValueError("Batch validation failed:\n- " + "\n- ".join(errors[:25]))


def batch_id(pipeline_name: str, logical_date: date) -> str:
    return f"{pipeline_name}:{logical_date.isoformat()}"


def get_batch(engine, pipeline_name: str, logical_date: date) -> dict[str, object] | None:
    with engine.connect() as connection:
        row = connection.execute(text("""
            select batch_id, status, rows_received, rows_inserted, rows_updated,
                   rows_deleted, rows_rejected, error_message
            from control.ingestion_batches
            where pipeline_name = :pipeline_name and logical_date = :logical_date
        """), {"pipeline_name": pipeline_name, "logical_date": logical_date}).mappings().first()
        return dict(row) if row else None


def completed_files_match(
    engine,
    pipeline_name: str,
    logical_date: date,
    checksums: dict[str, str],
) -> bool:
    with engine.connect() as connection:
        rows = connection.execute(text("""
            select source_file, file_checksum, status
            from control.ingested_files
            where pipeline_name = :pipeline_name and logical_date = :logical_date
        """), {"pipeline_name": pipeline_name, "logical_date": logical_date}).mappings()
        recorded = {row["source_file"]: (row["file_checksum"], row["status"]) for row in rows}
    return all(recorded.get(filename) == (checksum, "completed") for filename, checksum in checksums.items())


def start_batch(engine, pipeline_name: str, logical_date: date, checksums: dict[str, str]) -> str:
    identifier = batch_id(pipeline_name, logical_date)
    now = datetime.now(timezone.utc)
    manifest_checksum = hashlib.sha256("".join(sorted(checksums.values())).encode()).hexdigest()
    with engine.begin() as connection:
        existing = connection.execute(text("""
            select batch_id, status
            from control.ingestion_batches
            where pipeline_name = :pipeline_name and logical_date = :logical_date
            for update
        """), {"pipeline_name": pipeline_name, "logical_date": logical_date}).mappings().first()
        if existing and existing["status"] in {"completed", "ingested"}:
            if not completed_files_match(engine, pipeline_name, logical_date, checksums):
                raise ValueError("A successful batch exists for this date with different or incomplete files")
            return identifier
        if existing:
            connection.execute(text("""
                update control.ingestion_batches
                set status = 'started', started_at = :started_at, completed_at = null,
                    rows_received = 0, rows_inserted = 0, rows_updated = 0,
                    rows_deleted = 0, rows_rejected = 0, last_source_updated_at = null,
                    error_message = null, source_file = 'multiple', file_checksum = :file_checksum
                where batch_id = :batch_id
            """), {"started_at": now, "file_checksum": manifest_checksum, "batch_id": identifier})
        else:
            connection.execute(text("""
                insert into control.ingestion_batches
                    (batch_id, pipeline_name, logical_date, source_file, file_checksum, started_at, status)
                values
                    (:batch_id, :pipeline_name, :logical_date, 'multiple', :file_checksum, :started_at, 'started')
            """), {
                "batch_id": identifier,
                "pipeline_name": pipeline_name,
                "logical_date": logical_date,
                "file_checksum": manifest_checksum,
                "started_at": now,
            })
    return identifier


def mark_failed(engine, identifier: str, message: str) -> None:
    with engine.begin() as connection:
        connection.execute(text("""
            update control.ingestion_batches
            set status = 'failed', completed_at = :completed_at,
                rows_rejected = greatest(rows_rejected, 1), error_message = :error_message
            where batch_id = :batch_id
        """), {
            "completed_at": datetime.now(timezone.utc),
            "error_message": message[:4000],
            "batch_id": identifier,
        })


def load_transaction(
    engine,
    identifier: str,
    pipeline_name: str,
    logical_date: date,
    rows_by_entity: dict[str, list[dict[str, str]]],
    checksums: dict[str, str],
) -> dict[str, int]:
    ingested_at = datetime.now(timezone.utc)
    totals = Counter()
    latest_source_updated_at: datetime | None = None
    with engine.begin() as connection:
        for entity, spec in ENTITY_SPECS.items():
            table = f"{BRONZE_SCHEMA}.{quote(spec['table'])}"
            source_file = source_file_name(logical_date, entity)
            column_types = existing_column_types(engine, entity)
            target_columns = spec["columns"] + META_COLUMNS
            stage_name = f"stage_{entity}_{identifier.replace(':', '_').replace('-', '')}"
            connection.exec_driver_sql(
                f"CREATE TEMP TABLE {quote(stage_name)} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            stage_columns = ", ".join(quote(column) for column in target_columns)
            placeholders = ", ".join(f":v_{index}" for index in range(len(target_columns)))
            stage_insert = text(
                f"insert into {quote(stage_name)} ({stage_columns}) values ({placeholders})"
            )
            payload = []
            for row in rows_by_entity[entity]:
                source_updated_at = parse_timestamp(row["source_updated_at"])
                parsed_business_date = date.fromisoformat(row["business_date"])
                typed_business = [
                    convert_value(row.get(column, ""), column_types[column])
                    for column in spec["columns"]
                ]
                values = typed_business + [
                    identifier,
                    logical_date,
                    source_file,
                    checksums[source_file],
                    source_updated_at,
                    ingested_at,
                    row["operation"],
                    parsed_business_date,
                    int(row["_source_row_number"]),
                    source_record_hash(entity, row),
                ]
                payload.append({f"v_{index}": value for index, value in enumerate(values)})
                latest_source_updated_at = max(latest_source_updated_at, source_updated_at) if latest_source_updated_at else source_updated_at
            if payload:
                connection.execute(stage_insert, payload)
            target_list = ", ".join(quote(column) for column in target_columns)
            result = connection.execute(text(f"""
                insert into {table} ({target_list})
                select {target_list}
                from {quote(stage_name)}
                on conflict ({quote('source_record_hash')})
                    where {quote('source_record_hash')} is not null
                    do nothing
                returning operation
            """))
            inserted_operations = Counter(row[0] for row in result.fetchall())
            totals["rows_received"] += len(payload)
            totals["rows_inserted"] += inserted_operations["I"]
            totals["rows_updated"] += inserted_operations["U"]
            totals["rows_deleted"] += inserted_operations["D"]
            connection.execute(text("""
                insert into control.ingested_files (
                    pipeline_name, logical_date, source_file, batch_id, file_checksum,
                    started_at, completed_at, status, rows_received, rows_inserted,
                    rows_updated, rows_deleted, rows_rejected, last_source_updated_at
                ) values (
                    :pipeline_name, :logical_date, :source_file, :batch_id, :file_checksum,
                    :started_at, :completed_at, 'completed', :rows_received, :rows_inserted,
                    :rows_updated, :rows_deleted, 0, :last_source_updated_at
                )
                on conflict (pipeline_name, logical_date, source_file) do update set
                    batch_id = excluded.batch_id,
                    file_checksum = excluded.file_checksum,
                    started_at = excluded.started_at,
                    completed_at = excluded.completed_at,
                    status = excluded.status,
                    rows_received = excluded.rows_received,
                    rows_inserted = excluded.rows_inserted,
                    rows_updated = excluded.rows_updated,
                    rows_deleted = excluded.rows_deleted,
                    rows_rejected = excluded.rows_rejected,
                    last_source_updated_at = excluded.last_source_updated_at,
                    error_message = null
            """), {
                "pipeline_name": pipeline_name,
                "logical_date": logical_date,
                "source_file": source_file,
                "batch_id": identifier,
                "file_checksum": checksums[source_file],
                "started_at": ingested_at,
                "completed_at": ingested_at,
                "rows_received": len(payload),
                "rows_inserted": inserted_operations["I"],
                "rows_updated": inserted_operations["U"],
                "rows_deleted": inserted_operations["D"],
                "last_source_updated_at": max(
                    (parse_timestamp(row["source_updated_at"]) for row in rows_by_entity[entity]),
                    default=None,
                ),
            })

        connection.execute(text("""
            update control.ingestion_batches
            set status = 'ingested', completed_at = :completed_at,
                rows_received = :rows_received, rows_inserted = :rows_inserted,
                rows_updated = :rows_updated, rows_deleted = :rows_deleted,
                rows_rejected = 0, last_source_updated_at = :last_source_updated_at,
                error_message = null
            where batch_id = :batch_id
        """), {
            "completed_at": ingested_at,
            "rows_received": totals["rows_received"],
            "rows_inserted": totals["rows_inserted"],
            "rows_updated": totals["rows_updated"],
            "rows_deleted": totals["rows_deleted"],
            "last_source_updated_at": latest_source_updated_at,
            "batch_id": identifier,
        })
    return {key: int(value) for key, value in totals.items()}


def load_bronze(
    logical_date: date,
    source_dir: Path,
    pipeline_name: str,
) -> dict[str, object]:
    engine = create_db_engine()
    ensure_database(engine)
    rows_by_entity, checksums = read_batch(logical_date, source_dir)
    identifier = batch_id(pipeline_name, logical_date)
    existing = get_batch(engine, pipeline_name, logical_date)
    if existing and existing["status"] in {"completed", "ingested"}:
        if not completed_files_match(engine, pipeline_name, logical_date, checksums):
            raise ValueError("A successful batch exists for this date with different or incomplete files")
        LOG.info("Batch %s already %s; no-op", identifier, existing["status"])
        return {"batch_id": identifier, "status": existing["status"], "idempotent_noop": True}

    start_batch(engine, pipeline_name, logical_date, checksums)
    try:
        validate_batch(engine, logical_date, rows_by_entity)
        totals = load_transaction(engine, identifier, pipeline_name, logical_date, rows_by_entity, checksums)
    except Exception as exc:
        mark_failed(engine, identifier, str(exc))
        raise
    result = {"batch_id": identifier, "status": "ingested", "idempotent_noop": False, **totals}
    LOG.info("Bronze batch loaded: %s", json.dumps(result, sort_keys=True))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=parse_logical_date)
    parser.add_argument("--source-dir", type=Path)
    parser.add_argument("--pipeline-name", default="retail_daily")
    args = parser.parse_args()
    source_dir = args.source_dir or BASE_DIR / "data" / "incoming" / args.date.isoformat()
    if not source_dir.is_absolute():
        source_dir = BASE_DIR / source_dir
    load_bronze(args.date, source_dir, args.pipeline_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    try:
        main()
    except Exception as exc:
        LOG.error("Bronze load failed: %s", exc)
        sys.exit(1)
