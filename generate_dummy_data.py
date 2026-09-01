"""Generate deterministic initial and daily delta CSV batches."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import random
import tempfile
from copy import deepcopy
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from pipeline_utils import (
    BASE_DIR,
    CSV_FILES,
    ENTITY_SPECS,
    INCOMING_DIR,
    TRACKING_COLUMNS,
)

LOG = logging.getLogger(__name__)
DEFAULT_NEW_COUNTS = {"customers": 5, "promotions": 3, "orders": 20}
VALID_STATUSES = {"PAID", "PENDING", "CANCEL", "ERROR", "INVALID"}


def parse_logical_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from exc


def parse_source_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {path}")
        return [
            {column: (value or "") for column, value in row.items()}
            for row in reader
        ]


def base_state() -> dict[str, dict[str, dict[str, object]]]:
    state: dict[str, dict[str, dict[str, object]]] = {}
    for name, spec in ENTITY_SPECS.items():
        rows = read_csv(CSV_FILES[name])
        entities: dict[str, dict[str, object]] = {}
        for row in rows:
            key = row.get(spec["key"], "").strip()
            if not key or key in entities:
                raise ValueError(f"Invalid or duplicate {name} key in {CSV_FILES[name]}: {key!r}")
            entities[key] = {"row": {column: row.get(column, "") for column in spec["columns"]}, "active": True}
        state[name] = entities
    return state


def clone_state(state: dict[str, dict[str, dict[str, object]]]) -> dict[str, dict[str, dict[str, object]]]:
    return deepcopy(state)


def stable_rng(seed: int, logical_date: date, scope: str) -> random.Random:
    token = f"{seed}|{logical_date.isoformat()}|{scope}".encode("utf-8")
    return random.Random(int(hashlib.sha256(token).hexdigest()[:16], 16))


def numeric_key(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        return 2**63 - 1


def next_id(entities: dict[str, dict[str, object]]) -> str:
    ids = [numeric_key(key) for key in entities]
    return str(max(ids, default=0) + 1)


def active_ids(state: dict[str, dict[str, dict[str, object]]], entity: str) -> list[str]:
    return sorted(
        (key for key, value in state[entity].items() if value["active"]),
        key=numeric_key,
    )


def iso_timestamp(logical_date: date, sequence: int) -> str:
    value = datetime.combine(logical_date, time(0, 0), tzinfo=timezone.utc) + timedelta(microseconds=sequence)
    return value.isoformat().replace("+00:00", "Z")


def business_date(entity: str, row: dict[str, str], logical_date: date, orders: dict[str, dict[str, object]] | None = None) -> date:
    date_column = {
        "customers": "registration_date",
        "promotions": "start_date",
        "orders": "order_date",
    }.get(entity)
    if entity == "order_items" and orders:
        order = orders.get(row.get("order_id", ""))
        if order:
            return business_date("orders", order["row"], logical_date)
    if date_column:
        parsed = parse_source_date(row.get(date_column))
        if parsed and parsed <= logical_date:
            return parsed
    return logical_date


def add_change(
    state: dict[str, dict[str, dict[str, object]]],
    changes: dict[str, list[dict[str, object]]],
    entity: str,
    row: dict[str, str],
    operation: str,
    event_date: date,
) -> None:
    changes[entity].append({"row": row.copy(), "operation": operation, "business_date": event_date})
    key = row[ENTITY_SPECS[entity]["key"]]
    if operation == "D":
        if key in state[entity]:
            state[entity][key] = {"row": row.copy(), "active": False}
    else:
        state[entity][key] = {"row": row.copy(), "active": True}


def apply_batch(
    state: dict[str, dict[str, dict[str, object]]],
    rows_by_entity: dict[str, list[dict[str, str]]],
) -> None:
    for entity, rows in rows_by_entity.items():
        key_column = ENTITY_SPECS[entity]["key"]
        for source_row in rows:
            row = {column: source_row.get(column, "") for column in ENTITY_SPECS[entity]["columns"]}
            key = row[key_column]
            if source_row.get("operation", "I") == "D":
                if key in state[entity]:
                    state[entity][key] = {"row": row, "active": False}
            else:
                state[entity][key] = {"row": row, "active": True}


def initial_rows(logical_date: date) -> dict[str, list[dict[str, str]]]:
    state = base_state()
    rows_by_entity: dict[str, list[dict[str, str]]] = {}
    for entity, spec in ENTITY_SPECS.items():
        rows = []
        for index, key in enumerate(sorted(state[entity], key=numeric_key)):
            row = state[entity][key]["row"]
            rows.append(
                {
                    **row,
                    "operation": "I",
                    "source_updated_at": iso_timestamp(logical_date, index),
                    "business_date": business_date(entity, row, logical_date, state["orders"]).isoformat(),
                }
            )
        rows_by_entity[entity] = rows
    return rows_by_entity


def new_customer(customer_id: str, logical_date: date, rng: random.Random) -> dict[str, str]:
    categories = ["Beauty", "Books", "Clothing", "Electronics", "Home", "Sports"]
    segments = ["New_Customer", "Low_Value", "Medium_Value", "High_Value"]
    channels = ["Direct", "Email", "Push", "SMS", "Social"]
    return {
        "customer_id": customer_id,
        "first_name": f"Daily{customer_id}",
        "last_name": "Customer",
        "email": f"daily{customer_id}@example.com",
        "phone_number": f"62812{int(customer_id):08d}",
        "registration_date": f"{logical_date.isoformat()} 08:00:00",
        "customer_segment": rng.choice(segments),
        "preferred_channel": rng.choice(channels),
        "age_group": rng.choice(["18-25", "26-35", "36-45", "46-55", "55+"]),
        "income_level": rng.choice(["Low", "Medium", "High", "Premium"]),
        "avg_order_value": f"{rng.uniform(40, 300):.2f}",
        "promo_sensitivity": f"{rng.uniform(0.2, 0.9):.3f}",
        "email_opt_in": "TRUE",
        "sms_opt_in": "FALSE",
        "push_opt_in": "TRUE",
        "last_purchase_date": "",
        "total_lifetime_orders": "0",
        "preferred_category": rng.choice(categories),
    }


def new_promotion(promotion_id: str, logical_date: date, rng: random.Random) -> dict[str, str]:
    promo_type = rng.choice(["BOGO", "Fixed_Amount", "Free_Shipping", "Percentage_Discount"])
    start = logical_date - timedelta(days=1)
    end = logical_date + timedelta(days=rng.randint(20, 45))
    return {
        "promotion_id": promotion_id,
        "campaign_name": f"Daily_{promo_type}_{promotion_id}",
        "promo_type": promo_type,
        "discount_value": f"{rng.uniform(5, 50):.2f}",
        "min_order_value": f"{rng.uniform(20, 150):.2f}",
        "start_date": f"{start.isoformat()} 00:00:00",
        "end_date": f"{end.isoformat()} 23:59:59",
        "campaign_duration_days": str((end - start).days),
        "target_segment": rng.choice(["All_Customers", "High_Value", "Low_Value", "Medium_Value", "New_Customer"]),
        "campaign_channel": rng.choice(["Email", "In_App", "Push", "SMS", "Social", "Website_Banner"]),
        "campaign_objective": rng.choice(["Acquisition", "Cross_Sell", "Reactivation", "Retention", "Upsell"]),
        "target_category": rng.choice(["Beauty", "Books", "Clothing", "Electronics", "Home", "Sports"]),
        "expected_response_rate": f"{rng.uniform(0.05, 0.4):.3f}",
        "budget_allocated": f"{rng.uniform(5000, 50000):.2f}",
        "cost_per_acquisition": f"{rng.uniform(10, 60):.2f}",
    }


def new_order(
    order_id: str,
    customer: dict[str, object],
    promotion_id: str,
    event_date: date,
    rng: random.Random,
) -> dict[str, str]:
    attributed = bool(promotion_id)
    status = rng.choices(["PAID", "PENDING", "CANCEL"], weights=[75, 20, 5], k=1)[0]
    return {
        "order_id": order_id,
        "customer_id": customer["row"]["customer_id"],
        "order_date": f"{event_date.isoformat()} {rng.randint(8, 20):02d}:{rng.randint(0, 59):02d}:00",
        "order_status": status,
        "promotion_id": promotion_id,
        "order_channel": rng.choice(["Direct", "Email", "Push", "SMS", "Social"]),
        "order_value": f"{rng.uniform(25, 500):.2f}",
        "attributed_to_promo": "TRUE" if attributed else "FALSE",
        "customer_segment_at_time": customer["row"].get("customer_segment", "Unknown"),
    }


def make_delta(
    logical_date: date,
    state: dict[str, dict[str, dict[str, object]]],
    seed: int,
    late_records: int,
) -> tuple[dict[str, list[dict[str, str]]], dict[str, dict[str, dict[str, object]]]]:
    working = clone_state(state)
    raw_changes: dict[str, list[dict[str, object]]] = {entity: [] for entity in ENTITY_SPECS}

    customer_rng = stable_rng(seed, logical_date, "customers")
    existing_customer_ids = active_ids(working, "customers")
    for _ in range(DEFAULT_NEW_COUNTS["customers"]):
        customer_id = next_id(working["customers"])
        add_change(working, raw_changes, "customers", new_customer(customer_id, logical_date, customer_rng), "I", logical_date)
    update_ids = customer_rng.sample(existing_customer_ids, min(2, len(existing_customer_ids)))
    for customer_id in update_ids:
        row = working["customers"][customer_id]["row"].copy()
        row["preferred_channel"] = customer_rng.choice(["Direct", "Email", "Push", "SMS", "Social"])
        row["last_purchase_date"] = f"{logical_date.isoformat()} 12:00:00"
        add_change(working, raw_changes, "customers", row, "U", logical_date)

    promotion_rng = stable_rng(seed, logical_date, "promotions")
    existing_promotion_ids = active_ids(working, "promotions")
    for _ in range(DEFAULT_NEW_COUNTS["promotions"]):
        promotion_id = next_id(working["promotions"])
        add_change(working, raw_changes, "promotions", new_promotion(promotion_id, logical_date, promotion_rng), "I", logical_date)
    update_ids = promotion_rng.sample(existing_promotion_ids, min(1, len(existing_promotion_ids)))
    for promotion_id in update_ids:
        row = working["promotions"][promotion_id]["row"].copy()
        try:
            budget = float(row.get("budget_allocated") or 0)
        except ValueError:
            budget = 0
        row["budget_allocated"] = f"{budget + 100:.2f}"
        add_change(working, raw_changes, "promotions", row, "U", logical_date)

    order_rng = stable_rng(seed, logical_date, "orders")
    existing_order_ids = active_ids(working, "orders")
    customer_ids = active_ids(working, "customers")
    promotion_ids = active_ids(working, "promotions")
    new_order_rows: list[tuple[dict[str, str], date]] = []
    for index in range(DEFAULT_NEW_COUNTS["orders"] + late_records):
        order_id = next_id(working["orders"])
        customer_id = order_rng.choice(customer_ids)
        promotion_id = order_rng.choice(promotion_ids) if order_rng.random() < 0.35 else ""
        event_date = logical_date - timedelta(days=1 + index % 3) if index < late_records else logical_date
        row = new_order(order_id, working["customers"][customer_id], promotion_id, event_date, order_rng)
        add_change(working, raw_changes, "orders", row, "I", event_date)
        new_order_rows.append((row, event_date))

    update_ids = order_rng.sample(existing_order_ids, min(2, len(existing_order_ids)))
    for order_id in update_ids:
        row = working["orders"][order_id]["row"].copy()
        current_status = row.get("order_status", "").upper()
        if current_status == "PENDING":
            row["order_status"] = "PAID"
        elif current_status not in VALID_STATUSES:
            row["order_status"] = "CANCEL"
        else:
            try:
                row["order_value"] = f"{float(row.get('order_value') or 0) + 5:.2f}"
            except ValueError:
                row["order_value"] = "5.00"
        add_change(working, raw_changes, "orders", row, "U", logical_date)

    delete_candidates = [order_id for order_id in existing_order_ids if order_id not in update_ids]
    if delete_candidates:
        order_id = order_rng.choice(delete_candidates)
        row = working["orders"][order_id]["row"].copy()
        add_change(working, raw_changes, "orders", row, "D", logical_date)

    item_rng = stable_rng(seed, logical_date, "order_items")
    existing_item_ids = active_ids(working, "order_items")
    products = [
        (value["row"].get("product_id", ""), value["row"].get("product_category", "Unknown"))
        for value in working["order_items"].values()
        if value["active"] and value["row"].get("product_id")
    ]
    products = list(dict.fromkeys(products)) or [("1", "Unknown")]
    new_item_rows: list[tuple[dict[str, str], date]] = []
    for order_row, event_date in new_order_rows:
        item_id = next_id(working["order_items"])
        product_id, category = item_rng.choice(products)
        promotion_id = order_row["promotion_id"]
        row = {
            "order_item_id": item_id,
            "order_id": order_row["order_id"],
            "product_id": product_id,
            "product_category": category,
            "quantity": str(item_rng.randint(1, 4)),
            "unit_price": f"{item_rng.uniform(10, 350):.2f}",
            "promotion_id": promotion_id,
            "attributed_to_promo": order_row["attributed_to_promo"],
        }
        add_change(working, raw_changes, "order_items", row, "I", event_date)
        new_item_rows.append((row, event_date))

    update_ids = item_rng.sample(existing_item_ids, min(2, len(existing_item_ids)))
    deleted_order_ids = {
        change["row"]["order_id"]
        for change in raw_changes["orders"]
        if change["operation"] == "D"
    }
    for item_id in update_ids:
        row = working["order_items"][item_id]["row"].copy()
        if row.get("order_id") in deleted_order_ids:
            continue
        try:
            row["quantity"] = str(int(row.get("quantity") or 1) + 1)
        except ValueError:
            row["quantity"] = "2"
        add_change(working, raw_changes, "order_items", row, "U", logical_date)

    delete_candidates = [
        item_id
        for item_id in existing_item_ids
        if item_id not in update_ids
        and working["order_items"][item_id]["row"].get("order_id") not in deleted_order_ids
    ]
    if delete_candidates:
        item_id = item_rng.choice(delete_candidates)
        row = working["order_items"][item_id]["row"].copy()
        add_change(working, raw_changes, "order_items", row, "D", logical_date)

    rows_by_entity: dict[str, list[dict[str, str]]] = {}
    entity_offsets = {entity: index * 100_000 for index, entity in enumerate(ENTITY_SPECS)}
    for entity, changes in raw_changes.items():
        key_column = ENTITY_SPECS[entity]["key"]
        changes.sort(key=lambda change: (numeric_key(change["row"][key_column]), change["operation"]))
        rows_by_entity[entity] = [
            {
                **change["row"],
                "operation": change["operation"],
                "source_updated_at": iso_timestamp(logical_date, entity_offsets[entity] + index),
                "business_date": change["business_date"].isoformat(),
            }
            for index, change in enumerate(changes)
        ]
    return rows_by_entity, working


def state_from_initial_or_base(output_dir: Path, target_date: date, seed: int, late_records: int) -> dict[str, dict[str, dict[str, object]]]:
    initial_dirs = []
    for manifest_path in output_dir.glob("*/manifest.json"):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest.get("mode") == "initial":
                initial_dirs.append((date.fromisoformat(manifest["logical_date"]), manifest_path.parent))
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue

    if not initial_dirs:
        return base_state()

    initial_date, initial_dir = min(initial_dirs)
    if target_date <= initial_date:
        raise ValueError(f"delta date {target_date} must be after initial date {initial_date}")
    state = base_state()
    apply_batch(state, {entity: read_csv(initial_dir / f"{entity}.csv") for entity in ENTITY_SPECS})
    # ponytail: replay missing dates in memory; persist a state store only if backfills become slow.
    current = initial_date + timedelta(days=1)
    while current < target_date:
        candidate = output_dir / current.isoformat()
        manifest_path = candidate / "manifest.json"
        if manifest_path.exists() and all((candidate / f"{entity}.csv").exists() for entity in ENTITY_SPECS):
            rows = {entity: read_csv(candidate / f"{entity}.csv") for entity in ENTITY_SPECS}
            apply_batch(state, rows)
        else:
            _, state = make_delta(current, state, seed, late_records)
        current += timedelta(days=1)
    return state


def csv_bytes(entity: str, rows: list[dict[str, str]]) -> bytes:
    columns = ENTITY_SPECS[entity]["columns"] + TRACKING_COLUMNS
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows({column: row.get(column, "") for column in columns} for row in rows)
    return output.getvalue().encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def write_batch(
    output_dir: Path,
    logical_date: date,
    mode: str,
    seed: int,
    late_records: int,
    rows_by_entity: dict[str, list[dict[str, str]]],
    force: bool,
) -> dict[str, object]:
    target_dir = output_dir / logical_date.isoformat()
    target_dir.mkdir(parents=True, exist_ok=True)
    payloads = {
        f"{entity}.csv": csv_bytes(entity, rows)
        for entity, rows in rows_by_entity.items()
    }
    checksums = {
        filename: sha256_bytes(payload)
        for filename, payload in payloads.items()
    }
    manifest = {
        "logical_date": logical_date.isoformat(),
        "mode": mode,
        "seed": seed,
        "late_records": late_records,
        "files": checksums,
        "row_counts": {entity: len(rows) for entity, rows in rows_by_entity.items()},
        "operation_counts": {
            entity: {
                operation: sum(row["operation"] == operation for row in rows)
                for operation in ["I", "U", "D"]
            }
            for entity, rows in rows_by_entity.items()
        },
    }
    manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")

    expected_files = {**payloads, "manifest.json": manifest_bytes}
    mismatches = []
    for filename, payload in expected_files.items():
        path = target_dir / filename
        if path.exists() and path.read_bytes() != payload:
            mismatches.append(str(path))
    if mismatches and not force:
        raise FileExistsError(
            "Generated batch already exists with different content. "
            f"Use --force only after checking it: {', '.join(mismatches)}"
        )

    for filename, payload in expected_files.items():
        path = target_dir / filename
        if path.exists() and not force and path.read_bytes() == payload:
            continue
        with tempfile.NamedTemporaryFile("wb", dir=target_dir, prefix=f".{filename}.", delete=False) as handle:
            handle.write(payload)
            temporary_path = Path(handle.name)
        temporary_path.replace(path)
    return manifest


def generate(
    logical_date: date,
    mode: str,
    seed: int,
    late_records: int,
    output_dir: Path,
    force: bool,
) -> dict[str, object]:
    if late_records < 0:
        raise ValueError("late_records must be >= 0")
    initial_dirs = []
    for manifest_path in output_dir.glob("*/manifest.json"):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest.get("mode") == "initial":
                initial_dirs.append(date.fromisoformat(manifest["logical_date"]))
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue

    if mode == "auto":
        mode = "initial" if not initial_dirs or logical_date in initial_dirs else "delta"
    if mode == "initial":
        rows = initial_rows(logical_date)
    elif mode == "delta":
        state = state_from_initial_or_base(output_dir, logical_date, seed, late_records)
        rows, _ = make_delta(logical_date, state, seed, late_records)
    else:
        raise ValueError(f"unsupported mode: {mode}")
    manifest = write_batch(output_dir, logical_date, mode, seed, late_records, rows, force)
    LOG.info("Generated %s batch for %s: %s", mode, logical_date, manifest["row_counts"])
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=parse_logical_date)
    parser.add_argument("--mode", choices=["auto", "initial", "delta"], default="auto")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--late-records", type=int, default=2)
    parser.add_argument("--output-dir", type=Path, default=INCOMING_DIR)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    output_dir = args.output_dir if args.output_dir.is_absolute() else BASE_DIR / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    generate(args.date, args.mode, args.seed, args.late_records, output_dir, args.force)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    main()
