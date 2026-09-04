from __future__ import annotations

import ast
import hashlib
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from generate_dummy_data import generate
from pipeline_utils import check_batch_status


ROOT = Path(__file__).resolve().parents[1]


def rows(path: Path) -> list[dict[str, str]]:
    import csv

    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


class IncrementalPipelineTests(unittest.TestCase):
    def test_same_date_and_seed_are_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            generate(date(2026, 9, 1), "initial", 42, 2, Path(first), False)
            generate(date(2026, 9, 1), "initial", 42, 2, Path(second), False)
            for entity in ("customers", "promotions", "orders", "order_items"):
                first_hash = hashlib.sha256((Path(first) / "2026-09-01" / f"{entity}.csv").read_bytes()).hexdigest()
                second_hash = hashlib.sha256((Path(second) / "2026-09-01" / f"{entity}.csv").read_bytes()).hexdigest()
                self.assertEqual(first_hash, second_hash)

    def test_delta_is_referentially_valid_and_has_late_inserts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output_dir = Path(temporary)
            generate(date(2026, 9, 1), "initial", 42, 2, output_dir, False)
            generate(date(2026, 9, 2), "delta", 42, 2, output_dir, False)
            initial = {
                entity: rows(output_dir / "2026-09-01" / f"{entity}.csv")
                for entity in ("customers", "promotions", "orders", "order_items")
            }
            delta = {
                entity: rows(output_dir / "2026-09-02" / f"{entity}.csv")
                for entity in ("customers", "promotions", "orders", "order_items")
            }
            self.assertEqual(
                sum(row["business_date"] < "2026-09-02" for row in delta["orders"] if row["operation"] == "I"),
                2,
            )
            for entity, key in {
                "customers": "customer_id",
                "promotions": "promotion_id",
                "orders": "order_id",
                "order_items": "order_item_id",
            }.items():
                self.assertEqual(len(delta[entity]), len({row[key] for row in delta[entity]}))
            current = {
                entity: {row[key] for row in initial[entity]}
                for entity, key in {
                    "customers": "customer_id",
                    "promotions": "promotion_id",
                    "orders": "order_id",
                    "order_items": "order_item_id",
                }.items()
            }
            for entity, key in {
                "customers": "customer_id",
                "promotions": "promotion_id",
                "orders": "order_id",
                "order_items": "order_item_id",
            }.items():
                current[entity] |= {row[key] for row in delta[entity] if row["operation"] in {"I", "U"}}
                current[entity] -= {row[key] for row in delta[entity] if row["operation"] == "D"}
            for row in delta["orders"]:
                if row["operation"] != "D":
                    self.assertIn(row["customer_id"], current["customers"])
                    if row["promotion_id"]:
                        self.assertIn(row["promotion_id"], current["promotions"])
            for row in delta["order_items"]:
                if row["operation"] != "D":
                    self.assertIn(row["order_id"], current["orders"])
                    self.assertIn(row["product_id"], {item["product_id"] for item in initial["order_items"]})

    def test_existing_batch_is_not_silently_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output_dir = Path(temporary)
            generate(date(2026, 9, 1), "initial", 42, 2, output_dir, False)
            path = output_dir / "2026-09-01" / "customers.csv"
            path.write_bytes(path.read_bytes() + b"\n")
            with self.assertRaises(FileExistsError):
                generate(date(2026, 9, 1), "initial", 42, 2, output_dir, False)

    def test_auto_mode_selects_initial_then_delta(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output_dir = Path(temporary)
            initial = generate(date(2026, 9, 1), "auto", 42, 2, output_dir, False)
            delta = generate(date(2026, 9, 2), "auto", 42, 2, output_dir, False)
            self.assertEqual(initial["mode"], "initial")
            self.assertEqual(delta["mode"], "delta")

    def test_order_updates_keep_valid_status_transitions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output_dir = Path(temporary)
            generate(date(2026, 9, 1), "initial", 42, 2, output_dir, False)
            generate(date(2026, 9, 2), "delta", 42, 2, output_dir, False)
            before = {row["order_id"]: row for row in rows(output_dir / "2026-09-01" / "orders.csv")}
            after = rows(output_dir / "2026-09-02" / "orders.csv")
            for row in after:
                if row["operation"] != "U":
                    continue
                previous = before[row["order_id"]]["order_status"]
                current = row["order_status"]
                self.assertIn(current, {"PAID", "PENDING", "CANCEL", "ERROR", "INVALID"})
                self.assertTrue(
                    current == previous
                    or (previous == "PENDING" and current in {"PAID", "CANCEL"})
                    or (previous in {"ERROR", "INVALID"} and current == "CANCEL")
                )

    def test_dag_is_valid_python_and_uses_logical_date(self) -> None:
        source = (ROOT / "airflow" / "dags" / "retail_pipeline_dag.py").read_text(encoding="utf-8")
        ast.parse(source)
        self.assertIn("{{ ds }}", source)
        self.assertIn('schedule="*/5 * * * *"', source)
        self.assertIn("complete_pipeline", source)

    def test_dbt_short_circuit_status_and_dependency(self) -> None:
        logical_date = "2026-09-01"
        batch_id = "retail_daily:2026-09-01"

        def run_check(status: str | None):
            engine = MagicMock()
            connection = engine.connect.return_value.__enter__.return_value
            result = MagicMock()
            result.mappings.return_value.one_or_none.return_value = (
                None if status is None else {"batch_id": batch_id, "status": status}
            )
            connection.execute.return_value = result
            with patch("pipeline_utils.create_db_engine", return_value=engine):
                outcome = check_batch_status(logical_date)
            return outcome, connection

        with self.assertLogs("pipeline_utils", level="INFO") as completed_logs:
            completed, connection = run_check("completed")
        self.assertFalse(completed)
        self.assertIn(
            "Batch retail_daily:2026-09-01 is already completed; skipping dbt rebuild and tests.",
            "\n".join(completed_logs.output),
        )
        self.assertEqual(
            connection.execute.call_args.args[1],
            {"pipeline_name": "retail_daily", "logical_date": logical_date},
        )

        with self.assertLogs("pipeline_utils", level="INFO") as ingested_logs:
            ingested, _ = run_check("ingested")
        self.assertTrue(ingested)
        self.assertIn(
            "Batch retail_daily:2026-09-01 is ingested; running dbt rebuild and tests.",
            "\n".join(ingested_logs.output),
        )

        with self.assertRaisesRegex(RuntimeError, "No ingestion batch found"):
            run_check(None)
        for status in ("started", "failed"):
            with self.subTest(status=status):
                with self.assertRaisesRegex(RuntimeError, "Unexpected ingestion batch status"):
                    run_check(status)

        source = (ROOT / "airflow" / "dags" / "retail_pipeline_dag.py").read_text(encoding="utf-8")
        self.assertRegex(
            source,
            r"load_bronze\s+>>\s+should_run_dbt\s+>>\s+dbt_prep\s+>>\s+dbt_run\s+>>\s+dbt_test\s+>>\s+complete_pipeline",
        )


if __name__ == "__main__":
    unittest.main()
