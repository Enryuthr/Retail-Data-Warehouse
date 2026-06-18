import json
import logging

import pandas as pd

from pipeline_utils import CSV_FILES, REPORT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def parse_date(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    iso_date = text.where(text.str.match(r"^\d{4}-\d{2}-\d{2}")).str.slice(0, 10)
    return pd.to_datetime(iso_date, errors="coerce", format="%Y-%m-%d")


def parse_bool(series: pd.Series) -> pd.Series:
    normalized = series.astype(str).str.strip().str.lower()
    return normalized.isin(["true", "t", "yes", "y", "1"])


def table_profile(table_name: str, df: pd.DataFrame) -> dict:
    return {
        "table_name": table_name,
        "row_count": int(len(df)),
        "column_types": {column: str(dtype) for column, dtype in df.dtypes.items()},
        "null_counts": df.isna().sum().astype(int).to_dict(),
        "duplicate_row_count": int(df.duplicated().sum()),
        "sample_rows": df.head(5).astype(object).where(pd.notna(df), None).to_dict(orient="records"),
    }


def bad_data_summary(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    customers = tables["customers"]
    orders = tables["orders"]
    order_items = tables["order_items"]
    promotions = tables["promotions"]

    promotion_start_dates = parse_date(promotions["start_date"])
    promotion_end_dates = parse_date(promotions["end_date"])

    checks = [
        ("customers", "invalid_registration_date", parse_date(customers["registration_date"]).isna() & customers["registration_date"].notna(), "medium"),
        ("customers", "negative_phone_number", customers["phone_number"].astype(str).str.startswith("-"), "medium"),
        ("customers", "missing_last_purchase_date", customers["last_purchase_date"].isna(), "low"),
        ("orders", "missing_promotion_id", orders["promotion_id"].isna(), "low"),
        ("orders", "promo_attributed_without_promotion_id", parse_bool(orders["attributed_to_promo"]) & orders["promotion_id"].isna(), "high"),
        ("orders", "negative_order_value", pd.to_numeric(orders["order_value"], errors="coerce") < 0, "high"),
        ("order_items", "invalid_quantity", pd.to_numeric(order_items["quantity"], errors="coerce") <= 0, "high"),
        ("order_items", "invalid_unit_price", pd.to_numeric(order_items["unit_price"], errors="coerce") < 0, "high"),
        ("order_items", "promo_attributed_without_promotion_id", parse_bool(order_items["attributed_to_promo"]) & order_items["promotion_id"].isna(), "high"),
        ("promotions", "negative_discount_value", pd.to_numeric(promotions["discount_value"], errors="coerce") < 0, "high"),
        ("promotions", "invalid_expected_response_rate", ~pd.to_numeric(promotions["expected_response_rate"], errors="coerce").between(0, 1), "medium"),
        ("promotions", "invalid_budget_or_cpa", (pd.to_numeric(promotions["budget_allocated"], errors="coerce") < 0) | (pd.to_numeric(promotions["cost_per_acquisition"], errors="coerce") < 0), "high"),
        ("promotions", "invalid_date_range", promotion_start_dates.isna() | promotion_end_dates.isna() | (promotion_end_dates < promotion_start_dates), "high"),
    ]

    rows = [
        {
            "table_name": table_name,
            "issue_type": issue_type,
            "affected_row_count": int(mask.fillna(False).sum()),
            "severity": severity,
        }
        for table_name, issue_type, mask, severity in checks
    ]

    return pd.DataFrame(rows).query("affected_row_count > 0")


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    tables = {name: pd.read_csv(path) for name, path in CSV_FILES.items()}

    profiles = [table_profile(name, df) for name, df in tables.items()]
    bad_data = bad_data_summary(tables)

    (REPORT_DIR / "raw_data_profile.json").write_text(json.dumps(profiles, indent=2, default=str))
    bad_data.to_csv(REPORT_DIR / "raw_bad_data_summary.csv", index=False)

    for profile in profiles:
        logging.info(
            "%s: %s rows, %s duplicate rows",
            profile["table_name"],
            profile["row_count"],
            profile["duplicate_row_count"],
        )

    if not bad_data.empty:
        logging.info("Raw data issues detected:\n%s", bad_data.to_string(index=False))


if __name__ == "__main__":
    main()
