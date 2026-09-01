import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
INCOMING_DIR = DATA_DIR / "incoming"
ENV_FILE = BASE_DIR / ".env"
REPORT_DIR = BASE_DIR / "reports"
MIGRATION_FILE = BASE_DIR / "docker" / "postgres" / "init" / "02_incremental_ingestion.sql"

CSV_FILES = {
    "orders": DATA_DIR / "orders.csv",
    "customers": DATA_DIR / "customers.csv",
    "order_items": DATA_DIR / "order_items.csv",
    "promotions": DATA_DIR / "promotions.csv",
}

ENTITY_SPECS = {
    "customers": {
        "table": "customers_raw",
        "key": "customer_id",
        "columns": [
            "customer_id", "first_name", "last_name", "email", "phone_number",
            "registration_date", "customer_segment", "preferred_channel", "age_group",
            "income_level", "avg_order_value", "promo_sensitivity", "email_opt_in",
            "sms_opt_in", "push_opt_in", "last_purchase_date", "total_lifetime_orders",
            "preferred_category",
        ],
    },
    "promotions": {
        "table": "promotions_raw",
        "key": "promotion_id",
        "columns": [
            "promotion_id", "campaign_name", "promo_type", "discount_value",
            "min_order_value", "start_date", "end_date", "campaign_duration_days",
            "target_segment", "campaign_channel", "campaign_objective", "target_category",
            "expected_response_rate", "budget_allocated", "cost_per_acquisition",
        ],
    },
    "orders": {
        "table": "orders_raw",
        "key": "order_id",
        "columns": [
            "order_id", "customer_id", "order_date", "order_status", "promotion_id",
            "order_channel", "order_value", "attributed_to_promo", "customer_segment_at_time",
        ],
    },
    "order_items": {
        "table": "order_items_raw",
        "key": "order_item_id",
        "columns": [
            "order_item_id", "order_id", "product_id", "product_category", "quantity",
            "unit_price", "promotion_id", "attributed_to_promo",
        ],
    },
}

TRACKING_COLUMNS = [
    "operation", "source_updated_at", "business_date",
]

def read_env_file(file_path: Path = ENV_FILE) -> dict[str, str]:
    if not file_path.exists():
        return {}

    entries = {}
    for line in file_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        entries[key.strip()] = value.strip().strip('"').strip("'")

    return entries


def load_env_file(file_path: Path = ENV_FILE) -> None:
    for key, value in read_env_file(file_path).items():
        os.environ.setdefault(key, value)


def env_with_file(file_path: Path = ENV_FILE) -> dict[str, str]:
    env = os.environ.copy()
    for key, value in read_env_file(file_path).items():
        env.setdefault(key, value)
    return env


def create_db_engine():
    load_env_file()
    return create_engine(
        URL.create(
            "postgresql",
            username=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB", "retail_dw"),
        )
    )
