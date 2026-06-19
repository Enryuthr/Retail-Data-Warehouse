import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
ENV_FILE = BASE_DIR / ".env"
OUTPUT_DIR = BASE_DIR / "outputs"
REPORT_DIR = BASE_DIR / "reports"

CSV_FILES = {
    "orders": DATA_DIR / "orders.csv",
    "customers": DATA_DIR / "customers.csv",
    "order_items": DATA_DIR / "order_items.csv",
    "promotions": DATA_DIR / "promotions.csv",
}

EXPORT_TABLES = {
    "silver": [
        "silver_customers",
        "silver_orders",
        "silver_order_items",
        "silver_promotions",
        "data_quality_report",
    ],
    "gold": [
        "dim_customer",
        "dim_date",
        "dim_product",
        "dim_promotion",
        "fact_orders",
        "fact_order_items",
    ],
}


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
