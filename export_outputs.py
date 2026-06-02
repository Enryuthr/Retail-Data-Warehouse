import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "outputs"
ENV_FILE = BASE_DIR / ".env"

TABLES = {
    "silver": [
        "silver_customers",
        "silver_orders",
        "silver_order_items",
        "silver_promotions",
        "data_quality_report",
    ],
    "gold": [
        "gold_customer_360",
        "gold_promotion_performance",
        "gold_channel_performance",
        "gold_category_performance",
    ],
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_env_file(file_path: Path) -> None:
    if not file_path.exists():
        return

    for line in file_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def create_db_engine():
    load_env_file(ENV_FILE)
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


def export_table(engine, schema_name: str, table_name: str) -> None:
    layer_dir = OUTPUT_DIR / schema_name
    layer_dir.mkdir(parents=True, exist_ok=True)

    query = text(f'select * from "{schema_name}"."{table_name}"')
    df = pd.read_sql_query(query, engine)

    csv_path = layer_dir / f"{table_name}.csv"
    parquet_path = layer_dir / f"{table_name}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    logging.info("Exported %s.%s to %s and %s", schema_name, table_name, csv_path, parquet_path)


def main() -> None:
    engine = create_db_engine()
    for schema_name, table_names in TABLES.items():
        for table_name in table_names:
            export_table(engine, schema_name, table_name)


if __name__ == "__main__":
    main()
