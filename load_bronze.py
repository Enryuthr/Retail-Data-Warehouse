import logging
import os
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import URL

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
ENV_FILE = BASE_DIR / ".env"
def load_env_file(file_path):
    if not file_path.exists():
        return

    for line in file_path.read_text().splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

# ==================================================
# LOGGING SETUP
# ==================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ==================================================
# DATABASE CONFIG
# ==================================================

load_env_file(ENV_FILE)

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

if not DB_PASSWORD:
    logging.error("Set POSTGRES_PASSWORD before running the pipeline.")
    sys.exit(1)

# ==================================================
# CREATE DATABASE CONNECTION
# ==================================================

engine = create_engine(
    URL.create(
        "postgresql",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
)

with engine.begin() as connection:
    connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))

# ==================================================
# CSV FILE CONFIGURATION
# ==================================================

csv_files = {
    "orders": DATA_DIR / "orders.csv",
    "customers": DATA_DIR / "customers.csv",
    "order_items": DATA_DIR / "order_items.csv",
    "promotions": DATA_DIR / "promotions.csv"
}

# ==================================================
# LOAD CSV TO POSTGRESQL
# ==================================================

try:

    missing_files = [
        file_path for file_path in csv_files.values()
        if not file_path.exists()
    ]

    if missing_files:
        missing_list = ", ".join(str(file_path) for file_path in missing_files)
        raise FileNotFoundError(
            f"Missing CSV file(s): {missing_list}. "
            f"Create the data folder here: {DATA_DIR}"
        )

    for table_name, file_path in csv_files.items():

        logging.info(f"Reading file: {file_path}")

        # read csv
        df = pd.read_csv(file_path)

        logging.info(f"Rows loaded from {table_name}: {len(df)}")

        # load to postgres
        df.to_sql(
            name=f"{table_name}_raw",
            con=engine,
            schema="bronze",
            if_exists="replace",
            index=False
        )

        logging.info(
            f"Successfully loaded bronze.{table_name}_raw"
        )

    logging.info("All bronze tables loaded successfully!")

except Exception as e:

    logging.error(f"Pipeline failed: {e}")
    sys.exit(1)
