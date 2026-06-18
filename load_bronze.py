import logging
import sys

import pandas as pd
from sqlalchemy import text

from pipeline_utils import CSV_FILES, DATA_DIR, create_db_engine

# ==================================================
# LOGGING SETUP
# ==================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main() -> None:
    engine = create_db_engine()
    missing_files = [
        file_path for file_path in CSV_FILES.values()
        if not file_path.exists()
    ]

    if missing_files:
        missing_list = ", ".join(str(file_path) for file_path in missing_files)
        raise FileNotFoundError(
            f"Missing CSV file(s): {missing_list}. "
            f"Create the data folder here: {DATA_DIR}"
        )

    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))

    for table_name, file_path in CSV_FILES.items():
        logging.info("Reading file: %s", file_path)
        df = pd.read_csv(file_path)

        logging.info("Rows loaded from %s: %s", table_name, len(df))

        df.to_sql(
            name=f"{table_name}_raw",
            con=engine,
            schema="bronze",
            if_exists="replace",
            index=False,
        )

        logging.info("Successfully loaded bronze.%s_raw", table_name)

    logging.info("All bronze tables loaded successfully!")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logging.error("Pipeline failed: %s", exc)
        sys.exit(1)
