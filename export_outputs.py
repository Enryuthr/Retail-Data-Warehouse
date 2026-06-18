import logging

import pandas as pd
from sqlalchemy import text

from pipeline_utils import EXPORT_TABLES, OUTPUT_DIR, create_db_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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
    for schema_name, table_names in EXPORT_TABLES.items():
        for table_name in table_names:
            export_table(engine, schema_name, table_name)


if __name__ == "__main__":
    main()
