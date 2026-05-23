from pathlib import Path
import sqlite3

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from fastavro import writer, parse_schema


BASE_DIR = Path("/Users/sonu/Documents/gcp-pyspark-etl-lab")

RAW_DIR = BASE_DIR / "data/raw/banking_dataset_kaggle/data"
CSV_DIR = RAW_DIR / "csv"
DB_PATH = RAW_DIR / "database/bank_sqlite.db"

LANDING_DIR = BASE_DIR / "data/landing/banking_sources"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_csv_file(table_name: str) -> pd.DataFrame:
    file_path = CSV_DIR / f"{table_name}.csv"

    if not file_path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    return pd.read_csv(file_path)


def write_csv_sources() -> None:
    csv_tables = ["customers", "branches", "cards"]

    for table_name in csv_tables:
        df = read_csv_file(table_name)

        target_dir = LANDING_DIR / table_name
        ensure_dir(target_dir)

        df.to_csv(target_dir / f"{table_name}.csv", index=False)


def write_json_sources() -> None:
    json_tables = ["accounts", "merchants"]

    for table_name in json_tables:
        df = read_csv_file(table_name)

        target_dir = LANDING_DIR / table_name
        ensure_dir(target_dir)

        df.to_json(
            target_dir / f"{table_name}.json",
            orient="records",
            lines=True
        )


def write_loans_as_avro() -> None:
    df = read_csv_file("loans")

    target_dir = LANDING_DIR / "loans"
    ensure_dir(target_dir)

    df = df.astype(str)

    schema = {
        "type": "record",
        "name": "LoanRecord",
        "namespace": "banking.loan_servicing",
        "fields": [
            {"name": column_name, "type": ["null", "string"]}
            for column_name in df.columns
        ],
    }

    records = df.to_dict(orient="records")

    with open(target_dir / "loans.avro", "wb") as avro_file:
        writer(avro_file, parse_schema(schema), records)


def write_transactions_as_parquet() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"SQLite database not found: {DB_PATH}")

    target_dir = LANDING_DIR / "transactions"
    ensure_dir(target_dir)

    with sqlite3.connect(DB_PATH) as connection:
        df = pd.read_sql_query("SELECT * FROM transactions", connection)

    parquet_table = pa.Table.from_pandas(df)
    pq.write_table(parquet_table, target_dir / "transactions.parquet")


def main() -> None:
    write_csv_sources()
    write_json_sources()
    write_loans_as_avro()
    write_transactions_as_parquet()

    print(f"Prepared multi-format source files at: {LANDING_DIR}")


if __name__ == "__main__":
    main()