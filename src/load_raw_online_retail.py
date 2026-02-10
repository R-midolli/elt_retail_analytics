from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy import types as satypes


def getenv_required(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f"Missing env var: {key}")
    return v


def main() -> None:
    # Load .env automatically (works great on Windows + Git Bash)
    load_dotenv()

    # DB config (expected in .env)
    db_host = getenv_required("DB_HOST")
    db_port = getenv_required("DB_PORT")
    db_name = getenv_required("DB_NAME")
    db_user = getenv_required("DB_USER")
    db_password = getenv_required("DB_PASSWORD")

    # Source (original dataset)
    file_path = Path("data/raw/online_retail_II.xlsx")
    if not file_path.exists():
        raise FileNotFoundError(f"Missing file: {file_path}")

    # Read all sheets (two years) and concatenate
    sheets = pd.read_excel(file_path, sheet_name=None)
    df = pd.concat(sheets.values(), ignore_index=True)

    # Rename columns (based on your real headers)
    rename_map = {
        "Invoice": "invoice_no",
        "StockCode": "stock_code",
        "Description": "product_description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "Price": "unit_price",
        "Customer ID": "customer_id",
        "Country": "country",
    }
    df = df.rename(columns=rename_map)

    required = set(rename_map.values())
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing required columns after rename: {sorted(missing)}")

    # Minimal type normalization (does not change meaning of dates/country)
    # - customer_id can contain missing values -> use pandas nullable integer
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    # Create engine
    url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(url)

    # Ensure raw schema
    with engine.begin() as conn:
        conn.execute(text("create schema if not exists raw;"))

    # Force stable SQL types in Postgres
    dtype = {
        "invoice_no": satypes.Text(),
        "stock_code": satypes.Text(),
        "product_description": satypes.Text(),
        "quantity": satypes.BigInteger(),
        "invoice_date": satypes.DateTime(),
        "unit_price": satypes.Numeric(18, 4),
        "customer_id": satypes.BigInteger(),
        "country": satypes.Text(),
    }

    # Load into raw.sales (raw layer = immutable copy of the original file)
    df.to_sql(
        "sales",
        engine,
        schema="raw",
        if_exists="replace",
        index=False,
        chunksize=50_000,
        method="multi",
        dtype=dtype,
    )

    print("✅ Loaded raw.sales from data/raw/online_retail_II.xlsx")
    print(f"Rows: {len(df):,}")


if __name__ == "__main__":
    main()
