import pandas as pd
from faker import Faker
from datetime import datetime
from pathlib import Path
import random

# --- CONFIG ---
BASE_DIR = Path(__file__).resolve().parent

INPUT_FILE = BASE_DIR / "data" / "raw" / "online_retail_II.xlsx"
OUTPUT_FILE = BASE_DIR / "data" / "processed" / "sales_france_2026.csv"

TARGET_END_DATE = datetime(2026, 2, 28)
LOCALE = "fr_FR"

FRENCH_CITIES = [
    "Paris", "Lyon", "Marseille", "Bordeaux", "Lille", "Toulouse",
    "Nice", "Nantes", "Strasbourg", "Montpellier", "Cholet"
]

def stable_bool(key: str, threshold: float = 0.9) -> bool:
    """Deterministic pseudo-random boolean based on a key."""
    r = random.Random(key)
    return r.random() < threshold

def run_extraction() -> None:
    print("🚀 ELT Step 1 — Extract + Standardize + Localize (deterministic)")

    if not INPUT_FILE.exists():
        print(f"❌ File not found: {INPUT_FILE}")
        return

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    # 1) Load
    print("📥 Loading Excel (Sheet: Year 2010-2011)...")
    try:
        df = pd.read_excel(INPUT_FILE, sheet_name="Year 2010-2011")
    except Exception as e:
        print(f"❌ Error reading Excel: {e}")
        return

    df.columns = df.columns.str.strip()

    # Handle Invoice / InvoiceNo
    if "Invoice" in df.columns and "InvoiceNo" not in df.columns:
        df = df.rename(columns={"Invoice": "InvoiceNo"})

    required = {
        "InvoiceNo", "StockCode", "Description", "Quantity",
        "InvoiceDate", "Price", "Customer ID", "Country"
    }
    missing = required - set(df.columns)
    if missing:
        print(f"❌ Missing columns in Excel: {missing}")
        print(f"   Found: {list(df.columns)}")
        return

    # 2) Standardize types (keep rows; dbt will filter business rules)
    df["InvoiceNo"] = df["InvoiceNo"].astype(str)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")

    # Customer ID as nullable int
    df["Customer ID"] = pd.to_numeric(df["Customer ID"], errors="coerce").astype("Int64")

    # 3) Time shift (based on max date)
    print("⏳ Applying time shift to end at 2026-02-28...")
    max_date_original = df["InvoiceDate"].max()
    if pd.isna(max_date_original):
        print("❌ InvoiceDate parsing failed (max is NaT).")
        return

    time_delta = TARGET_END_DATE - max_date_original
    df["InvoiceDate"] = df["InvoiceDate"] + time_delta

    # 4) Flags (ELT-friendly)
    df["is_cancellation"] = df["InvoiceNo"].str.startswith("C")
    df["is_return"] = df["Quantity"].fillna(0).astype(float) < 0

    # 5) Deterministic Faker mapping (only where customer_id exists)
    print("🇫🇷 Generating deterministic French identities (Faker)...")
    fake = Faker(LOCALE)

    def make_name_city(customer_id):
        if pd.isna(customer_id):
            return (None, None)
        cid = int(customer_id)
        fake.seed_instance(cid)           # deterministic per customer
        rr = random.Random(str(cid))      # deterministic city selection
        return (fake.name(), rr.choice(FRENCH_CITIES))

    mapped = df["Customer ID"].apply(make_name_city)
    df["customer_name"] = mapped.apply(lambda x: x[0])
    df["customer_city"] = mapped.apply(lambda x: x[1])

    # 6) Country: deterministic 90% France / 10% Others
    print("🗺️ Adjusting country distribution (deterministic)...")
    def country_fr(row):
        key = str(row["Customer ID"]) if pd.notna(row["Customer ID"]) else row["InvoiceNo"]
        return "France" if stable_bool(key, 0.9) else "Europe (Others)"
    df["country_fr"] = df.apply(country_fr, axis=1)

    # 7) Rename columns to snake_case (DB-friendly)
    df = df.rename(columns={
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "Description": "product_description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "Price": "unit_price",
        "Customer ID": "customer_id",
        "Country": "country_original"
    })

    # Keep a clean column order
    cols = [
        "invoice_no", "stock_code", "product_description",
        "quantity", "unit_price", "invoice_date",
        "customer_id", "customer_name", "customer_city",
        "country_original", "country_fr",
        "is_cancellation", "is_return"
    ]
    df = df[cols]

    print(f"💾 Saving CSV: {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print("✅ Done.")

if __name__ == "__main__":
    run_extraction()
