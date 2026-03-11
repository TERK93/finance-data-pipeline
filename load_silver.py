import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# --- Settings ---
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")



engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- Create silver table if it doesn't exist ---
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS silver_stock_prices (
            date        TIMESTAMP,
            ticker      TEXT,
            open        FLOAT,
            high        FLOAT,
            low         FLOAT,
            close       FLOAT,
            volume      FLOAT,
            load_date   TIMESTAMP,
            status      TEXT
        )
    """))
    conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_silver_ticker_date
        ON silver_stock_prices (ticker, date)
    """))
    conn.commit()

# --- Check last loaded date in silver ---
with engine.connect() as conn:
    result = conn.execute(text("SELECT MAX(date) FROM silver_stock_prices"))
    last_silver_date = result.scalar()

# --- Read only new rows from bronze ---
if last_silver_date is None:
    df = pd.read_sql("SELECT * FROM bronze_stock_prices", engine)
    print(f"First run — loading all bronze data into silver.")
else:
    df = pd.read_sql(
        f"SELECT * FROM bronze_stock_prices WHERE date > '{last_silver_date}'",
        engine
    )
    print(f"Incremental run — loading data after {last_silver_date}")

if df.empty:
    print("No new data — silver already up to date!")
else:
    # --- Data quality checks ---
    def validate_row(row):
        # Null check
        if pd.isnull(row["close"]) or pd.isnull(row["open"]):
            return "invalid_null"
        # Price sanity check — close price must be positive
        if row["close"] <= 0 or row["open"] <= 0:
            return "invalid_price"
        # High must be >= low
        if row["high"] < row["low"]:
            return "invalid_high_low"
        # Volume must be positive
        if row["volume"] <= 0:
            return "invalid_volume"
        return "valid"

    df["status"] = df.apply(validate_row, axis=1)

    valid = df[df["status"] == "valid"]
    invalid = df[df["status"] != "valid"]

    print(f"Valid rows: {len(valid)}")
    print(f"Invalid rows: {len(invalid)}")
    if len(invalid) > 0:
        print(invalid[["date", "ticker", "status"]])

    df["load_date"] = datetime.now()
    df.to_sql("silver_stock_prices", engine, if_exists="append", index=False)
    print(f"Silver loaded: {len(df)} rows with load_date {datetime.now()}")

