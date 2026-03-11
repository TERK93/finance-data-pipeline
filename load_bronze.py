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

# --- Create bronze table if it doesn't exist ---
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS bronze_stock_prices (
            date        TIMESTAMP,
            ticker      TEXT,
            open        FLOAT,
            high        FLOAT,
            low         FLOAT,
            close       FLOAT,
            volume      FLOAT,
            load_date   TIMESTAMP
        )
    """))
    conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_bronze_ticker_date
        ON bronze_stock_prices (ticker, date)
    """))
    conn.commit()

# --- Read all from landing (landing is always fresh/staging) ---
df = pd.read_sql("SELECT * FROM landing_stock_prices", engine)
print(f"Loading {len(df)} rows from landing into bronze.")

if df.empty:
    print("No new data in landing — bronze already up to date!")
else:
    df["load_date"] = datetime.now()

    # Insert with ON CONFLICT DO NOTHING to prevent duplicates
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO bronze_stock_prices 
                    (date, ticker, open, high, low, close, volume, load_date)
                VALUES 
                    (:date, :ticker, :open, :high, :low, :close, :volume, :load_date)
                ON CONFLICT (ticker, date) DO NOTHING
            """), row.to_dict())
        conn.commit()
    print(f"Bronze loaded: {len(df)} rows — duplicates skipped automatically.")