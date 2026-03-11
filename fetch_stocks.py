import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

# --- Settings ---
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

TICKERS = ["MSFT", "GOOGL", "AMZN", "NVDA", "META", "SPOT", "TSLA", "JPM", "V"]

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- Check last loaded date in BRONZE (not landing) ---
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(date) FROM bronze_stock_prices"))
        last_date = result.scalar()
except Exception as e:
    print(f"Could not read bronze table (first run?): {e}")
    last_date = None

if last_date is None:
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    print(f"First run — loading full history from {start_date}")
else:
    start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Incremental run — loading from {start_date}")

end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# --- Fetch from API ---
print("Fetching stock data...")
raw = yf.download(TICKERS, start=start_date, end=end_date, interval="1d")

if raw.empty:
    print("No new data — pipeline already up to date!")
else:
    # Flatten multi-level columns into separate rows per ticker
    raw.columns = [f"{metric}_{ticker}" for metric, ticker in raw.columns]
    raw = raw.reset_index()

    rows = []
    for ticker in TICKERS:
        df_ticker = raw[["Date", f"Open_{ticker}", f"High_{ticker}",
                          f"Low_{ticker}", f"Close_{ticker}", f"Volume_{ticker}"]].copy()
        df_ticker.columns = ["date", "open", "high", "low", "close", "volume"]
        df_ticker["ticker"] = ticker
        rows.append(df_ticker)

    df = pd.concat(rows, ignore_index=True)
    df = df.dropna(subset=["close"])
    df = df[["date", "ticker", "open", "high", "low", "close", "volume"]]

    print(df.head(5))
    print(f"Rows fetched: {len(df)}")

    # --- Landing = replace (staging area, always fresh) ---
    df.to_sql("landing_stock_prices", engine, if_exists="replace", index=False)
    print(f"Landing loaded: {len(df)} rows — staging area refreshed.")