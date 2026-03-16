import yfinance as yf
import pandas as pd
from sqlalchemy import text
from datetime import datetime, timedelta
from config import get_engine, TICKERS, logger

engine = get_engine()

# --- Check last loaded date in bronze (not landing) ---
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(date) FROM bronze_stock_prices"))
        last_date = result.scalar()
except Exception as e:
    logger.warning(f"Could not read bronze table (first run?): {e}")
    last_date = None

if last_date is None:
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    logger.info(f"First run — loading full history from {start_date}")
else:
    start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Incremental run — loading from {start_date}")

end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# --- Fetch from Yahoo Finance API ---
logger.info("Fetching stock data from Yahoo Finance...")
try:
    raw = yf.download(TICKERS, start=start_date, end=end_date, interval="1d")
except Exception as e:
    logger.error(f"Failed to fetch stock data from Yahoo Finance: {e}")
    raise

if raw.empty:
    logger.info("No new data — pipeline already up to date!")
else:
    # Validate expected columns exist before accessing them
    expected_metrics = ["Open", "High", "Low", "Close", "Volume"]
    raw.columns = [f"{metric}_{ticker}" for metric, ticker in raw.columns]
    raw = raw.reset_index()

    rows = []
    for ticker in TICKERS:
        expected_cols = [f"{m}_{ticker}" for m in expected_metrics]
        missing = [c for c in expected_cols if c not in raw.columns]
        if missing:
            logger.warning(f"Skipping {ticker} — missing columns: {missing}")
            continue

        df_ticker = raw[["Date"] + expected_cols].copy()
        df_ticker.columns = ["date", "open", "high", "low", "close", "volume"]
        df_ticker["ticker"] = ticker
        rows.append(df_ticker)

    if not rows:
        logger.warning("No valid ticker data fetched — check API response.")
    else:
        df = pd.concat(rows, ignore_index=True)
        df = df.dropna(subset=["close"])
        df = df[["date", "ticker", "open", "high", "low", "close", "volume"]]

        logger.info(f"Rows fetched: {len(df)}")

        # Landing = replace each run (staging area, always fresh)
        df.to_sql("landing_stock_prices", engine, if_exists="replace", index=False)
        logger.info(f"Landing loaded: {len(df)} rows — staging area refreshed.")