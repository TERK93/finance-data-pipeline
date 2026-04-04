import yfinance as yf
import pandas as pd
from sqlalchemy import text
from datetime import datetime, timedelta
from config import get_engine, TICKERS, logger


def reshape_yfinance_response(raw: pd.DataFrame, tickers: list) -> pd.DataFrame:
    expected_metrics = ["Open", "High", "Low", "Close", "Volume"]
    raw = raw.copy()
    raw.columns = [f"{metric}_{ticker}" for metric, ticker in raw.columns]
    raw = raw.reset_index()

    rows = []
    for ticker in tickers:
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
        return pd.DataFrame()

    df = pd.concat(rows, ignore_index=True)
    df = df.dropna(subset=["close"])
    return df[["date", "ticker", "open", "high", "low", "close", "volume"]]


if __name__ == "__main__":
    import db
    engine = get_engine()

    # --- Check last loaded date in bronze ---
    try:
        last_date = db.get_max_date(engine, db.BRONZE)
    except Exception:
        logger.exception("Could not read bronze table (first run?)")
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
    except Exception:
        logger.exception("Failed to fetch stock data from Yahoo Finance")
        raise

    if raw.empty:
        logger.info("No new data — pipeline already up to date!")
    else:
        df = reshape_yfinance_response(raw, TICKERS)

        if df.empty:
            logger.warning("No valid ticker data fetched — check API response.")
        else:
            logger.info(f"Rows fetched: {len(df)}")
            db.replace_table(df, db.LANDING, engine)
            logger.info(f"Landing loaded: {len(df)} rows — staging area refreshed.")
