import time
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from config import get_engine, TICKERS, logger

# Yahoo returns an empty DataFrame (not an error) when it blocks the caller,
# common from CI IPs — so retry before treating empty as final.
MAX_FETCH_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5


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


def window_has_trading_days(start_date: str, end_date: str) -> bool:
    # end is exclusive, so a weekend-only window is legitimately empty
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    day = start
    while day < end:
        if day.weekday() < 5:
            return True
        day += timedelta(days=1)
    return False


def fetch_with_retries(tickers: list, start_date: str, end_date: str) -> pd.DataFrame:
    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            raw = yf.download(tickers, start=start_date, end=end_date, interval="1d")
        except Exception:
            logger.exception(f"yfinance download failed (attempt {attempt})")
            raw = pd.DataFrame()

        if not raw.empty:
            return raw

        if attempt < MAX_FETCH_RETRIES:
            wait = RETRY_BACKOFF_SECONDS * attempt
            logger.warning(f"Empty response — retrying in {wait}s")
            time.sleep(wait)

    return pd.DataFrame()


def main():
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

    # end is exclusive, so today loads through yesterday's complete bar
    end_date = datetime.now().strftime("%Y-%m-%d")

    if start_date >= end_date:
        logger.info("No new data — pipeline already up to date.")
        return

    # --- Fetch from Yahoo Finance API ---
    logger.info(f"Fetching stock data from {start_date} to {end_date}...")
    raw = fetch_with_retries(TICKERS, start_date, end_date)

    if raw.empty:
        if window_has_trading_days(start_date, end_date):
            raise RuntimeError(
                f"No data for {start_date}..{end_date} despite trading days — "
                f"Yahoo likely rate-limited."
            )
        logger.info("No trading days in window — nothing to fetch.")
        return

    df = reshape_yfinance_response(raw, TICKERS)

    if df.empty:
        raise RuntimeError("Data returned but no valid rows after reshape.")

    logger.info(f"Rows fetched: {len(df)}")
    db.replace_table(df, db.LANDING, engine)
    logger.info(f"Landing loaded: {len(df)} rows — staging area refreshed.")


if __name__ == "__main__":
    main()
