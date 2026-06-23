import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from config import get_engine, TICKERS, TIINGO_API_KEY, logger

MAX_FETCH_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
TIINGO_BASE_URL = "https://api.tiingo.com/tiingo/daily"


def fetch_ticker(ticker: str, start_date: str, end_date: str) -> list:
    url = f"{TIINGO_BASE_URL}/{ticker}/prices"
    params = {"startDate": start_date, "endDate": end_date, "token": TIINGO_API_KEY}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def reshape_response(raw_by_ticker: dict) -> pd.DataFrame:
    # Use the adjusted fields (split/dividend-adjusted) so returns and drawdown
    # stay correct — matches yfinance's previous auto_adjust default.
    rows = []
    for ticker, bars in raw_by_ticker.items():
        for bar in bars:
            rows.append({
                "date": bar["date"][:10],
                "ticker": ticker,
                "open": bar["adjOpen"],
                "high": bar["adjHigh"],
                "low": bar["adjLow"],
                "close": bar["adjClose"],
                "volume": bar["adjVolume"],
            })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["date", "ticker", "open", "high", "low", "close", "volume"])
    return df.dropna(subset=["close"])


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


def fetch_with_retries(tickers: list, start_date: str, end_date: str) -> dict:
    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            result = {t: fetch_ticker(t, start_date, end_date) for t in tickers}
            if any(result.values()):
                return result
        except Exception:
            logger.exception(f"Tiingo fetch failed (attempt {attempt})")

        if attempt < MAX_FETCH_RETRIES:
            wait = RETRY_BACKOFF_SECONDS * attempt
            logger.warning(f"Empty/failed response — retrying in {wait}s")
            time.sleep(wait)

    return {}


def main():
    import db
    engine = get_engine()

    if not TIINGO_API_KEY:
        raise RuntimeError("TIINGO_API_KEY is not set — add it to .env / GitHub Secrets.")

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

    # --- Fetch from Tiingo ---
    logger.info(f"Fetching stock data from Tiingo ({start_date} to {end_date})...")
    raw = fetch_with_retries(TICKERS, start_date, end_date)

    if not any(raw.values()):
        if window_has_trading_days(start_date, end_date):
            raise RuntimeError(
                f"No data for {start_date}..{end_date} despite trading days — "
                f"check the Tiingo API key or rate limits."
            )
        logger.info("No trading days in window — nothing to fetch.")
        return

    df = reshape_response(raw)

    if df.empty:
        raise RuntimeError("Data returned but no valid rows after reshape.")

    logger.info(f"Rows fetched: {len(df)}")
    db.replace_table(df, db.LANDING, engine)
    logger.info(f"Landing loaded: {len(df)} rows — staging area refreshed.")


if __name__ == "__main__":
    main()
