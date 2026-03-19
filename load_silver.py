import pandas as pd
from sqlalchemy import text
from datetime import datetime
from config import get_engine, logger

engine = get_engine()

# --- Create silver table if it doesn't exist ---
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS public.silver_stock_prices (
            date        TIMESTAMP,
            ticker      TEXT,
            open        FLOAT,
            high        FLOAT,
            low         FLOAT,
            close       FLOAT,
            volume      BIGINT,
            load_date   TIMESTAMP,
            status      TEXT
        )
    """))
    conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_silver_ticker_date
        ON public.silver_stock_prices (ticker, date)
    """))
    conn.commit()

# --- Check last loaded date in silver ---
with engine.connect() as conn:
    result = conn.execute(text("SELECT MAX(date) FROM public.silver_stock_prices"))
    last_silver_date = result.scalar()

# --- Read only new rows from bronze (parameterized query) ---
if last_silver_date is None:
    df = pd.read_sql("SELECT * FROM public.bronze_stock_prices", engine)
    logger.info("First run — loading all bronze data into silver.")
else:
    df = pd.read_sql(
        "SELECT * FROM public.bronze_stock_prices WHERE date > %(cutoff)s",
        engine,
        params={"cutoff": last_silver_date}
    )
    logger.info(f"Incremental run — loading data after {last_silver_date}")

if df.empty:
    logger.info("No new data — silver already up to date!")
else:
    # --- Data quality validation ---
    def validate_row(row):
        if pd.isnull(row["close"]) or pd.isnull(row["open"]):
            return "invalid_null"
        if row["close"] <= 0 or row["open"] <= 0:
            return "invalid_price"
        if row["high"] < row["low"]:
            return "invalid_high_low"
        # OHLCV integrity: close and open should be within high/low range
        if row["close"] > row["high"] or row["close"] < row["low"]:
            return "invalid_close_range"
        if row["volume"] <= 0:
            return "invalid_volume"
        return "valid"

    df["status"] = df.apply(validate_row, axis=1)
    df["load_date"] = datetime.now()

    valid_count   = (df["status"] == "valid").sum()
    invalid_count = (df["status"] != "valid").sum()

    logger.info(f"Validation complete — valid: {valid_count}, invalid: {invalid_count}")

    if invalid_count > 0:
        invalid_rows = df[df["status"] != "valid"][["date", "ticker", "status"]]
        logger.warning(f"Invalid rows:\n{invalid_rows.to_string(index=False)}")

    df.to_sql("silver_stock_prices", engine, if_exists="append", index=False, schema="public")
    logger.info(f"Silver loaded: {len(df)} rows.")