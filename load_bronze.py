import pandas as pd
from sqlalchemy import text
from config import get_engine, logger

engine = get_engine()

# --- Check last loaded date in bronze ---
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(date) FROM public.bronze_stock_prices"))
        last_date = result.scalar()
except Exception as e:
    logger.warning(f"Could not read bronze table (first run?): {e}")
    last_date = None

# --- Read from landing ---
if last_date is None:
    df = pd.read_sql("SELECT * FROM public.landing_stock_prices", engine)
    logger.info("First run — loading all landing data into bronze.")
else:
    df = pd.read_sql(
        "SELECT * FROM public.landing_stock_prices WHERE date > %(cutoff)s",
        engine,
        params={"cutoff": last_date}
    )
    logger.info(f"Incremental run — loading from {last_date}")

if df.empty:
    logger.info("No new data — bronze already up to date!")
else:
    df = df[["date", "ticker", "open", "high", "low", "close", "volume"]]
    
    # --- Append to bronze ---
    df.to_sql("bronze_stock_prices", engine, if_exists="append", index=False, schema="public")
    logger.info(f"Bronze loaded: {len(df)} rows appended.")