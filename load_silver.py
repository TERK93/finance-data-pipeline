import db
from sqlalchemy import text
from datetime import datetime
from config import get_engine, logger
from validators import validate_row

try:
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
    last_silver_date = db.get_max_date(engine, db.SILVER)

    if last_silver_date is None:
        logger.info("First run — loading all bronze data into silver.")
    else:
        logger.info(f"Incremental run — loading data after {last_silver_date}")

    # --- Read new rows from bronze ---
    df = db.read_new_rows(engine, db.BRONZE, last_silver_date, db.SILVER_COLUMNS)

    if df.empty:
        logger.info("No new data — silver already up to date!")
    else:
        df["status"] = df.apply(validate_row, axis=1)
        df["load_date"] = datetime.now()

        valid_count   = (df["status"] == "valid").sum()
        invalid_count = (df["status"] != "valid").sum()

        logger.info(f"Validation complete — valid: {valid_count}, invalid: {invalid_count}")

        if invalid_count > 0:
            invalid_rows = df[df["status"] != "valid"][["date", "ticker", "status"]]
            logger.warning(f"Invalid rows:\n{invalid_rows.to_string(index=False)}")

        db.append_rows(df, db.SILVER, engine)
        logger.info(f"Silver loaded: {len(df)} rows.")

except Exception:
    logger.exception("load_silver.py failed")
    raise
