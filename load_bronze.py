import db
from config import get_engine, logger

try:
    engine = get_engine()

    # --- Check last loaded date in bronze ---
    try:
        last_date = db.get_max_date(engine, db.BRONZE)
    except Exception:
        logger.exception("Could not read bronze table (first run?)")
        last_date = None

    # --- Read new rows from landing ---
    df = db.read_new_rows(engine, db.LANDING, last_date, db.BRONZE_COLUMNS)

    if last_date is None:
        logger.info("First run — loading all landing data into bronze.")
    else:
        logger.info(f"Incremental run — loading from {last_date}")

    if df.empty:
        logger.info("No new data — bronze already up to date!")
    else:
        db.append_rows(df, db.BRONZE, engine)
        logger.info(f"Bronze loaded: {len(df)} rows appended.")

except Exception:
    logger.exception("load_bronze.py failed")
    raise
