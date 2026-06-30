import db
from config import get_engine, logger

try:
    engine = get_engine()

    # --- Append landing into bronze, skipping any (ticker, date) duplicates ---
    inserted, skipped = db.insert_ignore_duplicates(
        engine, db.LANDING, db.BRONZE, db.BRONZE_COLUMNS
    )
    logger.info(f"Bronze loaded: {inserted} new rows inserted, {skipped} duplicates skipped.")

except Exception:
    logger.exception("load_bronze.py failed")
    raise
