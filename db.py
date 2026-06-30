# db.py
import pandas as pd
from sqlalchemy import text

# --- Table name constants ---
LANDING = "landing_stock_prices"
BRONZE  = "bronze_stock_prices"
SILVER  = "silver_stock_prices"
DIM     = "dim_ticker"

BRONZE_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume"]
SILVER_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume"]


def get_max_date(engine, table: str):
    """Returns the most recent date in a table, or None if empty."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT MAX(date) FROM public.{table}"))
        return result.scalar()


def read_new_rows(engine, table: str, since_date, columns: list) -> pd.DataFrame:
    """Returns rows from a table newer than since_date. If None, returns all rows."""
    cols = ", ".join(columns)
    if since_date is None:
        return pd.read_sql(f"SELECT {cols} FROM public.{table}", engine)
    return pd.read_sql(
        f"SELECT {cols} FROM public.{table} WHERE date > %(cutoff)s",
        engine,
        params={"cutoff": since_date}
    )


def append_rows(df: pd.DataFrame, table: str, engine):
    """Appends a DataFrame to a table."""
    df.to_sql(table, engine, if_exists="append", index=False, schema="public")


def insert_ignore_duplicates(engine, source: str, target: str, columns: list):
    """Insert source rows into target, skipping (ticker, date) duplicates.

    Returns (inserted, skipped). Requires a unique index on target (ticker, date).
    """
    cols = ", ".join(columns)
    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM public.{source}")).scalar()
        result = conn.execute(text(
            f"INSERT INTO public.{target} ({cols}) "
            f"SELECT {cols} FROM public.{source} "
            f"ON CONFLICT (ticker, date) DO NOTHING"
        ))
        conn.commit()
        inserted = result.rowcount
        return inserted, total - inserted


def replace_table(df: pd.DataFrame, table: str, engine):
    """Replaces a table entirely (used for landing)."""
    df.to_sql(table, engine, if_exists="replace", index=False, schema="public")
    # to_sql drops/recreates the table, wiping RLS — re-enable it each run
    with engine.connect() as conn:
        conn.execute(text(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY"))
        conn.commit()
