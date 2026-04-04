import pandas as pd
from sqlalchemy import text
from config import get_engine, logger

try:
    engine = get_engine()

    # --- Ticker dimension data ---
    tickers = [
        {"ticker": "MSFT",  "company_name": "Microsoft",       "sector": "Technology", "industry": "Software",           "currency": "USD"},
        {"ticker": "GOOGL", "company_name": "Alphabet",        "sector": "Technology", "industry": "Internet Services",  "currency": "USD"},
        {"ticker": "AMZN",  "company_name": "Amazon",          "sector": "Consumer",   "industry": "E-Commerce",         "currency": "USD"},
        {"ticker": "NVDA",  "company_name": "Nvidia",          "sector": "Technology", "industry": "Semiconductors",     "currency": "USD"},
        {"ticker": "META",  "company_name": "Meta Platforms",  "sector": "Technology", "industry": "Social Media",       "currency": "USD"},
        {"ticker": "SPOT",  "company_name": "Spotify",         "sector": "Consumer",   "industry": "Music Streaming",    "currency": "USD"},
        {"ticker": "TSLA",  "company_name": "Tesla",           "sector": "Consumer",   "industry": "Electric Vehicles",  "currency": "USD"},
        {"ticker": "JPM",   "company_name": "JPMorgan Chase",  "sector": "Finance",    "industry": "Banking",            "currency": "USD"},
        {"ticker": "V",     "company_name": "Visa",            "sector": "Finance",    "industry": "Payment Processing", "currency": "USD"},
    ]

    df = pd.DataFrame(tickers)

    # --- Create table if not exists ---
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_ticker (
                ticker          TEXT PRIMARY KEY,
                company_name    TEXT,
                sector          TEXT,
                industry        TEXT,
                currency        TEXT
            )
        """))
        conn.commit()

    # --- Upsert — insert or update on conflict, never drop table ---
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO dim_ticker (ticker, company_name, sector, industry, currency)
                VALUES (:ticker, :company_name, :sector, :industry, :currency)
                ON CONFLICT (ticker) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    sector       = EXCLUDED.sector,
                    industry     = EXCLUDED.industry,
                    currency     = EXCLUDED.currency
            """), row.to_dict())
        conn.commit()

    logger.info(f"dim_ticker upserted successfully — {len(df)} tickers loaded.")
    
except Exception:
    logger.exception("load_dimensions.py failed")
    raise