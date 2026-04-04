import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

# --- Database config ---
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")


def get_engine():
    password = quote_plus(DB_PASSWORD)
    return create_engine(
        f"postgresql://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


# --- Tickers ---
TICKERS = ["MSFT", "GOOGL", "AMZN", "NVDA", "META", "SPOT", "TSLA", "JPM", "V"]
