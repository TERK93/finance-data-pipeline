import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

# The column-restructuring block needs to be extracted into a function first
# e.g. def reshape_yfinance_response(raw: pd.DataFrame, tickers: list) -> pd.DataFrame
from fetch_stocks import reshape_yfinance_response

def make_raw_df(tickers):
    """Build a mock yf.download() multi-level column DataFrame."""
    metrics = ["Open", "High", "Low", "Close", "Volume"]
    cols = pd.MultiIndex.from_tuples([(m, t) for m in metrics for t in tickers])
    data = {(m, t): [100.0] for m in metrics for t in tickers}
    df = pd.DataFrame(data, columns=cols, index=pd.to_datetime(["2024-01-02"]))
    df.index.name = "Date"
    return df  # DatetimeIndex — no reset_index() here


def test_reshape_returns_expected_columns():
    raw = make_raw_df(["MSFT"])
    result = reshape_yfinance_response(raw, ["MSFT"])
    assert list(result.columns) == ["date", "ticker", "open", "high", "low", "close", "volume"]

def test_reshape_skips_ticker_with_missing_columns():
    raw = make_raw_df(["MSFT"])  # GOOGL is absent
    result = reshape_yfinance_response(raw, ["MSFT", "GOOGL"])
    assert "GOOGL" not in result["ticker"].values
    assert "MSFT" in result["ticker"].values

def test_reshape_drops_rows_with_null_close():
    raw = make_raw_df(["MSFT"])
    raw[("Close", "MSFT")] = float("nan")  # MultiIndex tuple, not "Close_MSFT"
    result = reshape_yfinance_response(raw, ["MSFT"])
    assert len(result) == 0

