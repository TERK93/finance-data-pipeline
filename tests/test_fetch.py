import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

# The column-restructuring block needs to be extracted into a function first
# e.g. def reshape_yfinance_response(raw: pd.DataFrame, tickers: list) -> pd.DataFrame
import fetch_stocks
from fetch_stocks import (
    reshape_yfinance_response,
    window_has_trading_days,
    fetch_with_retries,
)

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


# --- window_has_trading_days (yfinance `end` is exclusive) ---

def test_window_with_weekday_is_true():
    # 2026-06-22 is a Monday
    assert window_has_trading_days("2026-06-22", "2026-06-23") is True

def test_weekend_only_window_is_false():
    # 2026-06-20 Sat, 2026-06-21 Sun; end (Mon) excluded
    assert window_has_trading_days("2026-06-20", "2026-06-22") is False

def test_empty_window_is_false():
    assert window_has_trading_days("2026-06-22", "2026-06-22") is False


# --- fetch_with_retries (Yahoo returns empty, not an error, when blocked) ---

def test_fetch_retries_then_succeeds():
    frames = [pd.DataFrame(), pd.DataFrame({"x": [1]})]
    with patch.object(fetch_stocks.yf, "download", side_effect=frames), \
         patch.object(fetch_stocks.time, "sleep", lambda s: None):
        result = fetch_with_retries(["MSFT"], "2026-06-15", "2026-06-19")
    assert not result.empty

def test_fetch_returns_empty_after_exhausting_retries():
    with patch.object(fetch_stocks.yf, "download", return_value=pd.DataFrame()), \
         patch.object(fetch_stocks.time, "sleep", lambda s: None):
        result = fetch_with_retries(["MSFT"], "2026-06-15", "2026-06-19")
    assert result.empty

def test_fetch_recovers_from_download_exception():
    def boom_then_ok(*a, **k):
        if not hasattr(boom_then_ok, "called"):
            boom_then_ok.called = True
            raise RuntimeError("rate limited")
        return pd.DataFrame({"x": [1]})
    with patch.object(fetch_stocks.yf, "download", side_effect=boom_then_ok), \
         patch.object(fetch_stocks.time, "sleep", lambda s: None):
        result = fetch_with_retries(["MSFT"], "2026-06-15", "2026-06-19")
    assert not result.empty

