import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

import fetch_stocks
from fetch_stocks import (
    reshape_response,
    window_has_trading_days,
    fetch_with_retries,
)


def make_bars(n=1):
    """Mock Tiingo daily-price bars (adjusted OHLCV fields)."""
    return [
        {
            "date": "2024-01-02T00:00:00.000Z",
            "adjOpen": 98.0, "adjHigh": 102.0, "adjLow": 97.0,
            "adjClose": 100.0, "adjVolume": 1_000_000,
        }
        for _ in range(n)
    ]


# --- reshape_response (Tiingo JSON -> landing schema) ---

def test_reshape_returns_expected_columns():
    result = reshape_response({"MSFT": make_bars()})
    assert list(result.columns) == ["date", "ticker", "open", "high", "low", "close", "volume"]

def test_reshape_maps_each_ticker():
    result = reshape_response({"MSFT": make_bars(), "GOOGL": make_bars()})
    assert set(result["ticker"]) == {"MSFT", "GOOGL"}

def test_reshape_empty_input_returns_empty_df():
    assert reshape_response({"MSFT": []}).empty

def test_reshape_drops_rows_with_null_close():
    bars = make_bars()
    bars[0]["adjClose"] = float("nan")
    assert len(reshape_response({"MSFT": bars})) == 0


# --- window_has_trading_days (yfinance/Tiingo `end` is exclusive) ---

def test_window_with_weekday_is_true():
    # 2026-06-22 is a Monday
    assert window_has_trading_days("2026-06-22", "2026-06-23") is True

def test_weekend_only_window_is_false():
    # 2026-06-20 Sat, 2026-06-21 Sun; end (Mon) excluded
    assert window_has_trading_days("2026-06-20", "2026-06-22") is False

def test_empty_window_is_false():
    assert window_has_trading_days("2026-06-22", "2026-06-22") is False


# --- fetch_with_retries (Tiingo over HTTP) ---

def _resp(payload):
    m = MagicMock()
    m.json.return_value = payload
    m.raise_for_status.return_value = None
    return m

def test_fetch_returns_data():
    with patch.object(fetch_stocks.requests, "get", return_value=_resp(make_bars())), \
         patch.object(fetch_stocks.time, "sleep", lambda s: None):
        result = fetch_with_retries(["MSFT"], "2026-06-15", "2026-06-19")
    assert result["MSFT"]

def test_fetch_retries_then_succeeds():
    responses = [_resp([]), _resp(make_bars())]
    with patch.object(fetch_stocks.requests, "get", side_effect=responses), \
         patch.object(fetch_stocks.time, "sleep", lambda s: None):
        result = fetch_with_retries(["MSFT"], "2026-06-15", "2026-06-19")
    assert result["MSFT"]

def test_fetch_recovers_from_http_error():
    calls = {"n": 0}
    def get(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("rate limited")
        return _resp(make_bars())
    with patch.object(fetch_stocks.requests, "get", side_effect=get), \
         patch.object(fetch_stocks.time, "sleep", lambda s: None):
        result = fetch_with_retries(["MSFT"], "2026-06-15", "2026-06-19")
    assert result["MSFT"]
