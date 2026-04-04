# Assumes validate_row() is extracted to validators.py
import pandas as pd
from validators import validate_row

def _row(close=100.0, open=98.0, high=102.0, low=97.0, volume=1_000_000):
    return pd.Series({"close": close, "open": open, "high": high, "low": low, "volume": volume})

def test_valid_row():
    assert validate_row(_row()) == "valid"

def test_null_close_returns_invalid_null():
    assert validate_row(_row(close=float("nan"))) == "invalid_null"

def test_null_open_returns_invalid_null():
    assert validate_row(_row(open=float("nan"))) == "invalid_null"

def test_zero_close_returns_invalid_price():
    assert validate_row(_row(close=0.0)) == "invalid_price"

def test_negative_open_returns_invalid_price():
    assert validate_row(_row(open=-1.0)) == "invalid_price"

def test_high_below_low_returns_invalid_high_low():
    assert validate_row(_row(high=96.0, low=97.0)) == "invalid_high_low"

def test_close_above_high_returns_invalid_close_range():
    assert validate_row(_row(close=105.0, high=102.0)) == "invalid_close_range"

def test_close_below_low_returns_invalid_close_range():
    assert validate_row(_row(close=95.0, low=97.0)) == "invalid_close_range"

def test_zero_volume_returns_invalid_volume():
    assert validate_row(_row(volume=0)) == "invalid_volume"

def test_negative_volume_returns_invalid_volume():
    assert validate_row(_row(volume=-500)) == "invalid_volume"
