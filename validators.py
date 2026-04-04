import pandas as pd


def validate_row(row) -> str:
    if pd.isnull(row["close"]) or pd.isnull(row["open"]):
        return "invalid_null"
    if row["close"] <= 0 or row["open"] <= 0:
        return "invalid_price"
    if row["high"] < row["low"]:
        return "invalid_high_low"
    # OHLCV integrity: close and open should be within high/low range
    if row["close"] > row["high"] or row["close"] < row["low"]:
        return "invalid_close_range"
    if row["volume"] <= 0:
        return "invalid_volume"
    return "valid"
