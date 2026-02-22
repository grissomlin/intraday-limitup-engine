# markets/tw/aggregator/normalize.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


# =============================================================================
# Coercions (small, reusable)
# =============================================================================
def _safe_str(x: Any, default: str = "") -> str:
    if x is None:
        return default
    try:
        if pd.isna(x):
            return default
    except Exception:
        pass
    s = str(x).strip()
    return s if s else default


def _to_float_series(s: Any, default: float = 0.0) -> pd.Series:
    if isinstance(s, pd.Series):
        out = pd.to_numeric(s, errors="coerce").fillna(default)
        return out.astype("float64")
    # scalar / missing
    return pd.Series([], dtype="float64")


def _to_int_series(s: Any, default: int = 0) -> pd.Series:
    if isinstance(s, pd.Series):
        out = pd.to_numeric(s, errors="coerce").fillna(default)
        # avoid pandas nullable int surprises in downstream .sum() / json
        return out.astype("int64")
    return pd.Series([], dtype="int64")


def _ensure_cols(df: pd.DataFrame, defaults: Dict[str, Any]) -> pd.DataFrame:
    for k, v in (defaults or {}).items():
        if k not in df.columns:
            df[k] = v
    return df


def _normalize_sector(df: pd.DataFrame, *, default_sector: str = "未分類") -> None:
    if "sector" not in df.columns:
        df["sector"] = default_sector
        return
    df["sector"] = df["sector"].map(lambda x: _safe_str(x, default_sector))
    df.loc[df["sector"].eq(""), "sector"] = default_sector


def _normalize_symbol_name(df: pd.DataFrame) -> None:
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].map(lambda x: _safe_str(x, ""))
    else:
        df["symbol"] = ""

    if "name" in df.columns:
        df["name"] = df["name"].map(lambda x: _safe_str(x, ""))
    else:
        df["name"] = ""


def _normalize_market_fields(
    df: pd.DataFrame,
    *,
    default_market: str = "tw",
    default_market_detail: str = "",
    default_market_label: str = "",
) -> None:
    if "market" not in df.columns:
        df["market"] = default_market
    df["market"] = df["market"].map(lambda x: _safe_str(x, default_market)).str.lower()
    df.loc[df["market"].eq(""), "market"] = default_market

    if "market_detail" not in df.columns:
        df["market_detail"] = default_market_detail
    df["market_detail"] = df["market_detail"].map(lambda x: _safe_str(x, default_market_detail)).str.lower()

    if "market_label" not in df.columns:
        df["market_label"] = default_market_label
    df["market_label"] = df["market_label"].map(lambda x: _safe_str(x, default_market_label))


def _normalize_price_fields(df: pd.DataFrame) -> None:
    # keep them numeric & non-null so downstream ret/flags never crash
    for c in ["prev_close", "open", "high", "low", "close"]:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).astype("float64")

    if "volume" not in df.columns:
        df["volume"] = 0
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")

    # bar_date is a display field; keep as str
    if "bar_date" not in df.columns:
        df["bar_date"] = ""
    df["bar_date"] = df["bar_date"].map(lambda x: _safe_str(x, ""))


# =============================================================================
# Returns (ret/ret_high)
# =============================================================================
def _add_ret_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure ret/ret_high (+ pct) exist.

    Definitions:
      - ret      = close/prev_close - 1
      - ret_high = high/prev_close - 1
    """
    if df is None or df.empty:
        # allow caller to pass empty df and still get stable columns
        if df is None:
            df = pd.DataFrame()
        for c in ["ret", "ret_pct", "ret_high", "ret_high_pct"]:
            if c not in df.columns:
                df[c] = 0.0
        return df

    c = pd.to_numeric(df.get("close"), errors="coerce").fillna(0.0)
    h = pd.to_numeric(df.get("high"), errors="coerce").fillna(0.0)
    pc = pd.to_numeric(df.get("prev_close"), errors="coerce").fillna(0.0)

    can = pc > 0

    ret_close = pd.Series(0.0, index=df.index, dtype="float64")
    ret_high = pd.Series(0.0, index=df.index, dtype="float64")

    ret_close.loc[can] = (c.loc[can] / pc.loc[can]) - 1.0
    ret_high.loc[can] = (h.loc[can] / pc.loc[can]) - 1.0

    df["ret"] = ret_close.astype(float)
    df["ret_pct"] = (df["ret"] * 100.0).astype(float)
    df["ret_high"] = ret_high.astype(float)
    df["ret_high_pct"] = (df["ret_high"] * 100.0).astype(float)
    return df


# =============================================================================
# Snapshot normalizers (main / open)
# =============================================================================
def normalize_snapshot_main(rows: Optional[Iterable[Dict[str, Any]]]) -> pd.DataFrame:
    """
    Normalize TW snapshot_main rows into a stable DataFrame.

    Goals:
      - Always return a DataFrame with predictable columns
      - Clean symbol/name/sector
      - Coerce OHLCV numeric fields
      - Keep market fields stable
    """
    data = list(rows or [])
    df = pd.DataFrame(data)

    # stable base columns (downstream expects these to exist)
    _ensure_cols(
        df,
        {
            "symbol": "",
            "name": "",
            "sector": "未分類",
            "market": "tw",
            "market_detail": "",
            "market_label": "",
            "bar_date": "",
            "prev_close": 0.0,
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 0.0,
            "volume": 0,
        },
    )

    _normalize_symbol_name(df)
    _normalize_sector(df, default_sector="未分類")
    _normalize_market_fields(df, default_market="tw", default_market_detail="", default_market_label="")
    _normalize_price_fields(df)
    _add_ret_fields(df)

    return df


def normalize_snapshot_open(rows: Optional[Iterable[Dict[str, Any]]]) -> pd.DataFrame:
    """
    Normalize TW snapshot_open rows into a stable DataFrame.

    Note:
      - This is still "normal snapshot open" (上市/上櫃/興櫃都可能被你丟進來)
      - open-limit / emerging pool 的特殊欄位，後面會交給 open_limit.py 再 enrich
    """
    data = list(rows or [])
    df = pd.DataFrame(data)

    _ensure_cols(
        df,
        {
            "symbol": "",
            "name": "",
            "sector": "未分類",
            "market": "tw",
            "market_detail": "",
            "market_label": "",
            "bar_date": "",
            "prev_close": 0.0,
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 0.0,
            "volume": 0,
        },
    )

    _normalize_symbol_name(df)
    _normalize_sector(df, default_sector="未分類")
    _normalize_market_fields(df, default_market="tw", default_market_detail="", default_market_label="")
    _normalize_price_fields(df)
    _add_ret_fields(df)

    return df


__all__ = [
    "normalize_snapshot_main",
    "normalize_snapshot_open",
    "_add_ret_fields",
]
